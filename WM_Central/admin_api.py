"""Authenticated HTTP controls for a headless WM_Central deployment.

The API intentionally delegates writes to CentralKafkaService so validation,
SQLite state transitions, and outgoing Kafka messages keep using the existing
transactional path. Public deployments must sit behind an HTTPS terminator.
"""

from __future__ import annotations

import hmac
import json
import logging
import math
import os
import socket
import sys
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from .models import IrrigationStatus, StationStatus


LOGGER = logging.getLogger("wm_central.admin_api")
MAX_REQUEST_BYTES = 16 * 1024
MAX_CONCURRENT_ADMIN_REQUESTS = 32


class AdminConflict(Exception):
    """An authenticated command conflicts with current Central business state."""


class _BoundedHTTPServer(ThreadingHTTPServer):
    request_queue_size = 64

    def __init__(self, address: tuple[str, int], handler: type[BaseHTTPRequestHandler]) -> None:
        super().__init__(address, handler)
        self._request_slots = threading.BoundedSemaphore(MAX_CONCURRENT_ADMIN_REQUESTS)

    def process_request(self, request: socket.socket, client_address: tuple[str, int]) -> None:
        if not self._request_slots.acquire(blocking=False):
            LOGGER.warning("Rejecting Central admin connection: request capacity reached")
            request.close()
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._request_slots.release()
            raise

    def process_request_thread(self, request: socket.socket, client_address: tuple[str, int]) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()


class CentralAdminServer:
    """Threaded admin HTTP server. No token is generated or defaulted here."""

    def __init__(
        self,
        host: str,
        port: int,
        token: str,
        state: Any,
        service: Any,
        *,
        require_forwarded_https: bool = False,
    ) -> None:
        if not token:
            raise ValueError("CENTRAL_ADMIN_TOKEN is required to enable Central admin HTTP")
        if not 0 <= port <= 65535:
            raise ValueError("Central admin port must be between 0 and 65535")
        self.host, self.port = host, port
        self._token = token
        self._state, self._service = state, service
        self._require_forwarded_https = require_forwarded_https
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._server is not None:
            return
        try:
            server = _BoundedHTTPServer((self.host, self.port), _handler_type(self))
        except OSError:
            LOGGER.exception("Could not bind Central admin HTTP on %s:%s", self.host, self.port)
            raise
        server.daemon_threads = False
        server.block_on_close = True
        self._server = server
        self.port = server.server_address[1]
        self._thread = threading.Thread(target=server.serve_forever, name="central-admin-http", daemon=True)
        self._thread.start()
        LOGGER.info("Central admin HTTP listening on %s:%s", self.host, self.port)

    def stop(self) -> None:
        server, thread = self._server, self._thread
        self._server = None
        self._thread = None
        if server is not None:
            server.shutdown()
            server.server_close()
        if thread is not None:
            thread.join(timeout=3.0)


def _handler_type(owner: CentralAdminServer) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "WM_CentralAdmin"
        sys_version = ""
        protocol_version = "HTTP/1.1"

        def setup(self) -> None:
            self.request.settimeout(10.0)
            super().setup()

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            if not self._authorize():
                return
            if self.path != "/v1/stations":
                self._reply(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                return
            try:
                rows = []
                live = owner._service.db.live_irrigations()
                for view in owner._state.snapshot():
                    irrigation = live.get(view.ws_id)
                    rows.append({
                        "station_id": view.ws_id,
                        "location": view.location,
                        "status": view.status.value,
                        "admin_state": view.admin_state.value,
                        "operator_id": view.operator_id,
                        "flow_rate_lpm": view.flow_rate_lpm,
                        "total_volume_liters": view.accumulated_volume_liters,
                        "fault": view.fault,
                        "request_id": irrigation.request_id if irrigation else None,
                    })
                self._reply(HTTPStatus.OK, {"stations": rows})
            except Exception:
                LOGGER.exception("Central admin could not read station status")
                self._reply(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "internal_error"})

        def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            if not self._authorize():
                return
            if self.path != "/v1/control":
                self._reply(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                return
            try:
                command = self._read_command()
                idempotency_key = self._idempotency_key()
                response, status = _execute_command(owner, command, idempotency_key)
                self._reply(status, response)
            except ValueError as exc:
                self._reply(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            except LookupError as exc:
                self._reply(HTTPStatus.NOT_FOUND, {"error": str(exc)})
            except AdminConflict as exc:
                self._reply(HTTPStatus.CONFLICT, {"error": str(exc)})
            except TimeoutError:
                self._reply(HTTPStatus.REQUEST_TIMEOUT, {"error": "request_timeout"})
            except Exception:
                LOGGER.exception("Central admin control request failed")
                self._reply(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "internal_error"})

        def _authorize(self) -> bool:
            # Railway terminates public TLS and sets this header on every HTTPS
            # request. Check transport before reading or comparing credentials.
            if owner._require_forwarded_https and self.headers.get("X-Forwarded-Proto", "").lower() != "https":
                self._reply(HTTPStatus.FORBIDDEN, {"error": "https_required"})
                return False
            authorization = self.headers.get("Authorization", "")
            supplied = authorization[7:] if authorization.startswith("Bearer ") else ""
            valid = bool(supplied) and hmac.compare_digest(
                supplied.encode("utf-8"), owner._token.encode("utf-8")
            )
            if not valid:
                self._reply(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
                return False
            return True

        def _read_command(self) -> dict[str, Any]:
            if self.headers.get("Transfer-Encoding"):
                raise ValueError("Transfer-Encoding is not supported")
            if self.headers.get_content_type() != "application/json":
                raise ValueError("Content-Type must be application/json")
            raw_length = self.headers.get("Content-Length")
            try:
                length = int(raw_length or "")
            except ValueError as exc:
                raise ValueError("Content-Length is required") from exc
            if length <= 0 or length > MAX_REQUEST_BYTES:
                raise ValueError("Request body must be between 1 and 16384 bytes")
            raw = self.rfile.read(length)
            try:
                body = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError("Request body must be valid UTF-8 JSON") from exc
            if not isinstance(body, dict):
                raise ValueError("Request body must be a JSON object")
            return body

        def _idempotency_key(self) -> str:
            value = self.headers.get("Idempotency-Key", "")
            try:
                normalized = str(UUID(value))
            except (ValueError, AttributeError):
                raise ValueError("Idempotency-Key must be a UUID")
            return normalized

        def _reply(self, status: HTTPStatus, value: dict[str, Any]) -> None:
            self._last_status = int(status)
            data = json.dumps(value, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Connection", "close")
            self.end_headers()
            try:
                self.wfile.write(data)
            except OSError:
                LOGGER.warning("Admin response write failed for client %s", self.client_address[0])
            self.close_connection = True

        def log_message(self, _format: str, *_args: Any) -> None:
            # BaseHTTPRequestHandler's default log includes raw request text.
            # Keep a useful audit log without headers, body, or credentials.
            LOGGER.info(
                "Central admin HTTP method=%s status=%s client=%s",
                self.command, getattr(self, "_last_status", "handled"), self.client_address[0],
            )

    return Handler


def _execute_command(owner: CentralAdminServer, body: dict[str, Any], idempotency_key: str) -> tuple[dict[str, Any], HTTPStatus]:
    action = body.get("action")
    station_selector = body.get("station_id")
    if not isinstance(action, str) or action not in {"start", "block", "activate", "stop"}:
        raise ValueError("action must be start, block, activate, or stop")
    if not isinstance(station_selector, str) or not station_selector.strip():
        raise ValueError("station_id must be a registered ID or 'all'")
    station_selector = station_selector.strip()
    station_views = owner._state.snapshot()
    stations = {view.ws_id: view for view in station_views}
    all_stations = station_selector.lower() == "all"
    if all_stations:
        targets = [view.ws_id for view in station_views]
    elif station_selector in stations:
        targets = [station_selector]
    else:
        raise LookupError("unknown station_id")

    allowed = {"action", "station_id"}
    if action == "start":
        allowed |= {"duration_seconds", "target_volume_liters", "operator_id"}
    unknown = set(body) - allowed
    if unknown:
        raise ValueError(f"unknown field(s): {', '.join(sorted(unknown))}")

    results: list[dict[str, Any]] = []
    if action == "start":
        duration, target = body.get("duration_seconds"), body.get("target_volume_liters")
        if duration is not None and target is not None:
            raise ValueError("choose duration_seconds or target_volume_liters, not both")
        if "duration_seconds" in body:
            if isinstance(duration, bool) or not isinstance(duration, int) or duration <= 0:
                raise ValueError("duration_seconds must be a positive whole number")
        if "target_volume_liters" in body:
            if isinstance(target, bool) or not isinstance(target, (int, float)) or not math.isfinite(target) or target <= 0:
                raise ValueError("target_volume_liters must be a positive finite number")
        operator_id = body.get("operator_id")
        if operator_id is not None and not isinstance(operator_id, str):
            raise ValueError("operator_id must be a string")
        operators = owner._service.db.list_operators()
        if operator_id is None:
            operator_id = next((operator.operator_id for operator in operators if operator.active), None)
        if not operator_id or not any(operator.operator_id == operator_id and operator.active for operator in operators):
            raise AdminConflict("no active operator matches operator_id")
        if all_stations:
            targets = [ws_id for ws_id in targets if stations[ws_id].status is StationStatus.AVAILABLE]
        if not targets:
            raise AdminConflict("no eligible stations are available")
        for ws_id in targets:
            request_id = str(uuid5(NAMESPACE_URL, f"wm-admin-request:{idempotency_key}:{ws_id}"))
            request = owner._service.start_irrigation(
                ws_id,
                duration if "duration_seconds" in body else None,
                operator_id,
                target if "target_volume_liters" in body else None,
                request_id=request_id,
            )
            record = owner._service.db.get_irrigation(request)
            results.append({
                "station_id": ws_id,
                "request_id": request,
                "status": record.status.value if record else "UNKNOWN",
                "reason": record.deny_reason.value if record and record.deny_reason else None,
            })
        accepted = any(result["status"] == IrrigationStatus.AUTHORIZED.value for result in results)
        return {"results": results}, HTTPStatus.OK if accepted else HTTPStatus.CONFLICT

    if action == "stop":
        live = owner._service.db.live_irrigations()
        targets = [ws_id for ws_id in targets if ws_id in live]
        if not targets:
            raise AdminConflict("no active irrigation matches station_id")
        for ws_id in targets:
            record = live[ws_id]
            command_id = str(uuid5(NAMESPACE_URL, f"wm-admin-command:{idempotency_key}:stop:{ws_id}:{record.request_id}"))
            accepted = owner._service.issue_station_command(
                "STOP_IRRIGATION", ws_id, request_id=record.request_id, command_id=command_id
            )
            results.append({"station_id": ws_id, "request_id": record.request_id, "accepted": accepted})
    else:
        event_type = "BLOCK_WS" if action == "block" else "ACTIVATE_WS"
        for ws_id in targets:
            command_id = str(uuid5(NAMESPACE_URL, f"wm-admin-command:{idempotency_key}:{action}:{ws_id}"))
            accepted = owner._service.issue_station_command(event_type, ws_id, command_id=command_id)
            results.append({"station_id": ws_id, "accepted": accepted})

    accepted_any = any(row["accepted"] for row in results)
    return {"results": results}, HTTPStatus.OK if accepted_any else HTTPStatus.CONFLICT


STATUS_ANSI = {
    "AVAILABLE": "32",       # green
    "WATERING": "1;32",      # bold green; marker below remains visible if blink is unsupported
    "LEAK": "31",            # red
    "OUT_OF_SERVICE": "38;5;208",  # orange
    "DISCONNECTED": "90",    # gray
}


def format_status_response(value: dict[str, Any], *, color: bool | None = None) -> str:
    """Human-readable status for the standalone interactive admin client."""
    if color is None:
        color = sys.stdout.isatty() and "NO_COLOR" not in os.environ
    color = bool(color) and "NO_COLOR" not in os.environ
    stations = value.get("stations", [])
    lines = ["STATION     LOCATION                 STATUS             FLOW L/M    TOTAL L  OPERATOR   REQUEST ID"]
    for row in stations:
        status = "Out of Service" if row["status"] == "OUT_OF_SERVICE" else row["status"]
        if row["status"] == "WATERING":
            status = "● " + status
        visible_status = status
        if color and row["status"] in STATUS_ANSI:
            status = f"\033[{STATUS_ANSI[row['status']]}m{status}\033[0m"
            status_column = status + " " * max(0, 18 - len(visible_status))
        else:
            status_column = f"{status:<18}"
        operator_id = row.get("operator_id") if row["status"] == "WATERING" else None
        lines.append(
            f"{row['station_id']:<11} {row['location']:<24} {status_column} "
            f"{row['flow_rate_lpm'] if row['flow_rate_lpm'] is not None else '-':>8} "
            f"{row['total_volume_liters'] if row['total_volume_liters'] is not None else '-':>9}  "
            f"{operator_id or '-':<10} "
            f"{row['request_id'] or '-'}"
        )
        if row["fault"]:
            lines.append(f"    fault: {row['fault']}")
    if not stations:
        lines.append("(no stations registered)")
    return "\n".join(lines)

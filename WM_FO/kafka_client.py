from __future__ import annotations

import logging
import math
import threading
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from common.kafka_contract import (ACTIVATION_REQUESTS, OPERATOR_EVENTS,
                                   MessageValidationError, decode, envelope)
from common.event_panel import describe, feed
from common.kafka_transport import KafkaPublisher, KafkaWorker

from .display import show_event

LOGGER = logging.getLogger(__name__)
TERMINAL_EVENTS = {"ACTIVATION_DENIED", "IRRIGATION_COMPLETED", "COMMAND_REJECTED"}
TRACKED_REQUESTS = 100
# End reasons that mean the irrigation did not finish the way it was asked.
INTERRUPTED_END_REASONS = {"ENGINE_FAULT", "MONITOR_DISCONNECTED", "LEAK_DETECTED", "STATION_BLOCKED",
                           "CENTRAL_RESTART", "LEAK", "DISCONNECTED", "BLOCKED"}
STOPPED_END_REASONS = {"MANUAL_STOP", "CENTRAL_STOP", "MANUAL"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class FieldOperator:
    def __init__(
        self,
        operator_id: str,
        publisher: KafkaPublisher,
        *,
        consumer_group_id: str | None = None,
        station_id: str | None = None,
    ) -> None:
        self.operator_id = operator_id
        self.publisher = publisher
        self._consumer_group_id = consumer_group_id or f"wm-field-operator-{operator_id}-v1"
        self.station_id = station_id
        self._lock = threading.RLock()
        self._pending: dict[str, threading.Event] = {}
        # Set by Central's first answer to a request (authorized, denied, ...),
        # so a non-blocking menu can wait for the decision but not the outcome.
        self._decisions: dict[str, threading.Event] = {}
        self._results: dict[str, dict[str, Any]] = {}
        self._station_discoveries: dict[str, tuple[threading.Event, dict[str, Any] | None]] = {}
        self._shown_terminal_request_ids: set[str] = set()
        # What this FO knows about each of its recent requests, for its web
        # console: Central's decision and the station's progress and summary.
        self._tracked: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.central_link: dict[str, Any] = {"ok": None, "checked_at": None, "error": None}

    def worker(self, bootstrap: str, consumer: Any | None = None) -> KafkaWorker:
        return KafkaWorker(bootstrap, self._consumer_group_id,
                           [OPERATOR_EVENTS], self.handle_bytes, consumer,
                           auto_offset_reset="latest")

    def request_irrigation(self, ws_id: str, duration_seconds: int | None = None,
                           target_volume_liters: float | None = None) -> str:
        if duration_seconds is not None and target_volume_liters is not None:
            raise ValueError("choose duration_seconds or target_volume_liters, not both")
        if duration_seconds is not None and (isinstance(duration_seconds, bool) or not isinstance(duration_seconds, int) or duration_seconds <= 0):
            raise ValueError("duration_seconds must be a positive whole number")
        if target_volume_liters is not None and (isinstance(target_volume_liters, bool) or not isinstance(target_volume_liters, (int, float)) or not math.isfinite(target_volume_liters) or target_volume_liters <= 0):
            raise ValueError("target_volume_liters must be positive")
        request_id = str(uuid4())
        with self._lock:
            self._pending[request_id] = threading.Event()
            self._decisions[request_id] = threading.Event()
        payload = ({"duration_seconds": duration_seconds} if duration_seconds is not None else
                   {"target_volume_liters": float(target_volume_liters)} if target_volume_liters is not None else {})
        message = envelope("ACTIVATION_REQUEST", payload,
                           ws_id=ws_id, operator_id=self.operator_id, request_id=request_id)
        try:
            self.publisher.publish(ACTIVATION_REQUESTS, message, key=ws_id)
        except Exception:
            with self._lock:
                self._pending.pop(request_id, None)
                self._decisions.pop(request_id, None)
            raise
        feed().record("OUT", "WM_Central", "ACTIVATION_REQUEST", ws_id=ws_id, request_id=request_id,
                      detail=describe(payload) or "manual mode")
        self._track_submitted(request_id, ws_id, duration_seconds, target_volume_liters)
        mode = (f"{duration_seconds} seconds" if duration_seconds is not None else
                f"{target_volume_liters:g} litres" if target_volume_liters is not None else "manual mode")
        print(f"[{ws_id}] Request {request_id} submitted ({mode}).", flush=True)
        return request_id

    def request_available_stations(self, timeout: float = 10.0) -> list[dict[str, str]]:
        """Request a fresh station snapshot from Central and wait for its reply."""
        request_id = str(uuid4())
        completed = threading.Event()
        with self._lock:
            self._station_discoveries[request_id] = (completed, None)
        message = envelope("AVAILABLE_STATIONS_REQUEST", {},
                           operator_id=self.operator_id, request_id=request_id)
        try:
            self.publisher.publish(ACTIVATION_REQUESTS, message, key=self.operator_id)
            feed().record("OUT", "WM_Central", "AVAILABLE_STATIONS_REQUEST", request_id=request_id)
            if not completed.wait(timeout):
                self._central_seen(False, "Central did not answer in time")
                raise TimeoutError("Central did not return the available station list in time")
            with self._lock:
                response = self._station_discoveries[request_id][1]
            self._central_seen(True)
            if response is None:
                raise RuntimeError("Central returned an invalid station discovery response")
            error = response["payload"].get("error")
            if error:
                raise RuntimeError(f"Central could not list stations: {error}")
            stations = response["payload"].get("stations")
            if not isinstance(stations, list) or any(
                not isinstance(station, dict)
                or not isinstance(station.get("station_id"), str)
                or not isinstance(station.get("location"), str)
                for station in stations
            ):
                raise RuntimeError("Central returned an invalid station list")
            return stations
        finally:
            with self._lock:
                self._station_discoveries.pop(request_id, None)

    def wait_for_decision(self, request_id: str, timeout: float) -> bool:
        """Wait until Central has answered the request at all (not its outcome)."""
        with self._lock:
            decided = self._decisions.get(request_id)
            completed = self._pending.get(request_id)
        if decided is None:
            return completed is None or completed.is_set()
        return decided.wait(timeout)

    def wait_for_result(self, request_id: str, timeout: float | None) -> bool:
        with self._lock:
            completed = self._pending.get(request_id)
        if completed is None:
            return False
        finished = completed.wait(timeout)
        with self._lock:
            self._pending.pop(request_id, None)
            self._decisions.pop(request_id, None)
            self._results.pop(request_id, None)
        if not finished:
            if timeout is None:
                print(f"Request {request_id} is still waiting for its final result.", flush=True)
            else:
                print(f"Request {request_id} is still in progress; no final result arrived within {timeout:g} seconds.", flush=True)
        return finished

    def handle_bytes(self, raw: bytes) -> bool:
        try:
            message = decode(raw)
        except MessageValidationError as exc:
            LOGGER.warning("Discarding malformed operator event: %s", exc)
            return True
        if message.get("operator_id") != self.operator_id:
            return True
        request_id = message.get("request_id")
        event_type = message["event_type"]
        if event_type == "AVAILABLE_STATIONS_RESPONSE":
            if not isinstance(request_id, str):
                return True
            with self._lock:
                discovery = self._station_discoveries.get(request_id)
                if discovery is not None:
                    self._station_discoveries[request_id] = (discovery[0], message)
                    discovery[0].set()
            if discovery is not None:
                feed().record("IN", "WM_Central", event_type, request_id=request_id,
                              detail=describe(message["payload"]))
            return True
        if self.station_id is not None and message.get("ws_id") != self.station_id:
            return True
        with self._lock:
            waiter = self._pending.get(request_id) if isinstance(request_id, str) else None
            is_terminal = isinstance(request_id, str) and event_type in TERMINAL_EVENTS
            if is_terminal and request_id in self._shown_terminal_request_ids:
                return True
            recoverable = is_terminal
            if waiter is None and not recoverable:
                return True
            if is_terminal:
                self._shown_terminal_request_ids.add(request_id)
        if waiter is None and not recoverable:
            return True
        feed().record("IN", "WM_Central", event_type, ws_id=message.get("ws_id"), request_id=request_id,
                      detail=describe(message["payload"]))
        self._track_event(message)
        self._central_seen(True)
        show_event(message)
        if waiter is not None:
            with self._lock:
                decided = self._decisions.get(request_id)
            if decided is not None:
                decided.set()
        if waiter is not None and is_terminal:
            with self._lock:
                waiter = self._pending.get(request_id)
                if waiter is not None:
                    self._results[request_id] = message
                    waiter.set()
        return True

    # ----- request tracking for the web console -----

    def _central_seen(self, ok: bool, error: str | None = None) -> None:
        with self._lock:
            self.central_link = {"ok": ok, "checked_at": _now(), "error": error}

    def _track_submitted(self, request_id: str, ws_id: str, duration_seconds: int | None,
                         target_volume_liters: float | None) -> None:
        with self._lock:
            now = _now()
            self._tracked[request_id] = {
                "request_id": request_id, "ws_id": ws_id, "status": "SUBMITTED",
                "duration_seconds": duration_seconds,
                "target_volume_liters": float(target_volume_liters) if target_volume_liters is not None else None,
                "submitted_at": now, "authorized_at": None, "started_at": None, "finished_at": None,
                "updated_at": now, "reason": None, "end_reason": None,
                "elapsed_seconds": None, "volume_liters": None, "flow_rate_lpm": None,
                "history": [{"event": "SUBMITTED", "time": now}],
            }
            while len(self._tracked) > TRACKED_REQUESTS:
                self._tracked.popitem(last=False)

    def _track_event(self, message: dict[str, Any]) -> None:
        request_id = message.get("request_id")
        if not isinstance(request_id, str):
            return
        event_type, payload = message["event_type"], message.get("payload") or {}
        with self._lock:
            item = self._tracked.get(request_id)
            if item is None:
                # A result for a request sent before this FO restarted.
                self._track_submitted(request_id, str(message.get("ws_id") or "?"), None, None)
                item = self._tracked[request_id]
                item["submitted_at"] = None
            now = _now()
            item["updated_at"] = now
            if event_type != "IRRIGATION_PROGRESS":
                item["history"].append({"event": event_type, "time": now})
            for source, target in (("duration_seconds", "elapsed_seconds"),
                                   ("accumulated_volume_liters", "volume_liters"),
                                   ("flow_rate_lpm", "flow_rate_lpm")):
                if isinstance(payload.get(source), (int, float)) and event_type in {"IRRIGATION_PROGRESS", "IRRIGATION_COMPLETED"}:
                    item[target] = payload[source]
            if event_type == "ACTIVATION_AUTHORIZED":
                item["status"], item["authorized_at"] = "AUTHORIZED", now
                if "duration_seconds" in payload:
                    item["duration_seconds"] = payload["duration_seconds"]
                if "target_volume_liters" in payload:
                    item["target_volume_liters"] = payload["target_volume_liters"]
            elif event_type == "ACTIVATION_DENIED":
                item["status"], item["reason"], item["finished_at"] = "DENIED", payload.get("reason"), now
            elif event_type == "ACTIVATION_STATUS":
                status = payload.get("status")
                if status == "DENIED":
                    item["status"], item["reason"] = "DENIED", payload.get("reason")
                elif status in {"AUTHORIZED", "WATERING"} and item["status"] == "SUBMITTED":
                    item["status"] = status
            elif event_type in {"IRRIGATION_STARTED", "IRRIGATION_PROGRESS"}:
                if item["status"] in {"SUBMITTED", "AUTHORIZED"}:
                    item["status"] = "WATERING"
                    item["started_at"] = item["started_at"] or now
            elif event_type == "IRRIGATION_COMPLETED":
                end_reason = payload.get("end_reason")
                item["status"] = ("INTERRUPTED" if end_reason in INTERRUPTED_END_REASONS else
                                  "STOPPED" if end_reason in STOPPED_END_REASONS else "COMPLETED")
                item["end_reason"], item["finished_at"] = end_reason, now
                if "target_volume_liters" in payload:
                    item["target_volume_liters"] = payload["target_volume_liters"]
            elif event_type == "COMMAND_REJECTED":
                item["status"], item["reason"], item["finished_at"] = "REJECTED", payload.get("reason"), now

    def mark_no_result(self, request_id: str) -> None:
        """The watcher gave up waiting: keep the request visible as unresolved."""
        with self._lock:
            item = self._tracked.get(request_id)
            if item is not None and item["status"] in {"SUBMITTED", "AUTHORIZED", "WATERING"}:
                item["status"], item["updated_at"] = "NO_RESULT", _now()

    def requests_snapshot(self) -> list[dict[str, Any]]:
        """Tracked requests, newest first, as plain dicts."""
        with self._lock:
            return [{**item, "history": list(item["history"])} for item in reversed(self._tracked.values())]

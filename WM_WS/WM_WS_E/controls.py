"""Station controls shared by the Engine's terminal menu and its web console.

One code path for manual start/stop and simulated failure, so the terminal
and the page always behave the same way.
"""
from __future__ import annotations

import logging
import threading
from dataclasses import asdict
from http import HTTPStatus
from typing import Any

from common.web_console import ApiError, ApiRequest

LOGGER = logging.getLogger(__name__)


class ControlRefused(Exception):
    """The station cannot do what was asked in its current state."""


class StationControls:
    def __init__(self, engine: Any, local_operator: Any | None = None, kafka_service: Any | None = None) -> None:
        self.engine = engine
        self.local_operator = local_operator
        self.kafka_service = kafka_service
        self._lock = threading.Lock()
        self._local_request: str | None = None

    @property
    def local_enabled(self) -> bool:
        return self.local_operator is not None and self.kafka_service is not None and bool(self.engine.config.ws_id)

    @property
    def local_request(self) -> str | None:
        with self._lock:
            return self._local_request

    def start_manual(self) -> str:
        """Ask Central (through Kafka) for a manual irrigation on this station."""
        if not self.local_enabled:
            raise ControlRefused("Local menu is not configured.")
        with self._lock:
            if (self._local_request is not None or self.kafka_service.active_request_id is not None
                    or self.engine.irrigation.state.value != "IDLE"):
                raise ControlRefused("A local request is already pending or active.")
        request_id = self.local_operator.request_irrigation(self.engine.config.ws_id)
        with self._lock:
            self._local_request = request_id
        threading.Thread(target=self._watch, args=(request_id,),
                         name=f"wm-local-request-{self.engine.config.ws_id}", daemon=True).start()
        return request_id

    def _watch(self, request_id: str) -> None:
        try:
            self.local_operator.wait_for_result(request_id, None)
        finally:
            if self.kafka_service is not None:
                self.kafka_service.clear_local_stop(request_id)
            with self._lock:
                if self._local_request == request_id:
                    self._local_request = None

    def stop(self) -> str:
        if self.local_operator is not None:
            with self._lock:
                request_id = self._local_request
            if request_id is None and self.kafka_service is not None:
                request_id = self.kafka_service.active_request_id
            if request_id is None:
                raise ControlRefused("There is no active irrigation to stop.")
            if self.kafka_service is not None and self.kafka_service.stop_local_irrigation(request_id):
                return "Local irrigation stop requested."
            raise ControlRefused("Could not stop that irrigation request.")
        if self.kafka_service is not None and self.kafka_service.active_request_id is not None:
            if self.kafka_service.stop_local_irrigation(self.kafka_service.active_request_id):
                return "Irrigation stop requested."
        if self.engine.stop_irrigation():
            return "Irrigation stop requested."
        raise ControlRefused("There is no active irrigation to stop.")

    def simulate_failure(self) -> str:
        self.engine.simulate_failure()
        return "Simulated failure enabled: the Monitor will report KO to Central."

    def clear_failure(self) -> str:
        self.engine.clear_failure()
        return "Simulated failure cleared."

    # ----- web console -----

    def status(self) -> dict[str, Any]:
        engine, irrigation = self.engine, self.engine.irrigation
        telemetry = irrigation.telemetry[-1] if irrigation.telemetry else None
        active = self.kafka_service.active_request() if self.kafka_service is not None else None
        last = getattr(self.kafka_service, "last_completed", None) if self.kafka_service is not None else None
        return {
            "ws_id": engine.config.ws_id,
            "state": engine.state.value,
            "valve_open": bool(getattr(irrigation.valve, "is_open", False)),
            "failed": engine.failed,
            "blocked": engine.blocked,
            "monitor": {"connected": engine.monitor_available,
                        "host": engine.config.monitor_host, "port": engine.config.monitor_port},
            "central": {"reachable": engine.central_reachable, "via": "WM_WS_M"},
            "kafka": {"connected": self.kafka_service is not None},
            "local_menu": {"enabled": self.local_enabled,
                           "pending_request": self.local_request},
            "flow_rate_lpm": engine.config.flow_rate_lpm,
            "irrigation": active,
            "telemetry": asdict(telemetry) if telemetry is not None and irrigation.state.value == "WATERING" else None,
            "last_completed": last,
        }

    def routes(self) -> dict[tuple[str, str], Any]:
        def guarded(action):
            def route(_request: ApiRequest) -> dict[str, Any]:
                try:
                    return {"message": action()}
                except ControlRefused as exc:
                    raise ApiError(HTTPStatus.CONFLICT, str(exc)) from None
            return route

        def failure(request: ApiRequest) -> dict[str, Any]:
            active = (request.body or {}).get("active")
            if not isinstance(active, bool):
                raise ApiError(HTTPStatus.BAD_REQUEST, "active must be true or false")
            return {"message": self.simulate_failure() if active else self.clear_failure()}

        def start(_request: ApiRequest) -> dict[str, Any]:
            try:
                request_id = self.start_manual()
            except ControlRefused as exc:
                raise ApiError(HTTPStatus.CONFLICT, str(exc)) from None
            return {"message": "Manual irrigation requested from Central.", "request_id": request_id}

        return {
            ("GET", "/api/status"): lambda _request: self.status(),
            ("POST", "/api/failure"): failure,
            ("POST", "/api/irrigation/start"): start,
            ("POST", "/api/irrigation/stop"): guarded(self.stop),
        }

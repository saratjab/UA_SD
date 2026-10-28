from __future__ import annotations

import logging
import math
import threading
from typing import Any
from uuid import uuid4

from common.kafka_contract import (ACTIVATION_REQUESTS, OPERATOR_EVENTS,
                                   MessageValidationError, decode, envelope)
from common.kafka_transport import KafkaPublisher, KafkaWorker

from .display import show_event

LOGGER = logging.getLogger(__name__)
TERMINAL_EVENTS = {"ACTIVATION_DENIED", "IRRIGATION_COMPLETED", "COMMAND_REJECTED"}


class FieldOperator:
    def __init__(self, operator_id: str, publisher: KafkaPublisher) -> None:
        self.operator_id = operator_id
        self.publisher = publisher
        # Stable per-operator offsets let a restarted terminal resume events
        # produced after its last successful commit. Different operators keep
        # independent groups and therefore cannot consume one another's results.
        self._consumer_group_id = f"wm-field-operator-{operator_id}-v1"
        self._lock = threading.RLock()
        self._pending: dict[str, threading.Event] = {}
        self._results: dict[str, dict[str, Any]] = {}
        self._shown_terminal_request_ids: set[str] = set()

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
        payload = ({"duration_seconds": duration_seconds} if duration_seconds is not None else
                   {"target_volume_liters": float(target_volume_liters)} if target_volume_liters is not None else {})
        message = envelope("ACTIVATION_REQUEST", payload,
                           ws_id=ws_id, operator_id=self.operator_id, request_id=request_id)
        try:
            self.publisher.publish(ACTIVATION_REQUESTS, message, key=ws_id)
        except Exception:
            with self._lock:
                self._pending.pop(request_id, None)
            raise
        mode = (f"{duration_seconds} seconds" if duration_seconds is not None else
                f"{target_volume_liters:g} litres" if target_volume_liters is not None else "manual stop")
        print(f"[{ws_id}] Request {request_id} submitted ({mode}).", flush=True)
        return request_id

    def wait_for_result(self, request_id: str, timeout: float | None) -> bool:
        with self._lock:
            completed = self._pending.get(request_id)
        if completed is None:
            return False
        finished = completed.wait(timeout)
        with self._lock:
            self._pending.pop(request_id, None)
            self._results.pop(request_id, None)
        if not finished:
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
        with self._lock:
            waiter = self._pending.get(request_id) if isinstance(request_id, str) else None
            is_terminal = isinstance(request_id, str) and event_type in TERMINAL_EVENTS
            if is_terminal and request_id in self._shown_terminal_request_ids:
                return True
            # A completion can be waiting at the group's committed offset after
            # this FO process died. Show it even though the new process has no
            # in-memory waiter for the old request.
            recoverable = is_terminal
            if waiter is None and not recoverable:
                return True
            if is_terminal:
                self._shown_terminal_request_ids.add(request_id)
        if waiter is None and not recoverable:
            return True
        show_event(message)
        if waiter is not None and is_terminal:
            with self._lock:
                waiter = self._pending.get(request_id)
                if waiter is not None:
                    self._results[request_id] = message
                    waiter.set()
        return True

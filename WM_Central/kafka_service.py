"""Kafka-facing Central application service; policy stays backed by StateManager/SQLite."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from common.kafka_contract import (ACTIVATION_REQUESTS, OPERATOR_EVENTS, WS_COMMANDS, WS_EVENTS,
                                   MessageValidationError, decode, envelope)
from common.kafka_transport import KafkaPublisher, KafkaWorker

from .database import Database
from .models import DenyReason, EndReason, IrrigationStatus, StationStatus
from .state_manager import StateManager

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class LatestTelemetry:
    request_id: str
    sequence: int
    flow_rate_lpm: float
    accumulated_volume_liters: float
    duration_seconds: float


class CentralKafkaService:
    """Consumes FO requests/Engine facts and publishes commands/operator events.

    Consumers commit only after this service returns successfully.  SQLite's unique
    request id and idempotent transitions make an at-least-once Kafka delivery safe.
    """
    def __init__(self, db: Database, state: StateManager, publisher: KafkaPublisher) -> None:
        self.db, self.state, self.publisher = db, state, publisher
        self.latest_telemetry: dict[str, LatestTelemetry] = {}

    def activation_worker(self, bootstrap: str, consumer: Any | None = None) -> KafkaWorker:
        return KafkaWorker(bootstrap, "wm-central-activation-v1", [ACTIVATION_REQUESTS], self.handle_activation_bytes, consumer)

    def event_worker(self, bootstrap: str, consumer: Any | None = None) -> KafkaWorker:
        return KafkaWorker(bootstrap, "wm-central-events-v1", [WS_EVENTS], self.handle_engine_bytes, consumer)

    def handle_activation_bytes(self, raw: bytes) -> bool:
        try:
            message = decode(raw)
            self.handle_activation(message)
            return True
        except MessageValidationError as exc:
            LOGGER.warning("Discarding malformed activation request: %s", exc)
            return True  # poison messages must not block a partition forever

    def handle_activation(self, message: dict[str, Any]) -> None:
        if message.get("event_type") != "ACTIVATION_REQUEST":
            raise MessageValidationError("unsupported activation event_type")
        self._require(message, "ws_id", "operator_id", "request_id")
        payload = message["payload"]
        duration = payload.get("duration_seconds")
        if isinstance(duration, bool) or not isinstance(duration, (int, float)) or duration <= 0:
            raise MessageValidationError("payload.duration_seconds must be positive")
        if int(duration) != duration:
            raise MessageValidationError("duration_seconds must be a whole number")
        ws_id, operator_id, request_id = message["ws_id"], message["operator_id"], message["request_id"]
        operator = self.db.get_operator(operator_id)
        station = self.db.get_station(ws_id)
        if operator is None or station is None:
            self._notify(operator_id, request_id, ws_id, "ACTIVATION_DENIED", {"reason": "UNKNOWN_OPERATOR_OR_STATION"})
            return
        existing = self.db.get_irrigation(request_id)
        if existing is not None:
            self._notify(operator_id, request_id, ws_id, "ACTIVATION_STATUS", {"status": existing.status.value, "reason": existing.deny_reason.value if existing.deny_reason else None})
            return
        reason = None
        if not operator.active:
            reason = DenyReason.OPERATOR_INACTIVE
        else:
            status = self.state.status_of(ws_id)
            reason = {StationStatus.DISCONNECTED: DenyReason.WS_DISCONNECTED,
                      StationStatus.LEAK: DenyReason.WS_LEAK,
                      StationStatus.OUT_OF_SERVICE: DenyReason.WS_OUT_OF_SERVICE,
                      StationStatus.WATERING: DenyReason.WS_BUSY}.get(status)
        record = self.db.record_irrigation_request(request_id, ws_id, operator_id, int(duration), reason)
        if record.status is IrrigationStatus.DENIED:
            self._notify(operator_id, request_id, ws_id, "ACTIVATION_DENIED", {"reason": record.deny_reason.value})
            return
        command = envelope("START_IRRIGATION", {"duration_seconds": int(duration)}, ws_id=ws_id, operator_id=operator_id, request_id=request_id)
        try:
            self.publisher.publish(WS_COMMANDS, command)
        except Exception:
            # Do not leave a successful authorization pretending a command was delivered.
            self.db.end_irrigation(request_id, EndReason.CENTRAL_RESTART)
            self._notify(operator_id, request_id, ws_id, "ACTIVATION_DENIED", {"reason": "COMMAND_DELIVERY_FAILED"})
            raise
        self._notify(operator_id, request_id, ws_id, "ACTIVATION_AUTHORIZED", {"duration_seconds": int(duration)})

    def handle_engine_bytes(self, raw: bytes) -> bool:
        try:
            self.handle_engine_event(decode(raw)); return True
        except MessageValidationError as exc:
            LOGGER.warning("Discarding malformed Engine event: %s", exc); return True

    def handle_engine_event(self, message: dict[str, Any]) -> None:
        event_type = message.get("event_type")
        self._require(message, "ws_id", "request_id")
        request_id, ws_id, payload = message["request_id"], message["ws_id"], message["payload"]
        record = self.db.get_irrigation(request_id)
        if record is None or record.ws_id != ws_id:
            LOGGER.warning("Ignoring event for unknown/mismatched request %s", request_id); return
        if event_type == "IRRIGATION_STARTED":
            if self.db.mark_irrigation_started(request_id, message["timestamp"]):
                self._notify(record.operator_id, request_id, ws_id, "IRRIGATION_STARTED", {})
        elif event_type == "IRRIGATION_TELEMETRY":
            self._telemetry(record, message)
        elif event_type == "IRRIGATION_COMPLETED":
            reason = {"TIME_LIMIT": EndReason.DURATION, "MANUAL_STOP": EndReason.MANUAL,
                      "CENTRAL_STOP": EndReason.MANUAL, "STATION_BLOCKED": EndReason.BLOCKED,
                      "LEAK_DETECTED": EndReason.LEAK, "ENGINE_FAULT": EndReason.LEAK,
                      "MONITOR_DISCONNECTED": EndReason.DISCONNECTED}.get(payload.get("end_reason"))
            if reason is None: raise MessageValidationError("invalid completion end_reason")
            duration, volume = self._number(payload, "duration_seconds"), self._number(payload, "accumulated_volume_liters")
            if self.db.end_irrigation(request_id, reason, duration_s=duration, total_volume_l=volume, ended_at=message["timestamp"]):
                self._notify(record.operator_id, request_id, ws_id, "IRRIGATION_COMPLETED", payload)
            else:
                self.db.add_terminal_measurements(
                    request_id, duration_s=duration, total_volume_l=volume, ended_at=message["timestamp"]
                )
        elif event_type == "COMMAND_REJECTED":
            reason = {"ENGINE_BLOCKED": DenyReason.ENGINE_BLOCKED,
                      "ENGINE_FAILED": DenyReason.ENGINE_FAILED}.get(payload.get("reason"))
            if reason is None:
                raise MessageValidationError("invalid command rejection reason")
            if self.db.reject_authorized_irrigation(request_id, reason):
                self._notify(record.operator_id, request_id, ws_id, "COMMAND_REJECTED", payload)
        else: raise MessageValidationError("unsupported Engine event_type")

    def _telemetry(self, record: Any, message: dict[str, Any]) -> None:
        p, seq = message["payload"], message["payload"].get("sequence")
        if not isinstance(seq, int) or seq < 0: raise MessageValidationError("telemetry sequence invalid")
        values = LatestTelemetry(message["request_id"], seq, self._number(p, "flow_rate_lpm"), self._number(p, "accumulated_volume_liters"), self._number(p, "duration_seconds"))
        old = self.latest_telemetry.get(record.ws_id)
        if record.status is IrrigationStatus.WATERING and (old is None or old.request_id != values.request_id or seq > old.sequence):
            self.latest_telemetry[record.ws_id] = values
            self._notify(record.operator_id, record.request_id, record.ws_id, "IRRIGATION_PROGRESS", p)

    def issue_station_command(self, event_type: str, ws_id: str, *, request_id: str | None = None) -> bool:
        if event_type not in {"START_IRRIGATION", "BLOCK_WS", "ACTIVATE_WS", "STOP_IRRIGATION"}: raise ValueError("unsupported command")
        if self.db.get_station(ws_id) is None: return False
        if event_type == "BLOCK_WS": self.state.block_station(ws_id)
        elif event_type == "ACTIVATE_WS":
            if self.state.status_of(ws_id) in {StationStatus.DISCONNECTED, StationStatus.LEAK}: return False
            self.state.activate_station(ws_id)
        self.publisher.publish(WS_COMMANDS, envelope(event_type, {}, ws_id=ws_id, request_id=request_id))
        return True

    def _notify(self, operator_id: str | None, request_id: str, ws_id: str, event_type: str, payload: dict[str, Any]) -> None:
        if operator_id:
            self.publisher.publish(
                OPERATOR_EVENTS,
                envelope(event_type, payload, operator_id=operator_id, request_id=request_id, ws_id=ws_id),
                key=operator_id,
            )

    @staticmethod
    def _require(message: dict[str, Any], *fields: str) -> None:
        for field in fields:
            if not isinstance(message.get(field), str) or not message[field]: raise MessageValidationError(f"missing {field}")
    @staticmethod
    def _number(payload: dict[str, Any], name: str) -> float:
        value = payload.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0: raise MessageValidationError(f"invalid {name}")
        return float(value)

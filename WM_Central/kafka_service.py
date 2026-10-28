"""Kafka-facing Central application service; policy stays backed by StateManager/SQLite."""
from __future__ import annotations

import logging
import math
import threading
from uuid import NAMESPACE_URL, uuid5
from uuid import uuid4
from dataclasses import dataclass
from typing import Any

from common.kafka_contract import (ACTIVATION_REQUESTS, COMMAND_REJECTION_REASONS, OPERATOR_EVENTS, WS_COMMANDS, WS_EVENTS,
                                   MessageValidationError, decode, envelope, message_key)
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
        self._outbox_flush_lock = threading.Lock()
        self.flush_outbox()

    def flush_outbox(self) -> None:
        with self._outbox_flush_lock:
            for message_id, topic, key, message in self.db.pending_outgoing():
                try:
                    self.publisher.publish(topic, message, key=key)
                    self.db.mark_outgoing_delivered(message_id)
                except Exception:
                    LOGGER.exception("Central outbox item remains pending message_id=%s", message_id)
                    break

    @staticmethod
    def _outgoing(topic: str, event_type: str, payload: dict[str, Any], identity: str,
                  *, operator_id: str | None = None, ws_id: str | None = None,
                  request_id: str | None = None, key: str | None = None):
        message_id = str(uuid5(NAMESPACE_URL, f"wm-outbox:{topic}:{identity}"))
        message = envelope(event_type, payload, operator_id=operator_id, ws_id=ws_id, request_id=request_id)
        message["message_id"] = message_id
        if key is None:
            key = message_key(topic, message)
        return (message_id, topic, key, message)

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
        unknown = set(payload) - {"duration_seconds", "target_volume_liters"}
        if unknown:
            raise MessageValidationError(f"unsupported activation payload field(s): {', '.join(sorted(unknown))}")
        has_duration = "duration_seconds" in payload
        has_target = "target_volume_liters" in payload
        duration = payload.get("duration_seconds")
        target_volume = payload.get("target_volume_liters")
        if has_duration and has_target:
            raise MessageValidationError("choose duration_seconds or target_volume_liters, not both")
        if has_duration:
            if isinstance(duration, bool) or not isinstance(duration, (int, float)) or not math.isfinite(duration) or duration <= 0:
                raise MessageValidationError("payload.duration_seconds must be positive")
            if int(duration) != duration:
                raise MessageValidationError("duration_seconds must be a whole number")
        if has_target and (isinstance(target_volume, bool) or not isinstance(target_volume, (int, float)) or not math.isfinite(target_volume) or target_volume <= 0):
            raise MessageValidationError("payload.target_volume_liters must be positive")
        mode_payload = ({"duration_seconds": int(duration)} if has_duration else
                        {"target_volume_liters": float(target_volume)} if has_target else {})
        ws_id, operator_id, request_id = message["ws_id"], message["operator_id"], message["request_id"]
        operator = self.db.get_operator(operator_id)
        station = self.db.get_station(ws_id)
        if operator is None or station is None:
            self._notify(operator_id, request_id, ws_id, "ACTIVATION_DENIED", {"reason": "UNKNOWN_OPERATOR_OR_STATION"})
            return
        existing = self.db.get_irrigation(request_id)
        if existing is not None:
            outgoing = []
            if existing.status is IrrigationStatus.AUTHORIZED:
                existing_payload = ({"duration_seconds": existing.requested_duration_s} if existing.requested_duration_s is not None else
                                    {"target_volume_liters": existing.requested_volume_l} if existing.requested_volume_l is not None else {})
                outgoing.append(self._outgoing(WS_COMMANDS, "START_IRRIGATION", existing_payload, f"start:{request_id}", ws_id=ws_id, operator_id=operator_id, request_id=request_id))
            outgoing.append(self._outgoing(OPERATOR_EVENTS, "ACTIVATION_STATUS", {"status": existing.status.value, "reason": existing.deny_reason.value if existing.deny_reason else None}, f"activation-status:{request_id}", operator_id=operator_id, ws_id=ws_id, request_id=request_id, key=operator_id))
            for row in outgoing:
                self.db.add_outgoing(*row)
            self.flush_outbox()
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
        outgoing = []
        if reason is None:
            outgoing.extend([
                self._outgoing(WS_COMMANDS, "START_IRRIGATION", mode_payload, f"start:{request_id}", ws_id=ws_id, operator_id=operator_id, request_id=request_id),
                self._outgoing(OPERATOR_EVENTS, "ACTIVATION_AUTHORIZED", mode_payload, f"authorized:{request_id}", operator_id=operator_id, ws_id=ws_id, request_id=request_id, key=operator_id),
            ])
        denied_outgoing = [self._outgoing(OPERATOR_EVENTS, "ACTIVATION_DENIED", {"reason": (reason or DenyReason.WS_BUSY).value}, f"denied:{request_id}", operator_id=operator_id, ws_id=ws_id, request_id=request_id, key=operator_id)]
        record = self.db.record_irrigation_request(request_id, ws_id, operator_id, int(duration) if has_duration else None, reason, outgoing, denied_outgoing, float(target_volume) if has_target else None)
        if record.status is IrrigationStatus.DENIED:
            self.flush_outbox()
            return
        self.flush_outbox()

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
            outgoing = [self._outgoing(OPERATOR_EVENTS, "IRRIGATION_STARTED", {}, f"started:{request_id}", operator_id=record.operator_id, ws_id=ws_id, request_id=request_id, key=record.operator_id)] if record.operator_id else []
            self.db.mark_irrigation_started(request_id, message["timestamp"], outgoing)
            self.flush_outbox()
        elif event_type == "IRRIGATION_TELEMETRY":
            self._telemetry(record, message)
        elif event_type == "IRRIGATION_COMPLETED":
            reason = {"TIME_LIMIT": EndReason.DURATION, "TARGET_VOLUME": EndReason.TARGET_VOLUME, "MANUAL_STOP": EndReason.MANUAL,
                      "CENTRAL_STOP": EndReason.MANUAL, "STATION_BLOCKED": EndReason.BLOCKED,
                      "LEAK_DETECTED": EndReason.LEAK, "ENGINE_FAULT": EndReason.LEAK,
                      "MONITOR_DISCONNECTED": EndReason.DISCONNECTED}.get(payload.get("end_reason"))
            if reason is None: raise MessageValidationError("invalid completion end_reason")
            duration, volume = self._number(payload, "duration_seconds"), self._number(payload, "accumulated_volume_liters")
            outgoing = [self._outgoing(OPERATOR_EVENTS, "IRRIGATION_COMPLETED", payload, f"completed:{request_id}", operator_id=record.operator_id, ws_id=ws_id, request_id=request_id, key=record.operator_id)] if record.operator_id else []
            changed = self.db.end_irrigation(request_id, reason, duration_s=duration, total_volume_l=volume, ended_at=message["timestamp"], outgoing=outgoing)
            if changed:
                latest = self.latest_telemetry.get(ws_id)
                self.state.update_telemetry(ws_id, latest.flow_rate_lpm if latest and latest.request_id == request_id else 0.0, volume)
            else:
                self.db.add_terminal_measurements(
                    request_id, duration_s=duration, total_volume_l=volume, ended_at=message["timestamp"]
                )
                latest = self.latest_telemetry.get(ws_id)
                self.state.update_telemetry(ws_id, latest.flow_rate_lpm if latest and latest.request_id == request_id else 0.0, volume)
                # A repeated completion after the terminal transition still needs
                # to recreate/flush its stable notification if a prior publish failed.
                for row in outgoing:
                    self.db.add_outgoing(*row)
            self.flush_outbox()
        elif event_type == "COMMAND_REJECTED":
            reason = {"ENGINE_BLOCKED": DenyReason.ENGINE_BLOCKED,
                      "ENGINE_FAILED": DenyReason.ENGINE_FAILED,
                      "MONITOR_UNAVAILABLE": DenyReason.MONITOR_UNAVAILABLE}.get(payload.get("reason"))
            if payload.get("reason") not in COMMAND_REJECTION_REASONS or reason is None:
                raise MessageValidationError("invalid command rejection reason")
            outgoing = [self._outgoing(OPERATOR_EVENTS, "COMMAND_REJECTED", payload, f"rejected:{request_id}", operator_id=record.operator_id, ws_id=ws_id, request_id=request_id, key=record.operator_id)] if record.operator_id else []
            self.db.reject_authorized_irrigation(request_id, reason, outgoing)
            self.flush_outbox()
        else: raise MessageValidationError("unsupported Engine event_type")

    def _telemetry(self, record: Any, message: dict[str, Any]) -> None:
        p, seq = message["payload"], message["payload"].get("sequence")
        if not isinstance(seq, int) or seq < 0: raise MessageValidationError("telemetry sequence invalid")
        values = LatestTelemetry(message["request_id"], seq, self._number(p, "flow_rate_lpm"), self._number(p, "accumulated_volume_liters"), self._number(p, "duration_seconds"))
        old = self.latest_telemetry.get(record.ws_id)
        if record.status is IrrigationStatus.WATERING and (old is None or old.request_id != values.request_id or seq > old.sequence):
            self.latest_telemetry[record.ws_id] = values
            self.state.update_telemetry(record.ws_id, values.flow_rate_lpm, values.accumulated_volume_liters)
            self._notify(record.operator_id, record.request_id, record.ws_id, "IRRIGATION_PROGRESS", p)

    def start_irrigation(self, ws_id: str, duration_seconds: int | None, operator_id: str,
                         target_volume_liters: float | None = None, *, request_id: str | None = None) -> str:
        """Submit a CENTRAL request through the same validation and contract as FO."""
        request_id = request_id or str(uuid4())
        payload = ({"duration_seconds": duration_seconds} if duration_seconds is not None else
                   {"target_volume_liters": target_volume_liters} if target_volume_liters is not None else {})
        message = envelope("ACTIVATION_REQUEST", payload,
                           ws_id=ws_id, operator_id=operator_id, request_id=request_id)
        self.handle_activation(message)
        return request_id

    def issue_station_command(self, event_type: str, ws_id: str, *, request_id: str | None = None,
                              command_id: str | None = None) -> bool:
        if event_type not in {"START_IRRIGATION", "BLOCK_WS", "ACTIVATE_WS", "STOP_IRRIGATION"}: raise ValueError("unsupported command")
        if self.db.get_station(ws_id) is None: return False
        message = envelope(event_type, {}, ws_id=ws_id, request_id=request_id)
        message_id = str(uuid5(NAMESPACE_URL, f"wm-admin-command:{command_id}")) if command_id else str(uuid4())
        message["message_id"] = message_id
        outgoing = (message_id, WS_COMMANDS, ws_id, message)
        if event_type == "BLOCK_WS":
            if not self.state.block_station(ws_id, [outgoing]).ok: return False
        elif event_type == "ACTIVATE_WS":
            if not self.state.activate_station(ws_id, [outgoing]).ok: return False
        else:
            self.db.add_outgoing(*outgoing)
        self.flush_outbox()
        return True

    def _notify(self, operator_id: str | None, request_id: str, ws_id: str, event_type: str, payload: dict[str, Any]) -> None:
        LOGGER.info("%s ws=%s request=%s payload=%s", event_type, ws_id, request_id, payload)
        if operator_id:
            detail = payload.get("sequence", "")
            row = self._outgoing(OPERATOR_EVENTS, event_type, payload, f"{event_type}:{request_id}:{detail}", operator_id=operator_id, ws_id=ws_id, request_id=request_id, key=operator_id)
            self.db.add_outgoing(*row)
            self.flush_outbox()

    @staticmethod
    def _require(message: dict[str, Any], *fields: str) -> None:
        for field in fields:
            if not isinstance(message.get(field), str) or not message[field]: raise MessageValidationError(f"missing {field}")
    @staticmethod
    def _number(payload: dict[str, Any], name: str) -> float:
        value = payload.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0: raise MessageValidationError(f"invalid {name}")
        return float(value)

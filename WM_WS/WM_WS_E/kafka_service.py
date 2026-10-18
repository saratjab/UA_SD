"""Kafka command adapter for the existing WS Engine and irrigation controller."""
from __future__ import annotations

import logging
import threading
from typing import Any

from common.kafka_contract import WS_COMMANDS, WS_EVENTS, MessageValidationError, decode, envelope
from common.kafka_transport import KafkaPublisher, KafkaWorker
from .irrigation import IrrigationRequest, IrrigationResult, IrrigationTelemetry

LOGGER = logging.getLogger(__name__)


class EngineKafkaService:
    def __init__(self, engine: Any, publisher: KafkaPublisher) -> None:
        self.engine, self.publisher = engine, publisher
        self._request_id: str | None = None
        self._operator_id = ""
        self._sequence = 0
        self._end_reason = "MANUAL_STOP"
        self._lock = threading.RLock()
        engine.irrigation.telemetry_callback = self.on_telemetry
        engine.irrigation.result_callback = self.on_result

    def worker(self, bootstrap: str, consumer: Any | None = None) -> KafkaWorker:
        ws_id = self.engine.config.ws_id or "unassigned"
        return KafkaWorker(
            bootstrap,
            f"wm-ws-engine-{ws_id}-v1",
            [WS_COMMANDS],
            self.handle_bytes,
            consumer,
            group_instance_id=f"wm-ws-engine-{ws_id}-instance",
        )

    def handle_bytes(self, raw: bytes) -> bool:
        try:
            self.handle_command(decode(raw)); return True
        except MessageValidationError as exc:
            LOGGER.warning("Discarding malformed command: %s", exc); return True

    def handle_command(self, message: dict[str, Any]) -> None:
        LOGGER.info(
            "Engine command handler entered event=%s ws_id=%s request_id=%s",
            message.get("event_type"), message.get("ws_id"), message.get("request_id"),
        )
        if message.get("ws_id") != self.engine.config.ws_id: return
        event_type = message.get("event_type")
        if event_type == "START_IRRIGATION":
            request_id, operator_id = message.get("request_id"), message.get("operator_id")
            duration = message["payload"].get("duration_seconds")
            if not all(isinstance(v, str) and v for v in (request_id, operator_id)) or isinstance(duration, bool) or not isinstance(duration, (int, float)) or duration <= 0:
                raise MessageValidationError("invalid START_IRRIGATION")
            with self._lock:
                if self._request_id == request_id: return  # replay after a successful start
                if not self.engine.request_irrigation(operator_id, float(duration)):
                    reason = (
                        "ENGINE_FAILED" if self.engine.failed else
                        "ENGINE_BLOCKED" if self.engine.blocked else
                        "MONITOR_UNAVAILABLE"
                    )
                    self._publish("COMMAND_REJECTED", request_id, operator_id, {"reason": reason}); return
                self._request_id, self._operator_id, self._sequence, self._end_reason = request_id, operator_id, 0, "TIME_LIMIT"
            LOGGER.info("Engine irrigation accepted ws_id=%s request_id=%s", self.engine.config.ws_id, request_id)
            self._publish("IRRIGATION_STARTED", request_id, operator_id, {})
        elif event_type in {"STOP_IRRIGATION", "BLOCK_WS"}:
            with self._lock:
                if event_type == "STOP_IRRIGATION":
                    command_request_id = message.get("request_id")
                    if not isinstance(command_request_id, str) or not command_request_id or command_request_id != self._request_id:
                        LOGGER.info("Ignoring STOP_IRRIGATION for non-active request %r", command_request_id)
                        return
                request_id, operator_id = self._request_id, self._operator_id
                self._end_reason = "STATION_BLOCKED" if event_type == "BLOCK_WS" else "CENTRAL_STOP"
            if event_type == "BLOCK_WS": self.engine.blocked = True
            self.engine.stop_irrigation()
            if request_id: LOGGER.info("%s requested for %s", event_type, request_id)
        elif event_type == "ACTIVATE_WS":
            if not self.engine.failed: self.engine.blocked = False
        else: raise MessageValidationError("unsupported command")

    def on_telemetry(self, telemetry: IrrigationTelemetry) -> None:
        with self._lock:
            if not self._request_id or telemetry.final: return
            self._sequence += 1
            request_id, operator_id, sequence = self._request_id, self._operator_id, self._sequence
        self._publish("IRRIGATION_TELEMETRY", request_id, operator_id, {"sequence": sequence, "flow_rate_lpm": telemetry.flow_rate_lpm, "accumulated_volume_liters": telemetry.accumulated_volume_liters, "duration_seconds": telemetry.duration_seconds})

    def on_result(self, result: IrrigationResult, stopped_by: str) -> None:
        with self._lock:
            if not self._request_id: return
            request_id, operator_id = self._request_id, self._operator_id
            reason = "ENGINE_FAULT" if self.engine.failed else (
                "MONITOR_DISCONNECTED" if stopped_by == "monitor_disconnected" else self._end_reason
            )
            self._request_id = None
        LOGGER.info(
            "Engine irrigation completed ws_id=%s request_id=%s duration_seconds=%.3f volume_liters=%.3f stopped_by=%s",
            result.ws_id, request_id, result.duration_seconds,
            result.accumulated_volume_liters, stopped_by,
        )
        self._publish("IRRIGATION_COMPLETED", request_id, operator_id, {"end_reason": reason, "duration_seconds": result.duration_seconds, "accumulated_volume_liters": result.accumulated_volume_liters})

    def _publish(self, event_type: str, request_id: str, operator_id: str, payload: dict[str, Any]) -> None:
        self.publisher.publish(WS_EVENTS, envelope(event_type, payload, ws_id=self.engine.config.ws_id, request_id=request_id, operator_id=operator_id))
        LOGGER.info(
            "Engine event published event=%s ws_id=%s request_id=%s",
            event_type, self.engine.config.ws_id, request_id,
        )

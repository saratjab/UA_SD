"""Kafka command adapter for the existing WS Engine and irrigation controller."""
from __future__ import annotations

import logging
import json
import math
import sqlite3
import threading
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from common.kafka_contract import WS_COMMANDS, WS_EVENTS, MessageValidationError, decode, envelope
from common.kafka_transport import KafkaPublisher, KafkaWorker
from .irrigation import IrrigationRequest, IrrigationResult, IrrigationTelemetry

LOGGER = logging.getLogger(__name__)


class EngineKafkaService:
    def __init__(self, engine: Any, publisher: KafkaPublisher, state_path: str | Path = ":memory:") -> None:
        self.engine, self.publisher = engine, publisher
        self._request_id: str | None = None
        self._operator_id = ""
        self._target_volume_liters: float | None = None
        self._sequence = 0
        self._end_reason = "MANUAL_STOP"
        self._lock = threading.RLock()
        self._retry_stop = threading.Event()
        if str(state_path) != ":memory:":
            Path(state_path).parent.mkdir(parents=True, exist_ok=True)
        self._store = sqlite3.connect(str(state_path), check_same_thread=False, isolation_level=None)
        self._store.execute("PRAGMA journal_mode=WAL")
        self._store.executescript("""
            CREATE TABLE IF NOT EXISTS engine_active (ws_id TEXT PRIMARY KEY, request_id TEXT, operator_id TEXT, duration REAL, target_volume REAL, elapsed REAL, volume REAL, end_reason TEXT);
            CREATE TABLE IF NOT EXISTS engine_outbox (message_id TEXT PRIMARY KEY, message_json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS engine_requests (request_id TEXT PRIMARY KEY, status TEXT NOT NULL);
        """)
        active_columns = {row[1] for row in self._store.execute("PRAGMA table_info(engine_active)")}
        if "target_volume" not in active_columns:
            self._store.execute("ALTER TABLE engine_active ADD COLUMN target_volume REAL")
        self._restore_interrupted_request()
        engine.irrigation.telemetry_callback = self.on_telemetry
        engine.irrigation.result_callback = self.on_result
        self._retry_thread = threading.Thread(target=self._retry_loop, name="wm-ws-e-outbox", daemon=True)
        self._retry_thread.start()

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
            payload = message["payload"]
            has_duration = "duration_seconds" in payload
            has_target = "target_volume_liters" in payload
            duration = payload.get("duration_seconds")
            target_volume = payload.get("target_volume_liters")
            if set(payload) - {"duration_seconds", "target_volume_liters"}:
                raise MessageValidationError("unsupported START_IRRIGATION payload field")
            if has_duration and has_target:
                raise MessageValidationError("START_IRRIGATION cannot specify both duration and target volume")
            if has_duration and (isinstance(duration, bool) or not isinstance(duration, (int, float)) or not math.isfinite(duration) or duration <= 0):
                raise MessageValidationError("invalid START_IRRIGATION duration_seconds")
            if has_target and (isinstance(target_volume, bool) or not isinstance(target_volume, (int, float)) or not math.isfinite(target_volume) or target_volume <= 0):
                raise MessageValidationError("invalid START_IRRIGATION target_volume_liters")
            if not all(isinstance(v, str) and v for v in (request_id, operator_id)):
                raise MessageValidationError("invalid START_IRRIGATION")
            with self._lock:
                if self._store.execute("SELECT 1 FROM engine_requests WHERE request_id=?", (request_id,)).fetchone():
                    return  # duplicate start, including a request interrupted on a previous process
                if self._request_id is not None or self.engine.irrigation.state.value != "IDLE":
                    return
                self._request_id, self._operator_id, self._sequence, self._end_reason = request_id, operator_id, 0, "MANUAL_STOP"
                self._target_volume_liters = float(target_volume) if target_volume is not None else None
                self._store.execute("INSERT OR REPLACE INTO engine_active (ws_id, request_id, operator_id, duration, target_volume, elapsed, volume, end_reason) VALUES (?, ?, ?, ?, ?, 0, 0, 'MANUAL_STOP')", (self.engine.config.ws_id, request_id, operator_id, float(duration) if duration is not None else None, float(target_volume) if target_volume is not None else None))
                if not self.engine.request_irrigation(operator_id, float(duration) if duration is not None else None, float(target_volume) if target_volume is not None else None):
                    self._store.execute("DELETE FROM engine_active WHERE request_id=?", (request_id,))
                    self._request_id = None
                    self._target_volume_liters = None
                    reason = (
                        "ENGINE_FAILED" if self.engine.failed else
                        "ENGINE_BLOCKED" if self.engine.blocked else
                        "MONITOR_UNAVAILABLE"
                    )
                    self._publish("COMMAND_REJECTED", request_id, operator_id, {"reason": reason}); return
                self._store.execute("INSERT INTO engine_requests VALUES (?, 'ACTIVE')", (request_id,))
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
            self._store.execute("UPDATE engine_active SET elapsed=?, volume=? WHERE ws_id=? AND request_id=?", (telemetry.duration_seconds, telemetry.accumulated_volume_liters, self.engine.config.ws_id, request_id))
        payload = {"sequence": sequence, "flow_rate_lpm": telemetry.flow_rate_lpm, "accumulated_volume_liters": telemetry.accumulated_volume_liters, "duration_seconds": telemetry.duration_seconds}
        if self._target_volume_liters is not None:
            payload["target_volume_liters"] = self._target_volume_liters
        self._publish("IRRIGATION_TELEMETRY", request_id, operator_id, payload)

    def on_result(self, result: IrrigationResult, stopped_by: str) -> None:
        with self._lock:
            if not self._request_id: return
            request_id, operator_id = self._request_id, self._operator_id
            reason = ("ENGINE_FAULT" if self.engine.failed else
                      "MONITOR_DISCONNECTED" if stopped_by == "monitor_disconnected" else
                      "TIME_LIMIT" if stopped_by == "duration" else
                      "TARGET_VOLUME" if stopped_by == "target_volume" else self._end_reason)
            payload = {"end_reason": reason, "duration_seconds": result.duration_seconds, "accumulated_volume_liters": result.accumulated_volume_liters}
            if self._target_volume_liters is not None:
                payload["target_volume_liters"] = self._target_volume_liters
            message = self._event("IRRIGATION_COMPLETED", request_id, operator_id, payload)
            self._store.execute("BEGIN IMMEDIATE")
            try:
                self._store.execute("INSERT OR IGNORE INTO engine_outbox VALUES (?, ?)", (message["message_id"], json.dumps(message, separators=(",", ":"), sort_keys=True)))
                self._store.execute("DELETE FROM engine_active WHERE ws_id=? AND request_id=?", (self.engine.config.ws_id, request_id))
                self._store.execute("UPDATE engine_requests SET status='FINAL_PENDING' WHERE request_id=?", (request_id,))
                self._store.execute("COMMIT")
            except BaseException:
                self._store.execute("ROLLBACK")
                raise
            self._request_id = None
            self._target_volume_liters = None
        LOGGER.info(
            "Engine irrigation completed ws_id=%s request_id=%s duration_seconds=%.3f volume_liters=%.3f stopped_by=%s",
            result.ws_id, request_id, result.duration_seconds,
            result.accumulated_volume_liters, stopped_by,
        )
        self.flush_outbox()

    def _publish(self, event_type: str, request_id: str, operator_id: str, payload: dict[str, Any]) -> None:
        message = self._event(event_type, request_id, operator_id, payload)
        if event_type != "IRRIGATION_TELEMETRY":
            self._store.execute("INSERT OR IGNORE INTO engine_outbox VALUES (?, ?)", (message["message_id"], json.dumps(message, separators=(",", ":"), sort_keys=True)))
            self.flush_outbox()
        else:
            self.publisher.publish(WS_EVENTS, message)
        LOGGER.info(
            "Engine event published event=%s ws_id=%s request_id=%s",
            event_type, self.engine.config.ws_id, request_id,
        )

    def _event(self, event_type: str, request_id: str, operator_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        message = envelope(event_type, payload, ws_id=self.engine.config.ws_id, request_id=request_id, operator_id=operator_id)
        suffix = payload.get("sequence", "final")
        message["message_id"] = str(uuid5(NAMESPACE_URL, f"wm-engine:{self.engine.config.ws_id}:{request_id}:{event_type}:{suffix}"))
        return message

    def flush_outbox(self) -> None:
        with self._lock:
            rows = self._store.execute("SELECT message_id, message_json FROM engine_outbox ORDER BY message_id").fetchall()
            for message_id, raw in rows:
                try:
                    self.publisher.publish(WS_EVENTS, json.loads(raw))
                except Exception:
                    LOGGER.exception("Engine final event remains pending message_id=%s", message_id)
                    break
                message = json.loads(raw)
                if message.get("event_type") == "IRRIGATION_COMPLETED":
                    self._store.execute("UPDATE engine_requests SET status='COMPLETED' WHERE request_id=?", (message.get("request_id"),))
                self._store.execute("DELETE FROM engine_outbox WHERE message_id=?", (message_id,))

    def _restore_interrupted_request(self) -> None:
        self.engine.irrigation.valve.close()
        row = self._store.execute("SELECT request_id, operator_id, elapsed, volume, target_volume FROM engine_active LIMIT 1").fetchone()
        if row is not None:
            request_id, operator_id, elapsed, volume, target = row
            payload = {"end_reason": "ENGINE_FAULT", "duration_seconds": elapsed, "accumulated_volume_liters": volume}
            if target is not None:
                payload["target_volume_liters"] = target
            message = self._event("IRRIGATION_COMPLETED", request_id, operator_id, payload)
            self._store.execute("BEGIN IMMEDIATE")
            try:
                self._store.execute("INSERT OR IGNORE INTO engine_outbox VALUES (?, ?)", (message["message_id"], json.dumps(message, separators=(",", ":"), sort_keys=True)))
                self._store.execute("DELETE FROM engine_active WHERE ws_id=?", (self.engine.config.ws_id,))
                self._store.execute("INSERT OR REPLACE INTO engine_requests VALUES (?, 'FINAL_PENDING')", (request_id,))
                self._store.execute("COMMIT")
            except BaseException:
                self._store.execute("ROLLBACK")
                raise
        self.flush_outbox()

    def _retry_loop(self) -> None:
        while not self._retry_stop.is_set():
            self.flush_outbox()
            self._retry_stop.wait(1.0)

    def close(self) -> None:
        self._retry_stop.set()
        self._retry_thread.join(timeout=2.0)
        self._store.close()

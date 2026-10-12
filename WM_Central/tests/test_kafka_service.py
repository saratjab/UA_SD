from __future__ import annotations

import unittest

from common.kafka_contract import decode, envelope
from WM_Central.kafka_service import CentralKafkaService
from WM_Central.models import IrrigationStatus
from WM_Central.state_manager import StateManager
from .helpers import DatabaseTestCase


class FakePublisher:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict]] = []
    def publish(self, topic: str, message: dict, *, key: str | None = None) -> None:
        self.records.append((topic, message))


class CentralKafkaTests(DatabaseTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.state = StateManager(self.db)
        self.connection = object()
        self.state.register("WS_001", "North", self.connection)
        self.publisher = FakePublisher()
        self.service = CentralKafkaService(self.db, self.state, self.publisher)  # type: ignore[arg-type]

    def request(self, request_id: str = "request-1", duration: int = 3) -> dict:
        return envelope("ACTIVATION_REQUEST", {"duration_seconds": duration}, ws_id="WS_001", operator_id="OP_001", request_id=request_id)

    def test_authorizes_once_and_publishes_start_command(self) -> None:
        self.service.handle_activation(self.request())
        record = self.db.get_irrigation("request-1")
        self.assertEqual(record.status, IrrigationStatus.AUTHORIZED)  # type: ignore[union-attr]
        commands = [m for topic, m in self.publisher.records if topic == "wm.ws.commands"]
        self.assertEqual(commands[0]["event_type"], "START_IRRIGATION")
        self.service.handle_activation(self.request())
        self.assertEqual(len([m for topic, m in self.publisher.records if topic == "wm.ws.commands"]), 1)

    def test_telemetry_replaces_cumulative_measurement_and_completion_is_idempotent(self) -> None:
        self.service.handle_activation(self.request())
        self.service.handle_engine_event(envelope("IRRIGATION_STARTED", {}, ws_id="WS_001", request_id="request-1"))
        self.service.handle_engine_event(envelope("IRRIGATION_TELEMETRY", {"sequence": 2, "flow_rate_lpm": 6.0, "accumulated_volume_liters": 0.2, "duration_seconds": 2.0}, ws_id="WS_001", request_id="request-1"))
        self.service.handle_engine_event(envelope("IRRIGATION_TELEMETRY", {"sequence": 1, "flow_rate_lpm": 6.0, "accumulated_volume_liters": 99.0, "duration_seconds": 1.0}, ws_id="WS_001", request_id="request-1"))
        self.assertEqual(self.service.latest_telemetry["WS_001"].accumulated_volume_liters, 0.2)
        complete = envelope("IRRIGATION_COMPLETED", {"end_reason": "TIME_LIMIT", "duration_seconds": 3.0, "accumulated_volume_liters": 0.3}, ws_id="WS_001", request_id="request-1")
        self.service.handle_engine_event(complete); self.service.handle_engine_event(complete)
        self.assertEqual(self.db.get_irrigation("request-1").status, IrrigationStatus.ENDED)  # type: ignore[union-attr]

    def test_malformed_payload_is_consumed_without_crashing_partition(self) -> None:
        self.assertTrue(self.service.handle_activation_bytes(b"not json"))


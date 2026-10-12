from __future__ import annotations

import unittest

from common.kafka_contract import envelope
from WM_WS.WM_WS_E.config import EngineConfig
from WM_WS.WM_WS_E.engine import WateringStationEngine
from WM_WS.WM_WS_E.flow_meter import FlowMeter
from WM_WS.WM_WS_E.irrigation import IrrigationController
from WM_WS.WM_WS_E.kafka_service import EngineKafkaService
from WM_WS.WM_WS_E.valve import SolenoidValve


class FakePublisher:
    def __init__(self) -> None: self.records = []
    def publish(self, topic, message) -> None: self.records.append((topic, message))


class EngineKafkaTests(unittest.TestCase):
    def config(self) -> EngineConfig:
        return EngineConfig("WS_001", "localhost", 9092, "localhost", 1, 6, 1, .1, .01)

    def test_dispatches_targeted_start_and_ignores_other_station(self) -> None:
        valve = SolenoidValve()
        engine = WateringStationEngine(self.config(), IrrigationController("WS_001", valve, FlowMeter(6), .01))
        publisher = FakePublisher(); service = EngineKafkaService(engine, publisher)  # type: ignore[arg-type]
        service.handle_command(envelope("START_IRRIGATION", {"duration_seconds": .03}, ws_id="OTHER", operator_id="OP", request_id="other"))
        self.assertFalse(valve.is_open)
        service.handle_command(envelope("START_IRRIGATION", {"duration_seconds": .03}, ws_id="WS_001", operator_id="OP", request_id="r1"))
        self.assertTrue(engine.irrigation.wait_until_finished(1))
        kinds = [message["event_type"] for _, message in publisher.records]
        self.assertIn("IRRIGATION_STARTED", kinds); self.assertIn("IRRIGATION_COMPLETED", kinds)

    def test_block_closes_active_valve(self) -> None:
        valve = SolenoidValve(); engine = WateringStationEngine(self.config(), IrrigationController("WS_001", valve, FlowMeter(6), .1))
        service = EngineKafkaService(engine, FakePublisher())  # type: ignore[arg-type]
        service.handle_command(envelope("START_IRRIGATION", {"duration_seconds": 1}, ws_id="WS_001", operator_id="OP", request_id="r1"))
        service.handle_command(envelope("BLOCK_WS", {}, ws_id="WS_001"))
        self.assertTrue(engine.irrigation.wait_until_finished(1)); self.assertFalse(valve.is_open); self.assertTrue(engine.blocked)

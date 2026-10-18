from __future__ import annotations

import unittest
import time

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

    def test_monitor_disconnect_completion_uses_contract_reason(self) -> None:
        engine = WateringStationEngine(self.config(), IrrigationController("WS_001", SolenoidValve(), FlowMeter(6), .01))
        publisher = FakePublisher()
        service = EngineKafkaService(engine, publisher)  # type: ignore[arg-type]
        service.handle_command(envelope("START_IRRIGATION", {"duration_seconds": 1}, ws_id="WS_001", operator_id="OP", request_id="r1"))
        engine.irrigation.emergency_stop(reason="monitor_disconnected")
        self.assertTrue(engine.irrigation.wait_until_finished(1))
        completed = [m for _, m in publisher.records if m["event_type"] == "IRRIGATION_COMPLETED"]
        self.assertEqual(completed[0]["payload"]["end_reason"], "MONITOR_DISCONNECTED")

    def test_stop_irrigation_requires_matching_active_request_id(self) -> None:
        engine = WateringStationEngine(self.config(), IrrigationController("WS_001", SolenoidValve(), FlowMeter(6), .05))
        service = EngineKafkaService(engine, FakePublisher())  # type: ignore[arg-type]
        service.handle_command(envelope("START_IRRIGATION", {"duration_seconds": 1}, ws_id="WS_001", operator_id="OP", request_id="active"))
        for request_id in ("stale", None):
            service.handle_command(envelope("STOP_IRRIGATION", {}, ws_id="WS_001", request_id=request_id))
            time.sleep(.08)
            self.assertEqual(engine.irrigation.state.value, "WATERING")
        service.handle_command(envelope("STOP_IRRIGATION", {}, ws_id="WS_001", request_id="active"))
        self.assertTrue(engine.irrigation.wait_until_finished(1))
        self.assertEqual(engine.irrigation.last_result.stopped_by, "manual")  # type: ignore[union-attr]

    def test_stale_stop_after_completion_does_not_affect_next_irrigation(self) -> None:
        engine = WateringStationEngine(self.config(), IrrigationController("WS_001", SolenoidValve(), FlowMeter(6), .01))
        service = EngineKafkaService(engine, FakePublisher())  # type: ignore[arg-type]
        service.handle_command(envelope("START_IRRIGATION", {"duration_seconds": .02}, ws_id="WS_001", operator_id="OP", request_id="old"))
        self.assertTrue(engine.irrigation.wait_until_finished(1))
        service.handle_command(envelope("START_IRRIGATION", {"duration_seconds": 1}, ws_id="WS_001", operator_id="OP", request_id="new"))
        service.handle_command(envelope("STOP_IRRIGATION", {}, ws_id="WS_001", request_id="old"))
        time.sleep(.08)
        self.assertEqual(engine.irrigation.state.value, "WATERING")
        engine.stop_irrigation()
        engine.irrigation.wait_until_finished(1)

    def test_rejected_start_distinguishes_blocked_from_failed(self) -> None:
        for state, expected in (("blocked", "ENGINE_BLOCKED"), ("failed", "ENGINE_FAILED")):
            with self.subTest(state=state):
                engine = WateringStationEngine(self.config())
                setattr(engine, state, True)
                publisher = FakePublisher()
                service = EngineKafkaService(engine, publisher)  # type: ignore[arg-type]
                service.handle_command(envelope("START_IRRIGATION", {"duration_seconds": 1}, ws_id="WS_001", operator_id="OP", request_id="r1"))
                rejected = [m for _, m in publisher.records if m["event_type"] == "COMMAND_REJECTED"]
                self.assertEqual(rejected[0]["payload"]["reason"], expected)

from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout

from common.event_panel import reset_feed
from common.kafka_contract import encode, envelope
from WM_FO.kafka_client import FieldOperator
from WM_WS.WM_WS_E.config import EngineConfig
from WM_WS.WM_WS_E.engine import WateringStationEngine
from WM_WS.WM_WS_E.flow_meter import FlowMeter
from WM_WS.WM_WS_E.irrigation import IrrigationController
from WM_WS.WM_WS_E.kafka_service import EngineKafkaService
from WM_WS.WM_WS_E.valve import SolenoidValve
from WM_WS.WM_WS_M.config import MonitorConfig
from WM_WS.WM_WS_M.monitor import WateringStationMonitor

from .test_engine_kafka_service import FakePublisher
from .test_monitor import FakeCentralClient


class RecordingPublisher:
    def __init__(self) -> None: self.records = []
    def publish(self, topic, message, *, key=None) -> None: self.records.append((topic, message))


def names(feed) -> list[tuple[str, str]]:
    events, _ = feed.since(0)
    return [(e["direction"], e["event"]) for e in events]


class FieldOperatorFeedTests(unittest.TestCase):
    def test_fo_panel_shows_its_own_request_and_nothing_from_other_operators(self) -> None:
        feed = reset_feed()
        operator = FieldOperator("OP_001", RecordingPublisher())
        request_id = operator.request_irrigation("WS_001", 10)
        other = envelope("IRRIGATION_STARTED", {}, ws_id="WS_002", operator_id="OP_002", request_id="someone-else")
        mine = envelope("ACTIVATION_AUTHORIZED", {"duration_seconds": 10}, ws_id="WS_001",
                        operator_id="OP_001", request_id=request_id)
        with redirect_stdout(io.StringIO()):
            operator.handle_bytes(encode(other))
            operator.handle_bytes(encode(mine))
        self.assertEqual(names(feed), [("OUT", "ACTIVATION_REQUEST"), ("IN", "ACTIVATION_AUTHORIZED")])
        events, _ = feed.since(0)
        self.assertTrue(all(e["request_id"] == request_id for e in events))


class EngineFeedTests(unittest.TestCase):
    def test_engine_panel_shows_command_and_its_irrigation_events(self) -> None:
        feed = reset_feed()
        controller = IrrigationController("WS_001", SolenoidValve(), FlowMeter(60), .01)
        engine = WateringStationEngine(EngineConfig("WS_001", "localhost", 9092, "localhost", 1, 6, 1, .1, .01), controller)
        engine._monitor_ready.set()
        service = EngineKafkaService(engine, FakePublisher())
        self.addCleanup(service.close)
        service.handle_command(envelope("START_IRRIGATION", {"duration_seconds": .03}, ws_id="WS_001",
                                        operator_id="OP_001", request_id="r1"))
        self.assertTrue(controller.wait_until_finished(2))
        service.flush_outbox()
        timeline = names(feed)
        self.assertEqual(timeline[:2], [("IN", "START_IRRIGATION"), ("OUT", "IRRIGATION_STARTED")])
        self.assertIn(("OUT", "IRRIGATION_TELEMETRY"), timeline)
        self.assertEqual(timeline[-1], ("OUT", "IRRIGATION_COMPLETED"))
        service.handle_command(envelope("START_IRRIGATION", {}, ws_id="WS_999", operator_id="OP", request_id="x"))
        self.assertNotIn("x", [e["request_id"] for e in feed.since(0)[0]])


class MonitorFeedTests(unittest.TestCase):
    def test_monitor_panel_shows_a_fault_once_and_its_recovery(self) -> None:
        feed = reset_feed()
        central = FakeCentralClient()
        monitor = WateringStationMonitor(
            MonitorConfig("WS_001", "127.0.0.1", 5000, "127.0.0.1", 6000, .05, .01, .05),
            central_client=central, engine_server=object())
        monitor.activate_fault("timeout", "No health-check response received")
        monitor.activate_fault("timeout", "No health-check response received")
        monitor.resolve_fault_if_needed()
        self.assertEqual(names(feed), [("LOCAL", "ENGINE_TIMEOUT"), ("LOCAL", "ENGINE_HEALTHY")])


if __name__ == "__main__":
    unittest.main()

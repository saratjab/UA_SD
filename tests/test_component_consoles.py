"""REST APIs of the FO, Engine and Monitor web consoles."""
from __future__ import annotations

import io
import json
import threading
import time
import unittest
from contextlib import redirect_stdout
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from common.kafka_contract import encode, envelope
from common.web_console import ConsoleServer
from WM_FO.kafka_client import TRACKED_REQUESTS, FieldOperator
from WM_FO.WM_FO import _ActiveRequests, console_routes
from WM_WS.WM_WS_E.config import EngineConfig
from WM_WS.WM_WS_E.controls import ControlRefused, StationControls
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
    def __init__(self) -> None:
        self.records = []

    def publish(self, topic, message, *, key=None) -> None:
        self.records.append((topic, message))


def deliver(operator: FieldOperator, event_type: str, request_id: str, payload: dict, ws_id: str = "WS_001") -> None:
    with redirect_stdout(io.StringIO()):
        operator.handle_bytes(encode(envelope(event_type, payload, ws_id=ws_id,
                                              operator_id=operator.operator_id, request_id=request_id)))


def call(base: str, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if body is not None else {}
    try:
        with urlopen(Request(base + path, data=data, method=method, headers=headers), timeout=3) as response:
            return response.status, json.loads(response.read())
    except HTTPError as error:
        return error.code, json.loads(error.read())


class FieldOperatorTrackerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.operator = FieldOperator("OP_001", RecordingPublisher())

    def request(self, ws_id: str = "WS_001", duration: int = 30) -> str:
        with redirect_stdout(io.StringIO()):
            return self.operator.request_irrigation(ws_id, duration)

    def test_tracks_decision_progress_and_completion(self) -> None:
        request_id = self.request()
        [item] = self.operator.requests_snapshot()
        self.assertEqual((item["status"], item["ws_id"], item["duration_seconds"]), ("SUBMITTED", "WS_001", 30))
        deliver(self.operator, "ACTIVATION_AUTHORIZED", request_id, {"duration_seconds": 30})
        self.assertEqual(self.operator.requests_snapshot()[0]["status"], "AUTHORIZED")
        deliver(self.operator, "IRRIGATION_STARTED", request_id, {})
        deliver(self.operator, "IRRIGATION_PROGRESS", request_id,
                {"duration_seconds": 10.0, "accumulated_volume_liters": 1.0, "flow_rate_lpm": 6.0})
        item = self.operator.requests_snapshot()[0]
        self.assertEqual((item["status"], item["elapsed_seconds"], item["volume_liters"], item["flow_rate_lpm"]),
                         ("WATERING", 10.0, 1.0, 6.0))
        deliver(self.operator, "IRRIGATION_COMPLETED", request_id,
                {"end_reason": "TIME_LIMIT", "duration_seconds": 30.0, "accumulated_volume_liters": 3.0})
        item = self.operator.requests_snapshot()[0]
        self.assertEqual((item["status"], item["end_reason"], item["volume_liters"]), ("COMPLETED", "TIME_LIMIT", 3.0))
        self.assertEqual([h["event"] for h in item["history"]],
                         ["SUBMITTED", "ACTIVATION_AUTHORIZED", "IRRIGATION_STARTED", "IRRIGATION_COMPLETED"])

    def test_denial_rejection_and_interruption_are_distinct(self) -> None:
        denied, rejected, interrupted, stopped = (self.request(f"WS_00{i}") for i in range(1, 5))
        deliver(self.operator, "ACTIVATION_DENIED", denied, {"reason": "WS_BUSY"})
        deliver(self.operator, "COMMAND_REJECTED", rejected, {"reason": "ENGINE_FAILED"})
        deliver(self.operator, "IRRIGATION_COMPLETED", interrupted, {"end_reason": "MONITOR_DISCONNECTED"})
        deliver(self.operator, "IRRIGATION_COMPLETED", stopped, {"end_reason": "CENTRAL_STOP"})
        by_id = {item["request_id"]: item for item in self.operator.requests_snapshot()}
        self.assertEqual((by_id[denied]["status"], by_id[denied]["reason"]), ("DENIED", "WS_BUSY"))
        self.assertEqual(by_id[rejected]["status"], "REJECTED")
        self.assertEqual(by_id[interrupted]["status"], "INTERRUPTED")
        self.assertEqual(by_id[stopped]["status"], "STOPPED")

    def test_tracker_is_bounded_and_newest_first(self) -> None:
        ids = [self.request() for _ in range(TRACKED_REQUESTS + 5)]
        snapshot = self.operator.requests_snapshot()
        self.assertEqual(len(snapshot), TRACKED_REQUESTS)
        self.assertEqual(snapshot[0]["request_id"], ids[-1])

    def test_unanswered_request_is_marked_without_result(self) -> None:
        request_id = self.request()
        self.operator.mark_no_result(request_id)
        self.assertEqual(self.operator.requests_snapshot()[0]["status"], "NO_RESULT")


class FieldOperatorConsoleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.publisher = RecordingPublisher()
        self.operator = FieldOperator("OP_001", self.publisher)
        self.active = _ActiveRequests()
        self.server = ConsoleServer("127.0.0.1", 0, "fo", {"title": "FO"}, console_routes(self.operator, self.active, 30))
        self.server.start()
        self.addCleanup(self.server.stop)
        self.base = f"http://127.0.0.1:{self.server.port}"

    def test_request_returns_immediately_and_is_followed_in_the_background(self) -> None:
        started = time.monotonic()
        with redirect_stdout(io.StringIO()):
            code, body = call(self.base, "POST", "/api/irrigations", {"ws_id": "WS_001", "duration_seconds": 600})
            second, _ = call(self.base, "POST", "/api/irrigations", {"ws_id": "WS_002", "target_volume_liters": 5})
        self.assertLess(time.monotonic() - started, 2.0)
        self.assertEqual((code, second), (202, 202))
        self.assertEqual(self.active.count(), 2)
        self.assertEqual([m["ws_id"] for _, m in self.publisher.records], ["WS_001", "WS_002"])
        code, listing = call(self.base, "GET", "/api/irrigations")
        self.assertEqual([r["status"] for r in listing["requests"]], ["SUBMITTED", "SUBMITTED"])
        with redirect_stdout(io.StringIO()):
            deliver(self.operator, "ACTIVATION_DENIED", body["request_id"], {"reason": "WS_BUSY"})
        deadline = time.monotonic() + 2
        while self.active.count() != 1 and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertEqual(self.active.count(), 1)

    def test_invalid_requests_are_rejected_before_publishing(self) -> None:
        for body in ({"duration_seconds": 10}, {"ws_id": "WS_001"},
                     {"ws_id": "WS_001", "duration_seconds": 10, "target_volume_liters": 1},
                     {"ws_id": "WS_001", "duration_seconds": -1}, {"ws_id": "WS_001", "duration_seconds": 10, "x": 1}):
            with self.subTest(body=body):
                self.assertEqual(call(self.base, "POST", "/api/irrigations", body)[0], 400)
        self.assertEqual(self.publisher.records, [])

    def test_station_list_comes_from_central_and_times_out_cleanly(self) -> None:
        def answer() -> None:
            deadline = time.monotonic() + 2
            while not self.publisher.records and time.monotonic() < deadline:
                time.sleep(0.01)
            request_id = self.publisher.records[0][1]["request_id"]
            deliver(self.operator, "AVAILABLE_STATIONS_RESPONSE", request_id,
                    {"stations": [{"station_id": "WS_001", "location": "North"}], "error": None})
        threading.Thread(target=answer, daemon=True).start()
        code, body = call(self.base, "GET", "/api/stations")
        self.assertEqual((code, body["stations"][0]["station_id"]), (200, "WS_001"))
        self.assertTrue(call(self.base, "GET", "/api/status")[1]["central"]["ok"])


class EngineControlsTests(unittest.TestCase):
    def engine(self) -> WateringStationEngine:
        controller = IrrigationController("WS_001", SolenoidValve(), FlowMeter(60), .01)
        engine = WateringStationEngine(EngineConfig("WS_001", "localhost", 9092, "localhost", 1, 6, 1, .1, .01), controller)
        engine._monitor_ready.set()
        return engine

    def test_status_shows_irrigation_and_links_and_controls_change_them(self) -> None:
        engine = self.engine()
        service = EngineKafkaService(engine, FakePublisher())
        self.addCleanup(service.close)
        controls = StationControls(engine, None, service)
        service.handle_command(envelope("START_IRRIGATION", {"duration_seconds": 30}, ws_id="WS_001",
                                        operator_id="OP_001", request_id="r1"))
        status = controls.status()
        self.assertEqual(status["state"], "WATERING")
        self.assertEqual(status["irrigation"], {"request_id": "r1", "operator_id": "OP_001",
                                                "duration_seconds": 30.0, "target_volume_liters": None})
        self.assertTrue(status["monitor"]["connected"] and status["central"]["reachable"])
        controls.stop()
        self.assertTrue(engine.irrigation.wait_until_finished(2))
        status = controls.status()
        self.assertEqual((status["state"], status["irrigation"]), ("IDLE", None))
        self.assertEqual(status["last_completed"]["request_id"], "r1")
        self.assertEqual(status["last_completed"]["end_reason"], "MANUAL_STOP")
        controls.simulate_failure()
        self.assertEqual(controls.status()["state"], "FAILED")
        controls.clear_failure()
        self.assertFalse(controls.status()["failed"])

    def test_central_loss_is_visible_and_watering_continues_until_it_ends(self) -> None:
        engine = self.engine()
        controls = StationControls(engine)
        engine.request_irrigation("OP_001", 30)
        engine._apply_central_link(False)
        status = controls.status()
        self.assertFalse(status["central"]["reachable"])
        self.assertEqual(status["state"], "WATERING")
        self.assertFalse(engine.request_irrigation("OP_001", 30))
        engine.stop_irrigation()
        self.assertTrue(engine.irrigation.wait_until_finished(2))
        self.assertEqual(controls.status()["state"], "IDLE")

    def test_web_routes_refuse_invalid_actions(self) -> None:
        controls = StationControls(self.engine())
        server = ConsoleServer("127.0.0.1", 0, "engine", {"title": "E"}, controls.routes())
        server.start()
        self.addCleanup(server.stop)
        base = f"http://127.0.0.1:{server.port}"
        self.assertEqual(call(base, "POST", "/api/irrigation/start", {})[0], 409)
        self.assertEqual(call(base, "POST", "/api/irrigation/stop", {})[0], 409)
        self.assertEqual(call(base, "POST", "/api/failure", {"active": "yes"})[0], 400)
        self.assertEqual(call(base, "POST", "/api/failure", {"active": True})[0], 200)
        self.assertTrue(call(base, "GET", "/api/status")[1]["failed"])
        with self.assertRaises(ControlRefused):
            controls.start_manual()


class MonitorStatusTests(unittest.TestCase):
    def test_status_reports_engine_health_and_central_link(self) -> None:
        config = MonitorConfig("WS_001", "central", 5000, "127.0.0.1", 0, 1.0, .1, .2, location="North")
        monitor = WateringStationMonitor(config, FakeCentralClient(), engine_server=None)
        monitor._set_engine_connected(True)
        monitor._record_central(True)
        monitor._record_health(1, True)
        monitor._last_fault = ("health_ko", "simulated_failure")
        monitor._record_health(2, False)
        status = monitor.status()
        self.assertTrue(status["engine"]["connected"])
        self.assertTrue(status["central"]["connected"])
        self.assertEqual([h["ok"] for h in status["health"]], [True, False])
        self.assertEqual((status["last_health"]["fault"], status["last_health"]["detail"]), ("health_ko", "simulated_failure"))


if __name__ == "__main__":
    unittest.main()

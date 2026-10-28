from __future__ import annotations

import unittest
import time
import tempfile
from pathlib import Path
from unittest import mock

from common.kafka_contract import COMMAND_REJECTION_REASONS, encode, envelope
from common.kafka_transport import KafkaWorker
from WM_WS.WM_WS_E.config import EngineConfig, parse_args
from WM_WS.WM_WS_E.engine import WateringStationEngine
from WM_WS.WM_WS_E.flow_meter import FlowMeter
from WM_WS.WM_WS_E.irrigation import IrrigationController
from WM_WS.WM_WS_E.kafka_service import EngineKafkaService
from WM_WS.WM_WS_E.valve import SolenoidValve


class FakePublisher:
    def __init__(self) -> None: self.records = []; self.fail = False; self.fail_event = None
    def publish(self, topic, message) -> None:
        if self.fail or message.get("event_type") == self.fail_event: raise RuntimeError("simulated broker outage")
        self.records.append((topic, message))


class OneMessage:
    def __init__(self, value: bytes) -> None: self._value = value
    def error(self): return None
    def value(self): return self._value
    def topic(self): return "wm.ws.commands"
    def partition(self): return 0
    def offset(self): return 0
    def timestamp(self): return (1, int(time.time() * 1000))


class OneMessageConsumer:
    def __init__(self, value: bytes) -> None: self.message = OneMessage(value)
    def poll(self, _timeout):
        message, self.message = self.message, None
        return message
    def assignment(self): return []
    def commit(self, **_kwargs): pass
    def subscribe(self, _topics): pass
    def close(self): pass


class EngineKafkaTests(unittest.TestCase):
    def config(self) -> EngineConfig:
        return EngineConfig("WS_001", "localhost", 9092, "localhost", 1, 6, 1, .1, .01)

    def engine(self, controller: IrrigationController | None = None) -> WateringStationEngine:
        engine = WateringStationEngine(self.config(), controller)
        engine._monitor_ready.set()  # commands below model a completed HELLO session
        return engine

    def test_engine_state_path_is_configurable_by_cli_or_environment(self) -> None:
        arguments = ["--ws-id", "WS_001", "--kafka-host", "broker", "--kafka-port", "9092",
                     "--monitor-host", "monitor", "--monitor-port", "6001"]
        config = parse_args(arguments + ["--state-path", "/engine-state/custom.sqlite3"])
        self.assertEqual(config.state_path, "/engine-state/custom.sqlite3")
        with mock.patch.dict("os.environ", {"ENGINE_STATE_PATH": "/engine-state/from-env.sqlite3"}, clear=True):
            from_env = parse_args(arguments)
        self.assertEqual(from_env.state_path, "/engine-state/from-env.sqlite3")

    def test_engine_service_creates_parent_for_persistent_state_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "station-one" / "recovery.sqlite3"
            service = EngineKafkaService(self.engine(), FakePublisher(), path)  # type: ignore[arg-type]
            service.close()
            self.assertTrue(path.is_file())

    def test_dispatches_targeted_start_and_ignores_other_station(self) -> None:
        valve = SolenoidValve()
        engine = self.engine(IrrigationController("WS_001", valve, FlowMeter(6), .01))
        publisher = FakePublisher(); service = EngineKafkaService(engine, publisher)  # type: ignore[arg-type]
        service.handle_command(envelope("START_IRRIGATION", {"duration_seconds": .03}, ws_id="OTHER", operator_id="OP", request_id="other"))
        self.assertFalse(valve.is_open)
        service.handle_command(envelope("START_IRRIGATION", {"duration_seconds": .03}, ws_id="WS_001", operator_id="OP", request_id="r1"))
        self.assertTrue(engine.irrigation.wait_until_finished(1))
        kinds = [message["event_type"] for _, message in publisher.records]
        self.assertIn("IRRIGATION_STARTED", kinds); self.assertIn("IRRIGATION_COMPLETED", kinds)

    def test_kafka_command_is_dispatched_promptly_and_completes(self) -> None:
        valve = SolenoidValve()
        engine = self.engine(IrrigationController("WS_001", valve, FlowMeter(6), .005))
        publisher = FakePublisher()
        service = EngineKafkaService(engine, publisher)  # type: ignore[arg-type]
        command = envelope(
            "START_IRRIGATION", {"duration_seconds": .05}, ws_id="WS_001",
            operator_id="OP_001", request_id="prompt-command",
        )
        worker = KafkaWorker(
            "localhost", "engine-group", ["wm.ws.commands"], service.handle_bytes,
            OneMessageConsumer(encode(command)),
        )
        started = time.monotonic()
        self.assertTrue(worker.poll_once(.01))
        dispatch_elapsed = time.monotonic() - started
        self.assertLess(dispatch_elapsed, .5, "Engine command dispatch unexpectedly blocked")
        self.assertTrue(valve.is_open, "valve did not open during command dispatch")
        self.assertTrue(engine.irrigation.wait_until_finished(1))
        event_types = [message["event_type"] for _, message in publisher.records]
        self.assertIn("IRRIGATION_STARTED", event_types)
        self.assertIn("IRRIGATION_COMPLETED", event_types)

    def test_engine_worker_uses_stable_static_group_member_id(self) -> None:
        engine = self.engine()
        service = EngineKafkaService(engine, FakePublisher())  # type: ignore[arg-type]
        with mock.patch("common.kafka_transport.Consumer") as consumer_factory:
            service.worker("localhost")
        config = consumer_factory.call_args.args[0]
        self.assertEqual(config["group.id"], "wm-ws-engine-WS_001-v1")
        self.assertEqual(config["group.instance.id"], "wm-ws-engine-WS_001-instance")

    def test_block_closes_active_valve(self) -> None:
        valve = SolenoidValve(); engine = self.engine(IrrigationController("WS_001", valve, FlowMeter(6), .1))
        service = EngineKafkaService(engine, FakePublisher())  # type: ignore[arg-type]
        service.handle_command(envelope("START_IRRIGATION", {"duration_seconds": 1}, ws_id="WS_001", operator_id="OP", request_id="r1"))
        service.handle_command(envelope("BLOCK_WS", {}, ws_id="WS_001"))
        self.assertTrue(engine.irrigation.wait_until_finished(1)); self.assertFalse(valve.is_open); self.assertTrue(engine.blocked)

    def test_duplicate_start_delivery_does_not_start_a_second_cycle(self) -> None:
        engine = self.engine(IrrigationController("WS_001", SolenoidValve(), FlowMeter(6), .01))
        publisher = FakePublisher()
        service = EngineKafkaService(engine, publisher)  # type: ignore[arg-type]
        command = envelope("START_IRRIGATION", {"duration_seconds": .2}, ws_id="WS_001", operator_id="OP", request_id="duplicate-start")
        service.handle_command(command)
        service.handle_command(command)
        self.assertEqual(engine.irrigation.state.value, "WATERING")
        self.assertEqual(len([m for _, m in publisher.records if m["event_type"] == "IRRIGATION_STARTED"]), 1)
        engine.stop_irrigation()
        self.assertTrue(engine.irrigation.wait_until_finished(1))

    def test_target_volume_mode_completes_at_threshold(self) -> None:
        engine = self.engine(IrrigationController("WS_001", SolenoidValve(), FlowMeter(60), .1))
        publisher = FakePublisher()
        service = EngineKafkaService(engine, publisher)  # type: ignore[arg-type]
        service.handle_command(envelope("START_IRRIGATION", {"target_volume_liters": .15}, ws_id="WS_001", operator_id="OP", request_id="target-volume"))
        self.assertTrue(engine.irrigation.wait_until_finished(1))
        completed = [m for _, m in publisher.records if m["event_type"] == "IRRIGATION_COMPLETED"][0]
        self.assertEqual(completed["payload"]["end_reason"], "TARGET_VOLUME")
        self.assertEqual(completed["payload"]["target_volume_liters"], .15)
        self.assertGreaterEqual(completed["payload"]["accumulated_volume_liters"], .15)
        progress = [m for _, m in publisher.records if m["event_type"] == "IRRIGATION_TELEMETRY"]
        self.assertTrue(progress)
        self.assertTrue(all(m["payload"]["target_volume_liters"] == .15 for m in progress))

    def test_no_mode_runs_until_manual_stop(self) -> None:
        engine = self.engine(IrrigationController("WS_001", SolenoidValve(), FlowMeter(1), .01))
        publisher = FakePublisher()
        service = EngineKafkaService(engine, publisher)  # type: ignore[arg-type]
        service.handle_command(envelope("START_IRRIGATION", {}, ws_id="WS_001", operator_id="OP", request_id="manual-mode"))
        time.sleep(.04)
        self.assertEqual(engine.irrigation.state.value, "WATERING")
        service.handle_command(envelope("STOP_IRRIGATION", {}, ws_id="WS_001", request_id="manual-mode"))
        self.assertTrue(engine.irrigation.wait_until_finished(1))
        completed = [m for _, m in publisher.records if m["event_type"] == "IRRIGATION_COMPLETED"][0]
        self.assertEqual(completed["payload"]["end_reason"], "CENTRAL_STOP")

    def test_monitor_disconnect_completion_uses_contract_reason(self) -> None:
        engine = self.engine(IrrigationController("WS_001", SolenoidValve(), FlowMeter(6), .01))
        publisher = FakePublisher()
        service = EngineKafkaService(engine, publisher)  # type: ignore[arg-type]
        service.handle_command(envelope("START_IRRIGATION", {"duration_seconds": 1}, ws_id="WS_001", operator_id="OP", request_id="r1"))
        engine.irrigation.emergency_stop(reason="monitor_disconnected")
        self.assertTrue(engine.irrigation.wait_until_finished(1))
        completed = [m for _, m in publisher.records if m["event_type"] == "IRRIGATION_COMPLETED"]
        self.assertEqual(completed[0]["payload"]["end_reason"], "MONITOR_DISCONNECTED")

    def test_stop_irrigation_requires_matching_active_request_id(self) -> None:
        engine = self.engine(IrrigationController("WS_001", SolenoidValve(), FlowMeter(6), .05))
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
        engine = self.engine(IrrigationController("WS_001", SolenoidValve(), FlowMeter(6), .01))
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

    def test_start_is_rejected_while_monitor_is_unavailable(self) -> None:
        engine = WateringStationEngine(self.config())
        publisher = FakePublisher()
        service = EngineKafkaService(engine, publisher)  # type: ignore[arg-type]
        service.handle_command(envelope("START_IRRIGATION", {"duration_seconds": 1}, ws_id="WS_001", operator_id="OP", request_id="r1"))
        rejected = [message for _, message in publisher.records if message["event_type"] == "COMMAND_REJECTED"]
        self.assertEqual(rejected[0]["payload"]["reason"], "MONITOR_UNAVAILABLE")
        self.assertIn(rejected[0]["payload"]["reason"], COMMAND_REJECTION_REASONS)

    def test_final_event_stays_durable_after_publish_failure_and_retries(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "engine.db"
            publisher = FakePublisher()
            engine = self.engine(IrrigationController("WS_001", SolenoidValve(), FlowMeter(6), .005))
            service = EngineKafkaService(engine, publisher, path)  # type: ignore[arg-type]
            publisher.fail_event = "IRRIGATION_COMPLETED"
            service.handle_command(envelope("START_IRRIGATION", {"duration_seconds": .02}, ws_id="WS_001", operator_id="OP", request_id="durable-final"))
            self.assertTrue(engine.irrigation.wait_until_finished(1))
            pending = service._store.execute("SELECT message_id, message_json FROM engine_outbox").fetchone()
            self.assertIsNotNone(pending)
            message_id = pending[0]
            service.close()

            publisher.fail = False
            publisher.fail_event = None
            restarted = EngineKafkaService(self.engine(), publisher, path)  # type: ignore[arg-type]
            events = [m for _, m in publisher.records if m["event_type"] == "IRRIGATION_COMPLETED"]
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["message_id"], message_id)
            self.assertEqual(restarted._store.execute("SELECT COUNT(*) FROM engine_outbox").fetchone()[0], 0)

    def test_restart_closes_valve_and_reports_interrupted_result(self) -> None:
        import sqlite3
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "engine.db"
            original = EngineKafkaService(self.engine(), FakePublisher(), path)  # creates durable tables
            original.close()
            store = sqlite3.connect(path)
            store.execute("INSERT INTO engine_active VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ("WS_001", "interrupted", "OP", 20, None, 4.5, .45, "TIME_LIMIT"))
            store.commit(); store.close()
            valve = SolenoidValve(); valve.open()
            engine = self.engine(IrrigationController("WS_001", valve, FlowMeter(6), .01))
            publisher = FakePublisher()
            recovered = EngineKafkaService(engine, publisher, path)  # type: ignore[arg-type]
            self.assertFalse(valve.is_open)
            event = [m for _, m in publisher.records if m["event_type"] == "IRRIGATION_COMPLETED"][0]
            self.assertEqual(event["request_id"], "interrupted")
            self.assertEqual(event["payload"], {"end_reason": "ENGINE_FAULT", "duration_seconds": 4.5, "accumulated_volume_liters": .45})
            recovered.handle_command(envelope("START_IRRIGATION", {"duration_seconds": 20}, ws_id="WS_001", operator_id="OP", request_id="interrupted"))
            self.assertEqual(engine.irrigation.state.value, "IDLE")
            recovered.close()

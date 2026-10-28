from __future__ import annotations

import logging
import threading
import unittest
from unittest import mock

from common.kafka_contract import WS_COMMANDS, envelope
from WM_Central.kafka_service import CentralKafkaService
from WM_Central.monitoring import control_loop, execute_control_line, execute_menu_choice, format_station_table, select_station_ids, start_control_menu
from WM_Central.models import IrrigationStatus, StationStatus
from WM_Central.state_manager import StateManager

from .helpers import DatabaseTestCase


class Publisher:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict]] = []

    def publish(self, topic: str, message: dict, *, key: str | None = None) -> None:
        self.records.append((topic, message))


class ControlAndMonitoringTests(DatabaseTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.state = StateManager(self.db)
        self.state.register("WS_001", "North", object())
        self.state.register("WS_002", "South", object())
        self.publisher = Publisher()
        self.service = CentralKafkaService(self.db, self.state, self.publisher)  # type: ignore[arg-type]
        self.log = logging.getLogger("central-test")

    def test_station_selection_resolves_single_and_all(self) -> None:
        self.assertEqual(select_station_ids("WS_002", self.state), ["WS_002"])
        self.assertEqual(select_station_ids("all", self.state), ["WS_001", "WS_002"])
        self.assertEqual(select_station_ids("missing", self.state), [])

    def test_block_and_activate_target_one_or_all(self) -> None:
        execute_control_line("BLOCK WS_001", self.state, self.service, self.log)
        self.assertEqual(self.db.get_station("WS_001").admin_state.value, "BLOCKED")  # type: ignore[union-attr]
        self.assertEqual(self.db.get_station("WS_002").admin_state.value, "ACTIVE")  # type: ignore[union-attr]
        execute_control_line("ACTIVATE all", self.state, self.service, self.log)
        self.assertTrue(all(s.admin_state.value == "ACTIVE" for s in self.db.list_stations()))
        block_commands = [m for topic, m in self.publisher.records if topic == WS_COMMANDS and m["event_type"] == "BLOCK_WS"]
        activate_commands = [m for topic, m in self.publisher.records if topic == WS_COMMANDS and m["event_type"] == "ACTIVATE_WS"]
        self.assertEqual([m["ws_id"] for m in block_commands], ["WS_001"])
        self.assertEqual([m["ws_id"] for m in activate_commands], ["WS_001", "WS_002"])

    def test_start_uses_activation_validation_and_well_formed_engine_contract(self) -> None:
        execute_control_line("START WS_001 8 OP_001", self.state, self.service, self.log)
        command = next(m for topic, m in self.publisher.records if topic == WS_COMMANDS)
        record = self.db.get_irrigation(command["request_id"])
        self.assertEqual(record.status, IrrigationStatus.AUTHORIZED)  # type: ignore[union-attr]
        self.assertEqual(command["event_type"], "START_IRRIGATION")
        self.assertEqual(command["payload"], {"duration_seconds": 8})
        self.assertTrue(command["request_id"])
        self.assertEqual(command["operator_id"], "OP_001")

    def test_start_all_records_normal_denials_for_disconnected_stations(self) -> None:
        self.state.disconnect("WS_002", self.state._connections["WS_002"])
        execute_control_line("START all 4 OP_001", self.state, self.service, self.log)
        records = self.db.list_irrigations()
        self.assertEqual(len(records), 2)
        self.assertEqual(sum(record.status is IrrigationStatus.AUTHORIZED for record in records), 1)
        self.assertEqual(sum(record.status is IrrigationStatus.DENIED for record in records), 1)

    def test_invalid_start_input_does_not_publish(self) -> None:
        execute_control_line("START WS_001 -1", self.state, self.service, self.log)
        execute_control_line("START WS_001 1 UNKNOWN_OPERATOR", self.state, self.service, self.log)
        self.assertFalse(any(topic == WS_COMMANDS for topic, _ in self.publisher.records))

    def test_activate_refuses_disconnected_or_faulted_station(self) -> None:
        self.state.report_fault("WS_001", self.state._connections["WS_001"], "leak", "pipe")
        self.state.disconnect("WS_002", self.state._connections["WS_002"])
        execute_control_line("ACTIVATE all", self.state, self.service, self.log)
        self.assertFalse(any(topic == WS_COMMANDS and message["event_type"] == "ACTIVATE_WS"
                             for topic, message in self.publisher.records))

    def test_live_telemetry_fault_and_final_summary_are_displayed(self) -> None:
        request_id = self.service.start_irrigation("WS_001", 2, "OP_001")
        self.service.handle_engine_event(envelope("IRRIGATION_STARTED", {}, ws_id="WS_001", request_id=request_id))
        self.service.handle_engine_event(envelope("IRRIGATION_TELEMETRY", {
            "sequence": 1, "flow_rate_lpm": 5.5, "accumulated_volume_liters": 0.4, "duration_seconds": 1.0,
        }, ws_id="WS_001", request_id=request_id))
        table = format_station_table(self.state.snapshot())
        self.assertIn("WATERING", table)
        self.assertIn("5.5", table)
        self.assertIn("0.4", table)
        self.state.report_fault("WS_001", self.state._connections["WS_001"], "leak", "pipe")
        self.assertIn("LEAK", format_station_table(self.state.snapshot()))
        self.service.handle_engine_event(envelope("IRRIGATION_COMPLETED", {
            "end_reason": "LEAK_DETECTED", "duration_seconds": 1.2, "accumulated_volume_liters": 0.5,
        }, ws_id="WS_001", request_id=request_id))
        self.assertIn("0.5", format_station_table(self.state.snapshot()))

    def test_numbered_menu_dispatches_status_and_rejects_invalid_input(self) -> None:
        with self.assertLogs("central-test", level="ERROR") as captured:
            self.assertTrue(execute_menu_choice("8", self.state, self.service, self.log))
            self.assertTrue(execute_menu_choice("1", self.state, self.service, self.log))
        self.assertIn("Invalid menu choice", "\n".join(captured.output))

    def test_menu_station_and_all_station_controls(self) -> None:
        execute_menu_choice("4", self.state, self.service, self.log, input_fn=lambda _: "WS_001")
        self.assertEqual(self.db.get_station("WS_001").admin_state.value, "BLOCKED")  # type: ignore[union-attr]
        execute_menu_choice("5", self.state, self.service, self.log)
        self.assertTrue(all(s.admin_state.value == "BLOCKED" for s in self.db.list_stations()))
        execute_menu_choice("6", self.state, self.service, self.log, input_fn=lambda _: "WS_001")
        execute_menu_choice("7", self.state, self.service, self.log)
        activate_commands = [m for topic, m in self.publisher.records if topic == WS_COMMANDS and m["event_type"] == "ACTIVATE_WS"]
        self.assertEqual([m["ws_id"] for m in activate_commands], ["WS_001", "WS_001", "WS_002"])

    def test_menu_invalid_station_and_start_duration_are_visible(self) -> None:
        with self.assertLogs("central-test", level="ERROR") as captured:
            execute_menu_choice("6", self.state, self.service, self.log, input_fn=lambda _: "WS_MISSING")
            values = iter(["WS_001", "0"])
            execute_menu_choice("2", self.state, self.service, self.log, input_fn=lambda _: next(values))
        output = "\n".join(captured.output)
        self.assertIn("Invalid station ID", output)
        self.assertIn("positive whole number", output)
        self.assertFalse(self.publisher.records)

    def test_start_all_in_menu_limits_work_to_eligible_stations(self) -> None:
        self.state.disconnect("WS_002", self.state._connections["WS_002"])
        execute_menu_choice("3", self.state, self.service, self.log, input_fn=lambda _: "5")
        commands = [m for topic, m in self.publisher.records if topic == WS_COMMANDS and m["event_type"] == "START_IRRIGATION"]
        self.assertEqual([m["ws_id"] for m in commands], ["WS_001"])

    def test_headless_startup_does_not_start_menu_or_stop_central(self) -> None:
        stop = threading.Event()
        with mock.patch("WM_Central.monitoring.sys.stdin") as stdin:
            stdin.isatty.return_value = False
            thread = start_control_menu(self.state, self.service, stop)
        self.assertIsNone(thread)
        self.assertFalse(stop.is_set())

    def test_blocked_menu_input_does_not_block_other_workers(self) -> None:
        entered, release, worker_finished = threading.Event(), threading.Event(), threading.Event()
        stop = threading.Event()
        with mock.patch("WM_Central.monitoring.sys.stdin") as stdin, mock.patch(
            "WM_Central.monitoring.input", side_effect=lambda _prompt: (entered.set(), release.wait(2), "0")[2]
        ):
            stdin.isatty.return_value = True
            menu = threading.Thread(target=lambda: control_loop(self.state, self.service, stop))
            menu.start()
            self.assertTrue(entered.wait(1))
            worker = threading.Thread(target=worker_finished.set)
            worker.start()
            self.assertTrue(worker_finished.wait(1))
            release.set()
            menu.join(1)
        self.assertFalse(stop.is_set())

    def test_unexpected_menu_failure_is_logged_and_menu_remains_alive(self) -> None:
        failing_service = mock.Mock()
        failing_service.issue_station_command.side_effect = RuntimeError("broker unavailable")
        values = iter(["4", "WS_001", "0"])
        stop = threading.Event()
        with mock.patch("WM_Central.monitoring.sys.stdin") as stdin, mock.patch(
            "WM_Central.monitoring.input", side_effect=lambda _prompt: next(values)
        ), self.assertLogs("wm_central.control", level="ERROR") as captured:
            stdin.isatty.return_value = True
            control_loop(self.state, failing_service, stop)
        self.assertIn("Central menu action '4' failed", "\n".join(captured.output))
        self.assertFalse(stop.is_set())

if __name__ == "__main__":
    unittest.main()

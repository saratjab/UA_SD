from __future__ import annotations

import logging
import unittest

from common.kafka_contract import WS_COMMANDS, envelope
from WM_Central.kafka_service import CentralKafkaService
from WM_Central.monitoring import execute_control_line, format_station_table, select_station_ids
from WM_Central.models import IrrigationStatus
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

if __name__ == "__main__":
    unittest.main()

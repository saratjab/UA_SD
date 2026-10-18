from __future__ import annotations

import unittest

from common.kafka_contract import decode, envelope
from WM_Central.kafka_service import CentralKafkaService
from WM_Central.models import DenyReason, IrrigationStatus
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

    def test_completion_after_central_restart_reaches_operator_once_with_final_measurements(self) -> None:
        request_id = "central-restart-request"
        self.service.handle_activation(self.request(request_id, duration=30))
        self.service.handle_engine_event(envelope(
            "IRRIGATION_STARTED", {}, ws_id="WS_001", request_id=request_id,
        ))
        for sequence in (1, 2):
            self.service.handle_engine_event(envelope(
                "IRRIGATION_TELEMETRY",
                {"sequence": sequence, "flow_rate_lpm": 6.0,
                 "accumulated_volume_liters": sequence / 10, "duration_seconds": float(sequence)},
                ws_id="WS_001", request_id=request_id,
            ))

        # This is CENTRAL's startup recovery followed by service recreation.
        # A WATERING record must survive so the retained Engine completion can
        # perform the terminal transition after the event consumer restarts.
        self.assertEqual(self.db.recover_open_irrigations(), 0)
        restarted_state = StateManager(self.db)
        restarted_state.register("WS_001", "North", object())
        restarted_service = CentralKafkaService(self.db, restarted_state, self.publisher)  # type: ignore[arg-type]
        restarted_service.handle_engine_event(envelope(
            "IRRIGATION_TELEMETRY",
            {"sequence": 3, "flow_rate_lpm": 6.0,
             "accumulated_volume_liters": 0.3, "duration_seconds": 3.0},
            ws_id="WS_001", request_id=request_id,
        ))
        completion = envelope(
            "IRRIGATION_COMPLETED",
            {"end_reason": "TIME_LIMIT", "duration_seconds": 30.0,
             "accumulated_volume_liters": 3.0},
            ws_id="WS_001", request_id=request_id,
        )
        restarted_service.handle_engine_event(completion)
        restarted_service.handle_engine_event(completion)  # uncommitted source event replay

        record = self.db.get_irrigation(request_id)
        self.assertEqual(record.status, IrrigationStatus.ENDED)  # type: ignore[union-attr]
        self.assertEqual(record.end_reason.value, "DURATION")  # type: ignore[union-attr]
        self.assertEqual((record.duration_s, record.total_volume_l), (30.0, 3.0))  # type: ignore[union-attr]
        completions = [
            message for topic, message in self.publisher.records
            if topic == "wm.operator.events" and message["event_type"] == "IRRIGATION_COMPLETED"
            and message.get("request_id") == request_id
        ]
        self.assertEqual(len(completions), 1)
        self.assertEqual(completions[0]["operator_id"], "OP_001")
        self.assertEqual(completions[0]["payload"]["accumulated_volume_liters"], 3.0)

    def test_malformed_payload_is_consumed_without_crashing_partition(self) -> None:
        self.assertTrue(self.service.handle_activation_bytes(b"not json"))

    def test_monitor_disconnect_completion_records_disconnected_reason_and_measurements(self) -> None:
        self.service.handle_activation(self.request())
        self.service.handle_engine_event(envelope(
            "IRRIGATION_COMPLETED",
            {"end_reason": "MONITOR_DISCONNECTED", "duration_seconds": 2.0, "accumulated_volume_liters": 0.2},
            ws_id="WS_001", request_id="request-1",
        ))
        record = self.db.get_irrigation("request-1")
        self.assertEqual(record.end_reason.value, "DISCONNECTED")  # type: ignore[union-attr]
        self.assertEqual((record.duration_s, record.total_volume_l), (2.0, 0.2))  # type: ignore[union-attr]

    def test_fault_terminal_state_accepts_late_final_measurements_and_engine_end_time(self) -> None:
        self.service.handle_activation(self.request())
        self.service.handle_engine_event(envelope("IRRIGATION_STARTED", {}, ws_id="WS_001", request_id="request-1"))
        self.service.handle_engine_event(envelope(
            "IRRIGATION_TELEMETRY", {"sequence": 1, "flow_rate_lpm": 6.0, "accumulated_volume_liters": 0.1, "duration_seconds": 1.0},
            ws_id="WS_001", request_id="request-1",
        ))
        fault = self.state.report_fault("WS_001", self.connection, "monitor_disconnect")
        self.assertEqual(fault.ended_request_id, "request-1")
        completion = envelope("IRRIGATION_COMPLETED", {
            "end_reason": "ENGINE_FAULT", "duration_seconds": 1.2, "accumulated_volume_liters": 0.12,
        }, ws_id="WS_001", request_id="request-1")
        completion["timestamp"] = "2026-10-15T12:00:00Z"
        inferred_end = self.db.get_irrigation("request-1").ended_at  # type: ignore[union-attr]
        self.service.handle_engine_event(completion)
        self.service.handle_engine_event(completion)
        record = self.db.get_irrigation("request-1")
        self.assertEqual(record.end_reason.value, "LEAK")  # type: ignore[union-attr]
        self.assertEqual(record.ended_at, completion["timestamp"])  # type: ignore[union-attr]
        self.assertNotEqual(record.ended_at, inferred_end)  # type: ignore[union-attr]
        self.assertEqual((record.duration_s, record.total_volume_l), (1.2, 0.12))  # type: ignore[union-attr]

    def test_engine_crash_fault_without_completion_does_not_fabricate_measurements(self) -> None:
        self.service.handle_activation(self.request())
        self.service.handle_engine_event(envelope("IRRIGATION_STARTED", {}, ws_id="WS_001", request_id="request-1"))
        self.state.report_fault("WS_001", self.connection, "engine_crash")
        record = self.db.get_irrigation("request-1")
        self.assertEqual(record.end_reason.value, "LEAK")  # type: ignore[union-attr]
        self.assertIsNone(record.duration_s)  # type: ignore[union-attr]
        self.assertIsNone(record.total_volume_l)  # type: ignore[union-attr]

    def test_engine_blocked_rejection_is_recorded_and_notified_with_same_reason(self) -> None:
        self.service.handle_activation(self.request())
        self.service.handle_engine_event(envelope("COMMAND_REJECTED", {"reason": "ENGINE_BLOCKED"},
                                                  ws_id="WS_001", request_id="request-1"))
        record = self.db.get_irrigation("request-1")
        self.assertEqual(record.status, IrrigationStatus.DENIED)  # type: ignore[union-attr]
        self.assertEqual(record.deny_reason.value, "ENGINE_BLOCKED")  # type: ignore[union-attr]
        notifications = [m for topic, m in self.publisher.records if topic == "wm.operator.events" and m["event_type"] == "COMMAND_REJECTED"]
        self.assertEqual(notifications[-1]["payload"]["reason"], record.deny_reason.value)  # type: ignore[union-attr]

    def test_engine_failed_rejection_is_not_recorded_as_manual_stop(self) -> None:
        self.service.handle_activation(self.request())
        self.service.handle_engine_event(envelope("COMMAND_REJECTED", {"reason": "ENGINE_FAILED"},
                                                  ws_id="WS_001", request_id="request-1"))
        record = self.db.get_irrigation("request-1")
        self.assertEqual(record.status, IrrigationStatus.DENIED)  # type: ignore[union-attr]
        self.assertEqual(record.deny_reason.value, "ENGINE_FAILED")  # type: ignore[union-attr]

    def test_monitor_unavailable_rejection_is_valid_and_persisted(self) -> None:
        self.service.handle_activation(self.request())
        self.service.handle_engine_event(envelope(
            "COMMAND_REJECTED", {"reason": "MONITOR_UNAVAILABLE"},
            ws_id="WS_001", request_id="request-1",
        ))
        record = self.db.get_irrigation("request-1")
        self.assertEqual(record.status, IrrigationStatus.DENIED)  # type: ignore[union-attr]
        self.assertEqual(record.deny_reason, DenyReason.MONITOR_UNAVAILABLE)  # type: ignore[union-attr]


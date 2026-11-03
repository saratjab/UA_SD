from __future__ import annotations

import unittest

from WM_Central.models import (
    AdminState,
    EndReason,
    IrrigationStatus,
    StationStatus,
)
from WM_Central.state_manager import StateManager, effective_status

from .helpers import DatabaseTestCase

A = StationStatus
ACTIVE, BLOCKED = AdminState.ACTIVE, AdminState.BLOCKED
AUTHORIZED, WATERING = IrrigationStatus.AUTHORIZED, IrrigationStatus.WATERING


class EffectiveStatusTests(unittest.TestCase):
    """The precedence table from ADR-001, one row per rule."""

    CASES = [
        (False, ACTIVE,  False, None,       A.DISCONNECTED),
        (False, BLOCKED, True,  WATERING,   A.DISCONNECTED),
        (True,  ACTIVE,  True,  None,       A.LEAK),
        (True,  BLOCKED, True,  None,       A.LEAK),
        (True,  ACTIVE,  True,  WATERING,   A.LEAK),
        (True,  BLOCKED, False, None,       A.OUT_OF_SERVICE),
        (True,  ACTIVE,  False, WATERING,   A.WATERING),
        (True,  ACTIVE,  False, AUTHORIZED, A.AVAILABLE),
        (True,  ACTIVE,  False, None,       A.AVAILABLE),
    ]

    def test_precedence(self) -> None:
        for connected, admin, fault, irrigation, expected in self.CASES:
            with self.subTest(connected=connected, admin=admin, fault=fault, irrigation=irrigation):
                self.assertEqual(
                    effective_status(
                        connected=connected,
                        admin_state=admin,
                        has_open_fault=fault,
                        irrigation_status=irrigation,
                    ),
                    expected,
                )


class StateManagerTests(DatabaseTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.state = StateManager(self.db)
        self.conn = object()

    def register(self, ws_id: str = "WS_001", location: str | None = "North Garden", conn=None):
        return self.state.register(ws_id, location, conn or self.conn)

    def start_irrigation(self, request_id: str = "r1", operator: str = "OP_001") -> None:
        self.db.record_irrigation_request(request_id, "WS_001", operator, 60)
        self.db.mark_irrigation_started(request_id)

    def test_registered_station_is_available(self) -> None:
        outcome = self.register()
        self.assertTrue(outcome.ok and outcome.enrolled)
        self.assertEqual(self.state.status_of("WS_001"), A.AVAILABLE)

    def test_rejected_registration_leaves_no_trace(self) -> None:
        outcome = self.register(location=None)
        self.assertEqual((outcome.ok, outcome.reason), (False, "missing_location"))
        self.assertEqual(self.state.snapshot(), [])

    def test_unknown_station_has_no_status(self) -> None:
        self.assertIsNone(self.state.status_of("NOPE"))

    def test_second_registration_cannot_take_over_a_connected_station(self) -> None:
        self.register()
        intruder = object()
        outcome = self.register(conn=intruder)
        self.assertEqual((outcome.ok, outcome.reason), (False, "already_connected"))
        self.assertTrue(self.state.is_current("WS_001", self.conn))
        self.assertFalse(self.state.is_current("WS_001", intruder))

    def test_rejected_connection_cannot_disconnect_the_station(self) -> None:
        self.register()
        intruder = object()
        self.register(conn=intruder)
        self.assertFalse(self.state.disconnect("WS_001", intruder).was_current)
        self.assertEqual(self.state.status_of("WS_001"), A.AVAILABLE)

    def test_station_may_register_again_after_its_connection_closed(self) -> None:
        self.register()
        self.state.disconnect("WS_001", self.conn)
        newer = object()
        self.assertTrue(self.register(conn=newer).ok)
        self.assertTrue(self.state.is_current("WS_001", newer))

    def test_disconnect_marks_disconnected(self) -> None:
        self.register()
        self.assertTrue(self.state.disconnect("WS_001", self.conn).was_current)
        self.assertEqual(self.state.status_of("WS_001"), A.DISCONNECTED)

    def test_disconnect_ends_live_irrigation(self) -> None:
        self.register()
        self.start_irrigation()
        outcome = self.state.disconnect("WS_001", self.conn)
        self.assertEqual(outcome.ended_request_id, "r1")
        self.assertEqual(self.db.get_irrigation("r1").end_reason, EndReason.DISCONNECTED)

    def test_watering_station_shows_operator(self) -> None:
        self.register()
        self.start_irrigation()
        view = self.state.snapshot()[0]
        self.assertEqual((view.status, view.operator_id), (A.WATERING, "OP_001"))


    def test_fault_makes_station_leak(self) -> None:
        self.register()
        outcome = self.state.report_fault("WS_001", self.conn, "health_ko", "KO")
        self.assertTrue(outcome.ok and outcome.is_new)
        self.assertEqual(self.state.status_of("WS_001"), A.LEAK)
        self.assertIn("health_ko", self.state.snapshot()[0].fault)

    def test_fault_during_watering_ends_irrigation_immediately(self) -> None:
        self.register()
        self.start_irrigation()
        outcome = self.state.report_fault("WS_001", self.conn, "timeout")
        self.assertEqual(outcome.ended_request_id, "r1")
        self.assertEqual(self.db.get_irrigation("r1").end_reason, EndReason.LEAK)
        self.assertEqual(self.state.status_of("WS_001"), A.LEAK)

    def test_repeated_fault_is_acknowledged_but_not_new(self) -> None:
        self.register()
        self.state.report_fault("WS_001", self.conn, "health_ko")
        again = self.state.report_fault("WS_001", self.conn, "timeout")
        self.assertTrue(again.ok)
        self.assertFalse(again.is_new)

    def test_fault_from_a_stale_connection_is_refused(self) -> None:
        self.register()
        self.state.disconnect("WS_001", self.conn)
        self.register(conn=object())
        outcome = self.state.report_fault("WS_001", self.conn, "health_ko")
        self.assertEqual((outcome.ok, outcome.reason), (False, "not_registered"))
        self.assertEqual(self.state.status_of("WS_001"), A.AVAILABLE)

    def test_resolving_the_fault_restores_availability(self) -> None:
        self.register()
        self.state.report_fault("WS_001", self.conn, "health_ko")
        self.assertTrue(self.state.resolve_fault("WS_001", self.conn))
        self.assertEqual(self.state.status_of("WS_001"), A.AVAILABLE)
        self.assertFalse(self.state.resolve_fault("WS_001"))

    def test_leak_survives_a_disconnect(self) -> None:
        self.register()
        self.state.report_fault("WS_001", self.conn, "health_ko")
        self.state.disconnect("WS_001", self.conn)
        self.assertEqual(self.state.status_of("WS_001"), A.DISCONNECTED)
        self.register()
        self.assertEqual(self.state.status_of("WS_001"), A.LEAK)

    
    def test_block_ends_irrigation_and_marks_out_of_service(self) -> None:
        self.register()
        self.start_irrigation()
        outcome = self.state.block_station("WS_001")
        self.assertEqual(outcome.ended_request_id, "r1")
        self.assertEqual(self.db.get_irrigation("r1").end_reason, EndReason.BLOCKED)
        self.assertEqual(self.state.status_of("WS_001"), A.OUT_OF_SERVICE)

    def test_activate_returns_to_available(self) -> None:
        self.register()
        self.state.block_station("WS_001")
        self.assertTrue(self.state.activate_station("WS_001").ok)
        self.assertEqual(self.state.status_of("WS_001"), A.AVAILABLE)

    def test_commands_on_unknown_station_fail(self) -> None:
        self.assertFalse(self.state.block_station("NOPE").ok)
        self.assertFalse(self.state.activate_station("NOPE").ok)

    
    def test_after_restart_everything_is_disconnected_but_blocks_persist(self) -> None:
        self.register()
        self.register("WS_002", "East")
        self.state.block_station("WS_001")
        self.start_irrigation_on_ws2()

        self.db.recover_open_irrigations()
        restarted = StateManager(self.db)

        self.assertEqual(
            {view.ws_id: view.status for view in restarted.snapshot()},
            {"WS_001": A.DISCONNECTED, "WS_002": A.DISCONNECTED},
        )
        restarted.register("WS_001", None, object())
        restarted.register("WS_002", None, object())
        statuses = {view.ws_id: view.status for view in restarted.snapshot()}
        self.assertEqual(statuses, {"WS_001": A.OUT_OF_SERVICE, "WS_002": A.WATERING})

    def start_irrigation_on_ws2(self) -> None:
        self.db.record_irrigation_request("r-ws2", "WS_002", "OP_002", 60)
        self.db.mark_irrigation_started("r-ws2")


if __name__ == "__main__":
    unittest.main()

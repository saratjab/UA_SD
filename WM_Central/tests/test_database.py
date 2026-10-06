from __future__ import annotations

import sqlite3
import threading
import unittest

from WM_Central.models import (
    AdminState,
    DenyReason,
    EndReason,
    IrrigationStatus,
)

from .helpers import DatabaseTestCase


class SchemaTests(DatabaseTestCase):
    def test_initialize_is_idempotent(self) -> None:
        self.db.initialize()
        self.db.initialize()

    def test_foreign_keys_are_enforced(self) -> None:
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.open_fault("NO_SUCH_WS", "health_ko")

    def test_seed_file_loads_operators(self) -> None:
        self.db.load_seed_file()
        ids = [op.operator_id for op in self.db.list_operators()]
        self.assertIn("OP_003", ids)

    def test_recover_with_nothing_open_returns_zero(self) -> None:
        self.assertEqual(self.db.recover_open_irrigations(), 0)

    def test_use_after_close_fails_loudly(self) -> None:
        self.db.close()
        with self.assertRaises(RuntimeError):
            self.db.list_stations()


class OperatorTests(DatabaseTestCase):
    def test_add_get_and_upsert(self) -> None:
        self.db.add_operator("OP_900", "Ana", active=True)
        self.assertEqual(self.db.get_operator("OP_900").name, "Ana")
        self.db.add_operator("OP_900", "Ana B", active=False)
        operator = self.db.get_operator("OP_900")
        self.assertEqual((operator.name, operator.active), ("Ana B", False))

    def test_unknown_operator_is_none(self) -> None:
        self.assertIsNone(self.db.get_operator("NOPE"))


class StationTests(DatabaseTestCase):
    def test_enroll_new_station(self) -> None:
        result = self.db.register_station("WS_001", "North Garden")
        self.assertTrue(result.ok and result.enrolled)
        self.assertEqual(result.station.admin_state, AdminState.ACTIVE)
        self.assertEqual(result.station.location, "North Garden")

    def test_new_station_without_location_is_rejected(self) -> None:
        for location in (None, "", "   "):
            with self.subTest(location=location):
                result = self.db.register_station("WS_001", location)
                self.assertFalse(result.ok)
                self.assertEqual(result.reason, "missing_location")
        self.assertIsNone(self.db.get_station("WS_001"))

    def test_invalid_ids_are_rejected(self) -> None:
        for ws_id in ("", "   ", "X" * 65):
            with self.subTest(ws_id=ws_id):
                self.assertEqual(self.db.register_station(ws_id, "Park").reason, "invalid_ws_id")

    def test_known_station_authenticates_without_location(self) -> None:
        self.enroll()
        result = self.db.register_station("WS_001", None)
        self.assertTrue(result.ok)
        self.assertFalse(result.enrolled)
        self.assertEqual(result.station.location, "North Garden")

    def test_location_is_refreshed_when_it_changes(self) -> None:
        self.enroll()
        result = self.db.register_station("WS_001", "South Park")
        self.assertEqual(result.station.location, "South Park")

    def test_ids_are_trimmed(self) -> None:
        self.db.register_station("  WS_001 ", " Rose Garden ")
        self.assertEqual(self.db.get_station("WS_001").location, "Rose Garden")

    def test_block_and_activate(self) -> None:
        self.enroll()
        self.assertTrue(self.db.set_admin_state("WS_001", AdminState.BLOCKED))
        self.assertEqual(self.db.get_station("WS_001").admin_state, AdminState.BLOCKED)
        self.assertTrue(self.db.set_admin_state("WS_001", AdminState.ACTIVE))

    def test_admin_state_of_unknown_station(self) -> None:
        self.assertFalse(self.db.set_admin_state("NOPE", AdminState.BLOCKED))

    def test_block_survives_reconnect(self) -> None:
        self.enroll()
        self.db.set_admin_state("WS_001", AdminState.BLOCKED)
        self.db.register_station("WS_001", None)
        self.assertEqual(self.db.get_station("WS_001").admin_state, AdminState.BLOCKED)


class FaultTests(DatabaseTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.enroll()

    def test_only_one_open_fault_per_station(self) -> None:
        self.assertTrue(self.db.open_fault("WS_001", "health_ko", "KO"))
        self.assertFalse(self.db.open_fault("WS_001", "timeout"))
        self.assertEqual(self.db.get_open_fault("WS_001").fault_type, "health_ko")

    def test_resolve_then_new_fault_is_allowed(self) -> None:
        self.db.open_fault("WS_001", "health_ko")
        self.assertTrue(self.db.resolve_fault("WS_001"))
        self.assertFalse(self.db.resolve_fault("WS_001"))
        self.assertIsNone(self.db.get_open_fault("WS_001"))
        self.assertTrue(self.db.open_fault("WS_001", "timeout"))

    def test_open_faults_map(self) -> None:
        self.enroll("WS_002", "East")
        self.db.open_fault("WS_002", "timeout")
        self.assertEqual(set(self.db.open_faults()), {"WS_002"})


class IrrigationTests(DatabaseTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.enroll()

    def request(self, request_id: str, operator: str | None = "OP_001", **kwargs):
        return self.db.record_irrigation_request(request_id, "WS_001", operator, 60, **kwargs)

    def test_request_is_authorized(self) -> None:
        record = self.request("r1")
        self.assertEqual(record.status, IrrigationStatus.AUTHORIZED)
        self.assertIsNone(record.deny_reason)

    def test_second_request_on_busy_station_is_denied(self) -> None:
        self.request("r1")
        second = self.request("r2", "OP_002")
        self.assertEqual(second.status, IrrigationStatus.DENIED)
        self.assertEqual(second.deny_reason, DenyReason.WS_BUSY)

    def test_replayed_request_id_is_idempotent(self) -> None:
        first = self.request("r1")
        again = self.request("r1")
        self.assertEqual(first, again)
        self.assertEqual(self.db.get_live_irrigation("WS_001").request_id, "r1")

    def test_explicit_denial_is_recorded(self) -> None:
        record = self.request("r1", deny_reason=DenyReason.WS_LEAK)
        self.assertEqual(record.status, IrrigationStatus.DENIED)
        self.assertIsNone(self.db.get_live_irrigation("WS_001"))

    def test_started_by_central_has_no_operator(self) -> None:
        self.assertIsNone(self.request("r1", operator=None).operator_id)

    def test_full_lifecycle(self) -> None:
        self.request("r1")
        self.assertTrue(self.db.mark_irrigation_started("r1"))
        self.assertFalse(self.db.mark_irrigation_started("r1"))  # already WATERING
        self.assertEqual(self.db.get_live_irrigation("WS_001").status, IrrigationStatus.WATERING)

        self.assertTrue(
            self.db.end_irrigation("r1", EndReason.DURATION, duration_s=60.0, total_volume_l=6.0)
        )
        record = self.db.get_irrigation("r1")
        self.assertEqual(record.status, IrrigationStatus.ENDED)
        self.assertEqual((record.end_reason, record.total_volume_l), (EndReason.DURATION, 6.0))
        self.assertIsNone(self.db.get_live_irrigation("WS_001"))
        self.assertFalse(self.db.end_irrigation("r1", EndReason.MANUAL))  # idempotent
        self.assertEqual(self.db.get_irrigation("r1").end_reason, EndReason.DURATION)

    def test_station_is_free_again_after_the_end(self) -> None:
        self.request("r1")
        self.db.end_irrigation("r1", EndReason.MANUAL)
        self.assertEqual(self.request("r2").status, IrrigationStatus.AUTHORIZED)

    def test_end_live_irrigation_for_station(self) -> None:
        self.request("r1")
        ended = self.db.end_live_irrigation("WS_001", EndReason.BLOCKED)
        self.assertEqual((ended.request_id, ended.end_reason), ("r1", EndReason.BLOCKED))
        self.assertIsNone(self.db.end_live_irrigation("WS_001", EndReason.BLOCKED))

    def test_startup_recovery_closes_live_irrigations(self) -> None:
        self.request("r1")
        self.assertEqual(self.db.recover_open_irrigations(), 1)
        record = self.db.get_irrigation("r1")
        self.assertEqual(record.end_reason, EndReason.CENTRAL_RESTART)

    def test_unknown_operator_violates_foreign_key(self) -> None:
        with self.assertRaises(sqlite3.IntegrityError):
            self.request("r1", operator="OP_999")

    def test_concurrent_requests_authorize_exactly_one(self) -> None:
        """The race the ADR promises is safe: many threads, one winner."""
        results: list[IrrigationStatus] = []
        lock = threading.Lock()
        barrier = threading.Barrier(8)

        def worker(n: int) -> None:
            barrier.wait()
            record = self.request(f"race-{n}", "OP_001" if n % 2 else "OP_002")
            with lock:
                results.append(record.status)

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(results.count(IrrigationStatus.AUTHORIZED), 1)
        self.assertEqual(results.count(IrrigationStatus.DENIED), 7)


if __name__ == "__main__":
    unittest.main()

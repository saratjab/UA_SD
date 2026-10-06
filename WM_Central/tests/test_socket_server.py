"""Integration tests: a real TCP server on an ephemeral port, driven by raw
sockets that speak the shared protocol exactly like the Monitor does."""

from __future__ import annotations

import socket
import unittest

from common import protocol
from WM_Central.models import EndReason, StationStatus
from WM_Central.socket_server import CentralSocketServer
from WM_Central.state_manager import StateManager

from .helpers import DatabaseTestCase, wait_until

A = StationStatus
CLOSED = (protocol.ConnectionClosedError, ConnectionError)


class SocketServerTestCase(DatabaseTestCase):
    register_timeout = 5.0

    def setUp(self) -> None:
        super().setUp()
        self.state = StateManager(self.db)
        self.server = CentralSocketServer(
            "127.0.0.1", 0, self.state, register_timeout=self.register_timeout
        )
        self.server.start()
        self.addCleanup(self.server.stop)

    # --- helpers -----------------------------------------------------------

    def connect(self) -> socket.socket:
        sock = socket.create_connection(("127.0.0.1", self.server.port), timeout=3.0)
        self.addCleanup(sock.close)
        return sock

    @staticmethod
    def call(sock: socket.socket, message: dict) -> dict:
        protocol.send_message(sock, message)
        return protocol.receive_message(sock)

    def registered(self, ws_id: str = "WS_001", location: str = "North Garden") -> socket.socket:
        sock = self.connect()
        reply = self.call(sock, protocol.register_ws_message(ws_id, location))
        self.assertEqual(reply["type"], protocol.REGISTER_ACK)
        return sock

    def status(self, ws_id: str = "WS_001") -> StationStatus | None:
        return self.state.status_of(ws_id)

    def assert_closed(self, sock: socket.socket) -> None:
        with self.assertRaises(CLOSED):
            protocol.receive_message(sock)


class RegistrationTests(SocketServerTestCase):
    def test_new_station_enrolls_and_becomes_available(self) -> None:
        sock = self.connect()
        reply = self.call(sock, protocol.register_ws_message("WS_001", "North Garden"))
        self.assertEqual(reply, {"type": "REGISTER_ACK", "ws_id": "WS_001"})
        self.assertEqual(self.status(), A.AVAILABLE)
        self.assertEqual(self.db.get_station("WS_001").location, "North Garden")

    def test_new_station_without_location_is_rejected_and_closed(self) -> None:
        sock = self.connect()
        reply = self.call(sock, {"type": "REGISTER_WS", "ws_id": "WS_001"})  # old Monitor format
        self.assertEqual(reply, {"type": "REGISTER_NACK", "reason": "missing_location"})
        self.assert_closed(sock)
        self.assertIsNone(self.status())

    def test_known_station_may_register_without_location(self) -> None:
        self.enroll()
        sock = self.connect()
        reply = self.call(sock, {"type": "REGISTER_WS", "ws_id": "WS_001"})
        self.assertEqual(reply["type"], "REGISTER_ACK")
        self.assertEqual(self.status(), A.AVAILABLE)

    def test_invalid_field_types_are_rejected(self) -> None:
        sock = self.connect()
        reply = self.call(sock, {"type": "REGISTER_WS", "ws_id": 42, "location": "x"})
        self.assertEqual(reply["reason"], "invalid_ws_id")

    def test_registering_twice_on_one_socket(self) -> None:
        sock = self.registered()
        again = self.call(sock, protocol.register_ws_message("WS_001", "North Garden"))
        self.assertEqual(again["type"], "REGISTER_ACK")
        other = self.call(sock, protocol.register_ws_message("WS_002", "East"))
        self.assertEqual(other, {"type": "REGISTER_NACK", "reason": "already_registered"})
        self.assertEqual(self.status(), A.AVAILABLE)  # the connection is still fine

    def test_second_connection_replaces_the_first(self) -> None:
        first = self.registered()
        second = self.registered()
        self.assert_closed(first)
        # the old socket's cleanup must not disconnect the new registration
        self.assertFalse(wait_until(lambda: self.status() == A.DISCONNECTED, timeout=0.5))
        self.assertEqual(self.status(), A.AVAILABLE)
        reply = self.call(second, protocol.fault_resolved_message("WS_001", "t"))
        self.assertEqual(reply["type"], "FAULT_ACK")

    def test_silent_connection_is_closed_after_the_register_timeout(self) -> None:
        self.server.stop()
        server = CentralSocketServer("127.0.0.1", 0, self.state, register_timeout=0.3)
        server.start()
        self.addCleanup(server.stop)
        sock = socket.create_connection(("127.0.0.1", server.port), timeout=3.0)
        self.addCleanup(sock.close)
        self.assert_closed(sock)


class DisconnectTests(SocketServerTestCase):
    def test_closing_the_socket_marks_disconnected(self) -> None:
        sock = self.registered()
        sock.close()
        self.assertTrue(wait_until(lambda: self.status() == A.DISCONNECTED))

    def test_disconnect_ends_a_live_irrigation(self) -> None:
        sock = self.registered()
        self.db.record_irrigation_request("r1", "WS_001", "OP_001", 60)
        self.db.mark_irrigation_started("r1")
        sock.close()
        self.assertTrue(wait_until(lambda: self.status() == A.DISCONNECTED))
        self.assertEqual(self.db.get_irrigation("r1").end_reason, EndReason.DISCONNECTED)

    def test_stopping_the_server_disconnects_everyone(self) -> None:
        self.registered()
        self.server.stop()
        self.assertTrue(wait_until(lambda: self.status() == A.DISCONNECTED))


class FaultTests(SocketServerTestCase):
    def fault(self, ws_id: str = "WS_001", fault: str = "health_ko") -> dict:
        return protocol.fault_message(ws_id, fault, "KO from engine", "2026-10-05T12:00:00Z")

    def test_fault_makes_the_station_leak(self) -> None:
        sock = self.registered()
        reply = self.call(sock, self.fault())
        self.assertEqual(reply, {"type": "FAULT_ACK", "ws_id": "WS_001"})
        self.assertEqual(self.status(), A.LEAK)
        stored = self.db.get_open_fault("WS_001")
        self.assertEqual((stored.fault_type, stored.detected_at), ("health_ko", "2026-10-05T12:00:00Z"))

    def test_fault_ends_irrigation_in_progress(self) -> None:
        sock = self.registered()
        self.db.record_irrigation_request("r1", "WS_001", "OP_001", 60)
        self.db.mark_irrigation_started("r1")
        self.call(sock, self.fault())
        self.assertEqual(self.db.get_irrigation("r1").end_reason, EndReason.LEAK)

    def test_fault_before_registering_is_refused_and_closed(self) -> None:
        sock = self.connect()
        reply = self.call(sock, self.fault())
        self.assertEqual(reply, {"type": "FAULT_NACK", "reason": "not_registered"})
        self.assert_closed(sock)

    def test_fault_for_another_station_is_refused(self) -> None:
        sock = self.registered()
        reply = self.call(sock, self.fault(ws_id="WS_999"))
        self.assertEqual(reply, {"type": "FAULT_NACK", "reason": "ws_id_mismatch"})
        self.assertEqual(self.status(), A.AVAILABLE)

    def test_fault_without_a_name_is_refused(self) -> None:
        sock = self.registered()
        reply = self.call(sock, {"type": "WS_E_FAULT", "ws_id": "WS_001"})
        self.assertEqual(reply, {"type": "FAULT_NACK", "reason": "invalid_fault"})

    def test_repeated_fault_is_still_acknowledged(self) -> None:
        sock = self.registered()
        self.call(sock, self.fault())
        self.assertEqual(self.call(sock, self.fault(fault="timeout"))["type"], "FAULT_ACK")

    def test_fault_resolved_restores_availability(self) -> None:
        sock = self.registered()
        self.call(sock, self.fault())
        reply = self.call(sock, protocol.fault_resolved_message("WS_001", "t"))
        self.assertEqual(reply["type"], "FAULT_ACK")
        self.assertEqual(self.status(), A.AVAILABLE)


class BadInputTests(SocketServerTestCase):
    @staticmethod
    def bad_lrc_frame() -> bytes:
        data = b'{"type":"WS_E_FAULT"}'
        return bytes([protocol.STX]) + data + bytes([protocol.ETX, protocol.calculate_lrc(data) ^ 0xFF])

    def test_bad_frame_from_a_registered_station_is_nacked_and_tolerated(self) -> None:
        sock = self.registered()
        sock.sendall(self.bad_lrc_frame())
        self.assertEqual(protocol.receive_message(sock), {"type": "NACK", "reason": "invalid_lrc"})
        # the connection survives: the next valid message works
        reply = self.call(sock, protocol.fault_message("WS_001", "timeout", "d", "t"))
        self.assertEqual(reply["type"], "FAULT_ACK")

    def test_bad_frame_before_registration_closes_the_connection(self) -> None:
        sock = self.connect()
        sock.sendall(self.bad_lrc_frame())
        self.assertEqual(protocol.receive_message(sock)["reason"], "invalid_lrc")
        self.assert_closed(sock)

    def test_non_json_payload_is_malformed(self) -> None:
        sock = self.registered()
        data = b"not json"
        sock.sendall(bytes([protocol.STX]) + data + bytes([protocol.ETX, protocol.calculate_lrc(data)]))
        self.assertEqual(protocol.receive_message(sock)["reason"], "malformed_frame")

    def test_unknown_message_type_from_a_registered_station(self) -> None:
        sock = self.registered()
        self.assertEqual(
            self.call(sock, {"type": "SELF_DESTRUCT"}), {"type": "NACK", "reason": "unknown_type"}
        )
        self.assertEqual(self.status(), A.AVAILABLE)

    def test_unknown_message_type_before_registration_closes(self) -> None:
        sock = self.connect()
        self.assertEqual(self.call(sock, {"type": "HELLO"})["reason"], "unknown_type")
        self.assert_closed(sock)


class ManyStationsTests(SocketServerTestCase):
    def test_independent_stations_do_not_interfere(self) -> None:
        socks = {f"WS_{n:03d}": self.registered(f"WS_{n:03d}", f"Park {n}") for n in range(1, 6)}
        self.call(socks["WS_002"], protocol.fault_message("WS_002", "health_ko", "d", "t"))
        socks["WS_004"].close()
        self.assertTrue(wait_until(lambda: self.status("WS_004") == A.DISCONNECTED))
        self.assertEqual(
            {view.ws_id: view.status for view in self.state.snapshot()},
            {
                "WS_001": A.AVAILABLE,
                "WS_002": A.LEAK,
                "WS_003": A.AVAILABLE,
                "WS_004": A.DISCONNECTED,
                "WS_005": A.AVAILABLE,
            },
        )


if __name__ == "__main__":
    unittest.main()

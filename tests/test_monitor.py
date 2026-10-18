from __future__ import annotations

import socket
import threading
import unittest
from unittest.mock import patch

from common import protocol
from WM_WS.WM_WS_M.config import MonitorConfig, parse_args
from WM_WS.WM_WS_M.monitor import WateringStationMonitor


class FakeCentralClient:
    def __init__(self) -> None:
        self.faults: list[tuple[str, str, str]] = []
        self.resolutions: list[str] = []
        self.connected = False
        self.connect_calls = 0
        self.register_calls = 0

    def connect(self) -> None:
        self.connected = True
        self.connect_calls += 1

    def close(self) -> None:
        self.connected = False

    def check_connection(self) -> bool:
        if not self.connected:
            raise RuntimeError("Central client is not connected")
        return True

    def register(self) -> bool:
        self.register_calls += 1
        return True

    def report_fault(self, fault: str, details: str, timestamp: str) -> bool:
        self.faults.append((fault, details, timestamp))
        return True

    def report_fault_resolved(self, timestamp: str) -> bool:
        self.resolutions.append(timestamp)
        return True


class MonitorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.central = FakeCentralClient()
        self.monitor = WateringStationMonitor(
            config=MonitorConfig(
                ws_id="WS_001",
                central_host="127.0.0.1",
                central_port=5000,
                engine_host="127.0.0.1",
                engine_port=6000,
                engine_hello_timeout=0.05,
                health_interval=0.01,
                health_timeout=0.05,
            ),
            central_client=self.central,  # type: ignore[arg-type]
            engine_server=object(),  # type: ignore[arg-type]
        )

    def test_health_ok(self) -> None:
        monitor_sock, engine_sock = socket.socketpair()
        try:
            response = self._answer_one_health_check_in_thread(engine_sock, "HEALTH_OK")
            self.assertTrue(self.monitor.perform_health_check(monitor_sock, 1))
            response["thread"].join(timeout=2)
            self.assertEqual(response["type"], "HEALTH_CHECK")
            self.assertEqual(self.central.faults, [])
        finally:
            monitor_sock.close()
            engine_sock.close()

    def test_health_ko(self) -> None:
        monitor_sock, engine_sock = socket.socketpair()
        try:
            response = self._answer_one_health_check_in_thread(
                engine_sock,
                "HEALTH_KO",
                reason="sensor_error",
            )
            self.assertFalse(self.monitor.perform_health_check(monitor_sock, 1))
            response["thread"].join(timeout=2)
            self.assertEqual(self.central.faults[0][0], "health_ko")
            self.assertTrue(self.monitor._last_health_connection_usable)
        finally:
            monitor_sock.close()
            engine_sock.close()

    def test_health_ok_resolves_an_active_fault(self) -> None:
        self.monitor._fault_active = True
        monitor_sock, engine_sock = socket.socketpair()
        try:
            response = self._answer_one_health_check_in_thread(engine_sock, "HEALTH_OK")
            self.assertTrue(self.monitor.perform_health_check(monitor_sock, 1))
            response["thread"].join(timeout=2)  # type: ignore[union-attr]
            self.assertEqual(len(self.central.resolutions), 1)
            self.assertFalse(self.monitor._fault_active)
        finally:
            monitor_sock.close()
            engine_sock.close()

    def test_health_timeout(self) -> None:
        monitor_sock, engine_sock = socket.socketpair()
        try:
            self.assertFalse(self.monitor.perform_health_check(monitor_sock, 1))
            self.assertEqual(self.central.faults[0][0], "timeout")
        finally:
            monitor_sock.close()
            engine_sock.close()

    def test_engine_disconnection(self) -> None:
        monitor_sock, engine_sock = socket.socketpair()
        engine_sock.close()
        try:
            self.assertFalse(self.monitor.perform_health_check(monitor_sock, 1))
            self.assertEqual(self.central.faults[0][0], "connection_lost")
        finally:
            monitor_sock.close()

    def test_cli_defaults_health_interval_to_exactly_one_second(self) -> None:
        config = parse_args([
            "--ws-id", "WS_001", "--central-host", "127.0.0.1", "--central-port", "5000",
            "--engine-host", "127.0.0.1", "--engine-port", "6000",
        ])
        self.assertEqual(config.health_interval, 1.0)

    def test_monitor_engine_schedules_repeated_checks_at_configured_interval(self) -> None:
        self.monitor.config = parse_args([
            "--ws-id", "WS_001", "--central-host", "127.0.0.1", "--central-port", "5000",
            "--engine-host", "127.0.0.1", "--engine-port", "6000",
        ])
        monitor_sock, engine_sock = socket.socketpair()
        received_sequences: list[int] = []
        sleeps: list[float] = []

        class StopAfterChecks(Exception):
            pass

        def answer_checks() -> None:
            for _ in range(2):
                request = protocol.receive_message(engine_sock)
                received_sequences.append(request["sequence"])
                protocol.send_message(engine_sock, {
                    "type": "HEALTH_OK", "ws_id": "WS_001", "sequence": request["sequence"], "status": "OK",
                })

        responder = threading.Thread(target=answer_checks)
        responder.start()

        def stop_after_two(interval: float) -> None:
            sleeps.append(interval)
            if len(sleeps) == 2:
                raise StopAfterChecks

        try:
            with patch("WM_WS.WM_WS_M.monitor.time.sleep", side_effect=stop_after_two):
                with self.assertRaises(StopAfterChecks):
                    self.monitor.monitor_engine(monitor_sock)
            responder.join(timeout=1)
            self.assertFalse(responder.is_alive())
            self.assertEqual(received_sequences, [1, 2])
            self.assertEqual(sleeps, [1.0, 1.0])
            self.assertEqual(self.central.register_calls, 1)
            self.assertEqual(self.central.connect_calls, 1)
        finally:
            monitor_sock.close()
            engine_sock.close()

    def test_central_disconnect_causes_one_new_registration(self) -> None:
        self.assertTrue(self.monitor._ensure_central_registration())
        self.assertEqual(self.central.register_calls, 1)

        # Simulate Central closing the persistent session.
        self.central.connected = False
        self.assertTrue(self.monitor._ensure_central_registration())

        self.assertEqual(self.central.connect_calls, 2)
        self.assertEqual(self.central.register_calls, 2)

    def _answer_one_health_check_in_thread(
        self,
        engine_sock: socket.socket,
        response_type: str,
        reason: str | None = None,
    ) -> dict[str, object]:
        result: dict[str, object] = {}

        def answer() -> None:
            request = protocol.receive_message(engine_sock)
            response: dict[str, object] = {
                "type": response_type,
                "ws_id": "WS_001",
                "sequence": request["sequence"],
                "status": "OK" if response_type == "HEALTH_OK" else "KO",
            }
            if reason is not None:
                response["reason"] = reason
            protocol.send_message(engine_sock, response)
            result.update(request)

        thread = threading.Thread(target=answer)
        thread.start()
        result["thread"] = thread
        return result


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import socket
import threading
import unittest

from common import protocol
from WM_WS.WM_WS_M.config import MonitorConfig
from WM_WS.WM_WS_M.monitor import WateringStationMonitor


class FakeCentralClient:
    def __init__(self) -> None:
        self.faults: list[tuple[str, str, str]] = []
        self.resolutions: list[str] = []

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

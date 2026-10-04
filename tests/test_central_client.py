from __future__ import annotations

import socket
import threading
import unittest

from WM_WS.WM_WS_M import protocol
from WM_WS.WM_WS_M.central_client import CentralClient


class CentralClientTests(unittest.TestCase):
    def test_register_ack(self) -> None:
        result, received = self._run_registration({"type": "REGISTER_ACK", "ws_id": "WS_001", "status": "OK"})
        self.assertTrue(result)
        self.assertEqual(received["type"], "REGISTER_WS")

    def test_register_nack(self) -> None:
        result, received = self._run_registration(
            {"type": "REGISTER_NACK", "ws_id": "WS_001", "reason": "unknown_ws"}
        )
        self.assertFalse(result)
        self.assertEqual(received["ws_id"], "WS_001")

    def test_fault_notification(self) -> None:
        server_sock, client_sock = socket.socketpair()
        received: dict[str, object] = {}

        def server() -> None:
            received.update(protocol.receive_message(server_sock))
            protocol.send_message(server_sock, {"type": "FAULT_ACK", "ws_id": "WS_001", "status": "OK"})
            server_sock.close()

        thread = threading.Thread(target=server)
        thread.start()
        client = CentralClient("unused", 0, "WS_001")
        client.sock = client_sock
        try:
            self.assertTrue(client.report_fault("timeout", "No response", "2026-10-04T13:30:00Z"))
        finally:
            client.close()
            thread.join()

        self.assertEqual(received["type"], "WS_E_FAULT")
        self.assertEqual(received["fault"], "timeout")

    def _run_registration(self, response: dict[str, object]) -> tuple[bool, dict[str, object]]:
        server_sock, client_sock = socket.socketpair()
        received: dict[str, object] = {}

        def server() -> None:
            received.update(protocol.receive_message(server_sock))
            protocol.send_message(server_sock, response)
            server_sock.close()

        thread = threading.Thread(target=server)
        thread.start()
        client = CentralClient("unused", 0, "WS_001")
        client.sock = client_sock
        try:
            result = client.register()
        finally:
            client.close()
            thread.join()
        return result, received


if __name__ == "__main__":
    unittest.main()

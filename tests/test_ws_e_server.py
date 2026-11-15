from __future__ import annotations

import socket
import unittest

from common import protocol
from WM_WS.WM_WS_M.ws_e_server import EngineServer


class EngineServerTests(unittest.TestCase):
    def test_engine_connection(self) -> None:
        server_sock, client = socket.socketpair()
        server = EngineServer("127.0.0.1", 6000, "WS_001", hello_timeout=0.1)
        try:
            protocol.send_message(client, {"type": "HELLO_WS_E", "component": "WM_WS_E"})
            self.assertTrue(server.handle_engine_hello(server_sock))
            self.assertEqual(
                protocol.receive_message(client),
                {"type": "HELLO_ACK", "ws_id": "WS_001", "status": "OK"},
            )
        finally:
            client.close()
            server_sock.close()

    def test_reject_wrong_ws_id_when_legacy_engine_supplies_one(self) -> None:
        server_sock, client = socket.socketpair()
        server = EngineServer("127.0.0.1", 6000, "WS_001", hello_timeout=0.1)
        try:
            protocol.send_message(client, {"type": "HELLO_WS_E", "ws_id": "WS_999", "component": "WM_WS_E"})
            self.assertFalse(server.handle_engine_hello(server_sock))
            self.assertEqual(protocol.receive_message(client), {"type": "NACK", "reason": "wrong_ws_id"})
        finally:
            client.close()
            server_sock.close()

    def test_reject_silent_engine_after_hello_timeout(self) -> None:
        server_sock, client = socket.socketpair()
        server = EngineServer("127.0.0.1", 6000, "WS_001", hello_timeout=0.01)
        try:
            self.assertFalse(server.handle_engine_hello(server_sock))
        finally:
            client.close()
            server_sock.close()

    def test_accept_runs_idle_callback_while_no_engine_connects(self) -> None:
        server = EngineServer("127.0.0.1", 0, "WS_001", hello_timeout=0.1, accept_timeout=0.01)
        server.start()
        calls = []

        def idle() -> None:
            calls.append(1)
            if len(calls) == 2:
                server.close()

        server.on_idle = idle
        with self.assertRaises(OSError):
            server.accept_engine()
        self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()

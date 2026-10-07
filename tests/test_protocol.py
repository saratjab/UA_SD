from __future__ import annotations

import socket
import unittest

from common import protocol
from WM_WS.WM_WS_E import engine
from WM_WS.WM_WS_M import central_client, monitor, ws_e_server


class ProtocolTests(unittest.TestCase):
    def test_ws_components_import_the_shared_protocol(self) -> None:
        self.assertIs(engine.protocol, protocol)
        self.assertIs(central_client.protocol, protocol)
        self.assertIs(monitor.protocol, protocol)
        self.assertIs(ws_e_server.protocol, protocol)

    def test_lrc_calculation(self) -> None:
        self.assertEqual(protocol.calculate_lrc(b"ABC"), 64)

    def test_frame_creation(self) -> None:
        frame = protocol.create_frame({"type": "ACK"})
        self.assertEqual(frame[0], protocol.STX)
        self.assertEqual(frame[-2], protocol.ETX)
        self.assertEqual(frame[-1], protocol.calculate_lrc(frame[1:-2]))

    def test_frame_parsing(self) -> None:
        message = {"type": "REGISTER_WS", "ws_id": "WS_001"}
        frame = protocol.create_frame(message)
        self.assertEqual(protocol.parse_frame(frame), message)

    def test_invalid_lrc(self) -> None:
        frame = bytearray(protocol.create_frame({"type": "ACK"}))
        frame[-1] ^= 0xFF
        with self.assertRaises(protocol.InvalidLRCError):
            protocol.parse_frame(bytes(frame))

    def test_malformed_frame(self) -> None:
        with self.assertRaises(protocol.MalformedFrameError):
            protocol.parse_frame(b"not a frame")

    def test_socket_send_and_receive(self) -> None:
        first, second = socket.socketpair()
        try:
            protocol.send_message(first, {"type": "HEALTH_OK", "ws_id": "WS_001", "sequence": 3})
            self.assertEqual(
                protocol.receive_message(second),
                {"type": "HEALTH_OK", "ws_id": "WS_001", "sequence": 3},
            )
        finally:
            first.close()
            second.close()


if __name__ == "__main__":
    unittest.main()

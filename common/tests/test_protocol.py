from __future__ import annotations

import socket
import threading
import unittest

from common import protocol


class FramingTests(unittest.TestCase):
    def test_round_trip(self) -> None:
        message = {"type": "REGISTER_WS", "ws_id": "WS_001", "location": "Parque Ñandú ☂"}
        self.assertEqual(protocol.parse_frame(protocol.create_frame(message)), message)

    def test_frame_layout(self) -> None:
        frame = protocol.create_frame({"a": 1})
        data = b'{"a":1}'
        self.assertEqual(frame, bytes([0x02]) + data + bytes([0x03, protocol.calculate_lrc(data)]))

    def test_lrc_is_xor_of_payload_bytes(self) -> None:
        self.assertEqual(protocol.calculate_lrc(b""), 0)
        self.assertEqual(protocol.calculate_lrc(b"\x01\x02\x04"), 7)
        self.assertEqual(protocol.calculate_lrc(b"\xff\xff"), 0)

    def test_control_characters_in_values_cannot_forge_an_etx(self) -> None:
        message = {"details": "x\x03y\x02z"}
        frame = protocol.create_frame(message)
        self.assertEqual(frame.count(bytes([protocol.ETX])), 1)
        self.assertEqual(protocol.parse_frame(frame), message)

    def test_invalid_lrc(self) -> None:
        frame = bytearray(protocol.create_frame({"a": 1}))
        frame[-1] ^= 0x01
        with self.assertRaises(protocol.InvalidLRCError):
            protocol.parse_frame(bytes(frame))

    def test_malformed_frames(self) -> None:
        good = protocol.create_frame({"a": 1})
        cases = {
            "too short": b"\x02\x03",
            "missing STX": b"X" + good[1:],
            "missing ETX": good.replace(bytes([protocol.ETX]), b"?"),
            "bytes after LRC": good + b"!",
        }
        for name, frame in cases.items():
            with self.subTest(name):
                with self.assertRaises(protocol.MalformedFrameError):
                    protocol.parse_frame(frame)

    def test_payload_must_be_a_json_object(self) -> None:
        for data in (b"[1,2]", b'"text"', b"42", b"not json", b"\xff\xfe"):
            with self.subTest(data=data):
                frame = bytes([protocol.STX]) + data + bytes([protocol.ETX, protocol.calculate_lrc(data)])
                with self.assertRaises(protocol.MalformedFrameError):
                    protocol.parse_frame(frame)


class SocketTests(unittest.TestCase):
    def setUp(self) -> None:
        self.left, self.right = socket.socketpair()
        self.addCleanup(self.left.close)
        self.addCleanup(self.right.close)
        self.right.settimeout(2.0)

    def test_send_and_receive(self) -> None:
        protocol.send_message(self.left, {"type": "ACK"})
        self.assertEqual(protocol.receive_message(self.right), {"type": "ACK"})

    def test_noise_before_stx_is_skipped(self) -> None:
        self.left.sendall(b"\x00garbage\x7f" + protocol.create_frame({"type": "ACK"}))
        self.assertEqual(protocol.receive_message(self.right), {"type": "ACK"})

    def test_two_frames_in_one_segment(self) -> None:
        self.left.sendall(protocol.create_frame({"n": 1}) + protocol.create_frame({"n": 2}))
        self.assertEqual(protocol.receive_message(self.right), {"n": 1})
        self.assertEqual(protocol.receive_message(self.right), {"n": 2})

    def test_frame_split_across_segments(self) -> None:
        frame = protocol.create_frame({"type": "HEALTH_CHECK", "sequence": 7})
        for byte in frame:
            self.left.sendall(bytes([byte]))
        self.assertEqual(protocol.receive_message(self.right)["sequence"], 7)

    def test_closed_socket(self) -> None:
        self.left.close()
        with self.assertRaises(protocol.ConnectionClosedError):
            protocol.receive_message(self.right)

    def test_oversized_payload_is_refused(self) -> None:
        # 1 MiB does not fit in the socket buffers, so the sender must run in its
        # own thread; sendall() would otherwise block forever waiting for us to read.
        def flood() -> None:
            try:
                self.left.sendall(bytes([protocol.STX]) + b"a" * (protocol.MAX_DATA_SIZE + 1))
            except OSError:
                pass  # the reader gave up and the socket was closed: expected

        sender = threading.Thread(target=flood, daemon=True)
        sender.start()
        with self.assertRaises(protocol.MalformedFrameError):
            protocol.receive_message(self.right)
        self.right.close()
        sender.join(timeout=3.0)


class BuilderTests(unittest.TestCase):
    def test_contract_shapes(self) -> None:
        self.assertEqual(
            protocol.register_ws_message("WS_001", "North Garden"),
            {"type": "REGISTER_WS", "ws_id": "WS_001", "location": "North Garden"},
        )
        self.assertEqual(protocol.register_ack_message("WS_001"), {"type": "REGISTER_ACK", "ws_id": "WS_001"})
        self.assertEqual(protocol.register_nack_message("why"), {"type": "REGISTER_NACK", "reason": "why"})
        self.assertEqual(protocol.fault_ack_message("WS_001"), {"type": "FAULT_ACK", "ws_id": "WS_001"})
        self.assertEqual(protocol.fault_nack_message("why"), {"type": "FAULT_NACK", "reason": "why"})
        self.assertEqual(
            protocol.fault_message("WS_001", "health_ko", "d", "t"),
            {
                "type": "WS_E_FAULT",
                "ws_id": "WS_001",
                "component": "WM_WS_E",
                "fault": "health_ko",
                "details": "d",
                "timestamp": "t",
            },
        )
        self.assertEqual(
            protocol.health_check_message("WS_001", 3),
            {"type": "HEALTH_CHECK", "ws_id": "WS_001", "sequence": 3},
        )


if __name__ == "__main__":
    unittest.main()

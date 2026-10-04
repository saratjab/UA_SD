from __future__ import annotations

import unittest

from WM_WS.WM_WS_M import protocol


class MessageFactoryTests(unittest.TestCase):
    def test_register_ws(self) -> None:
        self.assertEqual(protocol.register_ws_message("WS_001"), {"type": "REGISTER_WS", "ws_id": "WS_001"})

    def test_register_ack_shape_from_central(self) -> None:
        frame = protocol.create_frame({"type": "REGISTER_ACK", "ws_id": "WS_001", "status": "OK"})
        self.assertEqual(
            protocol.parse_frame(frame),
            {"type": "REGISTER_ACK", "ws_id": "WS_001", "status": "OK"},
        )

    def test_register_nack_shape_from_central(self) -> None:
        frame = protocol.create_frame(
            {"type": "REGISTER_NACK", "ws_id": "WS_001", "reason": "unknown_ws"}
        )
        self.assertEqual(
            protocol.parse_frame(frame),
            {"type": "REGISTER_NACK", "ws_id": "WS_001", "reason": "unknown_ws"},
        )

    def test_health_check_message(self) -> None:
        self.assertEqual(
            protocol.health_check_message("WS_001", 7),
            {"type": "HEALTH_CHECK", "ws_id": "WS_001", "sequence": 7},
        )

    def test_fault_message(self) -> None:
        self.assertEqual(
            protocol.fault_message("WS_001", "timeout", "No response", "2026-10-04T13:30:00Z"),
            {
                "type": "WS_E_FAULT",
                "ws_id": "WS_001",
                "component": "WM_WS_E",
                "fault": "timeout",
                "details": "No response",
                "timestamp": "2026-10-04T13:30:00Z",
            },
        )


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest

from common.event_panel import reset_feed
from common.kafka_contract import encode, envelope
from WM_Central.kafka_service import CentralKafkaService
from WM_Central.state_manager import StateManager

from .helpers import DatabaseTestCase
from .test_kafka_service import FakePublisher


class CentralEventFeedTests(DatabaseTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.feed = reset_feed()
        self.state = StateManager(self.db)
        self.state.register("WS_001", "North", object())
        self.service = CentralKafkaService(self.db, self.state, FakePublisher())

    def timeline(self) -> list[tuple[str, str, str]]:
        events, _ = self.feed.since(0)
        return [(e["direction"], e["peer"], e["event"]) for e in events]

    def test_irrigation_is_traceable_from_request_to_final_summary(self) -> None:
        self.service.handle_activation_bytes(encode(envelope(
            "ACTIVATION_REQUEST", {"duration_seconds": 5}, ws_id="WS_001", operator_id="OP_001", request_id="r1")))
        for message in (
            envelope("IRRIGATION_STARTED", {}, ws_id="WS_001", request_id="r1"),
            envelope("IRRIGATION_COMPLETED", {"end_reason": "TIME_LIMIT", "duration_seconds": 5.0,
                                              "accumulated_volume_liters": 0.5}, ws_id="WS_001", request_id="r1"),
        ):
            self.service.handle_engine_bytes(encode(message))
        self.assertEqual(self.timeline(), [
            ("IN", "WM_FO OP_001", "ACTIVATION_REQUEST"),
            ("OUT", "WM_WS_E WS_001", "START_IRRIGATION"),
            ("OUT", "WM_FO OP_001", "ACTIVATION_AUTHORIZED"),
            ("IN", "WM_WS_E WS_001", "IRRIGATION_STARTED"),
            ("OUT", "WM_FO OP_001", "IRRIGATION_STARTED"),
            ("IN", "WM_WS_E WS_001", "IRRIGATION_COMPLETED"),
            ("OUT", "WM_FO OP_001", "IRRIGATION_COMPLETED"),
        ])
        events, _ = self.feed.since(0)
        self.assertTrue(all(e["request_id"] == "r1" for e in events))
        self.assertIn("duration_seconds=5", events[0]["detail"])

    def test_denied_request_shows_the_reason(self) -> None:
        self.service.issue_station_command("BLOCK_WS", "WS_001")
        self.service.handle_activation_bytes(encode(envelope(
            "ACTIVATION_REQUEST", {}, ws_id="WS_001", operator_id="OP_002", request_id="r2")))
        events, _ = self.feed.since(0)
        denied = [e for e in events if e["event"] == "ACTIVATION_DENIED"]
        self.assertEqual(denied[0]["peer"], "WM_FO OP_002")
        self.assertIn("WS_OUT_OF_SERVICE", denied[0]["detail"])


if __name__ == "__main__":
    unittest.main()

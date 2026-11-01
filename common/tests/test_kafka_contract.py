from __future__ import annotations
import unittest
from common.kafka_contract import (ACTIVATION_REQUESTS, MessageValidationError,
                                   decode, encode, envelope, message_key)

class KafkaContractTests(unittest.TestCase):
    def test_round_trip_and_invalid_messages(self) -> None:
        msg = envelope("X", {"x": 1}, ws_id="WS_1")
        self.assertEqual(decode(encode(msg))["event_type"], "X")
        with self.assertRaises(MessageValidationError): decode(b"[]")
        with self.assertRaises(MessageValidationError): decode(b'{"event_type":"x"}')

    def test_discovery_requests_use_operator_key_but_irrigation_uses_station_key(self) -> None:
        discovery = envelope("AVAILABLE_STATIONS_REQUEST", {},
                             operator_id="OP_002", request_id="list-1")
        activation = envelope("ACTIVATION_REQUEST", {"duration_seconds": 10},
                              operator_id="OP_002", ws_id="WS_001", request_id="r1")
        self.assertEqual(message_key(ACTIVATION_REQUESTS, discovery), "OP_002")
        self.assertEqual(message_key(ACTIVATION_REQUESTS, activation), "WS_001")

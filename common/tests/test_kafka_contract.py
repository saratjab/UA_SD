from __future__ import annotations
import unittest
from common.kafka_contract import MessageValidationError, decode, encode, envelope

class KafkaContractTests(unittest.TestCase):
    def test_round_trip_and_invalid_messages(self) -> None:
        msg = envelope("X", {"x": 1}, ws_id="WS_1")
        self.assertEqual(decode(encode(msg))["event_type"], "X")
        with self.assertRaises(MessageValidationError): decode(b"[]")
        with self.assertRaises(MessageValidationError): decode(b'{"event_type":"x"}')

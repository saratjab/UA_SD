from __future__ import annotations

import json
import unittest
from unittest import mock

from common.kafka_contract import envelope
from WM_FO.kafka_client import FieldOperator


class FieldOperatorKafkaTests(unittest.TestCase):
    def test_same_operator_processes_use_independent_groups_and_route_by_request(self) -> None:
        class Publisher:
            def publish(self, *_args, **_kwargs):
                pass

        publisher = Publisher()
        first = FieldOperator("OP_001", publisher)  # type: ignore[arg-type]
        second = FieldOperator("OP_001", publisher)  # type: ignore[arg-type]
        worker_groups = []

        def worker_factory(_bootstrap, group_id, _topics, _handler, consumer, **kwargs):
            worker_groups.append(group_id)
            self.assertEqual(kwargs["auto_offset_reset"], "latest")
            return object()

        with mock.patch("WM_FO.kafka_client.KafkaWorker", side_effect=worker_factory):
            first.worker("localhost", consumer=object())
            second.worker("localhost", consumer=object())
        self.assertNotEqual(worker_groups[0], worker_groups[1])

        first_request = first.request_irrigation("WS_001", 20)
        second_request = second.request_irrigation("WS_002", 20)
        event = envelope("ACTIVATION_DENIED", {"reason": "WS_BUSY"},
                         operator_id="OP_001", request_id=second_request)
        with mock.patch("WM_FO.kafka_client.show_event") as display:
            first.handle_bytes(json.dumps(event).encode())
            self.assertFalse(first._pending[first_request].is_set())
            self.assertEqual(display.call_count, 0)
            second.handle_bytes(json.dumps(event).encode())
            self.assertTrue(second._pending[second_request].is_set())
            self.assertEqual(display.call_count, 1)


if __name__ == "__main__":
    unittest.main()

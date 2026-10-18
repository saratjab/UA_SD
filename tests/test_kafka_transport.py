from __future__ import annotations

import unittest
from unittest import mock

from common.kafka_transport import KafkaWorker


class FakeConsumer:
    def __init__(self) -> None:
        self.assigned = False
        self.closed = False

    def subscribe(self, _topics) -> None:
        pass

    def assignment(self):
        return ["partition"] if self.assigned else []

    def poll(self, _timeout):
        self.assigned = True
        return None

    def close(self) -> None:
        self.closed = True


class KafkaWorkerTests(unittest.TestCase):
    def test_wait_until_assigned_polls_before_request_submission(self) -> None:
        consumer = FakeConsumer()
        worker = KafkaWorker("localhost", "fo-process", ["events"], lambda _raw: True, consumer)
        worker.start()
        self.assertTrue(worker.wait_until_assigned(timeout=0.2))

    def test_offset_reset_is_passed_to_new_consumer(self) -> None:
        with mock.patch("common.kafka_transport.Consumer") as consumer_factory:
            KafkaWorker("localhost", "fo-process", ["events"], lambda _raw: True,
                        auto_offset_reset="latest")
        self.assertEqual(consumer_factory.call_args.args[0]["auto.offset.reset"], "latest")


if __name__ == "__main__":
    unittest.main()

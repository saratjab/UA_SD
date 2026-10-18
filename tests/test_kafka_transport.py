from __future__ import annotations

import unittest
from unittest import mock

from common.kafka_transport import KafkaWorker


class FakePartition:
    def __init__(self, topic: str, partition: int, offset: int = -1001) -> None:
        self.topic, self.partition, self.offset = topic, partition, offset


class FakeConsumer:
    def __init__(self) -> None:
        self.assigned = False
        self.position_offset = -1001
        self.poll_count = 0
        self.seeked_offsets = []
        self.closed = False

    def subscribe(self, _topics) -> None:
        pass

    def assignment(self):
        return [FakePartition("events", 0, self.position_offset)] if self.assigned else []

    def position(self, partitions):
        return [FakePartition(part.topic, part.partition, self.position_offset) for part in partitions]

    def committed(self, partitions, timeout):
        return [FakePartition(part.topic, part.partition) for part in partitions]

    def get_watermark_offsets(self, _partition, timeout):
        return (0, 17)

    def seek(self, partition):
        self.seeked_offsets.append(partition.offset)

    def poll(self, _timeout):
        self.poll_count += 1
        self.assigned = True
        return None

    def close(self) -> None:
        self.closed = True


class KafkaWorkerTests(unittest.TestCase):
    def test_startup_seeks_latest_watermark_after_assignment_before_request(self) -> None:
        consumer = FakeConsumer()
        worker = KafkaWorker("localhost", "fo-process", ["events"], lambda _raw: True,
                             consumer, auto_offset_reset="latest")
        worker.start()
        self.assertTrue(worker.wait_until_assigned(timeout=0.2))
        self.assertEqual(consumer.poll_count, 1)
        self.assertEqual(consumer.seeked_offsets, [17])

    def test_offset_reset_is_passed_to_new_consumer(self) -> None:
        with mock.patch("common.kafka_transport.Consumer") as consumer_factory:
            KafkaWorker("localhost", "fo-process", ["events"], lambda _raw: True,
                        auto_offset_reset="latest")
        self.assertEqual(consumer_factory.call_args.args[0]["auto.offset.reset"], "latest")


if __name__ == "__main__":
    unittest.main()

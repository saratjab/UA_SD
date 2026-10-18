"""Small confluent-kafka adapter with optional imports for unit-testability."""
from __future__ import annotations

import logging
import time
from typing import Any, Callable

from .kafka_contract import encode

LOGGER = logging.getLogger(__name__)

try:  # The rest of the application remains usable without a broker dependency.
    from confluent_kafka import Consumer, Producer, KafkaException, TopicPartition  # type: ignore
except ImportError:  # pragma: no cover - exercised when dependency missing
    Consumer = Producer = None  # type: ignore
    KafkaException = Exception

    class TopicPartition:  # type: ignore[no-redef]
        """Small test-time stand-in; production uses confluent-kafka's class."""
        def __init__(self, topic: str, partition: int, offset: int = -1001) -> None:
            self.topic, self.partition, self.offset = topic, partition, offset


class KafkaUnavailable(RuntimeError):
    pass


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str, producer: Any | None = None) -> None:
        if producer is None:
            if Producer is None:
                raise KafkaUnavailable("confluent-kafka is not installed")
            producer = Producer({"bootstrap.servers": bootstrap_servers, "enable.idempotence": True})
        self._producer = producer

    def publish(self, topic: str, message: dict[str, Any], *, key: str | None = None) -> None:
        delivered: list[Exception] = []
        def report(err: Any, _msg: Any) -> None:
            if err:
                delivered.append(RuntimeError(str(err)))
        self._producer.produce(topic, encode(message), key=key, on_delivery=report)
        # Commands/results are state-changing. Wait for the delivery callback so a
        # caller never commits its source offset merely because the record was
        # accepted into a local client buffer.
        remaining = self._producer.flush(10)
        if remaining:
            raise RuntimeError("Kafka delivery timed out")
        if delivered:
            raise delivered[0]

    def close(self) -> None:
        self._producer.flush(10)


class KafkaWorker:
    """Manual-commit polling loop. Handler success is the commit boundary."""
    def __init__(self, bootstrap_servers: str, group_id: str, topics: list[str], handler: Callable[[bytes], bool], consumer: Any | None = None, *, auto_offset_reset: str = "earliest") -> None:
        if consumer is None:
            if Consumer is None:
                raise KafkaUnavailable("confluent-kafka is not installed")
            consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": group_id,
                                 "enable.auto.commit": False, "auto.offset.reset": auto_offset_reset})
        self._consumer, self._topics, self._handler = consumer, topics, handler
        self._auto_offset_reset = auto_offset_reset
        self._running = False

    def start(self) -> None:
        self._consumer.subscribe(self._topics)
        self._running = True

    def wait_until_assigned(self, timeout: float = 10.0) -> bool:
        """Wait for assignment and explicitly establish a latest start offset.

        ``position()`` reports the last consumed offset + 1 and can remain
        OFFSET_INVALID on an idle partition. For FO's fresh latest group, use
        the broker high watermark and seek there instead of mistaking assignment
        for completed offset resolution.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            assignment = self._consumer.assignment()
            if assignment:
                if self._auto_offset_reset != "latest":
                    return True
                try:
                    positions = self._consumer.position(assignment)
                    if len(positions) == len(assignment) and all(p.offset >= 0 for p in positions):
                        return True
                    remaining = max(0.0, deadline - time.monotonic())
                    committed = self._consumer.committed(assignment, timeout=remaining)
                    if committed is None or len(committed) != len(assignment):
                        return False
                    for partition, stored in zip(assignment, committed):
                        if stored.offset >= 0:
                            start_offset = stored.offset
                        else:
                            watermarks = self._consumer.get_watermark_offsets(
                                partition, timeout=max(0.0, deadline - time.monotonic()))
                            if watermarks is None or len(watermarks) != 2 or watermarks[1] < 0:
                                return False
                            start_offset = watermarks[1]
                        self._consumer.seek(TopicPartition(
                            partition.topic, partition.partition, start_offset))
                except Exception:
                    LOGGER.exception("Could not establish Field Operator Kafka start offsets")
                    return False
                return True
            self.poll_once(min(0.1, max(0.0, deadline - time.monotonic())))
        return False

    def poll_once(self, timeout: float = 1.0) -> bool:
        msg = self._consumer.poll(timeout)
        if msg is None:
            return False
        if msg.error():
            LOGGER.warning("Kafka consumer error: %s", msg.error())
            return False
        try:
            handled = self._handler(msg.value())
        except Exception:
            LOGGER.exception("Kafka message processing failed; offset not committed")
            return False
        if handled:
            self._consumer.commit(message=msg, asynchronous=False)
        return handled

    def close(self) -> None:
        self._running = False
        self._consumer.close()

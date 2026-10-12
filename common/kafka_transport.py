"""Small confluent-kafka adapter with optional imports for unit-testability."""
from __future__ import annotations

import logging
from typing import Any, Callable

from .kafka_contract import encode

LOGGER = logging.getLogger(__name__)

try:  # The rest of the application remains usable without a broker dependency.
    from confluent_kafka import Consumer, Producer, KafkaException  # type: ignore
except ImportError:  # pragma: no cover - exercised when dependency missing
    Consumer = Producer = None  # type: ignore
    KafkaException = Exception


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
    def __init__(self, bootstrap_servers: str, group_id: str, topics: list[str], handler: Callable[[bytes], bool], consumer: Any | None = None) -> None:
        if consumer is None:
            if Consumer is None:
                raise KafkaUnavailable("confluent-kafka is not installed")
            consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": group_id,
                                 "enable.auto.commit": False, "auto.offset.reset": "earliest"})
        self._consumer, self._topics, self._handler = consumer, topics, handler
        self._running = False

    def start(self) -> None:
        self._consumer.subscribe(self._topics)
        self._running = True

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

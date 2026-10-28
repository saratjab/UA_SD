"""Small confluent-kafka adapter with optional imports for unit-testability."""
from __future__ import annotations

import logging
import os
import ssl
import time
from collections.abc import Mapping
from typing import Any, Callable

from .kafka_contract import TOPIC_KEYS, encode, message_key

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


class KafkaConfigurationError(ValueError):
    """Kafka client security settings are incomplete or inconsistent."""


def _validate_ca_trust(ca_location: str, ca_pem: str) -> None:
    try:
        if ca_location:
            ssl.create_default_context(cafile=ca_location)
        else:
            ssl.create_default_context(cadata=ca_pem)
    except (OSError, ssl.SSLError, ValueError) as exc:
        raise KafkaConfigurationError("Kafka CA trust configuration is unreadable or invalid") from exc


def kafka_security_config(environ: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Build the shared librdkafka security config without logging secrets.

    Local development defaults to PLAINTEXT. SASL_SSL always verifies both
    the certificate chain and the broker hostname; custom CAs can be supplied
    by path or as PEM text in an environment secret.
    """
    values = os.environ if environ is None else environ
    require_raw = values.get("KAFKA_REQUIRE_SECURITY", "false").strip().lower()
    if require_raw not in {"1", "true", "yes", "0", "false", "no"}:
        raise KafkaConfigurationError("KAFKA_REQUIRE_SECURITY must be true or false")
    require_security = require_raw in {"1", "true", "yes"}
    configured_protocol = values.get("KAFKA_SECURITY_PROTOCOL")
    if require_security and not configured_protocol:
        raise KafkaConfigurationError("KAFKA_SECURITY_PROTOCOL must be explicitly set to SASL_SSL when KAFKA_REQUIRE_SECURITY is enabled")
    protocol = (configured_protocol or "PLAINTEXT").strip().upper()
    valid_protocols = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
    if protocol not in valid_protocols:
        raise KafkaConfigurationError("KAFKA_SECURITY_PROTOCOL must be PLAINTEXT, SSL, SASL_PLAINTEXT, or SASL_SSL")
    if require_security and protocol != "SASL_SSL":
        raise KafkaConfigurationError("KAFKA_REQUIRE_SECURITY requires KAFKA_SECURITY_PROTOCOL=SASL_SSL")

    config: dict[str, Any] = {"security.protocol": protocol}
    uses_tls = protocol in {"SSL", "SASL_SSL"}
    uses_sasl = protocol in {"SASL_PLAINTEXT", "SASL_SSL"}
    ca_location = values.get("KAFKA_SSL_CA_LOCATION", "").strip()
    ca_pem = values.get("KAFKA_SSL_CA_PEM", "").strip()
    if ca_location and ca_pem:
        raise KafkaConfigurationError("Configure only one of KAFKA_SSL_CA_LOCATION or KAFKA_SSL_CA_PEM")
    if uses_tls:
        config["enable.ssl.certificate.verification"] = True
        config["ssl.endpoint.identification.algorithm"] = "https"
        if ca_location:
            config["ssl.ca.location"] = ca_location
        elif ca_pem:
            config["ssl.ca.pem"] = ca_pem
        elif protocol == "SASL_SSL":
            raise KafkaConfigurationError("SASL_SSL requires KAFKA_SSL_CA_LOCATION or KAFKA_SSL_CA_PEM")
        if protocol == "SASL_SSL":
            _validate_ca_trust(ca_location, ca_pem)

    if uses_sasl:
        mechanism = values.get("KAFKA_SASL_MECHANISM", "").strip()
        username = values.get("KAFKA_SASL_USERNAME", "")
        password = values.get("KAFKA_SASL_PASSWORD", "")
        if mechanism.upper() not in {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}:
            raise KafkaConfigurationError("KAFKA_SASL_MECHANISM must be PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512")
        if not username.strip() or not password.strip():
            raise KafkaConfigurationError("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required for SASL")
        config.update({
            "sasl.mechanism": mechanism,
            "sasl.username": username,
            "sasl.password": password,
        })
    return config


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str, producer: Any | None = None) -> None:
        if producer is None:
            if Producer is None:
                raise KafkaUnavailable("confluent-kafka is not installed")
            producer = Producer({
                "bootstrap.servers": bootstrap_servers,
                **kafka_security_config(),
                "enable.idempotence": True,
            })
        self._producer = producer

    def publish(self, topic: str, message: dict[str, Any], *, key: str | None = None) -> None:
        # Enforce the application partition-key policy at the shared producer
        # boundary, including legacy callers and outbox retries.
        if topic in TOPIC_KEYS:
            expected_key = message_key(topic, message)
            if expected_key is None:
                raise ValueError(f"{topic} message is missing its {TOPIC_KEYS[topic]} partition key")
            if key is not None and key != expected_key:
                raise ValueError(f"{topic} partition key must match {TOPIC_KEYS[topic]}")
            key = expected_key
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
    def __init__(self, bootstrap_servers: str, group_id: str, topics: list[str], handler: Callable[[bytes], bool], consumer: Any | None = None, *, auto_offset_reset: str = "earliest", group_instance_id: str | None = None) -> None:
        if consumer is None:
            if Consumer is None:
                raise KafkaUnavailable("confluent-kafka is not installed")
            config = {"bootstrap.servers": bootstrap_servers, "group.id": group_id,
                      "enable.auto.commit": False, "auto.offset.reset": auto_offset_reset}
            config.update(kafka_security_config())
            if group_instance_id is not None:
                config["group.instance.id"] = group_instance_id
            consumer = Consumer(config)
        self._consumer, self._topics, self._handler = consumer, topics, handler
        self._group_id = group_id
        self._auto_offset_reset = auto_offset_reset
        self._running = False
        self._assignment_logged = False

    def start(self) -> None:
        self._consumer.subscribe(self._topics)
        self._running = True
        LOGGER.info("Kafka consumer subscribed group=%s topics=%s", self._group_id, self._topics)

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
        if not self._assignment_logged:
            assignment_fn = getattr(self._consumer, "assignment", None)
            assignment = assignment_fn() if callable(assignment_fn) else []
            if assignment:
                LOGGER.info("Kafka consumer assigned group=%s partitions=%s", self._group_id, assignment)
                self._assignment_logged = True
        if msg is None:
            return False
        if msg.error():
            LOGGER.warning("Kafka consumer error: %s", msg.error())
            return False
        record_timestamp = None
        timestamp_fn = getattr(msg, "timestamp", None)
        if callable(timestamp_fn):
            try:
                _timestamp_type, record_timestamp = timestamp_fn()
            except Exception:
                LOGGER.debug("Could not read Kafka record timestamp", exc_info=True)
        LOGGER.info(
            "Kafka record consumed group=%s topic=%s partition=%s offset=%s broker_timestamp_ms=%s",
            self._group_id,
            getattr(msg, "topic", lambda: "unknown")(),
            getattr(msg, "partition", lambda: "unknown")(),
            getattr(msg, "offset", lambda: "unknown")(),
            record_timestamp,
        )
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

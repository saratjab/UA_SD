from __future__ import annotations

import unittest
import ssl
import tempfile
from unittest import mock

from common.kafka_contract import (ACTIVATION_REQUESTS, OPERATOR_EVENTS,
                                   WS_COMMANDS, WS_EVENTS)
from common.kafka_transport import KafkaConfigurationError, KafkaPublisher, KafkaWorker, kafka_security_config


class FakeProducer:
    def __init__(self): self.records = []
    def produce(self, topic, value, *, key, on_delivery):
        self.records.append((topic, key, value))
        on_delivery(None, None)
    def flush(self, _timeout): return 0


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
    @staticmethod
    def trusted_ca_pem() -> str:
        roots = ssl.create_default_context().get_ca_certs(binary_form=True)
        if not roots:
            raise unittest.SkipTest("Python SSL installation exposes no trusted CA certificates")
        return ssl.DER_cert_to_PEM_cert(roots[0])

    def test_client_security_defaults_to_local_plaintext(self) -> None:
        self.assertEqual(kafka_security_config({}), {"security.protocol": "PLAINTEXT"})

    def test_secure_guard_requires_explicit_sasl_ssl(self) -> None:
        with self.assertRaisesRegex(KafkaConfigurationError, "explicitly set"):
            kafka_security_config({"KAFKA_REQUIRE_SECURITY": "true"})
        for protocol in ("PLAINTEXT", "SASL_PLAINTEXT", "SSL"):
            with self.subTest(protocol=protocol), self.assertRaisesRegex(KafkaConfigurationError, "requires KAFKA_SECURITY_PROTOCOL=SASL_SSL"):
                kafka_security_config({"KAFKA_REQUIRE_SECURITY": "true", "KAFKA_SECURITY_PROTOCOL": protocol})

    def test_secure_guard_requires_sasl_credentials_and_valid_ca(self) -> None:
        secure = {
            "KAFKA_REQUIRE_SECURITY": "true", "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
            "KAFKA_SASL_MECHANISM": "PLAIN", "KAFKA_SASL_USERNAME": "wm-client",
            "KAFKA_SASL_PASSWORD": "secret",
        }
        with self.assertRaisesRegex(KafkaConfigurationError, "CA"):
            kafka_security_config(secure)
        with self.assertRaisesRegex(KafkaConfigurationError, "invalid"):
            kafka_security_config(secure | {"KAFKA_SSL_CA_PEM": "-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----"})
        with self.assertRaisesRegex(KafkaConfigurationError, "USERNAME"):
            kafka_security_config(secure | {"KAFKA_SASL_USERNAME": "", "KAFKA_SSL_CA_PEM": self.trusted_ca_pem()})

    def test_client_sasl_ssl_config_enables_ca_and_hostname_verification(self) -> None:
        config = kafka_security_config({
            "KAFKA_REQUIRE_SECURITY": "true", "KAFKA_SECURITY_PROTOCOL": "SASL_SSL", "KAFKA_SASL_MECHANISM": "PLAIN",
            "KAFKA_SASL_USERNAME": "wm-client", "KAFKA_SASL_PASSWORD": "secret",
            "KAFKA_SSL_CA_PEM": self.trusted_ca_pem(),
        })
        self.assertEqual(config["security.protocol"], "SASL_SSL")
        self.assertEqual(config["sasl.mechanism"], "PLAIN")
        self.assertEqual(config["sasl.username"], "wm-client")
        self.assertEqual(config["sasl.password"], "secret")
        self.assertTrue(config["ssl.ca.pem"].startswith("-----BEGIN CERTIFICATE-----"))
        self.assertTrue(config["enable.ssl.certificate.verification"])
        self.assertEqual(config["ssl.endpoint.identification.algorithm"], "https")

    def test_sasl_ssl_requires_credentials_and_ca(self) -> None:
        with self.assertRaisesRegex(KafkaConfigurationError, "CA"):
            kafka_security_config({
                "KAFKA_SECURITY_PROTOCOL": "SASL_SSL", "KAFKA_SASL_MECHANISM": "PLAIN",
                "KAFKA_SASL_USERNAME": "user", "KAFKA_SASL_PASSWORD": "pass",
            })
        with self.assertRaisesRegex(KafkaConfigurationError, "USERNAME"):
            kafka_security_config({"KAFKA_SECURITY_PROTOCOL": "SASL_SSL", "KAFKA_SASL_MECHANISM": "PLAIN",
                                   "KAFKA_SSL_CA_PEM": self.trusted_ca_pem()})

    def test_invalid_security_protocol_is_rejected(self) -> None:
        with self.assertRaisesRegex(KafkaConfigurationError, "KAFKA_SECURITY_PROTOCOL"):
            kafka_security_config({"KAFKA_SECURITY_PROTOCOL": "PLAINTEXT_WITH_AUTH"})

    def test_publisher_and_worker_apply_shared_security_config(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ca_path = f"{directory}/ca.pem"
            with open(ca_path, "w", encoding="ascii") as ca_file:
                ca_file.write(self.trusted_ca_pem())
            environment = {
                "KAFKA_REQUIRE_SECURITY": "true", "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
                "KAFKA_SASL_MECHANISM": "PLAIN", "KAFKA_SASL_USERNAME": "wm-client",
                "KAFKA_SASL_PASSWORD": "secret", "KAFKA_SSL_CA_LOCATION": ca_path,
            }
            with mock.patch.dict("os.environ", environment, clear=True), mock.patch("common.kafka_transport.Producer") as producer_factory:
                KafkaPublisher("broker:9092")
            self.assertEqual(producer_factory.call_args.args[0]["security.protocol"], "SASL_SSL")
            self.assertEqual(producer_factory.call_args.args[0]["ssl.ca.location"], ca_path)
            with mock.patch.dict("os.environ", environment, clear=True), mock.patch("common.kafka_transport.Consumer") as consumer_factory:
                KafkaWorker("broker:9092", "group", ["events"], lambda _raw: True)
        self.assertEqual(consumer_factory.call_args.args[0]["security.protocol"], "SASL_SSL")
        self.assertEqual(consumer_factory.call_args.args[0]["sasl.username"], "wm-client")

    def test_publisher_applies_entity_key_policy_for_every_topic(self) -> None:
        producer = FakeProducer()
        publisher = KafkaPublisher("localhost:9092", producer)
        cases = [
            (ACTIVATION_REQUESTS, "ACTIVATION_REQUEST", {"ws_id": "WS_A", "operator_id": "OP_A"}, "WS_A"),
            *((WS_COMMANDS, kind, {"ws_id": "WS_B"}, "WS_B") for kind in ("START_IRRIGATION", "STOP_IRRIGATION", "BLOCK_WS", "ACTIVATE_WS")),
            *((WS_EVENTS, kind, {"ws_id": "WS_C"}, "WS_C") for kind in ("IRRIGATION_STARTED", "IRRIGATION_TELEMETRY", "IRRIGATION_COMPLETED", "COMMAND_REJECTED")),
            *((OPERATOR_EVENTS, kind, {"ws_id": "WS_D", "operator_id": "OP_D"}, "OP_D") for kind in ("ACTIVATION_AUTHORIZED", "ACTIVATION_DENIED", "ACTIVATION_STATUS", "IRRIGATION_STARTED", "IRRIGATION_PROGRESS", "IRRIGATION_COMPLETED", "COMMAND_REJECTED")),
        ]
        from common.kafka_contract import envelope
        for topic, event_type, identifiers, _expected_key in cases:
            publisher.publish(topic, envelope(event_type, {}, **identifiers))
        self.assertEqual([record[1] for record in producer.records], [case[3] for case in cases])

    def test_publisher_rejects_missing_or_wrong_partition_key(self) -> None:
        publisher = KafkaPublisher("localhost:9092", FakeProducer())
        from common.kafka_contract import envelope
        with self.assertRaisesRegex(ValueError, "missing its ws_id"):
            publisher.publish(WS_COMMANDS, envelope("STOP_IRRIGATION", {}))
        with self.assertRaisesRegex(ValueError, "must match ws_id"):
            publisher.publish(WS_COMMANDS, envelope("STOP_IRRIGATION", {}, ws_id="WS_001"), key="OP_001")

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

    def test_static_member_identity_is_passed_to_new_consumer(self) -> None:
        with mock.patch("common.kafka_transport.Consumer") as consumer_factory:
            KafkaWorker(
                "localhost", "engine-group", ["commands"], lambda _raw: True,
                group_instance_id="engine-instance",
            )
        self.assertEqual(consumer_factory.call_args.args[0]["group.instance.id"], "engine-instance")


if __name__ == "__main__":
    unittest.main()

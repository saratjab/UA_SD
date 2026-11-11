"""Opt-in ephemeral SASL_SSL broker verification (requires Docker + openssl)."""
from __future__ import annotations

import base64
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import unittest
from uuid import uuid4

from common.kafka_transport import kafka_security_config


IMAGE = "watermanagement-kafka:railway-verify"


@unittest.skipUnless(os.environ.get("RUN_KAFKA_SECURE_INTEGRATION") == "1", "set RUN_KAFKA_SECURE_INTEGRATION=1 to run an ephemeral secure Kafka broker")
class SecureKafkaIntegrationTests(unittest.TestCase):
    def test_authenticated_tls_publish_consume_and_invalid_credentials(self) -> None:
        with tempfile.TemporaryDirectory(prefix="wm-secure-kafka-") as directory:
            root = Path(directory)
            ca_key, ca_cert = root / "ca.key", root / "ca.pem"
            server_key, server_csr, server_cert = root / "server.key", root / "server.csr", root / "server.pem"
            p12, extensions = root / "server.p12", root / "san.ext"
            password, username = "KeystorePassword123", "wm_test_user"
            subprocess.run(["openssl", "genrsa", "-out", str(ca_key), "2048"], check=True, capture_output=True)
            subprocess.run(["openssl", "req", "-x509", "-new", "-key", str(ca_key), "-sha256", "-days", "2",
                            "-subj", "/CN=WM test CA", "-addext", "basicConstraints=critical,CA:TRUE",
                            "-addext", "keyUsage=critical,keyCertSign,cRLSign", "-out", str(ca_cert)], check=True, capture_output=True)
            subprocess.run(["openssl", "genrsa", "-out", str(server_key), "2048"], check=True, capture_output=True)
            subprocess.run(["openssl", "req", "-new", "-key", str(server_key), "-subj", "/CN=localhost",
                            "-out", str(server_csr)], check=True, capture_output=True)
            extensions.write_text("subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\nkeyUsage=digitalSignature,keyEncipherment\n")
            subprocess.run(["openssl", "x509", "-req", "-in", str(server_csr), "-CA", str(ca_cert), "-CAkey", str(ca_key),
                            "-CAcreateserial", "-days", "2", "-sha256", "-extfile", str(extensions), "-out", str(server_cert)],
                           check=True, capture_output=True)

            def make_keystore(cert_path: Path, key_path: Path, output: Path) -> None:
                subprocess.run(["openssl", "pkcs12", "-export", "-name", "kafka", "-inkey", str(key_path),
                                "-in", str(cert_path), "-certfile", str(ca_cert), "-passout", f"pass:{password}",
                                "-out", str(output)], check=True, capture_output=True)

            make_keystore(server_cert, server_key, p12)

            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            kafka_password = "S" * 40
            broker_env = os.environ | {
                "KAFKA_PRIVATE_HOST": "localhost", "KAFKA_PUBLIC_HOST": "localhost", "KAFKA_PUBLIC_PORT": str(port),
                "KAFKA_PUBLIC_LISTENER_ENABLED": "true",
                "KAFKA_SASL_USERNAME": username, "KAFKA_SASL_PASSWORD": kafka_password,
                "KAFKA_TLS_KEYSTORE_B64": base64.b64encode(p12.read_bytes()).decode("ascii"),
                "KAFKA_TLS_KEYSTORE_PASSWORD": password,
                "KAFKA_TLS_CA_CERT_B64": base64.b64encode(ca_cert.read_bytes()).decode("ascii"),
            }
            forwarded = ("KAFKA_PRIVATE_HOST", "KAFKA_PUBLIC_HOST", "KAFKA_PUBLIC_PORT", "KAFKA_PUBLIC_LISTENER_ENABLED", "KAFKA_SASL_USERNAME",
                         "KAFKA_SASL_PASSWORD", "KAFKA_TLS_KEYSTORE_B64", "KAFKA_TLS_KEYSTORE_PASSWORD",
                         "KAFKA_TLS_CA_CERT_B64")
            log = root / "broker.log"
            with log.open("wb") as logfile:
                broker = subprocess.Popen([
                    "docker", "run", "--rm", "--name", f"wm-secure-kafka-{uuid4().hex[:8]}",
                    "-p", f"127.0.0.1:{port}:9092",
                    *[item for name in forwarded for item in ("-e", name)],
                    IMAGE,
                ], env=broker_env, stdout=logfile, stderr=subprocess.STDOUT)
            try:
                client_env = {
                    "KAFKA_REQUIRE_SECURITY": "true", "KAFKA_SECURITY_PROTOCOL": "SASL_SSL", "KAFKA_SASL_MECHANISM": "PLAIN",
                    "KAFKA_SASL_USERNAME": username, "KAFKA_SASL_PASSWORD": kafka_password,
                    "KAFKA_SSL_CA_PEM": ca_cert.read_text(),
                }
                from confluent_kafka.admin import AdminClient, NewTopic
                bootstrap = f"localhost:{port}"
                deadline = time.monotonic() + 60
                last_error: Exception | None = None
                admin = AdminClient({"bootstrap.servers": bootstrap, **kafka_security_config(client_env), "log_level": 0})
                while time.monotonic() < deadline:
                    try:
                        admin.list_topics(timeout=3)
                        break
                    except Exception as exc:
                        last_error = exc
                        if broker.poll() is not None:
                            self.fail(f"secure broker exited during startup ({exc}):\n{log.read_text(errors='replace')[-6000:]}")
                        time.sleep(.5)
                else:
                    self.fail(f"secure Kafka did not become ready: {last_error}")
                startup_log = log.read_text(errors="replace")
                self.assertIn("Awaiting socket connections on 127.0.0.1:9093", startup_log)
                self.assertIn("Awaiting socket connections on 0.0.0.0:29092", startup_log)
                self.assertIn("Awaiting socket connections on 0.0.0.0:9092", startup_log)

                topic = f"wm.secure.integration.{uuid4().hex}"
                admin.create_topics([NewTopic(topic, 3, 1)])[topic].result(timeout=10)

                from confluent_kafka import Consumer, Producer
                producer = Producer({"bootstrap.servers": bootstrap, **kafka_security_config(client_env), "enable.idempotence": True,
                                     "log_level": 0})
                producer.produce(topic, b"tls-authenticated", key=b"station-secure")
                self.assertEqual(producer.flush(10), 0)
                consumer = Consumer({"bootstrap.servers": bootstrap, **kafka_security_config(client_env),
                                     "group.id": f"wm-secure-{uuid4().hex}", "auto.offset.reset": "earliest",
                                     "enable.auto.commit": False, "log_level": 0})
                consumer.subscribe([topic])
                record = None
                stop = time.monotonic() + 10
                while time.monotonic() < stop and record is None:
                    record = consumer.poll(.2)
                consumer.close()
                self.assertIsNotNone(record, "authenticated TLS consumer did not receive message")
                self.assertEqual(record.value(), b"tls-authenticated")
                self.assertEqual(record.key(), b"station-secure")

                bad = dict(client_env, KAFKA_SASL_PASSWORD="W" * 40)
                invalid_producer = Producer({"bootstrap.servers": bootstrap, **kafka_security_config(bad),
                                             "message.timeout.ms": 3000, "log_level": 0})
                delivery_errors: list[object] = []
                invalid_producer.produce(topic, b"must-not-arrive", key=b"station-secure",
                                         on_delivery=lambda error, _message: delivery_errors.append(error) if error else None)
                invalid_producer.flush(10)
                self.assertTrue(delivery_errors, "Kafka accepted a record with an invalid SASL password")

                wrong_ca = dict(client_env, KAFKA_SSL_CA_PEM="-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----")
                with self.assertRaisesRegex(ValueError, "CA"):
                    kafka_security_config(wrong_ca)
                wrong_ca_path = root / "wrong-ca.pem"
                wrong_ca_key = root / "wrong-ca.key"
                subprocess.run(["openssl", "genrsa", "-out", str(wrong_ca_key), "2048"], check=True, capture_output=True)
                subprocess.run(["openssl", "req", "-x509", "-new", "-key", str(wrong_ca_key), "-sha256", "-days", "2",
                                "-subj", "/CN=Wrong CA", "-addext", "basicConstraints=critical,CA:TRUE",
                                "-addext", "keyUsage=critical,keyCertSign,cRLSign", "-out", str(wrong_ca_path)],
                               check=True, capture_output=True)
                wrong_trust = dict(client_env, KAFKA_SSL_CA_PEM=wrong_ca_path.read_text())
                wrong_trust_admin = AdminClient({"bootstrap.servers": bootstrap, **kafka_security_config(wrong_trust), "socket.timeout.ms": 3000, "log_level": 0})
                with self.assertRaises(Exception):
                    wrong_trust_admin.list_topics(timeout=3)

                wrong_host_admin = AdminClient({"bootstrap.servers": "127.0.0.1:" + str(port),
                                                **kafka_security_config(client_env), "socket.timeout.ms": 3000, "log_level": 0})
                with self.assertRaises(Exception):
                    wrong_host_admin.list_topics(timeout=3)

                # Exercise the production entrypoint's startup checks too: it
                # must reject an unrelated trust anchor and a non-exact SAN.
                mismatch_ext = root / "mismatch.ext"
                mismatch_ext.write_text("subjectAltName=DNS:other.example\nextendedKeyUsage=serverAuth\n"
                                        "keyUsage=digitalSignature,keyEncipherment\n")
                mismatch_cert = root / "mismatch.pem"
                subprocess.run(["openssl", "x509", "-req", "-in", str(server_csr), "-CA", str(ca_cert), "-CAkey", str(ca_key),
                                "-CAcreateserial", "-days", "2", "-sha256", "-extfile", str(mismatch_ext), "-out", str(mismatch_cert)],
                               check=True, capture_output=True)
                mismatch_p12 = root / "mismatch.p12"
                make_keystore(mismatch_cert, server_key, mismatch_p12)

                def assert_startup_rejected(store_path: Path, ca_path: Path) -> None:
                    environment = os.environ | {
                        "KAFKA_PRIVATE_HOST": "localhost", "KAFKA_PUBLIC_LISTENER_ENABLED": "false",
                        "KAFKA_SASL_USERNAME": username, "KAFKA_SASL_PASSWORD": kafka_password,
                        "KAFKA_TLS_KEYSTORE_B64": base64.b64encode(store_path.read_bytes()).decode("ascii"),
                        "KAFKA_TLS_KEYSTORE_PASSWORD": password,
                        "KAFKA_TLS_CA_CERT_B64": base64.b64encode(ca_path.read_bytes()).decode("ascii"),
                    }
                    forwarded_startup = ("KAFKA_PRIVATE_HOST", "KAFKA_PUBLIC_LISTENER_ENABLED", "KAFKA_SASL_USERNAME",
                                         "KAFKA_SASL_PASSWORD", "KAFKA_TLS_KEYSTORE_B64", "KAFKA_TLS_KEYSTORE_PASSWORD",
                                         "KAFKA_TLS_CA_CERT_B64")
                    result = subprocess.run(["docker", "run", "--rm",
                                             *[item for name in forwarded_startup for item in ("-e", name)], IMAGE], env=environment,
                                            capture_output=True, text=True, timeout=30)
                    self.assertEqual(result.returncode, 78, result.stdout + result.stderr)
                    self.assertIn("TLS certificate must chain", result.stderr)
                    self.assertNotIn(password, result.stdout + result.stderr)

                assert_startup_rejected(p12, wrong_ca_path)
                assert_startup_rejected(mismatch_p12, ca_cert)
            finally:
                broker.terminate()
                try:
                    broker.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    broker.kill()
                    broker.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()

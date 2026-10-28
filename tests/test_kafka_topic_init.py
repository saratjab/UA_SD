from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "kafka-init-topics.sh"
TOPICS = ("wm.activation.requests", "wm.ws.commands", "wm.ws.events", "wm.operator.events")
ROOT = Path(__file__).resolve().parents[1]


FAKE_CLI = r'''#!/usr/bin/env python3
import os, pathlib, sys
args = sys.argv[1:]
topic = args[args.index("--topic") + 1]
path = pathlib.Path(os.environ["FAKE_TOPIC_STATE"]) / topic
if "--describe" in args:
    if not path.exists(): sys.exit(1)
    print(f"Topic: {topic}\tPartitionCount: {path.read_text()}\tReplicationFactor: 1")
elif "--create" in args:
    path.write_text(args[args.index("--partitions") + 1])
    with open(os.environ["FAKE_TOPIC_LOG"], "a") as out: out.write(f"create {topic} {path.read_text()} {args[args.index('--replication-factor') + 1]}\n")
elif "--alter" in args:
    path.write_text(args[args.index("--partitions") + 1])
    with open(os.environ["FAKE_TOPIC_LOG"], "a") as out: out.write(f"alter {topic} {path.read_text()}\n")
if "--command-config" in args:
    config = pathlib.Path(args[args.index("--command-config") + 1]).read_text()
    with open(os.environ["FAKE_TOPIC_LOG"], "a") as out: out.write("SECURE_CONFIG " + config.replace("\n", "|") + "\n")
'''


class KafkaTopicInitTests(unittest.TestCase):
    def run_initializer(self, root: Path, *, partitions: int | None, replication: int | None) -> list[str]:
        cli = root / "fake-kafka-topics"
        cli.write_text(FAKE_CLI, encoding="utf-8")
        cli.chmod(0o755)
        log = root / "calls.log"
        env = os.environ | {
            "KAFKA_TOPICS_BIN": str(cli), "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
            "FAKE_TOPIC_STATE": str(root / "state"), "FAKE_TOPIC_LOG": str(log),
        }
        if partitions is not None:
            env["KAFKA_PARTITIONS"] = str(partitions)
        else:
            env.pop("KAFKA_PARTITIONS", None)
        if replication is not None:
            env["KAFKA_REPLICATION_FACTOR"] = str(replication)
        else:
            env.pop("KAFKA_REPLICATION_FACTOR", None)
        (root / "state").mkdir(exist_ok=True)
        subprocess.run(["bash", str(SCRIPT)], check=True, env=env, capture_output=True, text=True)
        return log.read_text(encoding="utf-8").splitlines() if log.exists() else []

    def test_creates_all_descriptive_topics_with_configured_partitions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calls = self.run_initializer(root, partitions=4, replication=2)
            self.assertEqual(len(calls), 4)
            self.assertEqual({line.split()[1] for line in calls}, set(TOPICS))
            self.assertTrue(all(line.endswith("4 2") for line in calls))

    def test_defaults_to_three_partitions_and_single_broker_replication(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calls = self.run_initializer(root, partitions=None, replication=None)
            self.assertTrue(all(line.endswith("3 1") for line in calls))

    def test_increases_existing_topics_without_deleting_or_reducing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            (state / TOPICS[0]).write_text("1")
            (state / TOPICS[1]).write_text("3")
            (state / TOPICS[2]).write_text("5")
            calls = self.run_initializer(root, partitions=3, replication=1)
            self.assertIn(f"alter {TOPICS[0]} 3", calls)
            self.assertNotIn(f"alter {TOPICS[1]} 3", calls)
            self.assertFalse(any(line.startswith(f"alter {TOPICS[2]} ") for line in calls))
            self.assertIn(f"create {TOPICS[3]} 3 1", calls)
            self.assertEqual((state / TOPICS[2]).read_text(), "5")

    def test_kafka_38_server_properties_define_three_partition_single_broker_defaults(self) -> None:
        properties = ROOT / "config" / "kafka" / "server.properties"
        values = dict(
            line.split("=", 1) for line in properties.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        )
        self.assertEqual(values["num.partitions"], "3")
        self.assertEqual(values["default.replication.factor"], "1")
        self.assertEqual(values["process.roles"], "broker,controller")
        self.assertEqual(values["controller.listener.names"], "CONTROLLER")
        self.assertIn("INTERNAL://", values["listeners"])
        self.assertIn("HOST://", values["advertised.listeners"])

    def test_both_compose_files_mount_properties_and_configurable_replication(self) -> None:
        for filename in ("docker-compose.kafka.yml", "docker-compose.local.yml"):
            content = (ROOT / filename).read_text(encoding="utf-8")
            with self.subTest(compose=filename):
                self.assertIn("./config/kafka:/mnt/shared/config:ro", content)
                self.assertIn("KAFKA_DEFAULT_REPLICATION_FACTOR", content)
                self.assertIn("${KAFKA_REPLICATION_FACTOR:-1}", content)
                self.assertIn("KAFKA_BROKER_NAME:-kafka", content)
                self.assertIn("KAFKA_INTERNAL_HOST", content)
                self.assertIn("KAFKA_EXTERNAL_HOST", content)
                self.assertIn("KAFKA_EXTERNAL_PORT", content)
                if filename == "docker-compose.local.yml":
                    self.assertIn("KAFKA_REQUIRE_SECURITY", content)
        local = (ROOT / "docker-compose.local.yml").read_text(encoding="utf-8")
        self.assertIn("ws1-engine-state:/engine-state", local)
        self.assertIn("ws2-engine-state:/engine-state", local)
        self.assertIn("/engine-state/.wm_ws_e_${WS1_ID:-WS_001}.sqlite3", local)
        self.assertIn("/engine-state/.wm_ws_e_${WS2_ID:-WS_002}.sqlite3", local)

    def test_railway_kafka_image_and_template_keep_internal_and_tcp_proxy_listeners_configurable(self) -> None:
        dockerfile = (ROOT / "Dockerfile.kafka").read_text(encoding="utf-8")
        template = (ROOT / "config" / "kafka" / "railway.env.example").read_text(encoding="utf-8")
        railway = (ROOT / "config" / "kafka" / "server.railway.properties").read_text(encoding="utf-8")
        self.assertIn("FROM apache/kafka:3.8.0", dockerfile)
        self.assertIn("COPY config/kafka/server.railway.properties /mnt/shared/config/server.properties", dockerfile)
        self.assertIn("RUN javac -d /classes /src/VerifyKafkaCertificate.java", dockerfile)
        self.assertIn("COPY --from=cert-verifier-build /classes/VerifyKafkaCertificate.class", dockerfile)
        self.assertIn("SASL_SSL", template)
        self.assertIn("SASL_SSL", railway)
        self.assertIn("KAFKA_PUBLIC_HOST", template)
        self.assertIn("KAFKA_TLS_KEYSTORE_B64", template)
        self.assertIn("KAFKA_PUBLIC_LISTENER_ENABLED=true", template)
        self.assertIn("KAFKA_REQUIRE_SECURITY=true", (ROOT / "docs" / "railway-deployment.md").read_text())
        self.assertIn("CA:TRUE", (ROOT / "docs" / "railway-deployment.md").read_text())
        self.assertIn("exact", (ROOT / "docs" / "railway-deployment.md").read_text())

    def test_secure_topic_initializer_guard_rejects_missing_or_insecure_protocol(self) -> None:
        for env in (
            {"KAFKA_REQUIRE_SECURITY": "true"},
            {"KAFKA_REQUIRE_SECURITY": "true", "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT"},
            {"KAFKA_REQUIRE_SECURITY": "true", "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT"},
        ):
            result = subprocess.run(["bash", str(SCRIPT)], env=os.environ | env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("KAFKA_REQUIRE_SECURITY=true requires KAFKA_SECURITY_PROTOCOL=SASL_SSL", result.stderr)
        template = (ROOT / "config" / "kafka" / "railway.env.example").read_text(encoding="utf-8")
        self.assertIn("KAFKA_NUM_PARTITIONS=3", template)
        self.assertIn("KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093", template)

    def test_railway_startup_refuses_to_run_without_tls_credentials_and_listener_addresses(self) -> None:
        script = ROOT / "scripts" / "kafka-railway-entrypoint.sh"
        result = subprocess.run(["bash", str(script)], text=True, capture_output=True, env=os.environ | {"PATH": "/usr/bin:/bin"})
        self.assertEqual(result.returncode, 78)
        self.assertIn("KAFKA_PRIVATE_HOST is required", result.stderr)
        self.assertNotIn("password", result.stderr.lower())

    def test_railway_startup_rejects_plaintext_public_listener_configuration(self) -> None:
        script = ROOT / "scripts" / "kafka-railway-entrypoint.sh"
        env = os.environ | {
            "KAFKA_PRIVATE_HOST": "broker.railway.internal", "KAFKA_PUBLIC_HOST": "proxy.example.test",
            "KAFKA_PUBLIC_PORT": "12345", "KAFKA_PUBLIC_LISTENER_ENABLED": "true", "KAFKA_SASL_USERNAME": "wm-client",
            "KAFKA_SASL_PASSWORD": "P" * 40, "KAFKA_TLS_KEYSTORE_B64": "secret",
            "KAFKA_TLS_KEYSTORE_PASSWORD": "K" * 20, "KAFKA_TLS_CA_CERT_B64": "cert",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "INTERNAL:PLAINTEXT,HOST:PLAINTEXT,CONTROLLER:PLAINTEXT",
        }
        result = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 78)
        self.assertIn("All enabled client listeners must use SASL_SSL", result.stderr)
        self.assertNotIn("P" * 40, result.stderr)

    def test_railway_startup_requires_keystore_ca_and_sasl_secrets(self) -> None:
        script = ROOT / "scripts" / "kafka-railway-entrypoint.sh"
        base = {
            "KAFKA_PRIVATE_HOST": "broker.railway.internal", "KAFKA_PUBLIC_HOST": "proxy.example.test",
            "KAFKA_PUBLIC_PORT": "12345", "KAFKA_SASL_USERNAME": "wm-client",
            "KAFKA_SASL_PASSWORD": "P" * 40, "KAFKA_TLS_KEYSTORE_B64": "a2V5",
            "KAFKA_TLS_KEYSTORE_PASSWORD": "K" * 20,
        }
        for missing in ("KAFKA_TLS_KEYSTORE_B64", "KAFKA_TLS_KEYSTORE_PASSWORD", "KAFKA_TLS_CA_CERT_B64", "KAFKA_SASL_PASSWORD"):
            env = os.environ | base
            env.pop(missing, None)
            result = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 78, missing)
            self.assertIn(f"{missing} is required", result.stderr)

    def test_railway_public_listener_requires_allocated_hostname_and_port(self) -> None:
        script = ROOT / "scripts" / "kafka-railway-entrypoint.sh"
        base = os.environ | {
            "KAFKA_PRIVATE_HOST": "broker.railway.internal", "KAFKA_PUBLIC_LISTENER_ENABLED": "true",
            "KAFKA_SASL_USERNAME": "wm-client", "KAFKA_SASL_PASSWORD": "P" * 40,
            "KAFKA_TLS_KEYSTORE_B64": "a2V5", "KAFKA_TLS_KEYSTORE_PASSWORD": "K" * 20,
            "KAFKA_TLS_CA_CERT_B64": "cert",
        }
        for missing in ("KAFKA_PUBLIC_HOST", "KAFKA_PUBLIC_PORT"):
            env = dict(base)
            env.pop(missing, None)
            if missing == "KAFKA_PUBLIC_PORT":
                env["KAFKA_PUBLIC_HOST"] = "proxy.example.test"
            result = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 78, missing)
            self.assertIn(f"{missing} is required", result.stderr)

    def test_secure_initializer_requires_credentials_and_trust_material(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env = os.environ | {
                "KAFKA_REQUIRE_SECURITY": "true",
                "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
                "KAFKA_SASL_MECHANISM": "PLAIN",
                "KAFKA_SASL_USERNAME": "wm-client",
                "KAFKA_SASL_PASSWORD": "A" * 40,
                "KAFKA_TOPICS_BIN": "/does/not/need/to/run",
            }
            result = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("KAFKA_SSL_CA_PEM or KAFKA_SSL_CA_LOCATION", result.stderr)

    def test_secure_initializer_passes_sasl_ssl_java_client_config(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cli = root / "fake-kafka-topics"
            cli.write_text(FAKE_CLI, encoding="utf-8"); cli.chmod(0o755)
            keytool = root / "fake-keytool"
            keytool.write_text("#!/usr/bin/env bash\nwhile (($#)); do if [[ $1 == -keystore ]]; then touch \"$2\"; fi; shift; done\n", encoding="utf-8")
            keytool.chmod(0o755)
            log = root / "calls.log"
            env = os.environ | {
                "KAFKA_TOPICS_BIN": str(cli), "KAFKA_KEYTOOL_BIN": str(keytool),
                "KAFKA_BOOTSTRAP_SERVERS": "broker.example:9092",
                "KAFKA_REQUIRE_SECURITY": "true", "KAFKA_SECURITY_PROTOCOL": "SASL_SSL", "KAFKA_SASL_MECHANISM": "PLAIN",
                "KAFKA_SASL_USERNAME": "wm-client", "KAFKA_SASL_PASSWORD": "S" * 40,
                "KAFKA_SSL_CA_PEM": "-----BEGIN CERTIFICATE-----\nnot-real-for-fake-tool\n-----END CERTIFICATE-----",
                "FAKE_TOPIC_STATE": str(root / "state"), "FAKE_TOPIC_LOG": str(log),
            }
            (root / "state").mkdir()
            result = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            secure_lines = [line for line in log.read_text().splitlines() if line.startswith("SECURE_CONFIG")]
            self.assertEqual(len(secure_lines), 4)  # all topic creates use the secure config
            self.assertTrue(all("security.protocol=SASL_SSL" in line and "ssl.endpoint.identification.algorithm=https" in line for line in secure_lines))
            self.assertTrue(all("sasl.mechanism=PLAIN" in line and "wm-client" in line for line in secure_lines))
            self.assertNotIn("S" * 40, result.stdout + result.stderr)

    def test_initializer_never_deletes_topics_or_resets_offsets(self) -> None:
        script = SCRIPT.read_text(encoding="utf-8")
        self.assertNotIn("--delete", script)
        self.assertNotIn("--reset-offsets", script)

    def test_railway_instructions_use_allocated_endpoint_placeholders(self) -> None:
        for filename in ("docs/railway-deployment.md", "docs/local-docker-deployment.md"):
            content = (ROOT / filename).read_text(encoding="utf-8")
            with self.subTest(document=filename):
                self.assertNotIn("yamanote.proxy.rlwy.net", content)
                self.assertNotIn("iriguchi.proxy.rlwy.net", content)
                self.assertIn("<KAFKA_TCP_PROXY_HOST>", content)
                self.assertIn("<CENTRAL_TCP_PROXY_HOST>", content)

    def test_local_compose_publishes_admin_api_only_on_loopback_and_has_no_default_secret(self) -> None:
        content = (ROOT / "docker-compose.local.yml").read_text(encoding="utf-8")
        self.assertIn('"127.0.0.1:${CENTRAL_ADMIN_PUBLISHED_PORT:-8001}:${CENTRAL_ADMIN_PORT:-8001}"', content)
        self.assertIn('"${CENTRAL_ADMIN_TOKEN:-}"', content)
        self.assertIn('"${CENTRAL_ADMIN_PORT:-0}"', content)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import os
import threading
from types import SimpleNamespace
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

from common.kafka_contract import WS_COMMANDS
from WM_Central.admin_api import CentralAdminServer
from WM_Central.admin_client import AdminClientError, CentralAdminClient, _menu_command
from WM_Central.config import parse_args
from WM_Central.kafka_service import CentralKafkaService
from WM_Central.socket_server import CentralSocketServer
from WM_Central.state_manager import StateManager

from .helpers import DatabaseTestCase


class FakePublisher:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict, str | None]] = []
        self.fail = False

    def publish(self, topic: str, message: dict, *, key: str | None = None) -> None:
        if self.fail:
            raise RuntimeError("simulated broker outage")
        self.records.append((topic, message, key))


class CentralAdminApiTests(DatabaseTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.state = StateManager(self.db)
        self.state.register("WS_001", "North", object())
        self.state.register("WS_002", "South", object())
        self.publisher = FakePublisher()
        self.service = CentralKafkaService(self.db, self.state, self.publisher)  # type: ignore[arg-type]
        self.server = CentralAdminServer("127.0.0.1", 0, "very-secret-test-token", self.state, self.service)
        self.server.start()
        self.addCleanup(self.server.stop)
        self.base_url = f"http://127.0.0.1:{self.server.port}"

    def call(
        self,
        method: str,
        path: str,
        body: dict | None = None,
        *,
        token: str | None = "very-secret-test-token",
        idempotency_key: str | None = None,
        forwarded_proto: str | None = None,
    ) -> tuple[int, dict]:
        headers = {"Accept": "application/json"}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        if body is not None:
            headers["Content-Type"] = "application/json"
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        if forwarded_proto:
            headers["X-Forwarded-Proto"] = forwarded_proto
        data = json.dumps(body).encode() if body is not None else None
        request = Request(self.base_url + path, data=data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=3) as response:
                return response.status, json.loads(response.read())
        except HTTPError as exc:
            try:
                return exc.code, json.loads(exc.read())
            finally:
                exc.close()

    def post(self, body: dict, *, key: str | None = None) -> tuple[int, dict]:
        return self.call("POST", "/v1/control", body, idempotency_key=key or str(uuid4()))

    def test_missing_and_invalid_credentials_are_rejected_without_reflection(self) -> None:
        with self.assertLogs("wm_central.admin_api", level="INFO") as captured:
            for token in (None, "wrong-secret"):
                code, response = self.call("GET", "/v1/stations", token=token)
                self.assertEqual(code, 401)
                self.assertEqual(response, {"error": "unauthorized"})
                self.assertNotIn("very-secret-test-token", json.dumps(response))
        self.assertNotIn("very-secret-test-token", "\n".join(captured.output))

    def test_https_proxy_header_is_required_when_configured(self) -> None:
        self.server.stop()
        self.server = CentralAdminServer(
            "127.0.0.1", 0, "very-secret-test-token", self.state, self.service,
            require_forwarded_https=True,
        )
        self.server.start()
        self.addCleanup(self.server.stop)
        self.base_url = f"http://127.0.0.1:{self.server.port}"
        code, response = self.call("GET", "/v1/stations", forwarded_proto=None)
        self.assertEqual((code, response), (403, {"error": "https_required"}))
        code, response = self.call("GET", "/v1/stations", forwarded_proto="http")
        self.assertEqual((code, response), (403, {"error": "https_required"}))
        code, _ = self.call("GET", "/v1/stations", forwarded_proto="https")
        self.assertEqual(code, 200)

    def test_station_socket_and_admin_http_listeners_bind_separate_ports(self) -> None:
        socket_server = CentralSocketServer("127.0.0.1", 0, self.state)
        admin_server = CentralAdminServer(
            "127.0.0.1", 0, "very-secret-test-token", self.state, self.service,
            require_forwarded_https=True,
        )
        socket_server.start()
        admin_server.start()
        self.addCleanup(socket_server.stop)
        self.addCleanup(admin_server.stop)
        self.assertGreater(socket_server.port, 0)
        self.assertGreater(admin_server.port, 0)
        self.assertNotEqual(socket_server.port, admin_server.port)

    def test_status_returns_current_station_rows_only_when_authenticated(self) -> None:
        code, response = self.call("GET", "/v1/stations")
        self.assertEqual(code, 200)
        self.assertEqual([row["station_id"] for row in response["stations"]], ["WS_001", "WS_002"])
        self.assertEqual(response["stations"][0]["status"], "AVAILABLE")

    def test_invalid_action_station_mode_and_unknown_fields_are_rejected(self) -> None:
        cases = [
            ({"action": "restart", "station_id": "WS_001"}, 400),
            ({"action": "block", "station_id": "WS_NOPE"}, 404),
            ({"action": "start", "station_id": "WS_001", "duration_seconds": 3, "target_volume_liters": 2}, 400),
            ({"action": "start", "station_id": "WS_001", "duration_seconds": 0}, 400),
            ({"action": "block", "station_id": "WS_001", "duration_seconds": 3}, 400),
        ]
        for command, expected in cases:
            with self.subTest(command=command):
                code, _ = self.post(command)
                self.assertEqual(code, expected)
        code, response = self.post({"action": "block", "station_id": "very-secret-test-token"})
        self.assertEqual(code, 404)
        self.assertNotIn("very-secret-test-token", json.dumps(response))
        code, response = self.call(
            "POST", "/v1/control", {"action": "block", "station_id": "WS_001"}, idempotency_key=None
        )
        self.assertEqual((code, response["error"]), (400, "Idempotency-Key must be a UUID"))

    def test_station_and_all_station_block_activate_controls_use_central_logic(self) -> None:
        code, response = self.post({"action": "block", "station_id": "WS_001"})
        self.assertEqual((code, response["results"][0]["accepted"]), (200, True))
        self.assertEqual(self.db.get_station("WS_001").admin_state.value, "BLOCKED")  # type: ignore[union-attr]

        code, response = self.post({"action": "block", "station_id": "all"})
        self.assertEqual(code, 200)
        self.assertEqual(len(response["results"]), 2)
        self.assertTrue(all(row.admin_state.value == "BLOCKED" for row in self.db.list_stations()))

        code, response = self.post({"action": "activate", "station_id": "WS_001"})
        self.assertEqual((code, response["results"][0]["accepted"]), (200, True))
        code, response = self.post({"action": "activate", "station_id": "all"})
        self.assertEqual(code, 200)
        self.assertEqual(len(response["results"]), 2)
        activate_messages = [message for topic, message, _key in self.publisher.records
                            if topic == WS_COMMANDS and message["event_type"] == "ACTIVATE_WS"]
        self.assertEqual({message["ws_id"] for message in activate_messages}, {"WS_001", "WS_002"})

    def test_activate_disconnected_station_is_refused(self) -> None:
        self.state.disconnect("WS_002", self.state._connections["WS_002"])
        code, response = self.post({"action": "activate", "station_id": "WS_002"})
        self.assertEqual(code, 409)
        self.assertFalse(response["results"][0]["accepted"])

    def test_duration_target_volume_and_manual_start_use_durable_activation_flow(self) -> None:
        code, duration = self.post({"action": "start", "station_id": "WS_001", "duration_seconds": 8})
        self.assertEqual(code, 200)
        duration_record = self.db.get_irrigation(duration["results"][0]["request_id"])
        self.assertEqual(duration_record.requested_duration_s, 8)  # type: ignore[union-attr]

        code, volume = self.post({"action": "start", "station_id": "WS_002", "target_volume_liters": 2.5})
        self.assertEqual(code, 200)
        volume_record = self.db.get_irrigation(volume["results"][0]["request_id"])
        self.assertEqual(volume_record.requested_volume_l, 2.5)  # type: ignore[union-attr]
        command = next(message for topic, message, _key in self.publisher.records
                       if topic == WS_COMMANDS and message["request_id"] == volume_record.request_id)  # type: ignore[union-attr]
        self.assertEqual(command["payload"], {"target_volume_liters": 2.5})

        self.state.register("WS_003", "East", object())
        code, manual = self.post({"action": "start", "station_id": "WS_003"})
        self.assertEqual(code, 200)
        manual_record = self.db.get_irrigation(manual["results"][0]["request_id"])
        self.assertIsNone(manual_record.requested_duration_s)  # type: ignore[union-attr]
        self.assertIsNone(manual_record.requested_volume_l)  # type: ignore[union-attr]
        manual_command = next(message for topic, message, _key in self.publisher.records
                              if topic == WS_COMMANDS and message["request_id"] == manual_record.request_id)  # type: ignore[union-attr]
        self.assertEqual(manual_command["payload"], {})

    def test_start_all_selects_only_available_stations(self) -> None:
        self.state.disconnect("WS_002", self.state._connections["WS_002"])
        code, response = self.post({"action": "start", "station_id": "all", "duration_seconds": 5})
        self.assertEqual(code, 200)
        self.assertEqual([row["station_id"] for row in response["results"]], ["WS_001"])

    def test_stop_active_irrigation_uses_matching_request_id(self) -> None:
        code, started = self.post({"action": "start", "station_id": "WS_001", "duration_seconds": 60})
        self.assertEqual(code, 200)
        request_id = started["results"][0]["request_id"]
        code, stopped = self.post({"action": "stop", "station_id": "WS_001"})
        self.assertEqual(code, 200)
        self.assertEqual(stopped["results"][0]["request_id"], request_id)
        command = next(message for topic, message, _key in self.publisher.records
                       if topic == WS_COMMANDS and message["event_type"] == "STOP_IRRIGATION")
        self.assertEqual(command["request_id"], request_id)

        second_code, _ = self.post({"action": "start", "station_id": "WS_002", "duration_seconds": 60})
        self.assertEqual(second_code, 200)
        all_code, all_stopped = self.post({"action": "stop", "station_id": "all"})
        self.assertEqual(all_code, 200)
        self.assertEqual({row["station_id"] for row in all_stopped["results"]}, {"WS_001", "WS_002"})

    def test_admin_commands_are_committed_to_existing_outbox_and_replay_stably(self) -> None:
        self.publisher.fail = True
        key = str(uuid4())
        with self.assertLogs("WM_Central.kafka_service", level="ERROR"):
            first = self.post({"action": "block", "station_id": "WS_001"}, key=key)
            second = self.post({"action": "block", "station_id": "WS_001"}, key=key)
        self.assertEqual(first[0], 200)
        self.assertEqual(second[0], 200)
        pending = [row for row in self.db.pending_outgoing() if row[1] == WS_COMMANDS]
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0][2], "WS_001")
        self.assertEqual(pending[0][3]["event_type"], "BLOCK_WS")

    def test_repeated_start_with_same_idempotency_key_reuses_request(self) -> None:
        command = {"action": "start", "station_id": "WS_001", "duration_seconds": 15}
        key = str(uuid4())
        first_code, first = self.post(command, key=key)
        second_code, second = self.post(command, key=key)
        self.assertEqual((first_code, second_code), (200, 200))
        self.assertEqual(first["results"][0]["request_id"], second["results"][0]["request_id"])
        start_commands = [m for topic, m, _key in self.publisher.records
                         if topic == WS_COMMANDS and m["event_type"] == "START_IRRIGATION"]
        self.assertEqual(len(start_commands), 1)

    def test_parallel_control_requests_do_not_block_or_corrupt_central_state(self) -> None:
        def control(index: int) -> int:
            action = "block" if index % 2 else "activate"
            return self.post({"action": action, "station_id": "WS_001"})[0]

        with ThreadPoolExecutor(max_workers=8) as pool:
            codes = list(pool.map(control, range(16)))
        self.assertTrue(all(code == 200 for code in codes))
        self.assertIn(self.db.get_station("WS_001").admin_state.value, {"ACTIVE", "BLOCKED"})  # type: ignore[union-attr]

    def test_admin_api_operates_without_a_terminal(self) -> None:
        with mock.patch("WM_Central.monitoring.sys.stdin") as stdin:
            stdin.isatty.return_value = False
            status, result = self.call("GET", "/v1/stations")
        self.assertEqual(status, 200)
        self.assertEqual(len(result["stations"]), 2)

    def test_central_main_starts_and_shuts_down_cleanly_without_tty(self) -> None:
        from WM_Central import main as central_main

        config = SimpleNamespace(
            db_path=":memory:", seed_dev=False, log_level="INFO", kafka_host="localhost", kafka_port=9092,
            socket_host="0.0.0.0", socket_port=5000, admin_host="0.0.0.0", admin_port=8080,
            admin_token="secret-test-token", admin_require_forwarded_https=True,
        )
        fake_db = mock.Mock()
        fake_state = object()
        activation, events = mock.Mock(), mock.Mock()
        fake_service = mock.Mock(activation_worker=mock.Mock(return_value=activation), event_worker=mock.Mock(return_value=events))
        fake_server = mock.Mock()

        class StopImmediately:
            def wait(self, _timeout: float) -> bool:
                return True

            def set(self) -> None:
                pass

        with mock.patch.object(central_main, "parse_args", return_value=config), \
             mock.patch.object(central_main, "configure_logging"), \
             mock.patch.object(central_main, "Database", return_value=fake_db), \
             mock.patch.object(central_main, "StateManager", return_value=fake_state), \
             mock.patch.object(central_main, "log_station_table"), \
             mock.patch.object(central_main, "KafkaPublisher", return_value=object()), \
             mock.patch.object(central_main, "CentralKafkaService", return_value=fake_service), \
             mock.patch.object(central_main, "CentralSocketServer", return_value=fake_server), \
             mock.patch.object(central_main, "CentralAdminServer") as fake_admin_ctor, \
             mock.patch.object(central_main.threading, "Event", return_value=StopImmediately()), \
             mock.patch.object(central_main.threading, "Thread") as thread_ctor, \
             mock.patch("WM_Central.monitoring.sys.stdin") as stdin:
            stdin.isatty.return_value = False
            self.assertEqual(central_main.main([]), 0)

        fake_server.start.assert_called_once()
        fake_server.stop.assert_called_once()
        fake_admin_ctor.assert_called_once_with(
            "0.0.0.0", 8080, "secret-test-token", fake_state, fake_service,
            require_forwarded_https=True,
        )
        fake_admin_ctor.return_value.start.assert_called_once()
        fake_admin_ctor.return_value.stop.assert_called_once()
        fake_db.close.assert_called_once()
        activation.close.assert_called_once()
        events.close.assert_called_once()
        # Monitor and outbox workers are separate from stdin and still created.
        self.assertEqual(thread_ctor.call_count, 4)

    def test_listener_startup_failure_is_logged_and_database_closes(self) -> None:
        from WM_Central import main as central_main

        config = SimpleNamespace(
            db_path=":memory:", seed_dev=False, log_level="INFO", kafka_host="localhost", kafka_port=9092,
            socket_host="0.0.0.0", socket_port=5000, admin_host="0.0.0.0", admin_port=0,
            admin_token=None, admin_require_forwarded_https=True,
        )
        fake_db, activation, events = mock.Mock(), mock.Mock(), mock.Mock()
        fake_service = mock.Mock(activation_worker=mock.Mock(return_value=activation), event_worker=mock.Mock(return_value=events))
        fake_server = mock.Mock()
        fake_server.start.side_effect = OSError("port already in use")

        class StopEvent:
            def wait(self, _timeout: float) -> bool: return False
            def set(self) -> None: pass

        with mock.patch.object(central_main, "parse_args", return_value=config), \
             mock.patch.object(central_main, "configure_logging"), \
             mock.patch.object(central_main, "Database", return_value=fake_db), \
             mock.patch.object(central_main, "StateManager", return_value=object()), \
             mock.patch.object(central_main, "log_station_table"), \
             mock.patch.object(central_main, "KafkaPublisher", return_value=object()), \
             mock.patch.object(central_main, "CentralKafkaService", return_value=fake_service), \
             mock.patch.object(central_main, "CentralSocketServer", return_value=fake_server), \
             mock.patch.object(central_main.threading, "Event", return_value=StopEvent()), \
             mock.patch.object(central_main.threading, "Thread"), \
             mock.patch("WM_Central.monitoring.sys.stdin") as stdin, \
             self.assertLogs("wm_central", level="ERROR") as captured:
            stdin.isatty.return_value = False
            with self.assertRaisesRegex(OSError, "port already in use"):
                central_main.main([])
        self.assertTrue(any("listener startup or service loop failed" in line for line in captured.output))
        fake_server.stop.assert_called_once()
        fake_db.close.assert_called_once()

    def test_unexpected_control_failure_returns_generic_error(self) -> None:
        with mock.patch.object(self.service, "issue_station_command", side_effect=RuntimeError("private details")):
            with self.assertLogs("wm_central.admin_api", level="ERROR"):
                code, response = self.post({"action": "block", "station_id": "WS_001"})
        self.assertEqual((code, response), (500, {"error": "internal_error"}))
        self.assertNotIn("private details", json.dumps(response))

    def test_http_client_rejects_non_tls_remote_urls_and_menu_supports_modes(self) -> None:
        with self.assertRaisesRegex(ValueError, "HTTPS"):
            CentralAdminClient("http://central.example.com", "token")
        client = CentralAdminClient("http://127.0.0.1:8001", "token")
        self.assertEqual(client.base_url, "http://127.0.0.1:8001")
        values = iter(["WS_001", "volume", "3.5", ""])
        self.assertEqual(_menu_command("2", lambda _prompt: next(values)), {
            "action": "start", "station_id": "WS_001", "target_volume_liters": 3.5,
        })
        self.assertEqual(_menu_command("9", lambda _prompt: "unused"), {"action": "stop", "station_id": "all"})

    def test_admin_port_requires_explicit_token_and_accepts_configured_values(self) -> None:
        args = ["--socket-port", "5000", "--kafka-host", "localhost", "--kafka-port", "9092", "--admin-host", "127.0.0.1", "--admin-port", "8001"]
        with mock.patch.dict(os.environ, {}, clear=True), self.assertRaises(SystemExit):
            parse_args(args)
        with mock.patch.dict(os.environ, {"CENTRAL_ADMIN_TOKEN": "secret"}, clear=True):
            config = parse_args(args)
        self.assertEqual((config.admin_host, config.admin_port), ("127.0.0.1", 8001))
        self.assertTrue(config.admin_require_forwarded_https)

    def test_railway_environment_configures_both_listener_ports_and_database(self) -> None:
        with mock.patch.dict(os.environ, {
            "KAFKA_HOST": "<private-kafka-host>",
            "KAFKA_PORT": "29092",
            "DB_PATH": "/data/wm_central.db",
            "PORT": "8080",
            "CENTRAL_ADMIN_TOKEN": "configured-secret",
            "CENTRAL_ADMIN_REQUIRE_HTTPS": "true",
        }, clear=True):
            config = parse_args([])
        self.assertEqual((config.socket_host, config.socket_port), ("0.0.0.0", 5000))
        self.assertEqual((config.admin_host, config.admin_port), ("0.0.0.0", 8080))
        self.assertEqual((config.kafka_host, config.kafka_port), ("<private-kafka-host>", 29092))
        self.assertEqual(config.db_path, "/data/wm_central.db")
        self.assertTrue(config.admin_require_forwarded_https)

    def test_central_socket_port_can_be_overridden_and_missing_kafka_host_fails(self) -> None:
        with mock.patch.dict(os.environ, {"KAFKA_HOST": "localhost", "CENTRAL_SOCKET_PORT": "5100"}, clear=True):
            self.assertEqual(parse_args([]).socket_port, 5100)
        with mock.patch.dict(os.environ, {}, clear=True), self.assertRaises(SystemExit):
            parse_args([])


if __name__ == "__main__":
    unittest.main()

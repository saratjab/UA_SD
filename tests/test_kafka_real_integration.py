"""Opt-in real-broker Kafka lifecycle smoke test (never uses a mock broker)."""
from __future__ import annotations

import os
import socket
import tempfile
import threading
import time
import unittest
from uuid import uuid4

from common.kafka_contract import ACTIVATION_REQUESTS, WS_COMMANDS, WS_EVENTS, envelope
from common.kafka_transport import KafkaPublisher, KafkaWorker, kafka_security_config
from WM_Central.database import Database
from WM_Central.kafka_service import CentralKafkaService
from WM_Central.models import IrrigationStatus
from WM_Central.state_manager import StateManager
from WM_WS.WM_WS_E.config import EngineConfig
from WM_WS.WM_WS_E.engine import WateringStationEngine
from WM_WS.WM_WS_E.kafka_service import EngineKafkaService
from WM_WS.WM_WS_M.ws_e_server import EngineServer


@unittest.skipUnless(os.environ.get("RUN_KAFKA_INTEGRATION") == "1", "set RUN_KAFKA_INTEGRATION=1 with docker-compose.kafka.yml running")
class RealKafkaLifecycleTests(unittest.TestCase):
    def test_activation_to_completion(self) -> None:
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
        with tempfile.TemporaryDirectory() as temp:
            suffix = uuid4().hex
            ws_id, operator_id = f"WS_REAL_{suffix[:8]}", f"OP_REAL_{suffix[:8]}"
            request_id = f"real-{suffix}"
            db = Database(f"{temp}/central.db"); db.initialize(); db.add_operator(operator_id)
            state = StateManager(db); state.register(ws_id, "test", object())
            central_publisher, engine_publisher = KafkaPublisher(bootstrap), KafkaPublisher(bootstrap)
            central = CentralKafkaService(db, state, central_publisher)
            monitor_server = EngineServer("127.0.0.1", 0, ws_id, hello_timeout=2)
            monitor_server.start()
            monitor_port = monitor_server.server_sock.getsockname()[1]  # type: ignore[union-attr]
            config = EngineConfig(ws_id, "127.0.0.1", 9092, "127.0.0.1", monitor_port, 6, 1, .1, .05)
            engine = WateringStationEngine(config)
            engine_service = EngineKafkaService(engine, engine_publisher)
            accepted_sockets: list[socket.socket] = []
            accept_errors: list[BaseException] = []

            def accept_monitor_session() -> None:
                try:
                    accepted_sockets.append(monitor_server.accept_engine())
                except BaseException as exc:
                    accept_errors.append(exc)

            engine.connect_to_monitor()
            monitor_thread = threading.Thread(target=accept_monitor_session, daemon=True)
            monitor_thread.start()
            self.assertTrue(engine.send_hello(), "Engine did not complete the Monitor HELLO_ACK handshake")
            monitor_thread.join(timeout=3)
            self.assertFalse(monitor_thread.is_alive(), "Monitor did not finish accepting the Engine session")
            self.assertFalse(accept_errors, f"Monitor failed to accept the Engine session: {accept_errors!r}")
            self.assertTrue(engine.monitor_available)
            activation_worker = KafkaWorker(
                bootstrap, f"wm-test-activation-{suffix}", [ACTIVATION_REQUESTS],
                central.handle_activation_bytes, auto_offset_reset="latest",
            )
            event_worker = KafkaWorker(
                bootstrap, f"wm-test-events-{suffix}", [WS_EVENTS],
                central.handle_engine_bytes, auto_offset_reset="latest",
            )
            command_worker = KafkaWorker(
                bootstrap, f"wm-test-engine-{suffix}", [WS_COMMANDS],
                engine_service.handle_bytes, auto_offset_reset="latest",
            )
            workers = (activation_worker, event_worker, command_worker)
            for worker in workers:
                worker.start()
            request_publisher = KafkaPublisher(bootstrap)
            try:
                assignment_deadline = time.monotonic() + 15
                for worker in workers:
                    remaining = max(0.0, assignment_deadline - time.monotonic())
                    self.assertTrue(worker.wait_until_assigned(remaining), "Kafka worker did not receive a partition assignment")

                request_publisher.publish(ACTIVATION_REQUESTS, envelope("ACTIVATION_REQUEST", {"duration_seconds": 1}, ws_id=ws_id, operator_id=operator_id, request_id=request_id))
                deadline = time.monotonic() + 15
                saw_live = False
                completed = False
                while time.monotonic() < deadline:
                    activation_worker.poll_once(.1)
                    command_worker.poll_once(.1)
                    event_worker.poll_once(.1)
                    live = db.get_live_irrigation(ws_id)
                    if live is not None:
                        saw_live = True
                    elif saw_live:
                        record = db.get_irrigation(request_id)
                        completed = record is not None and record.status is IrrigationStatus.ENDED
                        break

                record = db.get_irrigation(request_id)
                self.assertTrue(saw_live, f"irrigation never appeared as live (record={record!r})")
                self.assertTrue(completed, f"timed out waiting for completion (record={record!r})")
                self.assertIsNotNone(record, "request was not recorded")
                self.assertEqual(record.status, IrrigationStatus.ENDED)  # type: ignore[union-attr]
                self.assertIsNotNone(record.started_at)  # type: ignore[union-attr]
                self.assertIsNotNone(record.ended_at)  # type: ignore[union-attr]
                self.assertIsNone(db.get_live_irrigation(ws_id))
            finally:
                request_publisher.close()
                for worker in workers:
                    worker.close()
                engine.close()
                for conn in accepted_sockets:
                    conn.close()
                monitor_server.close()
                if monitor_thread.is_alive():
                    monitor_thread.join(timeout=3)
                central_publisher.close()
                engine_publisher.close()
                db.close()


@unittest.skipUnless(os.environ.get("RUN_KAFKA_INTEGRATION") == "1", "set RUN_KAFKA_INTEGRATION=1 with a Kafka broker running")
class RealKafkaPartitioningTests(unittest.TestCase):
    def test_same_key_ordering_and_multiple_partitions(self) -> None:
        from confluent_kafka import Consumer, Producer
        from confluent_kafka.admin import AdminClient, NewTopic

        bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
        suffix = uuid4().hex
        topic = f"wm.partitioning.test.{suffix}"
        security = kafka_security_config()
        admin = AdminClient({"bootstrap.servers": bootstrap, **security})
        admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[topic].result(timeout=15)
        producer = Producer({"bootstrap.servers": bootstrap, **security, "enable.idempotence": True})
        keys = [f"station-{i}-{suffix}" for i in range(24)]
        expected: dict[str, list[int]] = {key: [0, 1, 2] for key in keys}
        for sequence in range(3):
            for key in keys:
                producer.produce(topic, str(sequence).encode(), key=key.encode())
        self.assertEqual(producer.flush(15), 0)

        consumer = Consumer({
            "bootstrap.servers": bootstrap, "group.id": f"wm-partition-test-{suffix}",
            "enable.auto.commit": False, "auto.offset.reset": "earliest",
            **security,
        })
        consumer.subscribe([topic])
        seen: dict[str, list[int]] = {key: [] for key in keys}
        partitions_by_key: dict[str, set[int]] = {key: set() for key in keys}
        deadline = time.monotonic() + 15
        try:
            while time.monotonic() < deadline and sum(map(len, seen.values())) < 3 * len(keys):
                message = consumer.poll(.2)
                if message is None or message.error():
                    continue
                key = message.key().decode()
                seen[key].append(int(message.value().decode()))
                partitions_by_key[key].add(message.partition())
        finally:
            consumer.close()
        self.assertEqual(seen, expected)
        self.assertTrue(all(len(parts) == 1 for parts in partitions_by_key.values()))
        self.assertGreater(len({next(iter(parts)) for parts in partitions_by_key.values()}), 1)

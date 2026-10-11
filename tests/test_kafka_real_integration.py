"""Opt-in real-broker Kafka lifecycle smoke test (never uses a mock broker)."""
from __future__ import annotations

import os
import tempfile
import time
import unittest

from common.kafka_contract import ACTIVATION_REQUESTS, envelope
from common.kafka_transport import KafkaPublisher
from WM_Central.database import Database
from WM_Central.kafka_service import CentralKafkaService
from WM_Central.state_manager import StateManager
from WM_WS.WM_WS_E.config import EngineConfig
from WM_WS.WM_WS_E.engine import WateringStationEngine
from WM_WS.WM_WS_E.kafka_service import EngineKafkaService


@unittest.skipUnless(os.environ.get("RUN_KAFKA_INTEGRATION") == "1", "set RUN_KAFKA_INTEGRATION=1 with docker-compose.kafka.yml running")
class RealKafkaLifecycleTests(unittest.TestCase):
    def test_activation_to_completion(self) -> None:
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
        with tempfile.TemporaryDirectory() as temp:
            db = Database(f"{temp}/central.db"); db.initialize(); db.add_operator("OP_REAL")
            state = StateManager(db); state.register("WS_REAL", "test", object())
            central = CentralKafkaService(db, state, KafkaPublisher(bootstrap))
            config = EngineConfig("WS_REAL", "localhost", 9092, "localhost", 1, 6, 1, .1, .05)
            engine = WateringStationEngine(config)
            engine_service = EngineKafkaService(engine, KafkaPublisher(bootstrap))
            activation_worker, event_worker, command_worker = central.activation_worker(bootstrap), central.event_worker(bootstrap), engine_service.worker(bootstrap)
            for worker in (activation_worker, event_worker, command_worker): worker.start()
            try:
                KafkaPublisher(bootstrap).publish(ACTIVATION_REQUESTS, envelope("ACTIVATION_REQUEST", {"duration_seconds": 1}, ws_id="WS_REAL", operator_id="OP_REAL", request_id=f"real-{time.time_ns()}"))
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline and db.get_live_irrigation("WS_REAL") is None:
                    activation_worker.poll_once(.1); command_worker.poll_once(.1); event_worker.poll_once(.1)
                self.assertTrue(engine.irrigation.wait_until_finished(3))
                for _ in range(20): event_worker.poll_once(.1)
                self.assertIsNone(db.get_live_irrigation("WS_REAL"))
            finally:
                for worker in (activation_worker, event_worker, command_worker): worker.close()
                db.close()

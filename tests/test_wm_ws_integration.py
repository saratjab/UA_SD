from __future__ import annotations

import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path

from WM_Central.database import Database
from WM_Central.models import StationStatus
from WM_Central.socket_server import CentralSocketServer
from WM_Central.state_manager import StateManager
from WM_WS.WM_WS_E.config import EngineConfig
from WM_WS.WM_WS_E.engine import WateringStationEngine
from WM_WS.WM_WS_E.flow_meter import FlowMeter
from WM_WS.WM_WS_E.irrigation import IrrigationController
from WM_WS.WM_WS_E.valve import SolenoidValve
from WM_WS.WM_WS_M.central_client import CentralClient
from WM_WS.WM_WS_M.config import MonitorConfig, parse_args as parse_monitor_args
from WM_WS.WM_WS_M.monitor import WateringStationMonitor
from WM_WS.WM_WS_M.ws_e_server import EngineServer


def wait_until(predicate: object, timeout: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():  # type: ignore[operator]
            return True
        time.sleep(0.01)
    return bool(predicate())  # type: ignore[operator]


class WMWSIntegrationTests(unittest.TestCase):
    """Real Central socket service, Monitor, and Engine; no Engine doubles."""

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db = Database(str(Path(self.tempdir.name) / "central.db"))
        self.db.initialize()
        self.state = StateManager(self.db)
        self.central = CentralSocketServer("127.0.0.1", 0, self.state)
        self.central.start()

        self.engine_server = EngineServer("127.0.0.1", 0, "WS_INT", hello_timeout=0.2)
        self.engine_server.start()
        engine_port = self.engine_server.server_sock.getsockname()[1]  # type: ignore[union-attr]
        config = MonitorConfig(
            ws_id="WS_INT",
            location="Integration Garden",
            central_host="127.0.0.1",
            central_port=self.central.port,
            engine_host="127.0.0.1",
            engine_port=engine_port,
            engine_hello_timeout=0.2,
            health_interval=0.02,
            health_timeout=0.2,
        )
        self.monitor = WateringStationMonitor(
            config,
            CentralClient(config.central_host, config.central_port, config.ws_id, config.location),
            self.engine_server,
        )
        self.monitor_thread = threading.Thread(target=self.monitor.run, daemon=True)
        self.monitor_thread.start()

        self.valve = SolenoidValve()
        controller = IrrigationController("", self.valve, FlowMeter(60.0), telemetry_interval=0.02)
        self.engine = WateringStationEngine(
            EngineConfig(
                ws_id=None,
                kafka_host="unused",
                kafka_port=9092,
                monitor_host="127.0.0.1",
                monitor_port=engine_port,
                flow_rate_lpm=60.0,
                default_irrigation_duration=1.0,
                socket_timeout=0.1,
                telemetry_interval=0.02,
            ),
            irrigation_controller=controller,
        )
        self.engine_thread = threading.Thread(target=self.engine.run, daemon=True)
        self.engine_thread.start()

    def tearDown(self) -> None:
        self.engine.close()
        self.engine_thread.join(timeout=2)
        self.engine_server.close()
        self.monitor_thread.join(timeout=2)
        self.central.stop()
        self.db.close()
        self.tempdir.cleanup()

    def test_fault_lifecycle_and_engine_reconnection(self) -> None:
        self.assertTrue(wait_until(lambda: self.state.status_of("WS_INT") is StationStatus.AVAILABLE))
        view = self.state.snapshot()[0]
        self.assertEqual(view.location, "Integration Garden")  # registration carries location
        self.assertEqual(self.engine.config.ws_id, "WS_INT")  # HELLO_ACK assignment

        self.assertTrue(self.engine.request_irrigation("operator-1", duration_seconds=1.0))
        self.assertTrue(wait_until(lambda: self.valve.is_open))
        self.assertTrue(wait_until(lambda: bool(self.engine.irrigation.telemetry)))

        self.engine.simulate_failure()
        self.assertFalse(self.valve.is_open)
        self.assertTrue(self.engine.irrigation.wait_until_finished(timeout=2))
        self.assertEqual(self.engine.irrigation.last_result.stopped_by, "manual")  # type: ignore[union-attr]
        self.assertTrue(wait_until(lambda: self.state.status_of("WS_INT") is StationStatus.LEAK))
        self.assertTrue(self.monitor_thread.is_alive())  # fault does not terminate Monitor

        old_socket = self.engine.monitor_socket
        self.engine._disconnect_monitor()
        self.assertTrue(wait_until(lambda: self.engine.monitor_socket is not None and self.engine.monitor_socket is not old_socket))
        self.assertTrue(self.monitor_thread.is_alive())

        self.engine.clear_failure()
        self.assertTrue(wait_until(lambda: self.state.status_of("WS_INT") is StationStatus.AVAILABLE))

    def test_monitor_assignment_arguments_have_internal_defaults(self) -> None:
        config = parse_monitor_args(
            [
                "--ws-id", "WS_DEFAULTS", "--location", "Garden", "--central-host", "127.0.0.1",
                "--central-port", "5000", "--engine-host", "127.0.0.1", "--engine-port", "6000",
            ]
        )
        self.assertEqual((config.engine_hello_timeout, config.health_interval, config.health_timeout), (5.0, 5.0, 2.0))

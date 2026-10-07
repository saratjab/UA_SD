from __future__ import annotations

import logging
import socket
import threading
import time

from common import protocol

from .config import EngineConfig
from .flow_meter import FlowMeter
from .irrigation import IrrigationController, IrrigationRequest, IrrigationState
from .valve import SolenoidValve


LOGGER = logging.getLogger(__name__)


class WateringStationEngine:
    def __init__(
        self,
        config: EngineConfig,
        irrigation_controller: IrrigationController | None = None,
    ) -> None:
        self.config = config
        self.irrigation = irrigation_controller or IrrigationController(
            ws_id=config.ws_id or "",
            valve=SolenoidValve(),
            flow_meter=FlowMeter(config.flow_rate_lpm),
            telemetry_interval=config.telemetry_interval,
        )
        self.failed = False
        self.monitor_socket: socket.socket | None = None
        self._stop_event = threading.Event()

    @property
    def state(self) -> IrrigationState:
        if self.failed:
            return IrrigationState.FAILED
        return self.irrigation.state

    def connect_to_monitor(self) -> None:
        LOGGER.info("Connecting to WM_WS_M at %s:%s", self.config.monitor_host, self.config.monitor_port)
        self.monitor_socket = socket.create_connection(
            (self.config.monitor_host, self.config.monitor_port),
            timeout=self.config.socket_timeout,
        )
        self.monitor_socket.settimeout(self.config.socket_timeout)
        LOGGER.info("Connected to WM_WS_M")

    def close(self) -> None:
        self._stop_event.set()
        if self.monitor_socket is not None:
            self.monitor_socket.close()
            self.monitor_socket = None

    def send_hello(self, sock: socket.socket | None = None) -> bool:
        conn = sock or self._require_monitor_socket()
        LOGGER.info("HELLO_WS_E sent")
        hello: dict[str, object] = {"type": protocol.HELLO_WS_E, "component": "WM_WS_E"}
        if self.config.ws_id is not None:  # compatibility with pre-assigned Engines
            hello["ws_id"] = self.config.ws_id
        protocol.send_message(conn, hello)
        response = protocol.receive_message(conn)
        assigned_ws_id = response.get("ws_id")
        if response.get("type") == protocol.HELLO_ACK and isinstance(assigned_ws_id, str):
            if self.config.ws_id is not None and assigned_ws_id != self.config.ws_id:
                LOGGER.error("WM_WS_M assigned a different WS ID: %s", response)
                return False
            self.config = EngineConfig(**{**self.config.__dict__, "ws_id": assigned_ws_id})
            self.irrigation.ws_id = assigned_ws_id
            LOGGER.info("HELLO_ACK received")
            return True
        LOGGER.error("WM_WS_M rejected Engine connection: %s", response)
        return False

    def run(self) -> int:
        LOGGER.info("WM_WS_E started")
        while not self._stop_event.is_set():
            try:
                self.connect_to_monitor()
                if self.send_hello():
                    self.handle_monitor_messages()
                self._disconnect_monitor()
            except (OSError, protocol.ProtocolError) as exc:
                LOGGER.error("WM_WS_E communication error: %s", exc)
                self._disconnect_monitor()
            if not self._stop_event.is_set():
                time.sleep(0.1)
        return 0

    def handle_monitor_messages(self) -> None:
        conn = self._require_monitor_socket()
        while not self._stop_event.is_set():
            try:
                message = protocol.receive_message(conn)
                response = self.handle_monitor_message(message)
                if response is not None:
                    protocol.send_message(conn, response)
            except socket.timeout:
                continue
            except protocol.ConnectionClosedError:
                LOGGER.error("Monitor disconnected")
                return
            except protocol.ProtocolError as exc:
                LOGGER.error("Invalid protocol message from Monitor: %s", exc)
                return
            except OSError as exc:
                LOGGER.error("Monitor connection error: %s", exc)
                return

    def handle_monitor_message(self, message: dict[str, object]) -> dict[str, object] | None:
        if message.get("type") != "HEALTH_CHECK":
            LOGGER.error("Unexpected message from Monitor: %s", message)
            return protocol.nack_message("unexpected_message")
        if message.get("ws_id") != self.config.ws_id:
            LOGGER.error("Invalid WS ID in Monitor message: %s", message)
            return protocol.nack_message("wrong_ws_id")
        sequence = message.get("sequence")
        if not isinstance(sequence, int):
            return protocol.nack_message("invalid_sequence")

        LOGGER.info("HEALTH_CHECK received")
        if self.failed:
            LOGGER.info("HEALTH_KO sent")
            return {
                "type": "HEALTH_KO",
                "ws_id": self.config.ws_id,
                "sequence": sequence,
                "status": "KO",
                "reason": "simulated_failure",
            }

        LOGGER.info("HEALTH_OK sent")
        return {
            "type": "HEALTH_OK",
            "ws_id": self.config.ws_id,
            "sequence": sequence,
            "status": "OK",
        }

    def simulate_failure(self) -> None:
        self.failed = True
        self.irrigation.emergency_stop()
        LOGGER.warning("Simulated KO enabled")

    def clear_failure(self) -> None:
        self.failed = False
        LOGGER.info("Simulated KO cleared")

    def request_irrigation(self, operator_id: str, duration_seconds: float | None = None) -> bool:
        if self.failed:
            LOGGER.warning("Irrigation rejected while Engine is failed")
            return False
        duration = duration_seconds
        if duration is None:
            duration = self.config.default_irrigation_duration
        request = IrrigationRequest(operator_id=operator_id, duration_seconds=duration)
        return self.irrigation.start(request)

    def stop_irrigation(self) -> bool:
        stopped = self.irrigation.stop()
        if not stopped:
            LOGGER.warning("Stop requested while Engine is not watering")
        return stopped

    def _require_monitor_socket(self) -> socket.socket:
        if self.monitor_socket is None:
            raise RuntimeError("Engine is not connected to WM_WS_M")
        return self.monitor_socket

    def _disconnect_monitor(self) -> None:
        if self.monitor_socket is not None:
            try:
                self.monitor_socket.close()
            finally:
                self.monitor_socket = None

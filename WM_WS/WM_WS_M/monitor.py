from __future__ import annotations

import queue
from datetime import datetime, timezone
import logging
import socket
import threading
import time

from common import protocol
from .central_client import CentralClient
from .config import MonitorConfig
from .ws_e_server import EngineServer


LOGGER = logging.getLogger(__name__)


class WateringStationMonitor:
    def __init__(
        self,
        config: MonitorConfig,
        central_client: CentralClient,
        engine_server: EngineServer,
    ) -> None:
        self.config = config
        self.central_client = central_client
        self.engine_server = engine_server
        self.sequence = 0
        self._fault_active = False
        self._last_health_connection_usable = False

    def run(self) -> int:
        LOGGER.info("Starting WM_WS_M for WS ID %s", self.config.ws_id)
        try:
            self.central_client.connect()
            if not self.central_client.register():
                return 1

            self.engine_server.start()
            while True:
                try:
                    engine_socket = self.wait_for_engine_connection()
                except OSError as exc:
                    # A closed listener is normal shutdown.  A transient
                    # accept failure must not turn an Engine fault into a
                    # Monitor/Central disconnection.
                    if self.engine_server.server_sock is None:
                        return 0
                    LOGGER.warning("Could not accept WM_WS_E connection: %s", exc)
                    continue
                try:
                    self.monitor_engine(engine_socket)
                finally:
                    try:
                        engine_socket.close()
                    except OSError:
                        pass
        except OSError as exc:
            LOGGER.error("WM_WS_M socket error: %s", exc)
            return 1
        finally:
            self.engine_server.close()
            self.central_client.close()

    def wait_for_engine_connection(self) -> socket.socket:
        result_queue: queue.Queue[socket.socket | BaseException] = queue.Queue(maxsize=1)

        def accept_engine() -> None:
            try:
                result_queue.put(self.engine_server.accept_engine())
            except BaseException as exc:
                result_queue.put(exc)

        thread = threading.Thread(target=accept_engine, name="wm-ws-e-accept")
        thread.start()
        LOGGER.info("Waiting for WM_WS_E connection")
        result = result_queue.get()
        thread.join()
        if isinstance(result, BaseException):
            raise result
        return result

    def monitor_engine(self, engine_socket: socket.socket) -> None:
        while True:
            self.sequence += 1
            if not self.perform_health_check(engine_socket, self.sequence):
                # HEALTH_KO is a valid response.  Keep this connection alive
                # so a recovered Engine can prove it is healthy; transport and
                # protocol failures return to accept() for a reconnection.
                if not self._last_health_connection_usable:
                    return
            time.sleep(self.config.health_interval)

    def perform_health_check(self, engine_socket: socket.socket, sequence: int) -> bool:
        self._last_health_connection_usable = False
        message = protocol.health_check_message(self.config.ws_id, sequence)
        try:
            protocol.send_message(engine_socket, message)
            engine_socket.settimeout(self.config.health_timeout)
            response = protocol.receive_message(engine_socket)
        except socket.timeout:
            LOGGER.error("WM_WS_E health-check timeout")
            self.activate_fault("timeout", "No health-check response received")
            return False
        except (protocol.ConnectionClosedError, ConnectionResetError, BrokenPipeError, OSError):
            LOGGER.error("WM_WS_E connection lost")
            self.activate_fault("connection_lost", "TCP connection to WM_WS_E was closed")
            return False
        except protocol.ProtocolError as exc:
            LOGGER.error("Malformed health response from WM_WS_E: %s", exc)
            self.activate_fault("malformed_response", str(exc))
            return False

        response_type = response.get("type")
        response_sequence = response.get("sequence")
        if response_sequence != sequence:
            LOGGER.error("WM_WS_E returned unexpected health sequence: %s", response)
            self.activate_fault("malformed_response", "Unexpected health-check sequence")
            return False

        if response_type == protocol.HEALTH_OK:
            self._last_health_connection_usable = True
            self.resolve_fault_if_needed()
            return True

        if response_type == protocol.HEALTH_KO:
            self._last_health_connection_usable = True
            reason = str(response.get("reason", "WM_WS_E returned KO"))
            LOGGER.error("WM_WS_E reported KO: %s", reason)
            self.activate_fault("health_ko", reason)
            return False

        LOGGER.error("WM_WS_E returned unexpected health response: %s", response)
        self.activate_fault("malformed_response", "Unexpected health-check response")
        return False

    def activate_fault(self, fault: str, details: str) -> bool:
        if self._fault_active:
            return True
        reported = self.report_fault(fault, details)
        if reported:
            self._fault_active = True
        return reported

    def resolve_fault_if_needed(self) -> bool:
        if not self._fault_active:
            return True
        try:
            resolved = self.central_client.report_fault_resolved(utc_timestamp())
        except (OSError, protocol.ProtocolError) as exc:
            LOGGER.error("Failed to notify WM_Central that WM_WS_E recovered: %s", exc)
            return False
        if resolved:
            self._fault_active = False
        return resolved

    def report_fault(self, fault: str, details: str) -> bool:
        try:
            return self.central_client.report_fault(fault, details, utc_timestamp())
        except (OSError, protocol.ProtocolError) as exc:
            LOGGER.error("Failed to notify WM_Central about fault %s: %s", fault, exc)
            return False


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

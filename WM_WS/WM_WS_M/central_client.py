from __future__ import annotations

import logging
import socket
from types import TracebackType
from typing import Any

from common import protocol


LOGGER = logging.getLogger(__name__)


class CentralClient:
    def __init__(self, host: str, port: int, ws_id: str, location: str = "", timeout: float = 5.0) -> None:
        self.host = host
        self.port = port
        self.ws_id = ws_id
        self.location = location
        self.timeout = timeout
        self.sock: socket.socket | None = None

    def connect(self) -> None:
        LOGGER.info("Connecting to WM_Central at %s:%s", self.host, self.port)
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        LOGGER.info("Connected to WM_Central")

    def close(self) -> None:
        if self.sock is not None:
            self.sock.close()
            self.sock = None

    def __enter__(self) -> "CentralClient":
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def register(self) -> bool:
        sock = self._require_socket()
        protocol.send_message(sock, protocol.register_ws_message(self.ws_id, self.location))
        response = protocol.receive_message(sock)
        response_type = response.get("type")

        if response_type == "REGISTER_ACK" and response.get("ws_id") == self.ws_id:
            LOGGER.info("WM_WS_M registered with WM_Central as %s", self.ws_id)
            return True

        if response_type == "REGISTER_NACK":
            LOGGER.error("WM_Central rejected registration: %s", response.get("reason", "unknown"))
            return False

        LOGGER.error("Unexpected registration response from WM_Central: %s", response)
        return False

    def report_fault(self, fault: str, details: str, timestamp: str) -> bool:
        sock = self._require_socket()
        message = protocol.fault_message(
            ws_id=self.ws_id,
            fault=fault,
            details=details,
            timestamp=timestamp,
        )
        LOGGER.info("Reporting WM_WS_E fault to WM_Central: %s", fault)
        protocol.send_message(sock, message)
        response = protocol.receive_message(sock)
        return self._handle_fault_response(response)

    def report_fault_resolved(self, timestamp: str) -> bool:
        sock = self._require_socket()
        LOGGER.info("Reporting WM_WS_E fault resolution to WM_Central")
        protocol.send_message(sock, protocol.fault_resolved_message(self.ws_id, timestamp))
        return self._handle_fault_response(protocol.receive_message(sock))

    def _handle_fault_response(self, response: dict[str, Any]) -> bool:
        response_type = response.get("type")
        if response_type == "FAULT_ACK":
            LOGGER.info("WM_Central acknowledged fault notification")
            return True
        if response_type == "FAULT_NACK":
            LOGGER.error("WM_Central rejected fault notification: %s", response.get("reason", "unknown"))
            return False
        LOGGER.error("Unexpected fault response from WM_Central: %s", response)
        return False

    def _require_socket(self) -> socket.socket:
        if self.sock is None:
            raise RuntimeError("Central client is not connected")
        return self.sock

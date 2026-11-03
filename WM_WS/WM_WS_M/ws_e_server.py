from __future__ import annotations

import logging
import socket
from typing import Callable

from common import protocol
from common.event_panel import feed


LOGGER = logging.getLogger(__name__)


class EngineServer:
    def __init__(self, host: str, port: int, ws_id: str, hello_timeout: float,
                 accept_timeout: float = 1.0) -> None:
        self.host = host
        self.port = port
        self.ws_id = ws_id
        self.hello_timeout = hello_timeout
        self.accept_timeout = accept_timeout
        self.on_idle: Callable[[], object] | None = None
        self.server_sock: socket.socket | None = None

    def start(self) -> None:
        if self.server_sock is not None:
            return
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(1)
        self.server_sock.settimeout(self.accept_timeout)
        LOGGER.info("Listening for WM_WS_E on %s:%s", self.host, self.port)

    def close(self) -> None:
        if self.server_sock is not None:
            self.server_sock.close()
            self.server_sock = None

    def accept_engine(self) -> socket.socket:
        while True:
            server_sock = self.server_sock
            if server_sock is None:
                raise OSError("Engine server is closed")
            try:
                conn, address = server_sock.accept()
            except socket.timeout:
                if self.on_idle is not None:
                    self.on_idle()
                continue
            except OSError:
                if self.server_sock is None or server_sock.fileno() < 0:
                    raise
                LOGGER.warning("Transient error accepting WM_WS_E connection", exc_info=True)
                continue
            conn.settimeout(None)
            LOGGER.info("WM_WS_E connection received from %s:%s", address[0], address[1])
            if self.handle_engine_hello(conn):
                LOGGER.info("WM_WS_E accepted for WS ID %s", self.ws_id)
                return conn
            conn.close()

    def handle_engine_hello(self, conn: socket.socket) -> bool:
        previous_timeout = conn.gettimeout()
        conn.settimeout(self.hello_timeout)
        try:
            hello = protocol.receive_message(conn)
            if self._is_valid_hello(hello):
                protocol.send_message(conn, protocol.hello_ack_message(self.ws_id))
                feed().record("IN", "WM_WS_E", "HELLO_WS_E", ws_id=self.ws_id, detail="Engine accepted")
                return True

            if hello.get("type") != protocol.HELLO_WS_E:
                reason = "unexpected_message"
            elif hello.get("component") != "WM_WS_E":
                reason = "invalid_component"
            else:
                reason = "wrong_ws_id"
            LOGGER.warning("Rejecting WM_WS_E connection: %s", reason)
            feed().record("IN", "WM_WS_E", "HELLO_REJECTED", ws_id=self.ws_id, detail=reason)
            protocol.send_message(conn, protocol.nack_message(reason))
            return False
        except socket.timeout:
            LOGGER.warning("Rejecting WM_WS_E connection: HELLO_WS_E timeout")
            return False
        except protocol.ProtocolError as exc:
            LOGGER.warning("Rejecting malformed WM_WS_E connection: %s", exc)
            try:
                protocol.send_message(conn, protocol.nack_message("malformed_frame"))
            except OSError:
                pass
            return False
        finally:
            try:
                conn.settimeout(previous_timeout)
            except OSError:
                pass

    def _is_valid_hello(self, message: dict[str, object]) -> bool:
        return (
            message.get("type") == protocol.HELLO_WS_E
            and message.get("component") == "WM_WS_E"
            and ("ws_id" not in message or message.get("ws_id") == self.ws_id)
        )

from __future__ import annotations

import logging
import socket

from . import protocol


LOGGER = logging.getLogger(__name__)


class EngineServer:
    def __init__(self, host: str, port: int, ws_id: str, hello_timeout: float) -> None:
        self.host = host
        self.port = port
        self.ws_id = ws_id
        self.hello_timeout = hello_timeout
        self.server_sock: socket.socket | None = None

    def start(self) -> None:
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(1)
        LOGGER.info("Listening for WM_WS_E on %s:%s", self.host, self.port)

    def close(self) -> None:
        if self.server_sock is not None:
            self.server_sock.close()
            self.server_sock = None

    def accept_engine(self) -> socket.socket:
        if self.server_sock is None:
            raise RuntimeError("Engine server is not started")

        while True:
            conn, address = self.server_sock.accept()
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
                return True

            reason = "wrong_ws_id"
            if hello.get("type") != "HELLO_WS_E":
                reason = "unexpected_message"
            LOGGER.warning("Rejecting WM_WS_E connection: %s", reason)
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
            message.get("type") == "HELLO_WS_E"
            and message.get("ws_id") == self.ws_id
            and message.get("component") == "WM_WS_E"
        )

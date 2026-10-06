"""Monitor <-> Central socket server.

Transport only: it parses frames, validates message shape, and delegates every
decision to ``StateManager``.

One thread per Monitor connection. A connection is a station's ONLY liveness
signal for CENTRAL: when its socket closes, the station is DISCONNECTED. Because
the Monitor sends nothing while idle, TCP keepalive is enabled so that a
silently dead peer (cable pulled, machine off) is eventually noticed too.

The socket is strictly request/response: CENTRAL only answers, it never sends
unsolicited messages (the Monitor reads this socket only right after sending).
"""

from __future__ import annotations

import logging
import socket
import threading
from typing import Any

from common import protocol

from .state_manager import StateManager


LOGGER = logging.getLogger(__name__)

DEFAULT_REGISTER_TIMEOUT = 10.0  # seconds a new connection may stay silent


class MonitorConnection:
    """One accepted Monitor socket."""

    def __init__(self, sock: socket.socket, address: tuple[str, int]) -> None:
        self.sock = sock
        self.address = address
        self.ws_id: str | None = None
        self._send_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._closed = False

    def send(self, message: dict[str, Any]) -> None:
        with self._send_lock:
            protocol.send_message(self.sock, message)

    def close(self) -> None:
        """Idempotent. shutdown() first so a thread blocked in recv() wakes up."""
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
        for action in (lambda: self.sock.shutdown(socket.SHUT_RDWR), self.sock.close):
            try:
                action()
            except OSError:
                pass

    def __repr__(self) -> str:
        return f"<MonitorConnection {self.address[0]}:{self.address[1]} ws_id={self.ws_id}>"


class CentralSocketServer:
    def __init__(
        self,
        host: str,
        port: int,
        state: StateManager,
        *,
        register_timeout: float = DEFAULT_REGISTER_TIMEOUT,
    ) -> None:
        self._host = host
        self._port = port
        self._state = state
        self._register_timeout = register_timeout
        self._server_sock: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._connections: set[MonitorConnection] = set()
        self._handler_threads: set[threading.Thread] = set()
        self._connections_lock = threading.Lock()

    @property
    def port(self) -> int:
        """The bound port (useful when started with port 0 in tests)."""
        if self._server_sock is None:
            raise RuntimeError("server is not started")
        return self._server_sock.getsockname()[1]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self._host, self._port))
        server.listen()
        server.settimeout(1.0)  # lets the accept loop notice stop()
        self._server_sock = server
        self._accept_thread = threading.Thread(
            target=self._accept_loop, name="wm-central-accept", daemon=True
        )
        self._accept_thread.start()
        LOGGER.info("Listening for WM_WS_M connections on %s:%s", self._host, self.port)

    def stop(self) -> None:
        self._stop.set()
        if self._server_sock is not None:
            for action in (lambda: self._server_sock.shutdown(socket.SHUT_RDWR), self._server_sock.close):
                try:
                    action()  # shutdown() wakes a blocked accept() immediately on Linux
                except OSError:
                    pass
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=3.0)
        with self._connections_lock:
            connections = list(self._connections)
            handlers = list(self._handler_threads)
        for connection in connections:
            connection.close()
        # Wait for the handlers: they update the state/DB while cleaning up, so the
        # caller may only close the database once they are done.
        for handler in handlers:
            handler.join(timeout=3.0)

    # ------------------------------------------------------------------
    # Accept / serve
    # ------------------------------------------------------------------

    def _accept_loop(self) -> None:
        assert self._server_sock is not None
        while not self._stop.is_set():
            try:
                sock, address = self._server_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break  # listening socket closed by stop()
            _enable_keepalive(sock)
            connection = MonitorConnection(sock, address)
            with self._connections_lock:
                self._connections.add(connection)
            handler = threading.Thread(
                target=self._serve,
                args=(connection,),
                name=f"wm-central-monitor-{address[0]}:{address[1]}",
                daemon=True,
            )
            with self._connections_lock:
                self._handler_threads.add(handler)
            handler.start()

    def _serve(self, connection: MonitorConnection) -> None:
        LOGGER.info("Monitor connection from %s:%s", *connection.address)
        try:
            # Until REGISTER_WS succeeds the peer may only stay silent briefly.
            connection.sock.settimeout(self._register_timeout)
            keep_open = True
            while keep_open and not self._stop.is_set():
                try:
                    message = protocol.receive_message(connection.sock)
                except socket.timeout:
                    LOGGER.warning("%s sent nothing within %.1fs; closing", connection, self._register_timeout)
                    break
                except protocol.ConnectionClosedError:
                    break
                except protocol.ProtocolError as exc:
                    LOGGER.warning("Bad frame from %s: %s", connection, exc)
                    if not self._send(connection, protocol.nack_message(_protocol_reason(exc))):
                        break
                    if connection.ws_id is None:
                        break  # an unregistered peer gets no second chance
                    continue
                except OSError:
                    break
                keep_open = self._dispatch(connection, message)
        finally:
            self._cleanup(connection)

    def _cleanup(self, connection: MonitorConnection) -> None:
        if connection.ws_id is not None:
            self._state.disconnect(connection.ws_id, connection)
        connection.close()
        with self._connections_lock:
            self._connections.discard(connection)
            self._handler_threads.discard(threading.current_thread())

    # ------------------------------------------------------------------
    # Message handling. Each handler returns True to keep the connection open.
    # ------------------------------------------------------------------

    def _dispatch(self, connection: MonitorConnection, message: dict[str, Any]) -> bool:
        message_type = message.get("type")
        if message_type == protocol.REGISTER_WS:
            return self._handle_register(connection, message)
        if message_type == protocol.WS_E_FAULT:
            return self._handle_fault(connection, message)
        if message_type == protocol.WS_E_FAULT_RESOLVED:
            return self._handle_fault_resolved(connection, message)

        LOGGER.warning("Unknown message type from %s: %r", connection, message_type)
        sent = self._send(connection, protocol.nack_message("unknown_type"))
        return sent and connection.ws_id is not None

    def _handle_register(self, connection: MonitorConnection, message: dict[str, Any]) -> bool:
        ws_id = message.get("ws_id")
        location = message.get("location")
        if not isinstance(ws_id, str):
            self._send(connection, protocol.register_nack_message("invalid_ws_id"))
            return False
        if location is not None and not isinstance(location, str):
            self._send(connection, protocol.register_nack_message("invalid_location"))
            return False

        if connection.ws_id is not None:  # registering twice on the same socket
            if connection.ws_id == ws_id.strip():
                return self._send(connection, protocol.register_ack_message(connection.ws_id))
            self._send(connection, protocol.register_nack_message("already_registered"))
            return True

        outcome = self._state.register(ws_id, location, connection)
        if not outcome.ok:
            LOGGER.warning("Registration rejected for %r: %s", ws_id, outcome.reason)
            self._send(connection, protocol.register_nack_message(outcome.reason or "rejected"))
            return False

        assert outcome.ws_id is not None
        connection.ws_id = outcome.ws_id
        connection.sock.settimeout(None)  # registered: wait for the next message indefinitely
        sent = self._send(connection, protocol.register_ack_message(outcome.ws_id))
        if outcome.replaced is not None:
            outcome.replaced.close()  # network I/O happens outside any state lock
        return sent

    def _handle_fault(self, connection: MonitorConnection, message: dict[str, Any]) -> bool:
        if connection.ws_id is None:
            self._send(connection, protocol.fault_nack_message("not_registered"))
            return False  # an unregistered peer gets no second chance
        if message.get("ws_id") != connection.ws_id:
            return self._send(connection, protocol.fault_nack_message("ws_id_mismatch"))

        fault = message.get("fault")
        if not isinstance(fault, str) or not fault.strip():
            return self._send(connection, protocol.fault_nack_message("invalid_fault"))
        details = message.get("details")
        timestamp = message.get("timestamp")

        outcome = self._state.report_fault(
            connection.ws_id,
            connection,
            fault.strip(),
            details if isinstance(details, str) else "",
            timestamp if isinstance(timestamp, str) else None,
        )
        if not outcome.ok:
            return self._send(connection, protocol.fault_nack_message(outcome.reason or "rejected"))
        return self._send(connection, protocol.fault_ack_message(connection.ws_id))

    def _handle_fault_resolved(self, connection: MonitorConnection, message: dict[str, Any]) -> bool:
        if connection.ws_id is None:
            self._send(connection, protocol.fault_nack_message("not_registered"))
            return False  # an unregistered peer gets no second chance
        if message.get("ws_id") != connection.ws_id:
            return self._send(connection, protocol.fault_nack_message("ws_id_mismatch"))
        # Idempotent: resolving when nothing is open is still acknowledged.
        self._state.resolve_fault(connection.ws_id, connection)
        return self._send(connection, protocol.fault_ack_message(connection.ws_id))

    @staticmethod
    def _send(connection: MonitorConnection, message: dict[str, Any]) -> bool:
        try:
            connection.send(message)
            return True
        except OSError as exc:
            LOGGER.warning("Could not answer %s: %s", connection, exc)
            return False


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _protocol_reason(exc: protocol.ProtocolError) -> str:
    return "invalid_lrc" if isinstance(exc, protocol.InvalidLRCError) else "malformed_frame"


def _enable_keepalive(sock: socket.socket) -> None:
    """Detect peers that vanish without closing the socket (~25 s)."""
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, "TCP_KEEPIDLE"):  # Linux
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 10)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        elif hasattr(socket, "TCP_KEEPALIVE"):  # macOS
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, 10)
        elif hasattr(socket, "SIO_KEEPALIVE_VALS"):  # Windows: (on, idle ms, interval ms)
            sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, 10_000, 5_000))
    except (OSError, AttributeError):
        LOGGER.debug("TCP keepalive tuning not available on this platform")

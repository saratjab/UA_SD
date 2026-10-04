from __future__ import annotations

import json
import socket
from typing import Any


STX = 0x02
ETX = 0x03
MAX_DATA_SIZE = 1024 * 1024


class ProtocolError(Exception):
    """Base class for protocol errors."""


class InvalidLRCError(ProtocolError):
    """Raised when the frame LRC does not match the payload."""


class MalformedFrameError(ProtocolError):
    """Raised when a frame cannot be parsed."""


class ConnectionClosedError(ProtocolError):
    """Raised when the socket closes while reading a frame."""


def calculate_lrc(data: bytes) -> int:
    lrc = 0
    for byte in data:
        lrc ^= byte
    return lrc


def create_frame(message: dict[str, Any]) -> bytes:
    data = json.dumps(message, separators=(",", ":")).encode("utf-8")
    return bytes([STX]) + data + bytes([ETX, calculate_lrc(data)])


def parse_frame(frame: bytes) -> dict[str, Any]:
    if len(frame) < 4:
        raise MalformedFrameError("frame too short")
    if frame[0] != STX:
        raise MalformedFrameError("missing STX")

    try:
        etx_index = frame.index(bytes([ETX]), 1)
    except ValueError as exc:
        raise MalformedFrameError("missing ETX") from exc

    if etx_index != len(frame) - 2:
        raise MalformedFrameError("unexpected bytes after LRC")

    data = frame[1:etx_index]
    expected_lrc = frame[-1]
    actual_lrc = calculate_lrc(data)
    if expected_lrc != actual_lrc:
        raise InvalidLRCError("invalid LRC")

    try:
        decoded = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MalformedFrameError("invalid JSON payload") from exc

    if not isinstance(decoded, dict):
        raise MalformedFrameError("JSON payload must be an object")
    return decoded


def recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionClosedError("socket closed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def receive_frame(sock: socket.socket) -> bytes:
    while True:
        first = recv_exact(sock, 1)
        if first[0] == STX:
            break

    data = bytearray()
    while True:
        current = recv_exact(sock, 1)
        if current[0] == ETX:
            break
        data.extend(current)
        if len(data) > MAX_DATA_SIZE:
            raise MalformedFrameError("frame payload too large")

    lrc = recv_exact(sock, 1)
    return bytes([STX]) + bytes(data) + bytes([ETX]) + lrc


def receive_message(sock: socket.socket) -> dict[str, Any]:
    return parse_frame(receive_frame(sock))


def send_message(sock: socket.socket, message: dict[str, Any]) -> None:
    sock.sendall(create_frame(message))


def ack_message() -> dict[str, str]:
    return {"type": "ACK"}


def nack_message(reason: str) -> dict[str, str]:
    return {"type": "NACK", "reason": reason}


def register_ws_message(ws_id: str) -> dict[str, str]:
    return {"type": "REGISTER_WS", "ws_id": ws_id}


def fault_message(
    ws_id: str,
    fault: str,
    details: str,
    timestamp: str,
) -> dict[str, str]:
    return {
        "type": "WS_E_FAULT",
        "ws_id": ws_id,
        "component": "WM_WS_E",
        "fault": fault,
        "details": details,
        "timestamp": timestamp,
    }


def hello_ack_message(ws_id: str) -> dict[str, str]:
    return {"type": "HELLO_ACK", "ws_id": ws_id, "status": "OK"}


def health_check_message(ws_id: str, sequence: int) -> dict[str, Any]:
    return {"type": "HEALTH_CHECK", "ws_id": ws_id, "sequence": sequence}

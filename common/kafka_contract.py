"""Shared Kafka topic names, envelopes and strict JSON validation."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

ACTIVATION_REQUESTS = "wm.activation.requests"
WS_COMMANDS = "wm.ws.commands"
WS_EVENTS = "wm.ws.events"
OPERATOR_EVENTS = "wm.operator.events"

TOPIC_KEYS = {
    ACTIVATION_REQUESTS: "ws_id",
    WS_COMMANDS: "ws_id",
    WS_EVENTS: "ws_id",
    OPERATOR_EVENTS: "operator_id",
}


def message_key(topic: str, message: dict[str, Any]) -> str | None:
    """Return the per-entity partition key assigned to a topic."""
    field = TOPIC_KEYS.get(topic)
    value = message.get(field) if field else None
    return value if isinstance(value, str) and value else None

# Reasons an Engine may include in COMMAND_REJECTED. Keep Engine output,
# Central validation, and persisted denial values aligned.
COMMAND_REJECTION_REASONS = frozenset({
    "ENGINE_BLOCKED",
    "ENGINE_FAILED",
    "MONITOR_UNAVAILABLE",
})

REQUIRED = ("event_type", "message_id", "timestamp", "payload")


class MessageValidationError(ValueError):
    pass


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def envelope(event_type: str, payload: dict[str, Any], **ids: str | None) -> dict[str, Any]:
    message: dict[str, Any] = {"event_type": event_type, "message_id": str(uuid4()),
                                "timestamp": utc_timestamp(), "payload": payload}
    message.update({key: value for key, value in ids.items() if value is not None})
    return message


def encode(message: dict[str, Any]) -> bytes:
    validate(message)
    return json.dumps(message, separators=(",", ":"), sort_keys=True).encode("utf-8")


def decode(value: bytes | str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(value, dict):
        message = value
    else:
        try:
            message = json.loads(value.decode("utf-8") if isinstance(value, bytes) else value)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MessageValidationError("message is not JSON") from exc
    validate(message)
    return message


def validate(message: object, *, required_ids: tuple[str, ...] = ()) -> None:
    if not isinstance(message, dict):
        raise MessageValidationError("envelope must be an object")
    for field in ("event_type", "message_id", "timestamp") + required_ids:
        if not isinstance(message.get(field), str) or not message[field].strip():
            raise MessageValidationError(f"missing or invalid {field}")
    if not isinstance(message["payload"], dict):
        raise MessageValidationError("payload must be an object")
    try:
        datetime.fromisoformat(message["timestamp"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise MessageValidationError("timestamp must be ISO-8601") from exc


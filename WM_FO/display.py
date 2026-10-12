from __future__ import annotations

from typing import Any


def show_event(message: dict[str, Any]) -> None:
    event_type = message["event_type"]
    payload = message["payload"]
    ws_id = message.get("ws_id", "unknown station")
    request_id = message.get("request_id", "")
    prefix = f"[{ws_id}]"
    if event_type == "ACTIVATION_AUTHORIZED":
        print(f"{prefix} Irrigation authorized ({request_id}).")
    elif event_type == "ACTIVATION_DENIED":
        print(f"{prefix} Irrigation denied: {payload.get('reason', 'unknown reason')}.")
    elif event_type == "ACTIVATION_STATUS":
        print(f"{prefix} Request status: {payload.get('status')} ({payload.get('reason') or 'no reason'}).")
    elif event_type == "IRRIGATION_STARTED":
        print(f"{prefix} Irrigation started.")
    elif event_type == "IRRIGATION_PROGRESS":
        print(f"{prefix} Irrigation progress: {payload.get('duration_seconds', '?')} s, "
              f"{payload.get('accumulated_volume_liters', '?')} L accumulated, "
              f"{payload.get('flow_rate_lpm', '?')} L/min.")
    elif event_type == "IRRIGATION_COMPLETED":
        print(f"{prefix} Irrigation completed: {payload.get('duration_seconds', '?')} s, "
              f"{payload.get('accumulated_volume_liters', '?')} L, "
              f"reason {payload.get('end_reason', 'unknown')}.")
    elif event_type == "COMMAND_REJECTED":
        print(f"{prefix} Station could not start irrigation: {payload.get('reason', 'unknown reason')}.")
    else:
        print(f"{prefix} {event_type}: {payload}")

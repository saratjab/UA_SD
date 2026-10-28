from __future__ import annotations

from typing import Any


def _print(message: str) -> None:
    print(message, flush=True)


def show_event(message: dict[str, Any]) -> None:
    event_type = message["event_type"]
    payload = message["payload"]
    ws_id = message.get("ws_id", "unknown station")
    request_id = message.get("request_id", "")
    prefix = f"[{ws_id}]"
    if event_type == "ACTIVATION_AUTHORIZED":
        mode = (f"duration {payload['duration_seconds']} s" if "duration_seconds" in payload else
                f"target {payload['target_volume_liters']} L" if "target_volume_liters" in payload else
                "until manually stopped")
        _print(f"{prefix} Request {request_id} authorized ({mode}).")
    elif event_type == "ACTIVATION_DENIED":
        _print(f"{prefix} Irrigation denied: {payload.get('reason', 'unknown reason')}.")
    elif event_type == "ACTIVATION_STATUS":
        _print(f"{prefix} Request status: {payload.get('status')} ({payload.get('reason') or 'no reason'}).")
    elif event_type == "IRRIGATION_STARTED":
        _print(f"{prefix} Request {request_id} started.")
    elif event_type == "IRRIGATION_PROGRESS":
        target = (f" of {payload['target_volume_liters']} L target"
                  if "target_volume_liters" in payload else "")
        _print(f"{prefix} Request {request_id} progress: elapsed {payload.get('duration_seconds', '?')} s, "
              f"total volume {payload.get('accumulated_volume_liters', '?')} L{target}, "
              f"{payload.get('flow_rate_lpm', '?')} L/min.")
    elif event_type == "IRRIGATION_COMPLETED":
        target = (f", target {payload['target_volume_liters']} L"
                  if "target_volume_liters" in payload else "")
        _print(f"{prefix} Request {request_id} completed: elapsed {payload.get('duration_seconds', '?')} s, "
              f"total volume {payload.get('accumulated_volume_liters', '?')} L{target}, "
              f"reason {payload.get('end_reason', 'unknown')}.")
    elif event_type == "COMMAND_REJECTED":
        _print(f"{prefix} Station could not start irrigation: {payload.get('reason', 'unknown reason')}.")
    else:
        _print(f"{prefix} {event_type}: {payload}")

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any


def _validate(item: Any, index: int) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise ValueError(f"request {index} must be a JSON object")
    unknown = set(item) - {"ws_id", "duration_seconds", "target_volume_liters"}
    if unknown:
        raise ValueError(f"request {index} has unknown field(s): {', '.join(sorted(unknown))}")
    ws_id = item.get("ws_id")
    if not isinstance(ws_id, str) or not ws_id.strip():
        raise ValueError(f"request {index} needs a non-empty ws_id")
    has_duration = "duration_seconds" in item
    has_target = "target_volume_liters" in item
    duration = item.get("duration_seconds")
    target = item.get("target_volume_liters")
    if has_duration and has_target:
        raise ValueError(f"request {index} must choose duration_seconds or target_volume_liters, not both")
    if has_duration and (isinstance(duration, bool) or not isinstance(duration, int) or duration <= 0):
        raise ValueError(f"request {index} needs a positive whole-number duration_seconds")
    if has_target and (isinstance(target, bool) or not isinstance(target, (int, float)) or not math.isfinite(target) or target <= 0):
        raise ValueError(f"request {index} needs a positive target_volume_liters")
    return item


def load_requests(path: str | Path) -> list[dict[str, Any]]:
    """Load a JSON array/object or JSONL records without silently skipping errors."""
    source = Path(path)
    content = source.read_text(encoding="utf-8")
    if not content.strip():
        raise ValueError("request file is empty")

    stripped = content.lstrip()
    if stripped.startswith("["):
        try:
            data = json.loads(content)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON request array: {exc.msg}") from exc
        if not isinstance(data, list) or not data:
            raise ValueError("request file must contain at least one request")
        return [_validate(item, i) for i, item in enumerate(data, start=1)]

    # A complete JSON object is accepted as the existing one-request format.
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        data = None
    if isinstance(data, dict):
        return [_validate(data, 1)]

    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(content.splitlines(), start=1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON on line {line_number}: {exc.msg}") from exc
        records.append(_validate(item, len(records) + 1))
    if not records:
        raise ValueError("request file must contain at least one request")
    return records

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_requests(path: str | Path) -> list[dict[str, Any]]:
    """Load requests from a JSON array or one JSON object per line (JSONL)."""
    source = Path(path)
    content = source.read_text(encoding="utf-8")
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        data = []
        for line_number, line in enumerate(content.splitlines(), start=1):
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            try:
                data.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON on line {line_number}: {exc.msg}") from exc
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list) or not data:
        raise ValueError("request file must contain a non-empty JSON array or JSONL records")
    for index, item in enumerate(data, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"request {index} must be a JSON object")
        ws_id, duration = item.get("ws_id"), item.get("duration_seconds")
        if not isinstance(ws_id, str) or not ws_id.strip():
            raise ValueError(f"request {index} needs a non-empty ws_id")
        if isinstance(duration, bool) or not isinstance(duration, int) or duration <= 0:
            raise ValueError(f"request {index} needs a positive integer duration_seconds")
    return data

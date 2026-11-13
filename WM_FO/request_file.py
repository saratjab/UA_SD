from __future__ import annotations

import json
import math
from dataclasses import dataclass
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


@dataclass(frozen=True)
class FileRequest:
    """One valid request read from a file, with the file line it came from."""
    line: int
    ws_id: str
    duration_seconds: int | None = None
    target_volume_liters: float | None = None


@dataclass(frozen=True)
class SkippedLine:
    """A file line that could not be used, and why."""
    line: int
    error: str


def _to_request(item: Any, line: int) -> FileRequest:
    _validate(item, line)
    target = item.get("target_volume_liters")
    return FileRequest(line, item["ws_id"].strip(), item.get("duration_seconds"),
                       float(target) if target is not None else None)


def _strip_request_prefix(error: ValueError, line: int) -> str:
    # _validate says "request N ..."; report it against the file line instead.
    message = str(error)
    prefix = f"request {line} "
    return message[len(prefix):] if message.startswith(prefix) else message


def parse_request_lines(content: str) -> tuple[list[FileRequest], list[SkippedLine]]:
    """Read every usable request; report each unusable line instead of failing.

    JSONL is the main format (one object per line, numbered by its real file
    line). A JSON array or a single JSON object is also accepted, numbered by
    position. Blank lines and lines starting with '#' are ignored.
    """
    requests: list[FileRequest] = []
    skipped: list[SkippedLine] = []
    if not content.strip():
        return requests, [SkippedLine(0, "the file is empty")]

    stripped = content.lstrip()
    whole: Any = None
    if stripped.startswith("[") or stripped.startswith("{"):
        try:
            whole = json.loads(content)
        except json.JSONDecodeError:
            whole = None  # not a single JSON document: read it as JSONL below
    if isinstance(whole, list):
        if not whole:
            return requests, [SkippedLine(0, "the file contains no requests")]
        items = list(enumerate(whole, start=1))
    elif isinstance(whole, dict):
        items = [(1, whole)]
    else:
        items = []
        for line_number, line in enumerate(content.splitlines(), start=1):
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            try:
                items.append((line_number, json.loads(line)))
            except json.JSONDecodeError as exc:
                skipped.append(SkippedLine(line_number, f"invalid JSON ({exc.msg})"))
    for line_number, item in items:
        try:
            requests.append(_to_request(item, line_number))
        except ValueError as exc:
            skipped.append(SkippedLine(line_number, _strip_request_prefix(exc, line_number)))
    skipped.sort(key=lambda entry: entry.line)
    return requests, skipped


def read_request_file(path: str | Path) -> tuple[list[FileRequest], list[SkippedLine]]:
    return parse_request_lines(Path(path).read_text(encoding="utf-8"))

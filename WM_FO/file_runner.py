"""Run the irrigation requests read from a file, as the spec describes.

Send the first request to Central, wait until it concludes (successfully or
not), wait 4 seconds, then send the next one. Lines that cannot be used are
reported with their line number and skipped; the rest are still sent.

One runner serves both the terminal (``--request-file``) and the web console,
so both behave and log the same way.
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from uuid import uuid4

from common.event_panel import feed

from .request_file import FileRequest, SkippedLine

LOGGER = logging.getLogger("wm_fo")
FILE_REQUEST_DELAY_SECONDS = 4.0
# Extra wait beyond a request's own duration: covers authorization, the Engine
# start and Central's START timeout before a terminal event must have arrived.
FILE_REQUEST_GRACE_SECONDS = 120.0

# Final request states, as the FO tracks them, grouped for the run summary.
_SUMMARY_GROUPS = (
    ("completed", {"COMPLETED"}),
    ("stopped", {"STOPPED"}),
    ("denied", {"DENIED"}),
    ("rejected", {"REJECTED"}),
    ("interrupted", {"INTERRUPTED"}),
    ("without result", {"NO_RESULT"}),
    ("not sent", {"SEND_FAILED"}),
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _mode(request: FileRequest) -> str:
    if request.duration_seconds is not None:
        return f"{request.duration_seconds} s"
    if request.target_volume_liters is not None:
        return f"{request.target_volume_liters:g} L"
    return "until stopped"


def request_wait(request: FileRequest, timeout: float) -> float:
    """How long to wait for one request's final result."""
    if request.duration_seconds:
        return max(timeout, request.duration_seconds + FILE_REQUEST_GRACE_SECONDS)
    return timeout


class FileRun:
    """One file being processed: its lines, their state, and the run's state.

    Item states: QUEUED -> SENT -> DONE, or SKIPPED (unusable line),
    SEND_FAILED (could not reach Kafka) and CANCELLED (run stopped first).
    The final request outcome (COMPLETED, DENIED, ...) is kept in ``outcome``.
    """

    def __init__(self, filename: str, requests: list[FileRequest], skipped: list[SkippedLine],
                 timeout: float, *, delay_seconds: float = FILE_REQUEST_DELAY_SECONDS) -> None:
        self.run_id = str(uuid4())
        self.filename = filename or "requests.jsonl"
        self.timeout = timeout
        self.delay_seconds = delay_seconds
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._requests = list(requests)
        self._state = "RUNNING"
        self._started_at = _now()
        self._finished_at: str | None = None
        self._next_at: str | None = None
        self._summary = ""
        items = [{"line": r.line, "ws_id": r.ws_id, "mode": _mode(r), "state": "QUEUED",
                  "request_id": None, "outcome": None, "reason": None, "error": None}
                 for r in requests]
        items += [{"line": s.line, "ws_id": None, "mode": None, "state": "SKIPPED",
                   "request_id": None, "outcome": None, "reason": None, "error": s.error}
                  for s in skipped]
        items.sort(key=lambda item: item["line"])
        self._items = items
        self._by_line = {item["line"]: item for item in items if item["state"] == "QUEUED"}

    # ----- state for the page -----

    @property
    def running(self) -> bool:
        with self._lock:
            return self._state in {"RUNNING", "WAITING", "CANCELLING"}

    @property
    def in_flight(self) -> int:
        with self._lock:
            return sum(1 for item in self._items if item["state"] == "SENT")

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            valid = [item for item in self._items if item["state"] != "SKIPPED"]
            return {
                "run_id": self.run_id,
                "filename": self.filename,
                "state": self._state,
                "started_at": self._started_at,
                "finished_at": self._finished_at,
                "next_at": self._next_at,
                "delay_seconds": self.delay_seconds,
                "total": len(valid),
                "sent": sum(1 for item in valid if item["request_id"] is not None),
                "skipped": len(self._items) - len(valid),
                "summary": self._summary,
                "items": [dict(item) for item in self._items],
            }

    def cancel(self) -> bool:
        """Send no further request. One already sent keeps running at its station."""
        with self._lock:
            if self._state not in {"RUNNING", "WAITING"}:
                return False
            self._state = "CANCELLING"
        self._stop.set()
        feed().record("LOCAL", "File", "FILE_CANCELLED", detail=f"{self.filename}: no further requests will be sent")
        LOGGER.info("File %s cancelled; no further requests will be sent", self.filename)
        return True

    # ----- processing -----

    def start(self, operator: Any) -> threading.Thread:
        thread = threading.Thread(target=self.run, args=(operator,), name=f"wm-fo-file-{self.run_id[:8]}",
                                  daemon=True)
        thread.start()
        return thread

    def run(self, operator: Any, sleep: Callable[[float], Any] | None = None) -> dict[str, Any]:
        """Process every valid line in order; return the final snapshot."""
        wait = sleep or self._stop.wait
        self._announce()
        try:
            for index, request in enumerate(self._requests):
                if self._stop.is_set():
                    break
                self._process(operator, request, index)
                if index + 1 < len(self._requests) and not self._stop.is_set():
                    self._pause(wait)
        except Exception:
            LOGGER.exception("Processing file %s failed", self.filename)
        finally:
            self._finish()
        return self.snapshot()

    def _announce(self) -> None:
        with self._lock:
            skipped = [item for item in self._items if item["state"] == "SKIPPED"]
        detail = f"{self.filename}: {len(self._requests)} request(s) to send one at a time"
        if skipped:
            detail += f", {len(skipped)} line(s) skipped"
        feed().record("LOCAL", "File", "FILE_LOADED", detail=detail)
        LOGGER.info("File %s loaded: %d request(s), %d skipped line(s)",
                    self.filename, len(self._requests), len(skipped))
        for item in skipped:
            feed().record("LOCAL", "File", "FILE_LINE_SKIPPED", detail=f"line {item['line']}: {item['error']}")
            LOGGER.warning("File %s line %d skipped: %s", self.filename, item["line"], item["error"])

    def _process(self, operator: Any, request: FileRequest, index: int) -> None:
        item = self._by_line[request.line]
        LOGGER.info("Processing file request %d of %d (line %d, %s)",
                    index + 1, len(self._requests), request.line, request.ws_id)
        origin = {"file": self.filename, "line": request.line, "run_id": self.run_id}
        try:
            request_id = operator.request_irrigation(request.ws_id, request.duration_seconds,
                                                     request.target_volume_liters, origin=origin)
        except Exception as exc:
            with self._lock:
                item["state"], item["outcome"], item["error"] = "SEND_FAILED", "SEND_FAILED", str(exc) or type(exc).__name__
            feed().record("LOCAL", "File", "FILE_LINE_NOT_SENT", ws_id=request.ws_id,
                          detail=f"line {request.line}: {item['error']}")
            LOGGER.error("File %s line %d could not be sent: %s", self.filename, request.line, item["error"])
            return
        with self._lock:
            item["state"], item["request_id"] = "SENT", request_id
        wait_seconds = request_wait(request, self.timeout)
        finished = operator.wait_for_result(request_id, wait_seconds)
        if not finished:
            mark = getattr(operator, "mark_no_result", None)
            if mark is not None:
                mark(request_id)
            LOGGER.warning("No final result for the request on %s within %.0f s; moving on",
                           request.ws_id, wait_seconds)
        tracked = getattr(operator, "tracked_request", None)
        final = tracked(request_id) if tracked is not None else None
        status = final.get("status") if isinstance(final, dict) else None
        with self._lock:
            item["state"] = "DONE"
            item["outcome"] = status if isinstance(status, str) else ("NO_RESULT" if not finished else "DONE")
            if isinstance(final, dict):
                item["reason"] = final.get("reason") or final.get("end_reason")

    def _pause(self, wait: Callable[[float], Any]) -> None:
        next_at = (datetime.now(timezone.utc) + timedelta(seconds=self.delay_seconds)) \
            .isoformat(timespec="milliseconds").replace("+00:00", "Z")
        with self._lock:
            if self._state != "RUNNING":
                return
            self._state, self._next_at = "WAITING", next_at
        feed().record("LOCAL", "File", "FILE_NEXT_REQUEST",
                      detail=f"next request in {self.delay_seconds:g} s")
        wait(self.delay_seconds)
        with self._lock:
            if self._state == "WAITING":
                self._state = "RUNNING"
            self._next_at = None

    def _finish(self) -> None:
        with self._lock:
            cancelled = self._state == "CANCELLING" or self._stop.is_set()
            for item in self._items:
                if item["state"] == "QUEUED":
                    item["state"] = "CANCELLED"
            outcomes = [item["outcome"] for item in self._items if item["outcome"]]
            parts = []
            for label, states in _SUMMARY_GROUPS:
                count = sum(1 for outcome in outcomes if outcome in states)
                if count:
                    parts.append(f"{count} {label}")
            not_sent = sum(1 for item in self._items if item["state"] == "CANCELLED")
            if not_sent:
                parts.append(f"{not_sent} cancelled")
            skipped = sum(1 for item in self._items if item["state"] == "SKIPPED")
            if skipped:
                parts.append(f"{skipped} skipped")
            self._summary = ", ".join(parts) or "nothing sent"
            self._state = "CANCELLED" if cancelled else "FINISHED"
            self._finished_at, self._next_at = _now(), None
            summary, state = self._summary, self._state
        feed().record("LOCAL", "File", "FILE_FINISHED" if state == "FINISHED" else "FILE_STOPPED",
                      detail=f"{self.filename}: {summary}")
        LOGGER.info("File %s %s: %s", self.filename, state.lower(), summary)

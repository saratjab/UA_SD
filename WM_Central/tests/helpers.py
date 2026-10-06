from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path
from typing import Callable

from WM_Central.database import Database


class FakeClock:
    """Deterministic, strictly increasing timestamps."""

    def __init__(self) -> None:
        self.ticks = 0

    def __call__(self) -> str:
        self.ticks += 1
        return f"2026-10-05T10:{self.ticks // 60:02d}:{self.ticks % 60:02d}Z"


class DatabaseTestCase(unittest.TestCase):
    """Gives each test a fresh SQLite file with the schema applied and two operators."""

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.db = Database(Path(tmp.name) / "test.db", clock=FakeClock())
        self.addCleanup(self.db.close)  # runs BEFORE tmp.cleanup (LIFO) - needed on Windows
        self.db.initialize()
        self.db.add_operator("OP_001", "One")
        self.db.add_operator("OP_002", "Two")

    def enroll(self, ws_id: str = "WS_001", location: str = "North Garden") -> None:
        result = self.db.register_station(ws_id, location)
        assert result.ok, result.reason


def wait_until(predicate: Callable[[], bool], timeout: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()

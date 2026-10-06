"""SQLite access layer for WM_Central.

Design rules (see docs/adr/ADR-001-state-model-and-schema.md):

* ONE sqlite3 connection shared by all threads, every operation guarded by a
  re-entrant lock. Central's database traffic is tiny (a few writes per
  irrigation) and SQLite serializes writers anyway, so the lock costs nothing -
  while a connection per thread would leak one connection for every Monitor
  thread that ever existed (Central must run indefinitely).
* Autocommit mode + explicit ``BEGIN IMMEDIATE`` transactions, so a "check then
  insert" sequence is atomic.
* Foreign keys are switched on (SQLite default is OFF).
* This layer stores facts. It does NOT decide policy (who may irrigate) - that
  belongs to ``authorization.py`` / ``state_manager.py``.
* Use a real file path. After close() every call raises RuntimeError, so stop
  every thread that uses the database BEFORE closing it.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

from .models import (
    AdminState,
    DenyReason,
    EndReason,
    Fault,
    IrrigationRecord,
    IrrigationStatus,
    Operator,
    RegistrationResult,
    Station,
)


LOGGER = logging.getLogger(__name__)

DB_DIR = Path(__file__).resolve().parent / "db"
SCHEMA_PATH = DB_DIR / "schema.sql"
SEED_DEV_PATH = DB_DIR / "seed_dev.sql"

MAX_WS_ID_LENGTH = 64

_LIVE_SQL = "('AUTHORIZED', 'WATERING')"


def utc_now() -> str:
    """ISO-8601 UTC, second precision, same format the Monitor produces."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class Database:
    def __init__(self, path: str | Path, *, clock: Callable[[], str] = utc_now) -> None:
        self._path = str(path)
        self._clock = clock
        self._lock = threading.RLock()
        self._connection: sqlite3.Connection | None = None
        self._closed = False

    # ------------------------------------------------------------------
    # Connection / transaction plumbing
    # ------------------------------------------------------------------

    def _conn(self) -> sqlite3.Connection:
        """The shared connection. Callers must already hold ``self._lock``."""
        if self._closed:
            raise RuntimeError("database is closed")
        if self._connection is None:
            conn = sqlite3.connect(
                self._path,
                isolation_level=None,     # autocommit; we manage transactions ourselves
                check_same_thread=False,  # shared across threads, guarded by self._lock
                timeout=5.0,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            self._connection = conn
        return self._connection

    def _one(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._conn().execute(sql, params).fetchone()

    def _all(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn().execute(sql, params).fetchall()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = self._conn()
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")

    def close(self) -> None:
        with self._lock:
            self._closed = True
            if self._connection is not None:
                try:
                    self._connection.close()
                except sqlite3.Error:
                    LOGGER.exception("Error closing SQLite connection")
                self._connection = None

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def initialize(self) -> None:
        """Create tables/indexes if missing (schema.sql is idempotent)."""
        with self._lock:
            self._conn().executescript(SCHEMA_PATH.read_text(encoding="utf-8"))

    def load_seed_file(self, path: str | Path = SEED_DEV_PATH) -> None:
        """Run a seed script (dev/demo data only)."""
        with self._lock:
            self._conn().executescript(Path(path).read_text(encoding="utf-8"))

    def recover_open_irrigations(self) -> int:
        """Startup recovery: after a restart nothing can still be watering
        under CENTRAL's control, so close every live irrigation."""
        now = self._clock()
        with self.transaction() as conn:
            cursor = conn.execute(
                "UPDATE irrigations SET status = 'ENDED', end_reason = ?, ended_at = ? "
                f"WHERE status IN {_LIVE_SQL}",
                (EndReason.CENTRAL_RESTART.value, now),
            )
        if cursor.rowcount:
            LOGGER.warning("Closed %d irrigation(s) left open by a previous run", cursor.rowcount)
        return cursor.rowcount

    # ------------------------------------------------------------------
    # Operators
    # ------------------------------------------------------------------

    def add_operator(self, operator_id: str, name: str | None = None, active: bool = True) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO operators (operator_id, name, active) VALUES (?, ?, ?) "
                "ON CONFLICT (operator_id) DO UPDATE SET name = excluded.name, active = excluded.active",
                (operator_id, name, int(active)),
            )

    def get_operator(self, operator_id: str) -> Operator | None:
        row = self._one("SELECT * FROM operators WHERE operator_id = ?", (operator_id,))
        return _operator(row) if row else None

    def list_operators(self) -> list[Operator]:
        return [_operator(row) for row in self._all("SELECT * FROM operators ORDER BY operator_id")]

    # ------------------------------------------------------------------
    # Stations
    # ------------------------------------------------------------------

    def register_station(self, ws_id: str, location: str | None) -> RegistrationResult:
        """Enroll a new station or re-authenticate a known one (one transaction).

        * unknown ID + location   -> enrolled
        * unknown ID, no location -> rejected ("missing_location")
        * known ID                -> authenticated; location refreshed if it changed
        """
        ws_id = ws_id.strip()
        if not ws_id or len(ws_id) > MAX_WS_ID_LENGTH:
            return RegistrationResult(False, "invalid_ws_id", None, False)
        location = location.strip() if isinstance(location, str) else None
        if not location:
            location = None

        now = self._clock()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM watering_stations WHERE ws_id = ?", (ws_id,)
            ).fetchone()
            if row is None:
                if location is None:
                    return RegistrationResult(False, "missing_location", None, False)
                conn.execute(
                    "INSERT INTO watering_stations (ws_id, location, admin_state, registered_at, last_seen_at) "
                    "VALUES (?, ?, 'ACTIVE', ?, ?)",
                    (ws_id, location, now, now),
                )
                enrolled = True
            else:
                conn.execute(
                    "UPDATE watering_stations SET location = ?, last_seen_at = ? WHERE ws_id = ?",
                    (location or row["location"], now, ws_id),
                )
                enrolled = False
            station = _station(
                conn.execute("SELECT * FROM watering_stations WHERE ws_id = ?", (ws_id,)).fetchone()
            )
        return RegistrationResult(True, None, station, enrolled)

    def get_station(self, ws_id: str) -> Station | None:
        row = self._one("SELECT * FROM watering_stations WHERE ws_id = ?", (ws_id,))
        return _station(row) if row else None

    def list_stations(self) -> list[Station]:
        return [_station(row) for row in self._all("SELECT * FROM watering_stations ORDER BY ws_id")]

    def set_admin_state(self, ws_id: str, state: AdminState) -> bool:
        with self.transaction() as conn:
            cursor = conn.execute(
                "UPDATE watering_stations SET admin_state = ? WHERE ws_id = ?",
                (state.value, ws_id),
            )
        return cursor.rowcount == 1

    def touch_last_seen(self, ws_id: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE watering_stations SET last_seen_at = ? WHERE ws_id = ?",
                (self._clock(), ws_id),
            )

    # ------------------------------------------------------------------
    # Faults (open fault == station is in LEAK)
    # ------------------------------------------------------------------

    def open_fault(
        self,
        ws_id: str,
        fault_type: str,
        details: str = "",
        detected_at: str | None = None,
    ) -> bool:
        """Record a fault. Returns False if the station already has an open one
        (the partial unique index makes the repeat a no-op)."""
        with self.transaction() as conn:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO faults (ws_id, fault_type, details, detected_at) "
                "VALUES (?, ?, ?, ?)",
                (ws_id, fault_type, details, detected_at or self._clock()),
            )
        return cursor.rowcount == 1

    def resolve_fault(self, ws_id: str, resolved_at: str | None = None) -> bool:
        with self.transaction() as conn:
            cursor = conn.execute(
                "UPDATE faults SET resolved_at = ? WHERE ws_id = ? AND resolved_at IS NULL",
                (resolved_at or self._clock(), ws_id),
            )
        return cursor.rowcount == 1

    def get_open_fault(self, ws_id: str) -> Fault | None:
        row = self._one("SELECT * FROM faults WHERE ws_id = ? AND resolved_at IS NULL", (ws_id,))
        return _fault(row) if row else None

    def open_faults(self) -> dict[str, Fault]:
        rows = self._all("SELECT * FROM faults WHERE resolved_at IS NULL")
        return {row["ws_id"]: _fault(row) for row in rows}

    # ------------------------------------------------------------------
    # Irrigations
    # ------------------------------------------------------------------

    def record_irrigation_request(
        self,
        request_id: str,
        ws_id: str,
        operator_id: str | None,
        requested_duration_s: int | None,
        deny_reason: DenyReason | None = None,
    ) -> IrrigationRecord:
        """Persist one request as AUTHORIZED, or DENIED if ``deny_reason`` is given.

        * Idempotent: a repeated ``request_id`` returns the stored record untouched.
        * If the caller wanted to authorize but the station already has a live
          irrigation, the request is stored as DENIED / WS_BUSY.
          (The partial unique index is the safety net behind this check.)
        """
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM irrigations WHERE request_id = ?", (request_id,)
            ).fetchone()
            if existing is not None:
                return _irrigation(existing)

            if deny_reason is None and conn.execute(
                f"SELECT 1 FROM irrigations WHERE ws_id = ? AND status IN {_LIVE_SQL}", (ws_id,)
            ).fetchone():
                deny_reason = DenyReason.WS_BUSY

            status = IrrigationStatus.DENIED if deny_reason else IrrigationStatus.AUTHORIZED
            conn.execute(
                "INSERT INTO irrigations "
                "(request_id, ws_id, operator_id, status, deny_reason, requested_duration_s, requested_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    request_id,
                    ws_id,
                    operator_id,
                    status.value,
                    deny_reason.value if deny_reason else None,
                    requested_duration_s,
                    self._clock(),
                ),
            )
            row = conn.execute(
                "SELECT * FROM irrigations WHERE request_id = ?", (request_id,)
            ).fetchone()
        return _irrigation(row)

    def mark_irrigation_started(self, request_id: str, started_at: str | None = None) -> bool:
        """AUTHORIZED -> WATERING (first sign of life from the Engine)."""
        with self.transaction() as conn:
            cursor = conn.execute(
                "UPDATE irrigations SET status = 'WATERING', started_at = ? "
                "WHERE request_id = ? AND status = 'AUTHORIZED'",
                (started_at or self._clock(), request_id),
            )
        return cursor.rowcount == 1

    def end_irrigation(
        self,
        request_id: str,
        end_reason: EndReason,
        *,
        duration_s: float | None = None,
        total_volume_l: float | None = None,
        ended_at: str | None = None,
    ) -> bool:
        """Close a live irrigation. Returns False if it was not live (idempotent)."""
        with self.transaction() as conn:
            cursor = conn.execute(
                "UPDATE irrigations SET status = 'ENDED', end_reason = ?, ended_at = ?, "
                "duration_s = ?, total_volume_l = ? "
                f"WHERE request_id = ? AND status IN {_LIVE_SQL}",
                (
                    end_reason.value,
                    ended_at or self._clock(),
                    duration_s,
                    total_volume_l,
                    request_id,
                ),
            )
        return cursor.rowcount == 1

    def end_live_irrigation(self, ws_id: str, end_reason: EndReason) -> IrrigationRecord | None:
        """Close whatever is live on a station (used when CENTRAL itself infers
        the end: disconnect, leak, block). Returns the closed record, if any."""
        now = self._clock()
        with self.transaction() as conn:
            row = conn.execute(
                f"SELECT * FROM irrigations WHERE ws_id = ? AND status IN {_LIVE_SQL}", (ws_id,)
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE irrigations SET status = 'ENDED', end_reason = ?, ended_at = ? "
                "WHERE irrigation_id = ?",
                (end_reason.value, now, row["irrigation_id"]),
            )
            ended = conn.execute(
                "SELECT * FROM irrigations WHERE irrigation_id = ?", (row["irrigation_id"],)
            ).fetchone()
        return _irrigation(ended)

    def get_irrigation(self, request_id: str) -> IrrigationRecord | None:
        row = self._one("SELECT * FROM irrigations WHERE request_id = ?", (request_id,))
        return _irrigation(row) if row else None

    def get_live_irrigation(self, ws_id: str) -> IrrigationRecord | None:
        row = self._one(
            f"SELECT * FROM irrigations WHERE ws_id = ? AND status IN {_LIVE_SQL}", (ws_id,)
        )
        return _irrigation(row) if row else None

    def live_irrigations(self) -> dict[str, IrrigationRecord]:
        rows = self._all(f"SELECT * FROM irrigations WHERE status IN {_LIVE_SQL}")
        return {row["ws_id"]: _irrigation(row) for row in rows}


# ----------------------------------------------------------------------
# Row -> dataclass mapping
# ----------------------------------------------------------------------

def _operator(row: sqlite3.Row) -> Operator:
    return Operator(row["operator_id"], row["name"], bool(row["active"]))


def _station(row: sqlite3.Row) -> Station:
    return Station(
        ws_id=row["ws_id"],
        location=row["location"],
        admin_state=AdminState(row["admin_state"]),
        registered_at=row["registered_at"],
        last_seen_at=row["last_seen_at"],
    )


def _fault(row: sqlite3.Row) -> Fault:
    return Fault(
        fault_id=row["fault_id"],
        ws_id=row["ws_id"],
        fault_type=row["fault_type"],
        details=row["details"],
        detected_at=row["detected_at"],
        resolved_at=row["resolved_at"],
    )


def _irrigation(row: sqlite3.Row) -> IrrigationRecord:
    return IrrigationRecord(
        irrigation_id=row["irrigation_id"],
        request_id=row["request_id"],
        ws_id=row["ws_id"],
        operator_id=row["operator_id"],
        status=IrrigationStatus(row["status"]),
        deny_reason=DenyReason(row["deny_reason"]) if row["deny_reason"] else None,
        requested_duration_s=row["requested_duration_s"],
        requested_at=row["requested_at"],
        started_at=row["started_at"],
        ended_at=row["ended_at"],
        duration_s=row["duration_s"],
        total_volume_l=row["total_volume_l"],
        end_reason=EndReason(row["end_reason"]) if row["end_reason"] else None,
    )

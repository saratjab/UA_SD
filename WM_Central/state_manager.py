"""Station state for WM_Central.

The displayed status of a station is DERIVED (ADR-001) from:

    connectivity        -> in memory only (``_connections``)
    admin_state         -> SQLite (blocked or not)
    open fault          -> SQLite (``faults`` table)
    live irrigation     -> SQLite (``irrigations`` table)

``StateManager`` is the SINGLE place that changes that state. Socket threads
(now) and Kafka threads (later) all go through it, so two channels can never
update a station in conflicting ways. One re-entrant lock protects the
memory + DB combination. The lock is only held for short local operations -
never while doing network I/O (callers close sockets *after* the call returns).
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Any

from .database import Database
from .models import (
    AdminState,
    EndReason,
    IrrigationStatus,
    StationStatus,
    StationView,
)


LOGGER = logging.getLogger(__name__)


def effective_status(
    *,
    connected: bool,
    admin_state: AdminState,
    has_open_fault: bool,
    irrigation_status: IrrigationStatus | None,
) -> StationStatus:
    """Displayed status, first match wins (see ADR-001)."""
    if not connected:
        return StationStatus.DISCONNECTED
    if has_open_fault:
        return StationStatus.LEAK
    if admin_state is AdminState.BLOCKED:
        return StationStatus.OUT_OF_SERVICE
    if irrigation_status is IrrigationStatus.WATERING:
        return StationStatus.WATERING
    return StationStatus.AVAILABLE


@dataclass(frozen=True)
class RegisterOutcome:
    ok: bool
    reason: str | None = None
    ws_id: str | None = None
    enrolled: bool = False
    replaced: Any = None  # previous connection for the same ws_id; caller must close it


@dataclass(frozen=True)
class DisconnectOutcome:
    was_current: bool
    ended_request_id: str | None = None


@dataclass(frozen=True)
class FaultOutcome:
    ok: bool
    reason: str | None = None
    is_new: bool = False
    ended_request_id: str | None = None


@dataclass(frozen=True)
class CommandOutcome:
    ok: bool
    ended_request_id: str | None = None


class StateManager:
    def __init__(self, db: Database) -> None:
        self._db = db
        self._lock = threading.RLock()
        # ws_id -> the connection object that currently represents the station.
        # Opaque to this class; the socket layer decides what it is.
        self._connections: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Connectivity (Monitor <-> Central socket)
    # ------------------------------------------------------------------

    def register(self, ws_id: str, location: str | None, connection: Any) -> RegisterOutcome:
        ws_id = ws_id.strip()
        with self._lock:
            result = self._db.register_station(ws_id, location)
            if not result.ok:
                return RegisterOutcome(False, result.reason)
            previous = self._connections.get(ws_id)
            self._connections[ws_id] = connection
            replaced = previous if previous is not connection else None
        LOGGER.info(
            "WS %s %s (%s)",
            ws_id,
            "ENROLLED" if result.enrolled else "authenticated",
            result.station.location if result.station else "?",
        )
        if replaced is not None:
            LOGGER.warning("WS %s registered again; the previous connection is replaced", ws_id)
        return RegisterOutcome(True, None, ws_id, result.enrolled, replaced)

    def is_current(self, ws_id: str, connection: Any) -> bool:
        with self._lock:
            return self._connections.get(ws_id) is connection

    def disconnect(self, ws_id: str, connection: Any) -> DisconnectOutcome:
        """The Monitor socket closed. Ignored if ``connection`` is stale
        (already replaced by a newer registration of the same station)."""
        with self._lock:
            if self._connections.get(ws_id) is not connection:
                return DisconnectOutcome(False)
            del self._connections[ws_id]
            self._db.touch_last_seen(ws_id)
            ended = self._db.end_live_irrigation(ws_id, EndReason.DISCONNECTED)
        LOGGER.warning("WS %s DISCONNECTED", ws_id)
        return DisconnectOutcome(True, ended.request_id if ended else None)

    def connected_ws_ids(self) -> set[str]:
        with self._lock:
            return set(self._connections)

    # ------------------------------------------------------------------
    # Faults
    # ------------------------------------------------------------------

    def report_fault(
        self,
        ws_id: str,
        connection: Any,
        fault_type: str,
        details: str = "",
        detected_at: str | None = None,
    ) -> FaultOutcome:
        """Fault reported by the station's Monitor. Any irrigation in progress
        ends immediately (PDF: "it must end immediately")."""
        with self._lock:
            if self._connections.get(ws_id) is not connection:
                return FaultOutcome(False, "not_registered")
            is_new = self._db.open_fault(ws_id, fault_type, details, detected_at)
            ended = self._db.end_live_irrigation(ws_id, EndReason.LEAK)
        LOGGER.error("WS %s LEAK (%s): %s%s", ws_id, fault_type, details, "" if is_new else " [repeat]")
        return FaultOutcome(True, None, is_new, ended.request_id if ended else None)

    def resolve_fault(self, ws_id: str, connection: Any = None) -> bool:
        """The contingency was resolved. If ``connection`` is given it must be
        the station's current one."""
        with self._lock:
            if connection is not None and self._connections.get(ws_id) is not connection:
                return False
            resolved = self._db.resolve_fault(ws_id)
        if resolved:
            LOGGER.info("WS %s fault resolved", ws_id)
        return resolved

    # ------------------------------------------------------------------
    # CENTRAL commands (state side; Kafka delivery comes later)
    # ------------------------------------------------------------------

    def block_station(self, ws_id: str) -> CommandOutcome:
        """BLOCK WS: end any irrigation in progress, then OUT OF SERVICE."""
        with self._lock:
            if not self._db.set_admin_state(ws_id, AdminState.BLOCKED):
                return CommandOutcome(False)
            ended = self._db.end_live_irrigation(ws_id, EndReason.BLOCKED)
        LOGGER.info("WS %s BLOCKED", ws_id)
        return CommandOutcome(True, ended.request_id if ended else None)

    def activate_station(self, ws_id: str) -> CommandOutcome:
        with self._lock:
            ok = self._db.set_admin_state(ws_id, AdminState.ACTIVE)
        if ok:
            LOGGER.info("WS %s ACTIVATED", ws_id)
        return CommandOutcome(ok)

    # ------------------------------------------------------------------
    # Read side (monitoring panel, authorization)
    # ------------------------------------------------------------------

    def status_of(self, ws_id: str) -> StationStatus | None:
        """None if the station is unknown."""
        for view in self.snapshot():
            if view.ws_id == ws_id:
                return view.status
        return None

    def snapshot(self) -> list[StationView]:
        """Consistent picture of every known station, sorted by ID."""
        with self._lock:
            stations = self._db.list_stations()
            faults = self._db.open_faults()
            live = self._db.live_irrigations()
            connected = set(self._connections)

        views: list[StationView] = []
        for station in stations:
            irrigation = live.get(station.ws_id)
            fault = faults.get(station.ws_id)
            status = effective_status(
                connected=station.ws_id in connected,
                admin_state=station.admin_state,
                has_open_fault=fault is not None,
                irrigation_status=irrigation.status if irrigation else None,
            )
            views.append(
                StationView(
                    ws_id=station.ws_id,
                    location=station.location,
                    status=status,
                    admin_state=station.admin_state,
                    operator_id=(
                        irrigation.operator_id
                        if irrigation and status is StationStatus.WATERING
                        else None
                    ),
                    fault=f"{fault.fault_type}: {fault.details}" if fault else None,
                )
            )
        return views

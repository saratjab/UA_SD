"""Domain vocabulary for WM_Central.

Enum *values* are the exact strings stored in SQLite (see db/schema.sql) and,
later, sent over Kafka. Never rename them silently.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class StationStatus(str, Enum):
    """The five states required by the PDF. DERIVED - never stored."""

    AVAILABLE = "AVAILABLE"            # green
    WATERING = "WATERING"              # green, blinking
    LEAK = "LEAK"                      # red
    OUT_OF_SERVICE = "OUT_OF_SERVICE"  # orange
    DISCONNECTED = "DISCONNECTED"      # gray


class AdminState(str, Enum):
    """The only durable per-station state: blocked by CENTRAL or not."""

    ACTIVE = "ACTIVE"
    BLOCKED = "BLOCKED"


class IrrigationStatus(str, Enum):
    DENIED = "DENIED"
    AUTHORIZED = "AUTHORIZED"
    WATERING = "WATERING"
    ENDED = "ENDED"


class DenyReason(str, Enum):
    WS_DISCONNECTED = "WS_DISCONNECTED"
    WS_LEAK = "WS_LEAK"
    WS_OUT_OF_SERVICE = "WS_OUT_OF_SERVICE"
    WS_BUSY = "WS_BUSY"
    OPERATOR_INACTIVE = "OPERATOR_INACTIVE"


class EndReason(str, Enum):
    # Reported by the Engine
    DURATION = "DURATION"
    MANUAL = "MANUAL"
    LEAK = "LEAK"
    BLOCKED = "BLOCKED"
    # Inferred by CENTRAL (no final event can arrive)
    DISCONNECTED = "DISCONNECTED"
    CENTRAL_RESTART = "CENTRAL_RESTART"


# Irrigation statuses that mean "this station is occupied".
LIVE_IRRIGATION_STATUSES = (IrrigationStatus.AUTHORIZED, IrrigationStatus.WATERING)


@dataclass(frozen=True)
class Operator:
    operator_id: str
    name: str | None
    active: bool


@dataclass(frozen=True)
class Station:
    ws_id: str
    location: str
    admin_state: AdminState
    registered_at: str
    last_seen_at: str | None


@dataclass(frozen=True)
class Fault:
    fault_id: int
    ws_id: str
    fault_type: str
    details: str | None
    detected_at: str
    resolved_at: str | None


@dataclass(frozen=True)
class IrrigationRecord:
    irrigation_id: int
    request_id: str
    ws_id: str
    operator_id: str | None
    status: IrrigationStatus
    deny_reason: DenyReason | None
    requested_duration_s: int | None
    requested_at: str
    started_at: str | None
    ended_at: str | None
    duration_s: float | None
    total_volume_l: float | None
    end_reason: EndReason | None


@dataclass(frozen=True)
class RegistrationResult:
    ok: bool
    reason: str | None
    station: Station | None
    enrolled: bool  # True = brand-new station, False = known station re-authenticated


@dataclass(frozen=True)
class StationView:
    """What the monitoring panel needs to draw one station."""

    ws_id: str
    location: str
    status: StationStatus
    admin_state: AdminState
    operator_id: str | None  # only while WATERING
    fault: str | None        # description of the open fault, if any

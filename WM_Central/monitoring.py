"""Monitoring-panel helpers.

For now: the status -> colour mapping required by the PDF and a plain-text
station table. The real panel (live refresh, blinking, flow/volume) is built
on top of ``StateManager.snapshot()`` in a later step.
"""

from __future__ import annotations

import logging
import threading

from .models import StationStatus, StationView
from .state_manager import StateManager


# Required by the PDF.
STATUS_COLOR: dict[StationStatus, str] = {
    StationStatus.AVAILABLE: "GREEN",
    StationStatus.WATERING: "GREEN BLINKING",
    StationStatus.LEAK: "RED",
    StationStatus.OUT_OF_SERVICE: "ORANGE",
    StationStatus.DISCONNECTED: "GRAY",
}


def format_station_table(views: list[StationView]) -> str:
    if not views:
        return "  (no stations registered yet)"
    lines = [f"  {'WS ID':<12}{'LOCATION':<24}{'STATUS':<17}{'FLOW L/M':>10}{'VOLUME L':>12}  FAULT"]
    for view in views:
        caption = "Out of Service" if view.status is StationStatus.OUT_OF_SERVICE else view.status.value
        lines.append(
            f"  {view.ws_id:<12}{view.location:<24}{caption:<17}"
            f"{view.flow_rate_lpm if view.flow_rate_lpm is not None else '-':>10}"
            f"{view.accumulated_volume_liters if view.accumulated_volume_liters is not None else '-':>12}  "
            f"{view.fault or '-'} ({STATUS_COLOR[view.status]})"
        )
    return "\n".join(lines)


def log_station_table(logger: logging.Logger, state: StateManager) -> None:
    logger.info("Registered stations:\n%s", format_station_table(state.snapshot()))


def monitor_loop(state: StateManager, stop: threading.Event, interval: float = 1.0) -> None:
    """Refresh the terminal view without holding up Kafka or socket workers."""
    logger = logging.getLogger("wm_central.monitor")
    previous: str | None = None
    while not stop.wait(interval):
        current = format_station_table(state.snapshot())
        if current != previous:
            logger.info("Live station panel:\n%s", current)
            previous = current


def select_station_ids(selection: str, state: StateManager) -> list[str]:
    """Resolve one WS ID or the `all` selector against the current registry."""
    stations = state.snapshot()
    if selection.strip().lower() == "all":
        return [station.ws_id for station in stations]
    wanted = selection.strip()
    return [station.ws_id for station in stations if station.ws_id == wanted]


def execute_control_line(line: str, state: StateManager, service: object, logger: logging.Logger | None = None) -> bool:
    """Run START/BLOCK/ACTIVATE line; return False for exit, True otherwise."""
    logger = logger or logging.getLogger("wm_central.control")
    args = line.split()
    if not args:
        return True
    command = args[0].upper()
    if command in {"QUIT", "EXIT"}:
        return False
    if command in {"HELP", "?"}:
        logger.info("Commands: START <ws_id|all> <seconds> [operator_id], BLOCK <ws_id|all>, ACTIVATE <ws_id|all>, HELP, QUIT")
        return True
    if command not in {"START", "BLOCK", "ACTIVATE"}:
        logger.warning("Unknown CENTRAL command: %s", args[0])
        return True
    if len(args) < 2:
        logger.warning("Usage: %s <ws_id|all>%s", command, " <seconds> [operator_id]" if command == "START" else "")
        return True
    targets = select_station_ids(args[1], state)
    if not targets:
        logger.warning("No registered station matches %s", args[1])
        return True
    if command == "START":
        if len(args) not in {3, 4}:
            logger.warning("Usage: START <ws_id|all> <seconds> [operator_id]")
            return True
        try:
            duration = int(args[2])
            if duration <= 0:
                raise ValueError
        except ValueError:
            logger.warning("Duration must be a positive whole number of seconds")
            return True
        operators = state.operators()
        operator_id = args[3] if len(args) == 4 else next((op.operator_id for op in operators if op.active), None)
        if operator_id is None:
            logger.warning("No active operator is registered; pass an operator_id or register one first")
            return True
        for ws_id in targets:
            request_id = service.start_irrigation(ws_id, duration, operator_id)
            record = state.irrigation(request_id)
            outcome = record.status.value if record else "UNKNOWN"
            reason = f" reason={record.deny_reason.value}" if record and record.deny_reason else ""
            logger.info("CENTRAL START %s ws=%s request=%s%s", outcome, ws_id, request_id, reason)
    else:
        event = "BLOCK_WS" if command == "BLOCK" else "ACTIVATE_WS"
        for ws_id in targets:
            ok = service.issue_station_command(event, ws_id)
            logger.info("CENTRAL %s ws=%s %s", command, ws_id, "accepted" if ok else "refused (disconnected or faulted)" if command == "ACTIVATE" else "failed")
    return True


def control_loop(state: StateManager, service: object, stop: threading.Event) -> None:
    """stdin runs on its own thread so socket and Kafka event processing continues."""
    while not stop.is_set():
        try:
            line = input("central> ")
        except (EOFError, OSError):
            stop.set()
            return
        if not execute_control_line(line, state, service):
            stop.set()
            return

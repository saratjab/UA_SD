"""Monitoring-panel helpers.

The live status table is built on ``StateManager.snapshot()`` and deliberately
uses plain text because it is also written to Railway logs.
"""

from __future__ import annotations

import logging
import sys
import threading
from collections.abc import Callable

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
    lines = [f"  {'WS ID':<12}{'LOCATION':<24}{'STATUS':<17}{'FLOW L/M':>10}{'VOLUME L':>12}  {'OPERATOR':<12} FAULT"]
    for view in views:
        caption = "Out of Service" if view.status is StationStatus.OUT_OF_SERVICE else view.status.value
        if view.status is StationStatus.WATERING:
            caption = "● " + caption
        lines.append(
            f"  {view.ws_id:<12}{view.location:<24}{caption:<17}"
            f"{view.flow_rate_lpm if view.flow_rate_lpm is not None else '-':>10}"
            f"{view.accumulated_volume_liters if view.accumulated_volume_liters is not None else '-':>12}  "
            f"{(view.operator_id if view.status is StationStatus.WATERING else '-'):<12} "
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
    """Run the optional terminal menu on its own thread.

    EOF disables the menu only; it must never stop the Central service. In
    headless deployments callers should avoid starting this thread at all.
    """
    logger = logging.getLogger("wm_central.control")
    if not _stdin_is_interactive():
        logger.info("Interactive menu disabled: stdin is not an interactive terminal")
        return
    while not stop.is_set():
        try:
            show_control_menu(logger)
            choice = input("Central choice: ").strip()
        except (EOFError, OSError):
            logger.info("Interactive menu input closed; Central service continues")
            return
        try:
            if not execute_menu_choice(choice, state, service, logger):
                logger.info("Interactive menu closed; Central service continues")
                return
        except Exception:
            logger.exception("Central menu action %r failed", choice)


def start_control_menu(state: StateManager, service: object, stop: threading.Event) -> threading.Thread | None:
    """Start the optional menu only when stdin is attached to a terminal."""
    if not _stdin_is_interactive():
        logging.getLogger("wm_central.control").info(
            "Interactive Central menu disabled: stdin is not a terminal; the service continues headlessly"
        )
        return None
    thread = threading.Thread(target=control_loop, args=(state, service, stop), name="central-control", daemon=True)
    thread.start()
    return thread


def _stdin_is_interactive() -> bool:
    return bool(getattr(sys.stdin, "isatty", lambda: False)())


def show_control_menu(logger: logging.Logger | None = None) -> None:
    logger = logger or logging.getLogger("wm_central.control")
    logger.info(
        "Central controls:\n"
        "  1. Display station status\n"
        "  2. Start irrigation for one station\n"
        "  3. Start irrigation on all eligible stations\n"
        "  4. Block one station\n"
        "  5. Block all stations\n"
        "  6. Activate one station\n"
        "  7. Activate all stations\n"
        "  0. Close menu"
    )


def execute_menu_choice(
    choice: str,
    state: StateManager,
    service: object,
    logger: logging.Logger | None = None,
    *,
    input_fn: Callable[[str], str] | None = None,
) -> bool:
    """Execute one numbered Central menu choice; return False for menu exit."""
    logger = logger or logging.getLogger("wm_central.control")
    read = input_fn or input
    choice = choice.strip()
    if choice == "0":
        return False
    if choice == "1":
        logger.info("Station status:\n%s", format_station_table(state.snapshot()))
        return True
    if choice not in {"2", "3", "4", "5", "6", "7"}:
        logger.error("Invalid menu choice %r. Enter a number from 0 to 7.", choice)
        return True

    action = {"2": "START", "3": "START", "4": "BLOCK", "5": "BLOCK", "6": "ACTIVATE", "7": "ACTIVATE"}[choice]
    all_stations = choice in {"3", "5", "7"}
    if all_stations:
        targets = [station.ws_id for station in state.snapshot()]
        if choice == "3":
            targets = [station.ws_id for station in state.snapshot() if station.status is StationStatus.AVAILABLE]
            if not targets:
                logger.warning("No eligible AVAILABLE stations to start")
                return True
    else:
        ws_id = read("Station ID: ").strip()
        targets = select_station_ids(ws_id, state)
        if not targets:
            logger.error("Invalid station ID %r; no registered station matches it", ws_id)
            return True

    if action == "START":
        raw_duration = read("Duration in whole seconds: ").strip()
        try:
            duration = int(raw_duration)
            if duration <= 0:
                raise ValueError
        except ValueError:
            logger.error("Duration must be a positive whole number of seconds")
            return True
        operators = state.operators()
        operator_id = next((op.operator_id for op in operators if op.active), None)
        if operator_id is None:
            logger.error("No active operator is registered; irrigation was not started")
            return True
        for target in targets:
            request_id = service.start_irrigation(target, duration, operator_id)
            record = state.irrigation(request_id)
            if record is None or record.status.value != "AUTHORIZED":
                reason = getattr(getattr(record, "deny_reason", None), "value", "unknown")
                logger.warning("Irrigation denied for station %s (request %s): %s", target, request_id, reason)
            else:
                logger.info("Irrigation authorized for station %s (request %s)", target, request_id)
        return True

    event = "BLOCK_WS" if action == "BLOCK" else "ACTIVATE_WS"
    for target in targets:
        ok = service.issue_station_command(event, target)
        if ok:
            logger.info("%s accepted for station %s", action, target)
        else:
            logger.warning("%s refused for station %s (disconnected, faulted, or invalid state)", action, target)
    return True

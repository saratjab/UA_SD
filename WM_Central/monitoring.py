"""Monitoring-panel helpers.

For now: the status -> colour mapping required by the PDF and a plain-text
station table. The real panel (live refresh, blinking, flow/volume) is built
on top of ``StateManager.snapshot()`` in a later step.
"""

from __future__ import annotations

import logging

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
    lines = [f"  {'WS ID':<12}{'LOCATION':<28}{'STATUS':<16}COLOR"]
    for view in views:
        caption = "Out of Service" if view.status is StationStatus.OUT_OF_SERVICE else view.status.value
        lines.append(
            f"  {view.ws_id:<12}{view.location:<28}{caption:<16}{STATUS_COLOR[view.status]}"
        )
    return "\n".join(lines)


def log_station_table(logger: logging.Logger, state: StateManager) -> None:
    logger.info("Registered stations:\n%s", format_station_table(state.snapshot()))

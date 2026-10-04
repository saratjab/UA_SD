from __future__ import annotations

import logging


LOGGER = logging.getLogger(__name__)


class SolenoidValve:
    def __init__(self) -> None:
        self.is_open = False

    def open(self) -> None:
        self.is_open = True
        LOGGER.info("Valve opened")

    def close(self) -> None:
        self.is_open = False
        LOGGER.info("Valve closed")


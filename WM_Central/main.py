"""WM_Central entry point.

Run from the repository root (the folder that contains WM_Central/ and common/):

    python -m WM_Central.main --socket-port 5000 --kafka-host localhost --kafka-port 9092 --seed-dev

Startup order follows the PDF ("Solution mechanics", step 1): open the DB, load
the registered stations, show them DISCONNECTED until they connect, then listen.
"""

from __future__ import annotations

import logging
import sys
import threading

from .config import parse_args
from .database import Database
from .monitoring import log_station_table
from .socket_server import CentralSocketServer
from .state_manager import StateManager


LOGGER = logging.getLogger("wm_central")


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv)
    configure_logging(config.log_level)

    db = Database(config.db_path)
    db.initialize()
    db.recover_open_irrigations()
    if config.seed_dev:
        db.load_seed_file()
        LOGGER.info("Development operators loaded")

    state = StateManager(db)
    log_station_table(LOGGER, state)  # every known station is DISCONNECTED right now

    # TODO(kafka): config.kafka_host / kafka_port are parsed but not used yet.
    server = CentralSocketServer(config.socket_host, config.socket_port, state)
    server.start()

    stop = threading.Event()
    try:
        while not stop.wait(1.0):  # 1 s ticks keep Ctrl+C responsive on every OS
            pass
    except KeyboardInterrupt:
        LOGGER.info("Shutting down")
    finally:
        server.stop()
        db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

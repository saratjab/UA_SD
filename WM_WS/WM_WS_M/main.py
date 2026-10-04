from __future__ import annotations

import logging
import sys

from .central_client import CentralClient
from .config import parse_args
from .monitor import WateringStationMonitor
from .ws_e_server import EngineServer


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    config = parse_args(argv)

    central_client = CentralClient(
        host=config.central_host,
        port=config.central_port,
        ws_id=config.ws_id,
    )
    engine_server = EngineServer(
        host=config.engine_host,
        port=config.engine_port,
        ws_id=config.ws_id,
        hello_timeout=config.engine_hello_timeout,
    )
    monitor = WateringStationMonitor(
        config=config,
        central_client=central_client,
        engine_server=engine_server,
    )
    return monitor.run()


if __name__ == "__main__":
    sys.exit(main())

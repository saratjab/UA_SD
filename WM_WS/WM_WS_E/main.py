from __future__ import annotations

import logging
import sys
import threading

from .config import parse_args
from .engine import WateringStationEngine


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def start_command_thread(engine: WateringStationEngine) -> threading.Thread:
    def command_loop() -> None:
        while True:
            try:
                command = input().strip().lower()
            except EOFError:
                return
            if command in {"f", "fail", "ko"}:
                engine.simulate_failure()
            elif command in {"c", "clear"}:
                engine.clear_failure()
            elif command in {"s", "stop"}:
                engine.stop_irrigation()
            elif command in {"q", "quit", "exit"}:
                engine.close()
                return

    thread = threading.Thread(target=command_loop, name="wm-ws-e-commands", daemon=True)
    thread.start()
    return thread


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    config = parse_args(argv)
    engine = WateringStationEngine(config)
    start_command_thread(engine)
    return engine.run()


if __name__ == "__main__":
    sys.exit(main())


from __future__ import annotations

import logging
import sys
import threading
import time

from .config import parse_args
from .engine import WateringStationEngine
from .kafka_service import EngineKafkaService
from common.kafka_transport import KafkaPublisher, KafkaUnavailable


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
    worker = None
    try:
        kafka = EngineKafkaService(engine, KafkaPublisher(f"{config.kafka_host}:{config.kafka_port}"))
        worker = kafka.worker(f"{config.kafka_host}:{config.kafka_port}")
        worker.start()
        threading.Thread(target=_poll_worker, args=(worker,), name="wm-ws-e-kafka", daemon=True).start()
    except KafkaUnavailable as exc:
        logging.getLogger(__name__).error("Kafka integration disabled: %s", exc)
    start_command_thread(engine)
    try:
        return engine.run()
    finally:
        if worker is not None:
            worker.close()


def _poll_worker(worker: object) -> None:
    while True:
        try:
            worker.poll_once(1.0)  # type: ignore[attr-defined]
        except Exception:
            logging.getLogger(__name__).exception("Kafka command worker stopped")
            return


if __name__ == "__main__":
    sys.exit(main())


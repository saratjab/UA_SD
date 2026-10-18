from __future__ import annotations

import logging
import signal
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
    signal.signal(signal.SIGTERM, lambda _signum, _frame: engine.close())
    worker = None
    poll_stop = threading.Event()
    poll_thread = None
    try:
        kafka = EngineKafkaService(engine, KafkaPublisher(f"{config.kafka_host}:{config.kafka_port}"))
        worker = kafka.worker(f"{config.kafka_host}:{config.kafka_port}")
        worker.start()
        poll_thread = threading.Thread(
            target=_poll_worker, args=(worker, poll_stop), name="wm-ws-e-kafka", daemon=True,
        )
        poll_thread.start()
    except KafkaUnavailable as exc:
        logging.getLogger(__name__).error("Kafka integration disabled: %s", exc)
    start_command_thread(engine)
    try:
        return engine.run()
    finally:
        if worker is not None:
            poll_stop.set()
            if poll_thread is not None:
                poll_thread.join(timeout=3.0)
            if poll_thread is not None and poll_thread.is_alive():
                logging.getLogger(__name__).warning(
                    "Engine Kafka polling thread did not stop; leaving its consumer open to avoid closing during poll"
                )
            else:
                worker.close()


def _poll_worker(worker: object, stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            worker.poll_once(1.0)  # type: ignore[attr-defined]
        except Exception:
            logging.getLogger(__name__).exception("Kafka command worker stopped")
            return


if __name__ == "__main__":
    sys.exit(main())


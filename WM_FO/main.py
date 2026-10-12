"""Field Operator entry point.

Run from the repository root with:
    python -m WM_FO.main --kafka-host localhost --kafka-port 9092 --operator-id OP_001
"""
from __future__ import annotations

import logging
import sys
import threading
import time

from common.kafka_transport import KafkaPublisher, KafkaUnavailable

from .config import parse_args
from .kafka_client import FieldOperator
from .request_file import load_requests

LOGGER = logging.getLogger("wm_fo")
FILE_REQUEST_DELAY_SECONDS = 4.0


def _poll_worker(worker: object) -> None:
    while True:
        try:
            worker.poll_once(1.0)  # type: ignore[attr-defined]
        except Exception:
            LOGGER.exception("Field Operator Kafka worker stopped")
            return


def _submit_and_wait(operator: FieldOperator, ws_id: str, duration: int, timeout: float) -> None:
    request_id = operator.request_irrigation(ws_id, duration)
    operator.wait_for_result(request_id, timeout)


def _file_mode(operator: FieldOperator, path: str, timeout: float) -> None:
    requests = load_requests(path)
    for index, request in enumerate(requests):
        LOGGER.info("Processing file request %d of %d", index + 1, len(requests))
        _submit_and_wait(operator, request["ws_id"], request["duration_seconds"], timeout)
        if index + 1 < len(requests):
            time.sleep(FILE_REQUEST_DELAY_SECONDS)


def _interactive_mode(operator: FieldOperator, timeout: float) -> None:
    print("Enter a station ID and irrigation duration in seconds. Type 'q' to quit.")
    while True:
        try:
            ws_id = input("WS ID: ").strip()
        except EOFError:
            return
        if ws_id.lower() in {"q", "quit", "exit"}:
            return
        if not ws_id:
            continue
        try:
            duration_text = input("Duration (seconds): ").strip()
            duration = int(duration_text)
            if duration <= 0:
                raise ValueError
        except (EOFError, ValueError):
            print("Duration must be a positive whole number.")
            continue
        _submit_and_wait(operator, ws_id, duration, timeout)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = parse_args(argv)
    bootstrap = f"{config.kafka_host}:{config.kafka_port}"
    worker = None
    publisher = None
    try:
        publisher = KafkaPublisher(bootstrap)
        operator = FieldOperator(config.operator_id, publisher)
        worker = operator.worker(bootstrap)
        worker.start()
        threading.Thread(target=_poll_worker, args=(worker,), name="wm-fo-kafka", daemon=True).start()
    except KafkaUnavailable as exc:
        LOGGER.error("Field Operator cannot start without Kafka: %s", exc)
        return 1
    try:
        if config.request_file:
            _file_mode(operator, config.request_file, config.request_timeout)
        elif config.ws_id:
            _submit_and_wait(operator, config.ws_id, config.duration_seconds or 0, config.request_timeout)
            # The command-line request starts the operator session; it should not
            # make the long-running Kafka consumer exit when that request ends.
            _interactive_mode(operator, config.request_timeout)
        else:
            _interactive_mode(operator, config.request_timeout)
    except (OSError, ValueError) as exc:
        LOGGER.error("Field Operator request failed: %s", exc)
        return 2
    except KeyboardInterrupt:
        LOGGER.info("Field Operator interrupted")
    finally:
        if worker is not None:
            worker.close()
        if publisher is not None:
            publisher.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

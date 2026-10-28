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
POLL_SHUTDOWN_TIMEOUT_SECONDS = 3.0


def _poll_worker(worker: object, ready: threading.Event | None = None,
                 failed: threading.Event | None = None,
                 stop: threading.Event | None = None) -> None:
    # The initial poll runs in the same thread that will keep consuming.  The
    # caller may publish only after this poll completes, so no request response
    # can arrive in the gap between starting the thread and entering poll().
    try:
        worker.poll_once(0.0)  # type: ignore[attr-defined]
    except Exception:
        LOGGER.exception("Field Operator Kafka worker stopped during startup")
        if failed is not None:
            failed.set()
        if ready is not None:
            ready.set()
        return
    if ready is not None:
        ready.set()
    while stop is None or not stop.is_set():
        try:
            worker.poll_once(1.0)  # type: ignore[attr-defined]
        except Exception:
            LOGGER.exception("Field Operator Kafka worker stopped")
            if failed is not None:
                failed.set()
            return


def _stop_poll_worker(poll_thread: threading.Thread | None,
                      stop: threading.Event) -> bool:
    """Stop and join the poller before its consumer may be closed."""
    stop.set()
    if poll_thread is None or poll_thread.ident is None:
        return True
    if poll_thread is threading.current_thread():
        LOGGER.warning("Cannot join the current FO Kafka polling thread; consumer left open")
        return False
    poll_thread.join(timeout=POLL_SHUTDOWN_TIMEOUT_SECONDS)
    if poll_thread.is_alive():
        LOGGER.warning("FO Kafka polling thread did not stop within %.1f seconds; "
                       "consumer left open to avoid closing it during poll",
                       POLL_SHUTDOWN_TIMEOUT_SECONDS)
        return False
    return True


def _submit_and_wait(operator: FieldOperator, ws_id: str, duration: int | None,
                     timeout: float | None, target_volume_liters: float | None = None) -> bool:
    request_id = operator.request_irrigation(ws_id, duration, target_volume_liters)
    return operator.wait_for_result(request_id, timeout)


def _file_mode(operator: FieldOperator, path: str, timeout: float) -> None:
    requests = load_requests(path)
    for index, request in enumerate(requests):
        LOGGER.info("Processing file request %d of %d", index + 1, len(requests))
        duration = request.get("duration_seconds")
        target = request.get("target_volume_liters")
        # File runs advance only after a terminal result, including a denial.
        # Indefinite mode therefore waits until the supported manual stop arrives.
        if not _submit_and_wait(operator, request["ws_id"], duration, None, target):
            raise RuntimeError(f"request {request.get('ws_id')} lost its terminal-result waiter; stopping file sequence")
        if index + 1 < len(requests):
            time.sleep(FILE_REQUEST_DELAY_SECONDS)


def _is_quit(value: str) -> bool:
    return value.strip().casefold() in {"q", "quit", "exit"}


def _interactive_mode(operator: FieldOperator, timeout: float) -> None:
    print("Enter a station ID and irrigation duration in seconds. Type 'q' to quit.")
    while True:
        try:
            ws_id = input("WS ID: ").strip()
        except EOFError:
            return
        if _is_quit(ws_id):
            return
        if not ws_id:
            continue
        try:
            duration_text = input("Duration (seconds): ").strip()
            if _is_quit(duration_text):
                return
            duration = int(duration_text)
            if duration <= 0:
                raise ValueError
        except EOFError:
            return
        except ValueError:
            print("Duration must be a positive whole number.")
            continue
        _submit_and_wait(operator, ws_id, duration, timeout)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = parse_args(argv)
    bootstrap = f"{config.kafka_host}:{config.kafka_port}"
    worker = None
    publisher = None
    poll_thread = None
    poll_stop = threading.Event()
    exit_code = 0
    try:
        try:
            publisher = KafkaPublisher(bootstrap)
            operator = FieldOperator(config.operator_id, publisher)
            worker = operator.worker(bootstrap)
            worker.start()
            if not worker.wait_until_assigned(timeout=10.0):
                raise KafkaUnavailable("Field Operator consumer did not resolve a valid starting position")
            poll_ready = threading.Event()
            poll_failed = threading.Event()
            poll_thread = threading.Thread(
                target=_poll_worker, args=(worker, poll_ready, poll_failed, poll_stop),
                name="wm-fo-kafka", daemon=True)
            poll_thread.start()
            if not poll_ready.wait(timeout=10.0) or poll_failed.is_set():
                raise KafkaUnavailable("Field Operator Kafka polling loop did not become ready")
        except KafkaUnavailable as exc:
            LOGGER.error("Field Operator cannot start without Kafka: %s", exc)
            exit_code = 1

        if exit_code == 0:
            try:
                if config.request_file:
                    _file_mode(operator, config.request_file, config.request_timeout)
                elif config.ws_id:
                    _submit_and_wait(operator, config.ws_id, config.duration_seconds or 0,
                                     config.request_timeout)
                    # Keep consuming after the command-line request ends.
                    _interactive_mode(operator, config.request_timeout)
                else:
                    _interactive_mode(operator, config.request_timeout)
            except (OSError, ValueError, RuntimeError) as exc:
                LOGGER.error("Field Operator request failed: %s", exc)
                exit_code = 2
            except KeyboardInterrupt:
                LOGGER.info("Field Operator interrupted")
    except KeyboardInterrupt:
        LOGGER.info("Field Operator interrupted")
    finally:
        poll_stopped = _stop_poll_worker(poll_thread, poll_stop)
        if not poll_stopped:
            exit_code = 1
        try:
            if worker is not None and poll_stopped:
                worker.close()
        except Exception:
            LOGGER.exception("Field Operator Kafka consumer failed to close")
            raise
        finally:
            if publisher is not None:
                publisher.close()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

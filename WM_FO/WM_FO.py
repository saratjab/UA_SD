"""Field Operator entry point.

Run from the repository root with:
    python -m WM_FO.WM_FO --kafka-host localhost --kafka-port 9092 --operator-id OP_001
"""
from __future__ import annotations

import logging
import sys
import threading
import time

from http import HTTPStatus
from pathlib import Path

from common.kafka_transport import KafkaPublisher, KafkaUnavailable
from common.web_console import (ApiError, ApiRequest, ConsoleServer, optional_positive_int,
                                optional_positive_number, require_str, start_console)

from .config import parse_args
from .file_runner import FILE_REQUEST_DELAY_SECONDS, FILE_REQUEST_GRACE_SECONDS, FileRun
from .kafka_client import FieldOperator
from .request_file import parse_request_lines, read_request_file

LOGGER = logging.getLogger("wm_fo")
POLL_SHUTDOWN_TIMEOUT_SECONDS = 3.0
# How long the interactive menu waits for Central to authorize or deny a
# request before showing the station list again.
DECISION_WAIT_SECONDS = 5.0
# The web console asks Central for the station list with a short timeout so
# the page stays responsive when Central is slow or down.
CONSOLE_DISCOVERY_TIMEOUT_SECONDS = 5.0


def _poll_worker(worker: object, ready: threading.Event | None = None,
                 failed: threading.Event | None = None,
                 stop: threading.Event | None = None) -> None:
    try:
        worker.poll_once(0.0)
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
            worker.poll_once(1.0)
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


def _file_mode(operator: FieldOperator, path: str, timeout: float) -> dict:
    """Send each request, wait for its outcome, pause 4 s, then send the next.

    The spec moves on once a request concludes "whether successfully or not",
    so one request whose outcome never arrives must not block the whole file.
    Unusable lines are reported with their line number and skipped.
    """
    requests, skipped = read_request_file(path)
    if not requests:
        problems = "; ".join(f"line {s.line}: {s.error}" if s.line else s.error for s in skipped)
        raise ValueError(f"no usable request in {path}: {problems}")
    run = FileRun(Path(path).name, requests, skipped, timeout)
    return run.run(operator, sleep=time.sleep)


class _FileRuns:
    """The web console's current (or last) file run; one runs at a time."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._run: FileRun | None = None

    def start(self, operator: FieldOperator, run: FileRun) -> bool:
        with self._lock:
            if self._run is not None and self._run.running:
                return False
            self._run = run
        run.start(operator)
        return True

    @property
    def current(self) -> FileRun | None:
        with self._lock:
            return self._run

    def in_flight(self) -> int:
        run = self.current
        return run.in_flight if run is not None else 0

    def snapshot(self) -> dict | None:
        run = self.current
        return run.snapshot() if run is not None else None


def _stdin_is_interactive() -> bool:
    return bool(getattr(sys.stdin, "isatty", lambda: False)())


def _is_quit(value: str) -> bool:
    return value.strip().casefold() in {"q", "quit", "exit"}


class _ActiveRequests:
    """Interactive requests still waiting for their final result.

    Each request gets its own watcher thread, so the menu is free to send the
    next request (to another station) while earlier irrigations are running.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ids: set[str] = set()

    def watch(self, operator: FieldOperator, request_id: str, timeout: float | None) -> threading.Thread:
        with self._lock:
            self._ids.add(request_id)

        def run() -> None:
            try:
                if not operator.wait_for_result(request_id, timeout):
                    mark = getattr(operator, "mark_no_result", None)
                    if mark is not None:
                        mark(request_id)
            except Exception:
                LOGGER.exception("Watching request %s failed", request_id)
            finally:
                with self._lock:
                    self._ids.discard(request_id)

        thread = threading.Thread(target=run, name=f"wm-fo-request-{str(request_id)[:8]}", daemon=True)
        thread.start()
        return thread

    def count(self) -> int:
        with self._lock:
            return len(self._ids)


def _quit_interactive(active: _ActiveRequests) -> None:
    running = active.count()
    if running:
        print(f"{running} irrigation(s) still running at the station(s); "
              "results will not be shown after exit.", flush=True)


def _interactive_mode(operator: FieldOperator, timeout: float, active: _ActiveRequests | None = None) -> None:
    print("Select an available station and enter an irrigation duration in seconds. Type 'q' to quit.")
    print("You can start irrigations on other stations while earlier ones are running.")
    active = active or _ActiveRequests()
    try:
        _interactive_loop(operator, timeout, active)
    finally:
        _quit_interactive(active)


def _interactive_loop(operator: FieldOperator, timeout: float, active: _ActiveRequests) -> None:
    while True:
        try:
            stations = operator.request_available_stations(timeout)
        except (TimeoutError, OSError, RuntimeError) as exc:
            print(f"Could not load available stations: {exc}")
            try:
                retry = input("Press Enter to retry or q to quit: ").strip()
            except EOFError:
                return
            if _is_quit(retry):
                return
            continue

        print("========================================")
        print("       AVAILABLE WATERING STATIONS")
        print("========================================")
        for station in stations:
            print(f"{station['station_id']:<12} {station['location']:<24} AVAILABLE")
        if not stations:
            print("No Watering Stations are currently available.")
            try:
                retry = input("Press Enter to refresh or q to quit: ").strip()
            except EOFError:
                return
            if _is_quit(retry):
                return
            continue
        try:
            ws_id = input("Enter station ID (or q to quit): ").strip()
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
        # Do not wait for the irrigation to finish here: its own thread shows
        # progress and the final summary while the menu takes the next request.
        request_id = operator.request_irrigation(ws_id, duration, None)
        active.watch(operator, request_id, max(timeout, duration + FILE_REQUEST_GRACE_SECONDS))
        # Wait only for Central's decision, so the next station list no
        # longer offers a station that has just been authorized.
        operator.wait_for_decision(request_id, DECISION_WAIT_SECONDS)


def console_routes(operator: FieldOperator, active: _ActiveRequests, timeout: float,
                   file_runs: _FileRuns | None = None) -> dict:
    """REST API of the FO web console. Requests are sent without waiting."""
    file_runs = file_runs if file_runs is not None else _FileRuns()

    def status(_request: ApiRequest) -> dict:
        with operator._lock:
            link = dict(operator.central_link)
        return {"operator_id": operator.operator_id, "central": link,
                "active_requests": active.count() + file_runs.in_flight()}

    def stations(_request: ApiRequest) -> dict:
        try:
            return {"stations": operator.request_available_stations(CONSOLE_DISCOVERY_TIMEOUT_SECONDS)}
        except TimeoutError as exc:
            raise ApiError(HTTPStatus.GATEWAY_TIMEOUT, str(exc)) from None
        except (RuntimeError, OSError) as exc:
            raise ApiError(HTTPStatus.BAD_GATEWAY, str(exc)) from None

    def irrigations(_request: ApiRequest) -> dict:
        return {"requests": operator.requests_snapshot()}

    def request_irrigation(request: ApiRequest) -> tuple[int, dict]:
        body = request.body or {}
        unknown = set(body) - {"ws_id", "duration_seconds", "target_volume_liters"}
        if unknown:
            raise ApiError(HTTPStatus.BAD_REQUEST, f"unknown field(s): {', '.join(sorted(unknown))}")
        ws_id = require_str(body, "ws_id")
        duration = optional_positive_int(body, "duration_seconds")
        target = optional_positive_number(body, "target_volume_liters")
        if (duration is None) == (target is None):
            raise ApiError(HTTPStatus.BAD_REQUEST, "choose duration_seconds or target_volume_liters")
        try:
            request_id = operator.request_irrigation(ws_id, duration, target)
        except ValueError as exc:
            raise ApiError(HTTPStatus.BAD_REQUEST, str(exc)) from None
        except Exception as exc:
            LOGGER.warning("Could not send irrigation request from the web console: %s", exc)
            raise ApiError(HTTPStatus.SERVICE_UNAVAILABLE, "Kafka is unavailable; the request was not sent") from None
        # Follow the request in the background; the HTTP call returns now.
        wait = max(timeout, duration + FILE_REQUEST_GRACE_SECONDS) if duration else timeout
        active.watch(operator, request_id, wait)
        return HTTPStatus.ACCEPTED, {"request_id": request_id, "status": "SUBMITTED"}

    def run_file(request: ApiRequest) -> tuple[int, dict]:
        """Start processing an uploaded request file (JSONL), one request at a time."""
        body = request.body or {}
        unknown = set(body) - {"filename", "content"}
        if unknown:
            raise ApiError(HTTPStatus.BAD_REQUEST, f"unknown field(s): {', '.join(sorted(unknown))}")
        content = body.get("content")
        if not isinstance(content, str):
            raise ApiError(HTTPStatus.BAD_REQUEST, "content must be the text of the request file")
        filename = body.get("filename") or "requests.jsonl"
        if not isinstance(filename, str):
            raise ApiError(HTTPStatus.BAD_REQUEST, "filename must be a string")
        filename = filename.replace("\\", "/").rsplit("/", 1)[-1][:120] or "requests.jsonl"
        current = file_runs.current
        if current is not None and current.running:
            raise ApiError(HTTPStatus.CONFLICT, f"{current.filename} is still being processed; "
                                                "wait for it to finish or cancel it")
        requests, skipped = parse_request_lines(content)
        if not requests:
            problems = "; ".join(f"line {s.line}: {s.error}" if s.line else s.error for s in skipped)
            raise ApiError(HTTPStatus.BAD_REQUEST, f"No usable request in {filename}: {problems}")
        run = FileRun(filename, requests, skipped, timeout)
        if not file_runs.start(operator, run):
            raise ApiError(HTTPStatus.CONFLICT, "another request file is still being processed")
        return HTTPStatus.ACCEPTED, run.snapshot()

    def cancel_file(_request: ApiRequest) -> dict:
        current = file_runs.current
        if current is None or not current.cancel():
            raise ApiError(HTTPStatus.CONFLICT, "no request file is being processed")
        return {"message": "No further requests from the file will be sent.", "file_run": current.snapshot()}

    return {
        ("GET", "/api/status"): status,
        ("GET", "/api/stations"): stations,
        ("GET", "/api/irrigations"): irrigations,
        ("POST", "/api/irrigations"): request_irrigation,
        ("POST", "/api/request-file"): run_file,
        ("POST", "/api/request-file/cancel"): cancel_file,
    }


def console_snapshot(operator: FieldOperator, active: _ActiveRequests, file_runs: _FileRuns | None = None):
    """What the FO page shows, pushed over its live stream."""
    def snapshot() -> dict:
        with operator._lock:
            link = dict(operator.central_link)
        in_flight = file_runs.in_flight() if file_runs is not None else 0
        return {"status": {"operator_id": operator.operator_id, "central": link,
                           "active_requests": active.count() + in_flight},
                "requests": operator.requests_snapshot(),
                "file_run": file_runs.snapshot() if file_runs is not None else None}
    return snapshot


def _serve_console_only() -> None:
    """Keep the FO running for its web console when there is no terminal."""
    print("No interactive terminal: the Field Operator runs from its web console. Press Ctrl+C to stop.", flush=True)
    stop = threading.Event()
    while not stop.wait(3600):
        pass


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = parse_args(argv)
    panel: ConsoleServer | None = None
    active = _ActiveRequests()
    file_runs = _FileRuns()
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
            panel = start_console(config.panel_host, config.panel_port, "fo",
                                  {"title": f"WM_FO {config.operator_id}", "component": "fo",
                                   "operatorId": config.operator_id},
                                  console_routes(operator, active, config.request_timeout, file_runs),
                                  console_snapshot(operator, active, file_runs))
            try:
                if config.request_file:
                    _file_mode(operator, config.request_file, config.request_timeout)
                elif config.ws_id:
                    _submit_and_wait(operator, config.ws_id, config.duration_seconds or 0,
                                     config.request_timeout)
                    _interactive_mode(operator, config.request_timeout, active)
                elif panel is not None and not _stdin_is_interactive():
                    _serve_console_only()
                else:
                    _interactive_mode(operator, config.request_timeout, active)
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
            if panel is not None:
                panel.stop()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

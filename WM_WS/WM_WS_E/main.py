from __future__ import annotations

import logging
from pathlib import Path
import signal
import sys
import threading

from .config import parse_args
from .engine import WateringStationEngine
from .kafka_service import EngineKafkaService
from WM_FO.kafka_client import FieldOperator
from common.kafka_transport import KafkaPublisher, KafkaUnavailable


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def start_command_thread(
    engine: WateringStationEngine,
    local_operator: FieldOperator | None = None,
    kafka_service: EngineKafkaService | None = None,
) -> threading.Thread:
    active_lock = threading.Lock()
    active_request: str | None = None
    ws_id = engine.config.ws_id

    def watch_request(request_id: str) -> None:
        nonlocal active_request
        try:
            local_operator.wait_for_result(request_id, None)  # type: ignore[union-attr]
        finally:
            if kafka_service is not None:
                kafka_service.clear_local_stop(request_id)
            with active_lock:
                if active_request == request_id:
                    active_request = None

    def command_loop() -> None:
        nonlocal active_request
        if local_operator is not None:
            print(
                f"Local menu: {ws_id} / {local_operator.operator_id} | "
                "m=start manual, s=stop, f=failure, c=clear, q=quit",
                flush=True,
            )
        else:
            print("Engine commands: f=failure, c=clear, s=stop, q=quit", flush=True)

        while True:
            try:
                command = input("Station> ").strip().lower()
            except EOFError:
                return

            if command in {"m", "manual", "start"}:
                if local_operator is None or kafka_service is None or not ws_id:
                    print("Local menu is not configured.", flush=True)
                    continue

                with active_lock:
                    if active_request is not None or kafka_service.active_request_id is not None or engine.irrigation.state.value != "IDLE":
                        print("A local request is already pending or active.", flush=True)
                        continue

                try:
                    request_id = local_operator.request_irrigation(ws_id)
                    with active_lock:
                        active_request = request_id
                    threading.Thread(
                        target=watch_request,
                        args=(request_id,),
                        name=f"wm-local-request-{ws_id}",
                        daemon=True,
                    ).start()
                except Exception as exc:
                    print(f"Could not submit local request: {exc}", flush=True)

            elif command in {"s", "stop"}:
                if local_operator is not None:
                    with active_lock:
                        request_id = active_request
                    if request_id is None:
                        request_id = kafka_service.active_request_id if kafka_service else None
                    if request_id is None:
                        print("There is no active irrigation to stop.", flush=True)
                    elif kafka_service and kafka_service.stop_local_irrigation(request_id):
                        print("Local irrigation stop requested.", flush=True)
                    else:
                        print("Could not stop that irrigation request.", flush=True)
                else:
                    engine.stop_irrigation()

            elif command in {"f", "fail", "ko"}:
                engine.simulate_failure()
            elif command in {"c", "clear"}:
                engine.clear_failure()
            elif command in {"h", "help", "?"}:
                print("m=start manual, s=stop, f=failure, c=clear, q=quit", flush=True)
            elif command in {"q", "quit", "exit"}:
                engine.close()
                return
            elif command:
                print("Unknown command. Type h for help.", flush=True)

    thread = threading.Thread(target=command_loop, name="wm-ws-e-commands", daemon=True)
    thread.start()
    return thread


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    config = parse_args(argv)
    engine = WateringStationEngine(config)
    signal.signal(signal.SIGTERM, lambda _signum, _frame: engine.close())

    publisher = None
    kafka = None
    worker = None
    local_worker = None
    poll_stop = threading.Event()
    local_poll_stop = threading.Event()
    poll_thread = None
    local_poll_thread = None
    local_operator = None

    try:
        state_path = Path(config.state_path) if config.state_path else Path(
            f".wm_ws_e_{config.ws_id or 'unassigned'}.sqlite3"
        )
        publisher = KafkaPublisher(f"{config.kafka_host}:{config.kafka_port}")
        kafka = EngineKafkaService(engine, publisher, state_path)

        worker = kafka.worker(f"{config.kafka_host}:{config.kafka_port}")
        worker.start()
        poll_thread = threading.Thread(
            target=_poll_worker,
            args=(worker, poll_stop),
            name="wm-ws-e-kafka",
            daemon=True,
        )
        poll_thread.start()

        if config.local_operator_id and config.ws_id:
            local_operator = FieldOperator(
                config.local_operator_id,
                publisher,
                consumer_group_id=f"wm-ws-local-{config.ws_id}-v1",
                station_id=config.ws_id,
            )
            local_worker = local_operator.worker(f"{config.kafka_host}:{config.kafka_port}")
            local_worker.start()

            if local_worker.wait_until_assigned(timeout=10.0):
                local_poll_thread = threading.Thread(
                    target=_poll_worker,
                    args=(local_worker, local_poll_stop),
                    name=f"wm-ws-local-events-{config.ws_id}",
                    daemon=True,
                )
                local_poll_thread.start()
                logging.getLogger(__name__).info(
                    "Local menu enabled ws_id=%s operator_id=%s",
                    config.ws_id, config.local_operator_id,
                )
            else:
                logging.getLogger(__name__).error(
                    "Local operator consumer did not get an assignment; local menu disabled"
                )
                local_worker.close()
                local_worker = None
                local_operator = None

    except KafkaUnavailable as exc:
        logging.getLogger(__name__).error("Kafka integration disabled: %s", exc)

    start_command_thread(engine, local_operator, kafka)

    try:
        return engine.run()
    finally:
        for name, stop_event, consumer, thread in (
            ("Engine", poll_stop, worker, poll_thread),
            ("Local operator", local_poll_stop, local_worker, local_poll_thread),
        ):
            if consumer is None:
                continue
            stop_event.set()
            if thread is not None:
                thread.join(timeout=3.0)
            if thread is not None and thread.is_alive():
                logging.getLogger(__name__).warning(
                    "%s Kafka polling thread did not stop; leaving consumer open", name
                )
            else:
                consumer.close()

        if kafka is not None:
            kafka.close()
        if publisher is not None:
            publisher.close()


def _poll_worker(worker: object, stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            worker.poll_once(1.0)  # type: ignore[attr-defined]
        except Exception:
            logging.getLogger(__name__).exception("Kafka polling thread stopped")
            return


if __name__ == "__main__":
    sys.exit(main())

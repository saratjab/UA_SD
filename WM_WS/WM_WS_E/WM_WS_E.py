from __future__ import annotations

import logging
from pathlib import Path
import signal
import sys
import threading

from .config import parse_args
from .controls import ControlRefused, StationControls
from .engine import WateringStationEngine
from .kafka_service import EngineKafkaService
from WM_FO.kafka_client import FieldOperator
from common.kafka_contract import LOCAL_OPERATOR_ID
from common.web_console import start_console
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
    controls: StationControls | None = None,
) -> threading.Thread:
    controls = controls or StationControls(engine, local_operator, kafka_service)
    ws_id = engine.config.ws_id

    def run(action) -> None:
        try:
            message = action()
        except ControlRefused as exc:
            message = str(exc)
        except Exception as exc:
            message = f"Could not submit local request: {exc}"
        if message:
            print(message, flush=True)

    def command_loop() -> None:
        if local_operator is not None:
            print(
                f"Local menu: {ws_id} | "
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
                run(lambda: None if controls.start_manual() else None)
            elif command in {"s", "stop"}:
                run(controls.stop)
            elif command in {"f", "fail", "ko"}:
                controls.simulate_failure()
            elif command in {"c", "clear"}:
                controls.clear_failure()
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

    # The Monitor session runs from the start so health checks are answered
    # while Kafka is set up. Without --ws-id (the spec's mode) the WS ID comes
    # from WM_WS_M's HELLO_ACK, and every Kafka resource is named after it.
    monitor_thread = threading.Thread(target=engine.run, name="wm-ws-e-monitor-session", daemon=True)
    monitor_thread.start()
    try:
        ready = engine.wait_until_monitor_ready()
    except KeyboardInterrupt:
        ready = False
    if not ready:
        engine.close()
        monitor_thread.join(timeout=3.0)
        return 0
    config = engine.config

    try:
        state_path = Path(config.state_path) if config.state_path else Path(
            f".wm_ws_e_{config.ws_id}.sqlite3"
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

        if config.ws_id:
            # The station's own menu needs no Field Operator: requests go to
            # Central under the reserved LOCAL identity, and replies are
            # matched to this station by ws_id.
            local_operator = FieldOperator(
                LOCAL_OPERATOR_ID,
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
                    "Local menu enabled ws_id=%s", config.ws_id,
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

    controls = StationControls(engine, local_operator, kafka)
    console = start_console(config.panel_host, config.panel_port, "engine",
                            {"title": f"WM_WS_E {config.ws_id}", "component": "engine", "wsId": config.ws_id},
                            controls.routes(), controls.status)
    start_command_thread(engine, local_operator, kafka, controls)

    try:
        while monitor_thread.is_alive():
            monitor_thread.join(timeout=0.5)
        return 0
    except KeyboardInterrupt:
        engine.close()
        monitor_thread.join(timeout=3.0)
        return 0
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
        if console is not None:
            console.stop()


def _poll_worker(worker: object, stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            worker.poll_once(1.0)
        except Exception:
            logging.getLogger(__name__).exception("Kafka polling thread stopped")
            return


if __name__ == "__main__":
    sys.exit(main())

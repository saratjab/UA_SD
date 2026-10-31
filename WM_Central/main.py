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
from .admin_api import CentralAdminServer
from .monitoring import log_station_table, monitor_loop, start_control_menu
from .socket_server import CentralSocketServer
from .state_manager import StateManager
from .kafka_service import CentralKafkaService
from common.kafka_transport import KafkaPublisher, KafkaUnavailable


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

    kafka_threads: list[threading.Thread] = []
    workers = []
    kafka = None
    stop = threading.Event()
    try:
        publisher = KafkaPublisher(f"{config.kafka_host}:{config.kafka_port}")
        kafka = CentralKafkaService(db, state, publisher)
        workers = [kafka.activation_worker(f"{config.kafka_host}:{config.kafka_port}"), kafka.event_worker(f"{config.kafka_host}:{config.kafka_port}")]
        for worker in workers:
            worker.start()
            thread = threading.Thread(target=lambda w=worker: _poll_worker(w), daemon=True)
            thread.start(); kafka_threads.append(thread)
    except KafkaUnavailable as exc:
        LOGGER.error("Kafka integration disabled: %s", exc)
    server = CentralSocketServer(config.socket_host, config.socket_port, state)
    outbox_thread: threading.Thread | None = None
    admin_server = (
        CentralAdminServer(
            config.admin_host,
            config.admin_port,
            config.admin_token or "",
            state,
            kafka,
            require_forwarded_https=config.admin_require_forwarded_https,
        )
        if kafka is not None and config.admin_port > 0
        else None
    )
    if kafka is not None:
        start_control_menu(state, kafka, stop)
        outbox_thread = threading.Thread(target=_outbox_loop, args=(kafka, stop), name="central-outbox", daemon=True)
        outbox_thread.start()
    else:
        LOGGER.warning("CENTRAL control interface unavailable because Kafka is not connected")
    try:
        server.start()
        if admin_server is not None:
            admin_server.start()
        threading.Thread(target=monitor_loop, args=(state, stop), name="central-monitor", daemon=True).start()
        while not stop.wait(1.0):
            pass
    except KeyboardInterrupt:
        LOGGER.info("Shutting down")
    except Exception:
        LOGGER.exception("WM_Central listener startup or service loop failed")
        raise
    finally:
        stop.set()
        if admin_server is not None:
            admin_server.stop()
        for worker in workers:
            worker.close()
        for thread in kafka_threads:
            thread.join(timeout=2.0)
        if outbox_thread is not None:
            outbox_thread.join(timeout=12.0)
        server.stop()
        db.close()
    return 0

def _poll_worker(worker: object) -> None:
    # KafkaWorker catches handler failures and leaves the offset uncommitted.
    while True:
        try:
            worker.poll_once(1.0)  # type: ignore[attr-defined]
        except Exception:
            LOGGER.exception("Kafka worker stopped")
            return


def _outbox_loop(service: CentralKafkaService, stop: threading.Event) -> None:
    while not stop.wait(1.0):
        service.flush_outbox()


if __name__ == "__main__":
    sys.exit(main())

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass


DEFAULT_DB_PATH = "wm_central.db"


@dataclass(frozen=True)
class CentralConfig:
    socket_host: str
    socket_port: int
    kafka_host: str
    kafka_port: int
    db_path: str
    seed_dev: bool
    log_level: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="WaterManagement Central (WM_Central)")
    # --- required by the PDF ------------------------------------------------
    parser.add_argument(
        "--socket-port", required=True, type=int,
        help="Listening port of the socket server offered to WM_WS_M",
    )
    parser.add_argument("--kafka-host", required=True, help="Kafka Broker/Bootstrap IP")
    parser.add_argument("--kafka-port", required=True, type=int, help="Kafka Broker/Bootstrap port")
    # --- our additions (all optional) ---------------------------------------
    parser.add_argument(
        "--socket-host", default="0.0.0.0",
        help="Interface to bind the socket server to (default: all interfaces)",
    )
    parser.add_argument(
        "--db-path", default=os.environ.get("DB_PATH", DEFAULT_DB_PATH),
        help=f"SQLite file (default: $DB_PATH or {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--seed-dev", action="store_true",
        help="Load demo operators (db/seed_dev.sql) - development only",
    )
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser


def parse_args(argv: list[str] | None = None) -> CentralConfig:
    args = build_parser().parse_args(argv)
    return CentralConfig(
        socket_host=args.socket_host,
        socket_port=args.socket_port,
        kafka_host=args.kafka_host,
        kafka_port=args.kafka_port,
        db_path=args.db_path,
        seed_dev=args.seed_dev,
        log_level=args.log_level,
    )

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass


DEFAULT_DB_PATH = "wm_central.db"
DEFAULT_SOCKET_PORT = 5000
DEFAULT_KAFKA_PORT = 9092


@dataclass(frozen=True)
class CentralConfig:
    socket_host: str
    socket_port: int
    kafka_host: str
    kafka_port: int
    db_path: str
    seed_dev: bool
    log_level: str
    admin_host: str
    admin_port: int
    admin_token: str | None
    admin_require_forwarded_https: bool


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="WaterManagement Central (WM_Central)")
    # --- required by the PDF ------------------------------------------------
    parser.add_argument(
        "--socket-port", type=int,
        default=os.environ.get("CENTRAL_SOCKET_PORT", str(DEFAULT_SOCKET_PORT)),
        help=f"Listening port for WM_WS_M (default: $CENTRAL_SOCKET_PORT or {DEFAULT_SOCKET_PORT})",
    )
    parser.add_argument("--kafka-host", default=os.environ.get("KAFKA_HOST"), help="Kafka Broker/Bootstrap host")
    parser.add_argument(
        "--kafka-port", type=int, default=os.environ.get("KAFKA_PORT", str(DEFAULT_KAFKA_PORT)),
        help=f"Kafka Broker/Bootstrap port (default: $KAFKA_PORT or {DEFAULT_KAFKA_PORT})",
    )
    # --- our additions (all optional) ---------------------------------------
    parser.add_argument(
        "--socket-host", default=os.environ.get("CENTRAL_SOCKET_HOST", "0.0.0.0"),
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
    parser.add_argument(
        "--admin-host", default=os.environ.get("CENTRAL_ADMIN_HOST", "0.0.0.0"),
        help="HTTP admin API bind host (default: all interfaces)",
    )
    parser.add_argument(
        "--admin-port", type=int,
        default=os.environ.get("CENTRAL_ADMIN_PORT", os.environ.get("PORT", "0")),
        help="HTTP admin API port (defaults to CENTRAL_ADMIN_PORT, then Railway PORT; zero disables it)",
    )
    parser.add_argument(
        "--admin-require-forwarded-https", action="store_true",
        default=os.environ.get("CENTRAL_ADMIN_REQUIRE_HTTPS", "true").lower() in {"1", "true", "yes"},
        help="Require X-Forwarded-Proto: https from a trusted TLS-terminating proxy",
    )
    return parser


def parse_args(argv: list[str] | None = None) -> CentralConfig:
    parser = build_parser()
    args = parser.parse_args(argv)
    admin_token = os.environ.get("CENTRAL_ADMIN_TOKEN")
    if not args.kafka_host:
        parser.error("--kafka-host or KAFKA_HOST is required")
    if not 0 <= args.socket_port <= 65535:
        parser.error("--socket-port must be between 0 and 65535")
    if not 1 <= args.kafka_port <= 65535:
        parser.error("--kafka-port must be between 1 and 65535")
    if args.admin_port < 0 or args.admin_port > 65535:
        parser.error("--admin-port must be between 0 and 65535")
    if args.admin_port and not admin_token:
        parser.error("CENTRAL_ADMIN_TOKEN is required when --admin-port is enabled")
    return CentralConfig(
        socket_host=args.socket_host,
        socket_port=args.socket_port,
        kafka_host=args.kafka_host,
        kafka_port=args.kafka_port,
        db_path=args.db_path,
        seed_dev=args.seed_dev,
        log_level=args.log_level,
        admin_host=args.admin_host,
        admin_port=args.admin_port,
        admin_token=admin_token,
        admin_require_forwarded_https=args.admin_require_forwarded_https,
    )

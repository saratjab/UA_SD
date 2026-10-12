from __future__ import annotations

import argparse
from dataclasses import dataclass


@dataclass(frozen=True)
class FieldOperatorConfig:
    kafka_host: str
    kafka_port: int
    operator_id: str
    request_file: str | None
    ws_id: str | None
    duration_seconds: int | None
    request_timeout: float


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="WaterManagement Field Operator")
    parser.add_argument("--kafka-host", required=True, help="Kafka bootstrap host")
    parser.add_argument("--kafka-port", required=True, type=int, help="Kafka bootstrap port")
    parser.add_argument("--operator-id", required=True, help="Operator ID registered in WM_Central")
    parser.add_argument("--request-file", help="JSON array or JSONL file of irrigation requests")
    parser.add_argument("--ws-id", help="Watering Station ID for a one-off request")
    parser.add_argument("--duration-seconds", type=int, help="Duration for a one-off request")
    parser.add_argument("--request-timeout", type=float, default=300.0,
                        help="Seconds to wait for a request result (default: 300)")
    return parser


def parse_args(argv: list[str] | None = None) -> FieldOperatorConfig:
    args = build_parser().parse_args(argv)
    if args.kafka_port < 1 or args.kafka_port > 65535:
        raise SystemExit("--kafka-port must be between 1 and 65535")
    if args.request_timeout <= 0:
        raise SystemExit("--request-timeout must be positive")
    if bool(args.ws_id) != (args.duration_seconds is not None):
        raise SystemExit("--ws-id and --duration-seconds must be provided together")
    if args.duration_seconds is not None and args.duration_seconds <= 0:
        raise SystemExit("--duration-seconds must be positive")
    if args.request_file and args.ws_id:
        raise SystemExit("choose either --request-file or a one-off --ws-id request")
    return FieldOperatorConfig(args.kafka_host, args.kafka_port, args.operator_id,
                               args.request_file, args.ws_id, args.duration_seconds,
                               args.request_timeout)

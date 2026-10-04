from __future__ import annotations

import argparse
from dataclasses import dataclass


@dataclass(frozen=True)
class EngineConfig:
    ws_id: str
    kafka_host: str
    kafka_port: int
    monitor_host: str
    monitor_port: int
    flow_rate_lpm: float
    default_irrigation_duration: float
    socket_timeout: float
    telemetry_interval: float


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="WaterManagement Watering Station Engine")
    parser.add_argument("--ws-id", required=True, help="Watering Station ID")
    parser.add_argument("--kafka-host", required=True, help="Kafka bootstrap host")
    parser.add_argument("--kafka-port", required=True, type=int, help="Kafka bootstrap port")
    parser.add_argument("--monitor-host", required=True, help="WM_WS_M TCP host")
    parser.add_argument("--monitor-port", required=True, type=int, help="WM_WS_M TCP port")
    parser.add_argument(
        "--flow-rate-lpm",
        type=float,
        default=6.0,
        help="Simulated fixed flow rate in liters per minute",
    )
    parser.add_argument(
        "--default-irrigation-duration",
        type=float,
        default=10.0,
        help="Default irrigation duration in seconds",
    )
    parser.add_argument(
        "--socket-timeout",
        type=float,
        default=1.0,
        help="Socket timeout used while waiting for monitor messages",
    )
    parser.add_argument(
        "--telemetry-interval",
        type=float,
        default=1.0,
        help="Seconds between generated irrigation telemetry records",
    )
    return parser


def parse_args(argv: list[str] | None = None) -> EngineConfig:
    args = build_parser().parse_args(argv)
    return EngineConfig(
        ws_id=args.ws_id,
        kafka_host=args.kafka_host,
        kafka_port=args.kafka_port,
        monitor_host=args.monitor_host,
        monitor_port=args.monitor_port,
        flow_rate_lpm=args.flow_rate_lpm,
        default_irrigation_duration=args.default_irrigation_duration,
        socket_timeout=args.socket_timeout,
        telemetry_interval=args.telemetry_interval,
    )


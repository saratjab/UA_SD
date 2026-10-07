from __future__ import annotations

import argparse
from dataclasses import dataclass


@dataclass(frozen=True)
class MonitorConfig:
    ws_id: str
    central_host: str
    central_port: int
    engine_host: str
    engine_port: int
    engine_hello_timeout: float
    health_interval: float
    health_timeout: float
    location: str = ""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="WaterManagement Watering Station Monitor")
    parser.add_argument("--ws-id", required=True, help="Watering Station ID")
    parser.add_argument("--location", default="", help="Watering Station location")
    parser.add_argument("--central-host", required=True, help="WM_Central TCP host")
    parser.add_argument("--central-port", required=True, type=int, help="WM_Central TCP port")
    parser.add_argument(
        "--engine-host",
        required=True,
        help="Host/interface where WM_WS_M listens for WM_WS_E",
    )
    parser.add_argument(
        "--engine-port",
        required=True,
        type=int,
        help="TCP port where WM_WS_M listens for WM_WS_E",
    )
    parser.add_argument(
        "--engine-hello-timeout",
        type=float,
        default=5.0,
        help="Seconds to wait for WM_WS_E HELLO_WS_E after accepting a connection",
    )
    parser.add_argument(
        "--health-interval",
        type=float,
        default=5.0,
        help="Seconds between health checks",
    )
    parser.add_argument(
        "--health-timeout",
        type=float,
        default=2.0,
        help="Seconds to wait for a health response",
    )
    return parser


def parse_args(argv: list[str] | None = None) -> MonitorConfig:
    args = build_parser().parse_args(argv)
    return MonitorConfig(
        ws_id=args.ws_id,
        central_host=args.central_host,
        central_port=args.central_port,
        engine_host=args.engine_host,
        engine_port=args.engine_port,
        engine_hello_timeout=args.engine_hello_timeout,
        health_interval=args.health_interval,
        health_timeout=args.health_timeout,
        location=args.location,
    )

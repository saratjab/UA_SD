from __future__ import annotations

import argparse
import os
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
    panel_host: str = "127.0.0.1"
    panel_port: int = 0


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
        default=1.0,
        help="Seconds between health checks",
    )
    parser.add_argument(
        "--health-timeout",
        type=float,
        default=2.0,
        help="Seconds to wait for a health response",
    )
    parser.add_argument("--panel-host", default=os.environ.get("PANEL_HOST", "127.0.0.1"),
                        help="Interface for the live event panel (default: $PANEL_HOST or 127.0.0.1)")
    parser.add_argument("--panel-port", type=int, default=int(os.environ.get("PANEL_PORT", "0")),
                        help="Port for the live event panel web page; 0 disables it (default: $PANEL_PORT or 0)")
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
        panel_host=args.panel_host,
        panel_port=args.panel_port,
    )

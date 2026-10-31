"""Interactive PC-side client for the headless Central admin API."""

from __future__ import annotations

import argparse
import getpass
import ipaddress
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable
from uuid import uuid4

from .admin_api import format_status_response


class AdminClientError(RuntimeError):
    pass


class CentralAdminClient:
    def __init__(self, base_url: str, token: str, *, timeout: float = 15.0) -> None:
        if not token:
            raise ValueError("CENTRAL_ADMIN_TOKEN is required")
        parsed = urllib.parse.urlsplit(base_url.rstrip("/"))
        if parsed.scheme not in {"https", "http"} or not parsed.hostname:
            raise ValueError("Admin URL must be an http(s) URL with a host")
        if parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path:
            raise ValueError("Admin URL must contain only scheme and host, without credentials or a path")
        if parsed.scheme == "http" and not _is_loopback(parsed.hostname):
            raise ValueError("HTTP is allowed only for a loopback admin URL; use HTTPS for remote Central")
        self.base_url = base_url.rstrip("/")
        self._token = token
        self.timeout = timeout

    def status(self) -> dict[str, Any]:
        return self._request("GET", "/v1/stations")

    def control(self, command: dict[str, Any], *, idempotency_key: str | None = None) -> dict[str, Any]:
        return self._request("POST", "/v1/control", command, idempotency_key=idempotency_key or str(uuid4()))

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        data = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        request = urllib.request.Request(self.base_url + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            try:
                details = json.loads(exc.read().decode("utf-8"))
                message = (
                    details.get("error") or json.dumps(details, sort_keys=True)
                    if isinstance(details, dict)
                    else json.dumps(details, sort_keys=True)
                )
            except (UnicodeDecodeError, json.JSONDecodeError):
                message = "request rejected"
            finally:
                exc.close()
            raise AdminClientError(f"Central returned HTTP {exc.code}: {message}") from None
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise AdminClientError(f"Could not reach Central admin service: {exc}") from None
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AdminClientError("Central returned an invalid JSON response") from None
        if not isinstance(payload, dict):
            raise AdminClientError("Central returned an invalid response")
        return payload


def _is_loopback(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def run_menu(
    client: CentralAdminClient,
    *,
    input_fn: Callable[[str], str] = input,
    output_fn: Callable[[str], None] = print,
) -> None:
    if not sys.stdin.isatty() and input_fn is input:
        raise RuntimeError("The admin menu client requires an interactive terminal")
    while True:
        output_fn(
            "Central admin controls:\n"
            "  1. Display station status\n"
            "  2. Start irrigation for one station\n"
            "  3. Start irrigation on all eligible stations\n"
            "  4. Block one station\n"
            "  5. Block all stations\n"
            "  6. Activate one station\n"
            "  7. Activate all stations\n"
            "  8. Stop irrigation on one station\n"
            "  9. Stop all active irrigations\n"
            "  0. Quit"
        )
        try:
            choice = input_fn("Choice: ").strip()
        except (EOFError, OSError):
            output_fn("Admin menu input closed")
            return
        if choice == "0":
            return
        try:
            if choice == "1":
                output_fn(format_status_response(client.status()))
                continue
            command = _menu_command(choice, input_fn)
            if command is None:
                output_fn("Invalid menu choice. Choose a number from 0 to 9.")
                continue
            response = client.control(command)
            output_fn(json.dumps(response, indent=2, sort_keys=True))
        except (ValueError, AdminClientError) as exc:
            output_fn(f"Control failed: {exc}")


def _menu_command(choice: str, read: Callable[[str], str]) -> dict[str, Any] | None:
    if choice == "2":
        action, station = "start", read("Station ID: ").strip()
    elif choice == "3":
        action, station = "start", "all"
    elif choice == "4":
        action, station = "block", read("Station ID: ").strip()
    elif choice == "5":
        action, station = "block", "all"
    elif choice == "6":
        action, station = "activate", read("Station ID: ").strip()
    elif choice == "7":
        action, station = "activate", "all"
    elif choice == "8":
        action, station = "stop", read("Station ID: ").strip()
    elif choice == "9":
        action, station = "stop", "all"
    else:
        return None
    command: dict[str, Any] = {"action": action, "station_id": station}
    if action == "start":
        mode = read("Mode (duration/volume/manual): ").strip().lower()
        if mode == "duration":
            command["duration_seconds"] = int(read("Duration in whole seconds: ").strip())
        elif mode == "volume":
            command["target_volume_liters"] = float(read("Target volume in liters: ").strip())
        elif mode != "manual":
            raise ValueError("Mode must be duration, volume, or manual")
        operator_id = read("Operator ID (blank uses an active operator): ").strip()
        if operator_id:
            command["operator_id"] = operator_id
    return command


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Interactive WM_Central remote admin menu")
    parser.add_argument("--url", default=os.environ.get("CENTRAL_ADMIN_URL"), help="Central base URL (HTTPS remotely)")
    args = parser.parse_args(argv)
    if not args.url:
        parser.error("--url or CENTRAL_ADMIN_URL is required")
    token = os.environ.get("CENTRAL_ADMIN_TOKEN")
    if token is None:
        token = getpass.getpass("Central admin token: ")
    try:
        run_menu(CentralAdminClient(args.url, token))
    except (ValueError, RuntimeError, AdminClientError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

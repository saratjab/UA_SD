"""Small interactive web console shared by every component.

Each process can serve one page (``common/web/<page>.html``) plus a JSON API.
Only the standard library is used:

- ``GET /`` serves the page, ``GET /static/<name>`` serves the shared CSS/JS
  from a fixed whitelist (the files in ``common/web``), ``GET /events`` serves
  the component's live event feed.
- Component routes are plain callables registered as ``("GET", "/api/x")``.
  Each request runs on its own thread, so a slow handler (for example an FO
  waiting for Central's station list) never blocks the page.

The page has no authentication because it is published on ``127.0.0.1`` only.
To stop another website open in the same browser from driving it, every POST
must be ``application/json`` (which a cross-site form cannot send without a
CORS preflight we never answer) and a supplied ``Origin`` must match ``Host``.
"""
from __future__ import annotations

import json
import logging
import math
import threading
import time
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlsplit

from .event_panel import EventFeed, events_payload, feed

LOGGER = logging.getLogger(__name__)

WEB_DIR = Path(__file__).resolve().parent / "web"
MAX_BODY_BYTES = 16 * 1024
_CONTENT_TYPES = {
    ".css": "text/css; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".svg": "image/svg+xml",
}
# The page keeps its configuration in a non-executable JSON block, so the
# policy can forbid every inline script.
CONTENT_SECURITY_POLICY = (
    "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; "
    "connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
)


class ApiError(Exception):
    """Raised by a route to answer with a JSON error and an HTTP status."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = int(status)
        self.message = message


@dataclass
class ApiRequest:
    method: str
    path: str
    query: dict[str, str] = field(default_factory=dict)
    body: dict[str, Any] | None = None


Route = Callable[[ApiRequest], Any]


def static_assets() -> dict[str, tuple[str, bytes]]:
    """Whitelisted static files: name -> (content type, bytes)."""
    assets: dict[str, tuple[str, bytes]] = {}
    for path in WEB_DIR.iterdir():
        content_type = _CONTENT_TYPES.get(path.suffix)
        if content_type and path.is_file():
            assets[path.name] = (content_type, path.read_bytes())
    return assets


def render_page(page: str, config: dict[str, Any]) -> str:
    """The HTML page with its JSON configuration embedded as inert data."""
    template = (WEB_DIR / f"{page}.html").read_text(encoding="utf-8")
    data = json.dumps(config).replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
    return template.replace("__CONFIG__", data)


def security_headers() -> dict[str, str]:
    return {
        "Cache-Control": "no-store",
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "no-referrer",
        "X-Frame-Options": "DENY",
        "Content-Security-Policy": CONTENT_SECURITY_POLICY,
    }


def origin_matches_host(origin: str | None, host: str | None) -> bool:
    """True when no Origin was sent or it names the same host:port as ``Host``."""
    if not origin:
        return True
    if not host:
        return False
    netloc = urlsplit(origin).netloc
    return netloc.lower() == host.lower()


def require_str(body: dict[str, Any], name: str) -> str:
    value = body.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ApiError(HTTPStatus.BAD_REQUEST, f"{name} is required")
    return value.strip()


def optional_positive_int(body: dict[str, Any], name: str) -> int | None:
    value = body.get(name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ApiError(HTTPStatus.BAD_REQUEST, f"{name} must be a positive whole number")
    return value


def optional_positive_number(body: dict[str, Any], name: str) -> float | None:
    value = body.get(name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ApiError(HTTPStatus.BAD_REQUEST, f"{name} must be a positive number")
    return float(value)


STREAM_HEADERS = {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache, no-store",
    "X-Accel-Buffering": "no",
    "X-Content-Type-Options": "nosniff",
    "Connection": "close",
}
STREAM_STATE_INTERVAL = 1.0
STREAM_HEARTBEAT_SECONDS = 15.0
MAX_STREAMS = 16


def sse(event: str, data: Any) -> bytes:
    """One Server-Sent Events message (compact JSON never contains a newline)."""
    return f"event: {event}\ndata: {json.dumps(data, separators=(',', ':'))}\n\n".encode("utf-8")


def run_stream(write: Callable[[bytes], None], source: EventFeed, snapshot: Callable[[], Any] | None,
               after: int, stop: threading.Event, *,
               interval: float = STREAM_STATE_INTERVAL,
               heartbeat: float = STREAM_HEARTBEAT_SECONDS) -> None:
    """Push a component's live data to one browser until it disconnects.

    - ``events``: every new feed event, sent the moment it is recorded.
    - ``state``: the component's current snapshot, sent right after an event
      and whenever it changes (checked every ``interval`` seconds).
    A comment line every ``heartbeat`` seconds keeps proxies from closing an
    idle connection. A write error means the browser left; the stream ends.
    """
    last_state: str | None = None
    last_write = time.monotonic()
    initial = True
    while not stop.is_set():
        if not initial:
            source.wait_for_new(after, interval)
            if stop.is_set():
                return
        events, latest = source.since(after)
        chunks = []
        if events or initial:
            chunks.append(sse("events", {"events": events, "last": latest}))
        after = max(after, latest)
        if snapshot is not None:
            state = snapshot()
            text = json.dumps(state, separators=(",", ":"), sort_keys=True)
            if text != last_state:
                last_state = text
                chunks.append(sse("state", state))
        if not chunks and time.monotonic() - last_write >= heartbeat:
            chunks.append(b": keep-alive\n\n")
        if chunks:
            write(b"".join(chunks))
            last_write = time.monotonic()
        initial = False


def parse_after(raw: str | None) -> int:
    try:
        return max(0, int(raw or "0"))
    except ValueError:
        return 0


class ConsoleServer:
    """Threaded HTTP server for one component's page, JSON API and live stream."""

    def __init__(self, host: str, port: int, page: str, config: dict[str, Any],
                 routes: dict[tuple[str, str], Route] | None = None,
                 source: EventFeed | None = None,
                 snapshot: Callable[[], Any] | None = None) -> None:
        self.host, self.port = host, port
        self._page = page
        self._config = {"eventsPath": "events", "streamPath": "stream", **config}
        self._routes = dict(routes or {})
        self._source = source
        self._snapshot = snapshot
        self._stop = threading.Event()
        self._streams = threading.BoundedSemaphore(MAX_STREAMS)
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        page = render_page(self._page, self._config).encode("utf-8")
        assets = static_assets()
        routes, source, snapshot = self._routes, self._source, self._snapshot
        stop, streams = self._stop, self._streams

        class Handler(BaseHTTPRequestHandler):
            server_version = "WM_Console"
            sys_version = ""

            def do_GET(self) -> None:
                parts = urlsplit(self.path)
                if parts.path in {"/", "/index.html"}:
                    self._send(HTTPStatus.OK, "text/html; charset=utf-8", page)
                elif parts.path.startswith("/static/"):
                    asset = assets.get(parts.path[len("/static/"):])
                    if asset is None:
                        self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                    else:
                        self._send(HTTPStatus.OK, asset[0], asset[1])
                elif parts.path == "/events":
                    after = parse_qs(parts.query).get("after", ["0"])[0]
                    self._json(HTTPStatus.OK, events_payload(after, source or feed()))
                elif parts.path == "/stream":
                    self._stream(parse_after(parse_qs(parts.query).get("after", ["0"])[0]))
                else:
                    self._dispatch("GET", parts.path, parts.query, None)

            def do_POST(self) -> None:
                parts = urlsplit(self.path)
                if not origin_matches_host(self.headers.get("Origin"), self.headers.get("Host")):
                    self._json(HTTPStatus.FORBIDDEN, {"error": "cross-origin request refused"})
                    return
                try:
                    body = self._read_json()
                except ApiError as exc:
                    self._json(exc.status, {"error": exc.message})
                    return
                self._dispatch("POST", parts.path, parts.query, body)

            def _dispatch(self, method: str, path: str, query: str, body: dict[str, Any] | None) -> None:
                route = routes.get((method, path))
                if route is None:
                    status = HTTPStatus.METHOD_NOT_ALLOWED if any(p == path for _, p in routes) else HTTPStatus.NOT_FOUND
                    self._json(status, {"error": "not_found" if status == HTTPStatus.NOT_FOUND else "method_not_allowed"})
                    return
                request = ApiRequest(method, path, {k: v[0] for k, v in parse_qs(query).items()}, body)
                try:
                    result = route(request)
                except ApiError as exc:
                    self._json(exc.status, {"error": exc.message})
                    return
                except Exception:
                    LOGGER.exception("Console route %s %s failed", method, path)
                    self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "internal_error"})
                    return
                status = HTTPStatus.OK
                if isinstance(result, tuple):
                    status, result = result
                self._json(status, result)

            def _stream(self, after: int) -> None:
                if not streams.acquire(blocking=False):
                    self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "too many live streams"})
                    return
                try:
                    self.send_response(HTTPStatus.OK)
                    for name, value in STREAM_HEADERS.items():
                        self.send_header(name, value)
                    self.end_headers()
                    self.close_connection = True

                    def write(chunk: bytes) -> None:
                        self.wfile.write(chunk)
                        self.wfile.flush()

                    run_stream(write, source or feed(), snapshot, after, stop)
                except OSError:
                    pass  # the browser closed the page
                except Exception:
                    LOGGER.exception("Live stream failed")
                finally:
                    streams.release()

            def _read_json(self) -> dict[str, Any]:
                if self.headers.get("Transfer-Encoding"):
                    raise ApiError(HTTPStatus.BAD_REQUEST, "Transfer-Encoding is not supported")
                if self.headers.get_content_type() != "application/json":
                    raise ApiError(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, "Content-Type must be application/json")
                try:
                    length = int(self.headers.get("Content-Length") or "0")
                except ValueError:
                    raise ApiError(HTTPStatus.BAD_REQUEST, "invalid Content-Length") from None
                if length < 0 or length > MAX_BODY_BYTES:
                    raise ApiError(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "request body too large")
                raw = self.rfile.read(length) if length else b"{}"
                try:
                    body = json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    raise ApiError(HTTPStatus.BAD_REQUEST, "request body must be JSON") from None
                if not isinstance(body, dict):
                    raise ApiError(HTTPStatus.BAD_REQUEST, "request body must be a JSON object")
                return body

            def _json(self, status: int, value: Any) -> None:
                self._send(status, "application/json; charset=utf-8",
                           json.dumps(value, separators=(",", ":")).encode("utf-8"))

            def _send(self, status: int, content_type: str, body: bytes) -> None:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                for name, value in security_headers().items():
                    self.send_header(name, value)
                self.end_headers()
                try:
                    self.wfile.write(body)
                except OSError:
                    pass

            def log_message(self, _format: str, *_args: Any) -> None:
                pass

        self._server = ThreadingHTTPServer((self.host, self.port), Handler)
        self._server.daemon_threads = True
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever, name="web-console", daemon=True)
        self._thread.start()
        LOGGER.info("Web console (%s) on http://%s:%s/", self._config.get("title", self._page), self.host, self.port)

    def stop(self) -> None:
        self._stop.set()
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None


def start_console(host: str, port: int, page: str, config: dict[str, Any],
                  routes: dict[tuple[str, str], Route] | None = None,
                  snapshot: Callable[[], Any] | None = None) -> ConsoleServer | None:
    """Start a console when ``port`` is non-zero; a bind failure never stops the component."""
    if not port:
        return None
    console = ConsoleServer(host, port, page, config, routes, snapshot=snapshot)
    try:
        console.start()
    except OSError:
        LOGGER.exception("Could not start the web console on %s:%s", host, port)
        return None
    return console

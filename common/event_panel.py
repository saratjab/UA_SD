"""Live event feed and a tiny read-only web panel for every component.

Each process (WM_Central, WM_FO, WM_WS_E, WM_WS_M) records the messages it
exchanges with other components in one in-memory ``EventFeed``. A small HTTP
page polls ``events?after=<seq>`` every second and shows them as a live stream,
so an irrigation can be followed from the FO through Central to the station.

Only the standard library is used. The page never inserts event data as HTML
(rows are built with ``textContent``), because event fields such as ``ws_id``
come from other, untrusted components.
"""
from __future__ import annotations

import json
import logging
import threading
from collections import deque
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

LOGGER = logging.getLogger(__name__)

DEFAULT_CAPACITY = 300
MAX_EVENTS_PER_RESPONSE = 200


class EventFeed:
    """Thread-safe ring buffer of the most recent component events."""

    def __init__(self, capacity: int = DEFAULT_CAPACITY) -> None:
        self._events: deque[dict[str, Any]] = deque(maxlen=capacity)
        self._seq = 0
        self._lock = threading.Lock()
        # Wakes live streams the moment an event is recorded.
        self._changed = threading.Condition(self._lock)

    def wait_for_new(self, after: int, timeout: float) -> bool:
        """Block until an event newer than ``after`` exists, or ``timeout`` passes."""
        with self._changed:
            if self._seq <= after:
                self._changed.wait(timeout)
            return self._seq > after

    def record(self, direction: str, peer: str, event: str, *, ws_id: str | None = None,
               request_id: str | None = None, detail: Any = "") -> dict[str, Any]:
        """Add one event. ``direction`` is IN, OUT or LOCAL, seen from this component."""
        with self._lock:
            self._seq += 1
            item = {
                "seq": self._seq,
                "time": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                "direction": direction,
                "peer": peer,
                "event": event,
                "ws_id": ws_id,
                "request_id": request_id,
                "detail": str(detail) if detail is not None else "",
            }
            self._events.append(item)
            self._changed.notify_all()
            return item

    def since(self, after: int, limit: int = MAX_EVENTS_PER_RESPONSE) -> tuple[list[dict[str, Any]], int]:
        """Events with ``seq > after`` (oldest first) and the latest sequence number."""
        with self._lock:
            newer = [dict(item) for item in self._events if item["seq"] > after]
            return newer[-limit:], self._seq


_FEED = EventFeed()


def feed() -> EventFeed:
    """The process-wide feed (one component per process)."""
    return _FEED


def reset_feed() -> EventFeed:
    """Replace the process-wide feed; used by tests."""
    global _FEED
    _FEED = EventFeed()
    return _FEED


def describe(payload: Any) -> str:
    """Compact one-line summary of a message payload for the panel."""
    if not isinstance(payload, dict):
        return "" if payload is None else str(payload)
    parts = []
    for key, value in payload.items():
        if isinstance(value, list):
            parts.append(f"{key}={len(value)}")
        elif isinstance(value, float):
            parts.append(f"{key}={value:.2f}")
        elif value is not None:
            parts.append(f"{key}={value}")
    return ", ".join(parts)


def events_payload(after_raw: str | None, source: EventFeed | None = None) -> dict[str, Any]:
    try:
        after = max(0, int(after_raw or "0"))
    except ValueError:
        after = 0
    events, last = (source or feed()).since(after)
    return {"events": events, "last": last}


def render_panel(title: str, events_path: str, *, requires_token: bool = False) -> str:
    """The panel page. Configuration is embedded as JSON, never as raw HTML."""
    config = json.dumps({"title": title, "eventsPath": events_path, "requiresToken": requires_token})
    return _PANEL_TEMPLATE.replace("__CONFIG__", config.replace("</", "<\\/"))


class EventPanelServer:
    """Read-only HTTP server for one component's panel (GET / and GET /events)."""

    def __init__(self, host: str, port: int, title: str, source: EventFeed | None = None) -> None:
        self.host, self.port, self.title = host, port, title
        self._source = source
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        page = render_panel(self.title, "events").encode("utf-8")
        source = self._source

        class Handler(BaseHTTPRequestHandler):
            server_version = "WM_EventPanel"
            sys_version = ""

            def do_GET(self) -> None:
                parts = urlsplit(self.path)
                if parts.path in {"/", "/index.html"}:
                    self._reply(HTTPStatus.OK, "text/html; charset=utf-8", page)
                elif parts.path == "/events":
                    after = parse_qs(parts.query).get("after", ["0"])[0]
                    body = json.dumps(events_payload(after, source or feed())).encode("utf-8")
                    self._reply(HTTPStatus.OK, "application/json; charset=utf-8", body)
                else:
                    self._reply(HTTPStatus.NOT_FOUND, "application/json; charset=utf-8", b'{"error":"not_found"}')

            def _reply(self, status: HTTPStatus, content_type: str, body: bytes) -> None:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, _format: str, *_args: Any) -> None:
                pass

        self._server = ThreadingHTTPServer((self.host, self.port), Handler)
        self._server.daemon_threads = True
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever, name="event-panel", daemon=True)
        self._thread.start()
        LOGGER.info("Live event panel for %s on http://%s:%s/", self.title, self.host, self.port)

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None


def start_panel(host: str, port: int, title: str) -> EventPanelServer | None:
    """Start a panel when ``port`` is non-zero; a bind failure never stops the component."""
    if not port:
        return None
    panel = EventPanelServer(host, port, title)
    try:
        panel.start()
    except OSError:
        LOGGER.exception("Could not start the live event panel on %s:%s", host, port)
        return None
    return panel


_PANEL_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WaterManagement live events</title>
<style>
  :root { --bg:#f6f8f7; --fg:#1d2721; --muted:#66746b; --row:#ffffff; --line:#dfe6e1;
          --ok:#1f8a4c; --bad:#c4372c; --warn:#d9822b; --off:#8a948e; --info:#2c6fb7; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#111614; --fg:#e3ebe6; --muted:#8fa096; --row:#18201c; --line:#26312b;
            --ok:#47c47c; --bad:#ef6a5e; --warn:#f0a04b; --off:#7d8882; --info:#6ea8ea; } }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--fg); font:14px/1.4 system-ui, sans-serif; }
  header { padding:16px; display:flex; flex-wrap:wrap; gap:12px; align-items:center; border-bottom:1px solid var(--line); }
  h1 { font-size:18px; margin:0; flex:1 1 auto; }
  #status { color:var(--muted); font-size:13px; }
  #status.live::before { content:"\\25CF "; color:var(--ok); }
  #status.down::before { content:"\\25CF "; color:var(--bad); }
  input { padding:6px 8px; border:1px solid var(--line); border-radius:6px; background:var(--row); color:var(--fg); min-width:200px; }
  main { padding:0 16px 16px; overflow-x:auto; }
  table { width:100%; border-collapse:collapse; font-family:ui-monospace, Menlo, monospace; font-size:13px; }
  th { text-align:left; color:var(--muted); font-weight:600; padding:8px 6px; position:sticky; top:0; background:var(--bg); }
  td { padding:6px; border-top:1px solid var(--line); background:var(--row); white-space:nowrap; }
  td.detail { white-space:normal; color:var(--muted); }
  td.event { font-weight:600; border-left:4px solid var(--info); }
  tr.ok td.event { border-left-color:var(--ok); color:var(--ok); }
  tr.bad td.event { border-left-color:var(--bad); color:var(--bad); }
  tr.warn td.event { border-left-color:var(--warn); color:var(--warn); }
  tr.off td.event { border-left-color:var(--off); color:var(--off); }
  .empty { color:var(--muted); padding:24px 6px; }
</style>
</head>
<body>
<header>
  <h1 id="title"></h1>
  <input id="filter" placeholder="Filter by station, request or event" aria-label="Filter events">
  <span id="status">connecting</span>
</header>
<main>
  <table>
    <thead><tr><th>Time</th><th>Dir</th><th>Peer</th><th>Event</th><th>Station</th><th>Request</th><th>Detail</th></tr></thead>
    <tbody id="rows"></tbody>
  </table>
  <div id="empty" class="empty">Waiting for events...</div>
</main>
<script>
const CONFIG = __CONFIG__;
const rows = document.getElementById("rows");
const statusEl = document.getElementById("status");
const filterEl = document.getElementById("filter");
document.getElementById("title").textContent = CONFIG.title + " \\u2014 live events";
document.title = CONFIG.title + " live events";
let last = 0;
const ARROWS = { IN: "\\u2190 in", OUT: "out \\u2192", LOCAL: "\\u25CB local" };

function category(name) {
  if (/LEAK|FAULT|KO|REJECT|DENIED|TIMEOUT|EXPIRED|ERROR/.test(name)) return "bad";
  if (/BLOCK/.test(name)) return "warn";
  if (/DISCONNECT|LOST|UNREACHABLE/.test(name)) return "off";
  if (/START|PROGRESS|TELEMETRY|COMPLETED|AUTHORIZED|ACTIVATE|REGISTER|HEALTHY|RESOLVED|CONNECTED|ACK/.test(name)) return "ok";
  return "";
}

function cell(text, cls) {
  const td = document.createElement("td");
  td.textContent = text == null ? "" : String(text);
  if (cls) td.className = cls;
  return td;
}

function matches(tr) {
  const q = filterEl.value.trim().toLowerCase();
  return !q || tr.dataset.search.includes(q);
}

function addRow(e) {
  const tr = document.createElement("tr");
  tr.className = category(e.event);
  const time = new Date(e.time).toLocaleTimeString();
  const request = e.request_id ? String(e.request_id).slice(0, 8) : "";
  [cell(time), cell(ARROWS[e.direction] || e.direction), cell(e.peer), cell(e.event, "event"),
   cell(e.ws_id), cell(request), cell(e.detail, "detail")].forEach(td => tr.appendChild(td));
  tr.title = e.request_id || "";
  tr.dataset.search = [e.peer, e.event, e.ws_id, e.request_id, e.detail].join(" ").toLowerCase();
  tr.hidden = !matches(tr);
  rows.insertBefore(tr, rows.firstChild);
  while (rows.children.length > 500) rows.removeChild(rows.lastChild);
  document.getElementById("empty").hidden = true;
}

function headers() {
  if (!CONFIG.requiresToken) return {};
  let token = null;
  try { token = sessionStorage.getItem("wm-panel-token"); } catch (e) {}
  if (!token) {
    token = window.prompt("Central admin token");
    try { if (token) sessionStorage.setItem("wm-panel-token", token); } catch (e) {}
  }
  return token ? { Authorization: "Bearer " + token } : {};
}

async function poll() {
  try {
    const response = await fetch(CONFIG.eventsPath + "?after=" + last, { headers: headers(), cache: "no-store" });
    if (response.status === 401) {
      try { sessionStorage.removeItem("wm-panel-token"); } catch (e) {}
      throw new Error("unauthorized");
    }
    if (!response.ok) throw new Error("HTTP " + response.status);
    const data = await response.json();
    data.events.forEach(addRow);
    last = data.last;
    statusEl.textContent = "live";
    statusEl.className = "live";
  } catch (error) {
    statusEl.textContent = "disconnected (" + error.message + ")";
    statusEl.className = "down";
  }
  setTimeout(poll, 1000);
}

filterEl.addEventListener("input", () => {
  for (const tr of rows.children) tr.hidden = !matches(tr);
});
poll();
</script>
</body>
</html>
"""

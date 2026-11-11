from __future__ import annotations

import json
import unittest
from http import HTTPStatus
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import threading
import time

from common.event_panel import EventFeed
from common.web_console import ApiError, ConsoleServer, origin_matches_host, render_page, run_stream


class ConsoleServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.calls = []

        def echo(request):
            self.calls.append(request.body)
            return HTTPStatus.ACCEPTED, {"got": request.body}

        def refuse(_request):
            raise ApiError(HTTPStatus.CONFLICT, "busy")

        self.server = ConsoleServer("127.0.0.1", 0, "engine", {"title": "</script><b>x", "wsId": "WS_001"}, {
            ("GET", "/api/status"): lambda request: {"ok": True, "query": request.query},
            ("POST", "/api/echo"): echo,
            ("POST", "/api/refuse"): refuse,
        })
        self.server.start()
        self.addCleanup(self.server.stop)
        self.base = f"http://127.0.0.1:{self.server.port}"

    def request(self, method: str, path: str, body=None, headers=None):
        data = json.dumps(body).encode() if body is not None else None
        request = Request(self.base + path, data=data, method=method, headers=headers or {})
        try:
            with urlopen(request, timeout=3) as response:
                return response.status, response.headers, response.read()
        except HTTPError as error:
            return error.code, error.headers, error.read()

    def test_page_embeds_config_as_inert_json_with_a_strict_policy(self) -> None:
        status, headers, body = self.request("GET", "/")
        page = body.decode()
        self.assertEqual(status, 200)
        self.assertIn("script-src 'self'", headers["Content-Security-Policy"])
        self.assertNotIn("</script><b>", page)
        self.assertIn('type="application/json"', page)

    def test_static_assets_come_from_a_whitelist_only(self) -> None:
        self.assertEqual(self.request("GET", "/static/console.js")[0], 200)
        self.assertEqual(self.request("GET", "/static/engine.html")[0], 404)
        self.assertEqual(self.request("GET", "/static/..%2Fweb_console.py")[0], 404)

    def test_get_route_receives_query_and_unknown_paths_are_404(self) -> None:
        status, _, body = self.request("GET", "/api/status?x=1")
        self.assertEqual((status, json.loads(body)), (200, {"ok": True, "query": {"x": "1"}}))
        self.assertEqual(self.request("GET", "/api/missing")[0], 404)
        self.assertEqual(self.request("GET", "/api/echo")[0], 405)

    def test_post_requires_json_and_same_origin(self) -> None:
        json_headers = {"Content-Type": "application/json"}
        status, _, body = self.request("POST", "/api/echo", {"a": 1}, json_headers)
        self.assertEqual((status, json.loads(body)), (202, {"got": {"a": 1}}))
        form = Request(self.base + "/api/echo", data=b"a=1", method="POST",
                       headers={"Content-Type": "application/x-www-form-urlencoded"})
        with self.assertRaises(HTTPError) as caught:
            urlopen(form, timeout=3)
        self.assertEqual(caught.exception.code, 415)
        status, _, _ = self.request("POST", "/api/echo", {"a": 1}, {**json_headers, "Origin": "http://evil.example"})
        self.assertEqual(status, 403)
        self.assertEqual(self.calls, [{"a": 1}])

    def test_route_errors_become_json_errors(self) -> None:
        status, _, body = self.request("POST", "/api/refuse", {}, {"Content-Type": "application/json"})
        self.assertEqual((status, json.loads(body)), (409, {"error": "busy"}))
        status, _, _ = self.request("POST", "/api/echo", [1, 2], {"Content-Type": "application/json"})
        self.assertEqual(status, 400)

    def test_origin_check(self) -> None:
        self.assertTrue(origin_matches_host(None, "localhost:7201"))
        self.assertTrue(origin_matches_host("http://localhost:7201", "localhost:7201"))
        self.assertFalse(origin_matches_host("http://localhost:9999", "localhost:7201"))

    def test_every_page_loads_only_local_scripts_and_never_uses_inner_html(self) -> None:
        from common.web_console import WEB_DIR
        for page in ("central", "fo", "engine", "monitor"):
            with self.subTest(page=page):
                html = render_page(page, {"title": "t"})
                self.assertIn(f'src="static/{page}.js"', html)
                self.assertNotIn("http://", html.replace("http://www.w3.org", ""))
                self.assertNotIn("innerHTML", (WEB_DIR / f"{page}.js").read_text())
        self.assertNotIn("innerHTML", (WEB_DIR / "console.js").read_text())


def parse_messages(raw: bytes) -> list[tuple[str, dict]]:
    messages = []
    for block in raw.decode().split("\n\n"):
        lines = dict(line.split(": ", 1) for line in block.splitlines() if line and not line.startswith(":"))
        if "event" in lines:
            messages.append((lines["event"], json.loads(lines["data"])))
    return messages


class LiveStreamTests(unittest.TestCase):
    def run_stream_in_thread(self, feed, snapshot, after=0, **kwargs):
        chunks, stop = [], threading.Event()
        thread = threading.Thread(target=run_stream, args=(chunks.append, feed, snapshot, after, stop),
                                  kwargs=kwargs, daemon=True)
        thread.start()
        self.addCleanup(lambda: (stop.set(), thread.join(2)))
        return chunks, stop, thread

    def wait_until(self, condition, timeout=2.0):
        deadline = time.monotonic() + timeout
        while not condition() and time.monotonic() < deadline:
            time.sleep(0.01)
        return condition()

    def test_events_are_pushed_as_soon_as_they_are_recorded(self) -> None:
        feed = EventFeed()
        feed.record("IN", "WM_Central", "OLD")
        chunks, _, _ = self.run_stream_in_thread(feed, None, interval=5.0)
        self.assertTrue(self.wait_until(lambda: chunks))
        self.assertEqual([e["event"] for e in parse_messages(chunks[0])[0][1]["events"]], ["OLD"])
        started = time.monotonic()
        feed.record("IN", "WM_Central", "NEW")
        self.assertTrue(self.wait_until(lambda: len(chunks) == 2))
        self.assertLess(time.monotonic() - started, 1.0, "the event must not wait for the 5 s interval")
        [(kind, data)] = parse_messages(chunks[1])
        self.assertEqual((kind, [e["event"] for e in data["events"]]), ("events", ["NEW"]))

    def test_resume_skips_events_already_seen(self) -> None:
        feed = EventFeed()
        first = feed.record("IN", "x", "SEEN")
        feed.record("IN", "x", "MISSED")
        chunks, _, _ = self.run_stream_in_thread(feed, None, after=first["seq"])
        self.assertTrue(self.wait_until(lambda: chunks))
        self.assertEqual([e["event"] for e in parse_messages(chunks[0])[0][1]["events"]], ["MISSED"])

    def test_state_is_pushed_only_when_it_changes(self) -> None:
        feed, state = EventFeed(), {"n": 1}
        chunks, _, _ = self.run_stream_in_thread(feed, lambda: dict(state), interval=0.05)
        self.assertTrue(self.wait_until(lambda: chunks))
        time.sleep(0.3)
        states = [d for chunk in chunks for kind, d in parse_messages(chunk) if kind == "state"]
        self.assertEqual(states, [{"n": 1}])
        state["n"] = 2
        self.assertTrue(self.wait_until(lambda: any(d == {"n": 2} for chunk in chunks for kind, d in parse_messages(chunk) if kind == "state")))

    def test_stream_ends_when_the_browser_leaves_or_the_server_stops(self) -> None:
        feed = EventFeed()
        def gone(_chunk):
            raise BrokenPipeError
        with self.assertRaises(BrokenPipeError):
            run_stream(gone, feed, None, 0, threading.Event())
        chunks, stop, thread = self.run_stream_in_thread(feed, None, interval=0.05)
        stop.set()
        thread.join(2)
        self.assertFalse(thread.is_alive())

    def test_console_server_serves_the_stream(self) -> None:
        server = ConsoleServer("127.0.0.1", 0, "monitor", {"title": "M"}, {}, source=EventFeed(),
                               snapshot=lambda: {"ok": True})
        server.start()
        self.addCleanup(server.stop)
        with urlopen(f"http://127.0.0.1:{server.port}/stream", timeout=3) as response:
            self.assertIn("text/event-stream", response.headers["Content-Type"])
            raw = response.read1(4096)
        kinds = [kind for kind, _ in parse_messages(raw)]
        self.assertIn("state", kinds)


if __name__ == "__main__":
    unittest.main()

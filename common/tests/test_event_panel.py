from __future__ import annotations

import json
import threading
import unittest
from urllib.request import urlopen

from common.event_panel import EventFeed, EventPanelServer, render_panel


class EventFeedTests(unittest.TestCase):
    def test_since_returns_only_newer_events_and_latest_sequence(self) -> None:
        feed = EventFeed()
        first = feed.record("IN", "WM_FO", "ACTIVATION_REQUEST", ws_id="WS_001", request_id="r1")
        feed.record("OUT", "WM_WS_E", "START_IRRIGATION", ws_id="WS_001", request_id="r1")
        events, last = feed.since(first["seq"])
        self.assertEqual([e["event"] for e in events], ["START_IRRIGATION"])
        self.assertEqual(last, 2)
        self.assertEqual(feed.since(last), ([], 2))

    def test_ring_buffer_keeps_only_the_most_recent_events(self) -> None:
        feed = EventFeed(capacity=3)
        for index in range(5):
            feed.record("LOCAL", "self", f"E{index}")
        events, last = feed.since(0)
        self.assertEqual([e["event"] for e in events], ["E2", "E3", "E4"])
        self.assertEqual(last, 5)

    def test_concurrent_records_get_unique_sequence_numbers(self) -> None:
        feed = EventFeed(capacity=10_000)
        threads = [threading.Thread(target=lambda: [feed.record("IN", "x", "E") for _ in range(200)])
                   for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        events, last = feed.since(0, limit=10_000)
        self.assertEqual(last, 1600)
        self.assertEqual(len({e["seq"] for e in events}), 1600)


class PanelPageTests(unittest.TestCase):
    def test_page_never_inserts_event_data_as_html(self) -> None:
        page = render_panel("WM_FO OP_001", "events")
        self.assertNotIn("innerHTML", page)
        self.assertIn("textContent", page)

    def test_title_cannot_break_out_of_the_script_block(self) -> None:
        page = render_panel("</script><script>alert(1)</script>", "events")
        self.assertNotIn("</script><script>alert(1)", page)

    def test_server_serves_page_and_incremental_events(self) -> None:
        feed = EventFeed()
        server = EventPanelServer("127.0.0.1", 0, "WM_WS_E WS_001", source=feed)
        server.start()
        self.addCleanup(server.stop)
        base = f"http://127.0.0.1:{server.port}"
        with urlopen(base + "/") as response:
            self.assertIn("text/html", response.headers["Content-Type"])
            self.assertIn("WM_WS_E WS_001", response.read().decode())
        first = feed.record("IN", "WM_Central", "START_IRRIGATION", ws_id="WS_001")
        feed.record("OUT", "WM_Central", "IRRIGATION_STARTED", ws_id="WS_001")
        with urlopen(f"{base}/events?after={first['seq']}") as response:
            self.assertEqual(response.headers["Cache-Control"], "no-store")
            body = json.loads(response.read())
        self.assertEqual([e["event"] for e in body["events"]], ["IRRIGATION_STARTED"])
        self.assertEqual(body["last"], 2)
        with urlopen(f"{base}/events?after=nonsense") as response:
            self.assertEqual(len(json.loads(response.read())["events"]), 2)


if __name__ == "__main__":
    unittest.main()

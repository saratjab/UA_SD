"""Request files: parsing with line numbers, the sequential runner, and the FO web routes."""
from __future__ import annotations

import io
import threading
import time
import unittest
from contextlib import redirect_stdout
from unittest import mock

from common.event_panel import feed
from common.web_console import ConsoleServer
from WM_FO.file_runner import FileRun
from WM_FO.kafka_client import FieldOperator
from WM_FO.request_file import FileRequest, SkippedLine, parse_request_lines
from WM_FO.WM_FO import _ActiveRequests, _FileRuns, console_routes, console_snapshot

from .test_component_consoles import RecordingPublisher, call, deliver

SAMPLE = (
    '{"ws_id": "WS_001", "duration_seconds": 5}\n'
    '{"ws_id": "WS_999", "duration_seconds": 5}\n'
    '{"ws_id": "WS_002", "duration_seconds": 5}\n'
)


def wait_until(predicate, timeout: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


class ParseRequestLinesTests(unittest.TestCase):
    def test_sample_file_is_read_in_order_with_line_numbers(self) -> None:
        requests, skipped = parse_request_lines(SAMPLE)
        self.assertEqual(skipped, [])
        self.assertEqual(requests, [FileRequest(1, "WS_001", 5), FileRequest(2, "WS_999", 5),
                                    FileRequest(3, "WS_002", 5)])

    def test_bad_lines_are_skipped_with_their_real_line_number(self) -> None:
        content = ('{"ws_id": "WS_001", "duration_seconds": 5}\n'
                   '\n'
                   '# a comment\n'
                   '{"ws_id": "WS_002", "duration_seconds": 0}\n'
                   '{not json\n'
                   '{"duration_seconds": 5}\n'
                   '{"ws_id": "WS_003", "target_volume_liters": 2.5}\n')
        requests, skipped = parse_request_lines(content)
        self.assertEqual(requests, [FileRequest(1, "WS_001", 5), FileRequest(7, "WS_003", None, 2.5)])
        self.assertEqual([entry.line for entry in skipped], [4, 5, 6])
        self.assertIn("duration_seconds", skipped[0].error)
        self.assertIn("invalid JSON", skipped[1].error)
        self.assertIn("ws_id", skipped[2].error)
        self.assertFalse(skipped[0].error.startswith("request "))

    def test_empty_file_has_no_requests(self) -> None:
        self.assertEqual(parse_request_lines("  \n"), ([], [SkippedLine(0, "the file is empty")]))

    def test_json_array_is_still_accepted(self) -> None:
        requests, skipped = parse_request_lines('[{"ws_id": "WS_001", "duration_seconds": 5}, {"ws_id": ""}]')
        self.assertEqual(requests, [FileRequest(1, "WS_001", 5)])
        self.assertEqual([entry.line for entry in skipped], [2])


class FileRunTests(unittest.TestCase):
    def setUp(self) -> None:
        self.publisher = RecordingPublisher()
        self.operator = FieldOperator("OP_001", self.publisher)

    def answer(self, outcomes: list[tuple[str, dict]]) -> threading.Thread:
        """Answer each request as it is published, in order (as Central would)."""
        def run() -> None:
            for index, (event_type, payload) in enumerate(outcomes):
                if not wait_until(lambda: len(self.publisher.records) > index):
                    return
                request_id = self.publisher.records[index][1]["request_id"]
                ws_id = self.publisher.records[index][1]["ws_id"]
                deliver(self.operator, event_type, request_id, payload, ws_id=ws_id)
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        return thread

    def test_requests_are_sent_one_at_a_time_and_outcomes_recorded(self) -> None:
        requests, skipped = parse_request_lines(SAMPLE + "{broken\n")
        run = FileRun("demo.jsonl", requests, skipped, timeout=2)
        sleeps: list[float] = []
        sent_before_sleep: list[int] = []

        def sleep(seconds: float) -> None:
            sent_before_sleep.append(len(self.publisher.records))
            sleeps.append(seconds)

        self.answer([("IRRIGATION_COMPLETED", {"end_reason": "DURATION_REACHED"}),
                     ("ACTIVATION_DENIED", {"reason": "UNKNOWN_OPERATOR_OR_STATION"}),
                     ("IRRIGATION_COMPLETED", {"end_reason": "DURATION_REACHED"})])
        with redirect_stdout(io.StringIO()):
            result = run.run(self.operator, sleep=sleep)

        # Spec: the next request is only sent after the previous one concluded, plus 4 s.
        self.assertEqual(sleeps, [4.0, 4.0])
        self.assertEqual(sent_before_sleep, [1, 2])
        self.assertEqual([m["ws_id"] for _, m in self.publisher.records], ["WS_001", "WS_999", "WS_002"])
        self.assertEqual(result["state"], "FINISHED")
        by_line = {item["line"]: item for item in result["items"]}
        self.assertEqual(by_line[1]["outcome"], "COMPLETED")
        self.assertEqual((by_line[2]["outcome"], by_line[2]["reason"]), ("DENIED", "UNKNOWN_OPERATOR_OR_STATION"))
        self.assertEqual(by_line[3]["outcome"], "COMPLETED")
        self.assertEqual((by_line[4]["state"], by_line[4]["request_id"]), ("SKIPPED", None))
        self.assertEqual(result["summary"], "2 completed, 1 denied, 1 skipped")
        self.assertEqual((result["total"], result["sent"], result["skipped"]), (3, 3, 1))
        # Every request carries the file line it came from.
        tracked = {r["request_id"]: r for r in self.operator.requests_snapshot()}
        self.assertEqual(tracked[by_line[2]["request_id"]]["origin"]["line"], 2)

    def test_events_table_shows_the_file_steps(self) -> None:
        requests, skipped = parse_request_lines('{"ws_id": "WS_001", "duration_seconds": 5}\nnope\n')
        run = FileRun("demo.jsonl", requests, skipped, timeout=2)
        _, before = feed().since(0)
        self.answer([("ACTIVATION_DENIED", {"reason": "WS_BUSY"})])
        with redirect_stdout(io.StringIO()):
            run.run(self.operator, sleep=lambda _s: None)
        events, _ = feed().since(before)
        names = [e["event"] for e in events]
        self.assertEqual(names[:2], ["FILE_LOADED", "FILE_LINE_SKIPPED"])
        self.assertIn("ACTIVATION_REQUEST", names)
        self.assertEqual(names[-1], "FILE_FINISHED")
        request_event = next(e for e in events if e["event"] == "ACTIVATION_REQUEST")
        self.assertIn("file line 1", request_event["detail"])
        self.assertIn("line 2", next(e for e in events if e["event"] == "FILE_LINE_SKIPPED")["detail"])

    def test_send_failure_is_reported_and_the_run_continues(self) -> None:
        operator = mock.Mock()
        operator.request_irrigation.side_effect = [RuntimeError("broker down"), "r2"]
        operator.wait_for_result.return_value = True
        operator.tracked_request.return_value = {"status": "COMPLETED"}
        run = FileRun("f.jsonl", [FileRequest(1, "WS_001", 5), FileRequest(2, "WS_002", 5)], [], timeout=1)
        with self.assertLogs("wm_fo", level="ERROR"):
            result = run.run(operator, sleep=lambda _s: None)
        self.assertEqual([item["state"] for item in result["items"]], ["SEND_FAILED", "DONE"])
        self.assertIn("broker down", result["items"][0]["error"])
        self.assertEqual(result["summary"], "1 completed, 1 not sent")

    def test_unanswered_request_is_marked_without_result(self) -> None:
        operator = mock.Mock()
        operator.request_irrigation.return_value = "r1"
        operator.wait_for_result.return_value = False
        operator.tracked_request.return_value = {"status": "NO_RESULT"}
        run = FileRun("f.jsonl", [FileRequest(1, "WS_001")], [], timeout=1)
        with self.assertLogs("wm_fo", level="WARNING"):
            result = run.run(operator, sleep=lambda _s: None)
        operator.mark_no_result.assert_called_once_with("r1")
        self.assertEqual(result["items"][0]["outcome"], "NO_RESULT")

    def test_cancel_during_the_pause_sends_nothing_more(self) -> None:
        operator = mock.Mock()
        operator.request_irrigation.return_value = "r1"
        operator.wait_for_result.return_value = True
        operator.tracked_request.return_value = {"status": "COMPLETED"}
        run = FileRun("f.jsonl", [FileRequest(1, "WS_001", 5), FileRequest(2, "WS_002", 5)], [], timeout=1)
        result = run.run(operator, sleep=lambda _s: run.cancel())
        operator.request_irrigation.assert_called_once()
        self.assertEqual(result["state"], "CANCELLED")
        self.assertEqual([item["state"] for item in result["items"]], ["DONE", "CANCELLED"])


class FileRunRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.publisher = RecordingPublisher()
        self.operator = FieldOperator("OP_001", self.publisher)
        self.active = _ActiveRequests()
        self.file_runs = _FileRuns()
        self.server = ConsoleServer("127.0.0.1", 0, "fo", {"title": "FO"},
                                    console_routes(self.operator, self.active, 30, self.file_runs),
                                    console_snapshot(self.operator, self.active, self.file_runs))
        self.server.start()
        self.addCleanup(self.server.stop)
        self.base = f"http://127.0.0.1:{self.server.port}"

    def tearDown(self) -> None:
        run = self.file_runs.current
        if run is not None:
            run.cancel()
            for record in list(self.publisher.records):
                deliver(self.operator, "ACTIVATION_DENIED", record[1]["request_id"], {"reason": "WS_BUSY"},
                        ws_id=record[1]["ws_id"])

    def test_upload_starts_a_sequential_run_and_refuses_a_second_one(self) -> None:
        with redirect_stdout(io.StringIO()):
            code, run = call(self.base, "POST", "/api/request-file", {"filename": "C:\\files\\demo.jsonl", "content": SAMPLE})
            self.assertEqual(code, 202)
            self.assertEqual((run["filename"], run["total"]), ("demo.jsonl", 3))
            # Only the first request is sent until it concludes.
            self.assertTrue(wait_until(lambda: len(self.publisher.records) == 1))
            time.sleep(0.1)
            self.assertEqual(len(self.publisher.records), 1)
            second, body = call(self.base, "POST", "/api/request-file", {"content": SAMPLE})
            self.assertEqual(second, 409)
            self.assertIn("demo.jsonl", body["error"])
            snapshot = console_snapshot(self.operator, self.active, self.file_runs)()
            self.assertEqual(snapshot["file_run"]["items"][0]["state"], "SENT")
            self.assertEqual(snapshot["status"]["active_requests"], 1)
            deliver(self.operator, "ACTIVATION_DENIED", self.publisher.records[0][1]["request_id"],
                    {"reason": "WS_BUSY"})
            self.assertTrue(wait_until(lambda: self.file_runs.snapshot()["state"] == "WAITING"))
            self.assertIsNotNone(self.file_runs.snapshot()["next_at"])
            code, cancelled = call(self.base, "POST", "/api/request-file/cancel", {})
            self.assertEqual(code, 200)
            self.assertTrue(wait_until(lambda: self.file_runs.snapshot()["state"] == "CANCELLED"))
        self.assertEqual(len(self.publisher.records), 1)
        self.assertEqual(self.file_runs.snapshot()["summary"], "1 denied, 2 cancelled")

    def test_file_without_usable_lines_is_rejected(self) -> None:
        code, body = call(self.base, "POST", "/api/request-file", {"filename": "bad.jsonl", "content": "{x\n{\"ws_id\": 3}\n"})
        self.assertEqual(code, 400)
        self.assertIn("line 1", body["error"])
        self.assertIn("line 2", body["error"])
        self.assertEqual(self.publisher.records, [])

    def test_invalid_bodies_are_refused(self) -> None:
        for body in ({}, {"content": 5}, {"content": SAMPLE, "extra": 1}, {"content": SAMPLE, "filename": 3}):
            with self.subTest(body=body):
                self.assertEqual(call(self.base, "POST", "/api/request-file", body)[0], 400)
        self.assertEqual(call(self.base, "POST", "/api/request-file/cancel", {})[0], 409)
        self.assertEqual(self.publisher.records, [])


if __name__ == "__main__":
    unittest.main()

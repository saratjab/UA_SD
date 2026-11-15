from __future__ import annotations

import io
import threading
import time
import unittest
from contextlib import redirect_stdout
from unittest import mock

from WM_FO.WM_FO import _file_mode, _interactive_mode, main
from WM_FO.request_file import FileRequest

REAL_THREAD = threading.Thread


class FieldOperatorInteractiveTests(unittest.TestCase):
    def test_file_mode_waits_sequentially_and_waits_four_seconds_after_denial(self) -> None:
        events = []

        class Operator:
            count = 0
            terminal_results = iter(["ACTIVATION_DENIED", "IRRIGATION_COMPLETED", "IRRIGATION_COMPLETED"])

            def request_irrigation(self, ws_id, duration=None, target=None, origin=None):
                self.count += 1
                request_id = f"r{self.count}"
                events.append(("request", ws_id, duration, target, request_id))
                return request_id

            def wait_for_result(self, request_id, timeout):
                result = next(self.terminal_results)
                events.append(("terminal", request_id, timeout, result))
                return True

        records = [
            FileRequest(1, "WS_001", 5), FileRequest(2, "WS_002", None, 2.0), FileRequest(3, "WS_003"),
        ]
        with mock.patch("WM_FO.WM_FO.read_request_file", return_value=(records, [])), \
                mock.patch("WM_FO.WM_FO.time.sleep", side_effect=lambda seconds: events.append(("sleep", seconds))):
            _file_mode(Operator(), "unused.jsonl", timeout=1)

        self.assertEqual(events, [
            ("request", "WS_001", 5, None, "r1"), ("terminal", "r1", 125.0, "ACTIVATION_DENIED"), ("sleep", 4.0),
            ("request", "WS_002", None, 2.0, "r2"), ("terminal", "r2", 1, "IRRIGATION_COMPLETED"), ("sleep", 4.0),
            ("request", "WS_003", None, None, "r3"), ("terminal", "r3", 1, "IRRIGATION_COMPLETED"),
        ])

    def test_file_mode_moves_on_when_a_request_never_concludes(self) -> None:
        operator = mock.Mock()
        operator.request_irrigation.side_effect = ["r1", "r2"]
        operator.wait_for_result.side_effect = [False, True]
        operator.tracked_request.return_value = None
        records = [FileRequest(1, "WS_001"), FileRequest(2, "WS_002", 2)]
        with mock.patch("WM_FO.WM_FO.read_request_file", return_value=(records, [])), \
                mock.patch("WM_FO.WM_FO.time.sleep") as sleep:
            with self.assertLogs("wm_fo", level="WARNING"):
                _file_mode(operator, "unused.jsonl", timeout=1)
        self.assertEqual(operator.request_irrigation.call_count, 2)
        sleep.assert_called_once_with(4.0)

    def test_q_at_station_prompt_exits_without_submitting(self) -> None:
        operator = mock.Mock()
        operator.request_available_stations.return_value = [
            {"station_id": "WS_009", "location": "Orchard"},
        ]
        with mock.patch("builtins.input", side_effect=["  Q  "]):
            with redirect_stdout(io.StringIO()):
                _interactive_mode(operator, timeout=5)
        operator.request_irrigation.assert_not_called()
        operator.request_available_stations.assert_called_once_with(5)

    def test_q_at_duration_prompt_exits_without_submitting(self) -> None:
        operator = mock.Mock()
        operator.request_available_stations.return_value = [
            {"station_id": "WS_001", "location": "North Garden"},
        ]
        with mock.patch("builtins.input", side_effect=["WS_001", "q"]):
            with redirect_stdout(io.StringIO()):
                _interactive_mode(operator, timeout=5)
        operator.request_irrigation.assert_not_called()

    def test_menu_shows_current_station_ids_and_locations_before_selection(self) -> None:
        operator = mock.Mock()
        operator.request_available_stations.side_effect = [
            [{"station_id": "WS_004", "location": "West Orchard"}],
            [],
        ]
        output = io.StringIO()
        with mock.patch("builtins.input", side_effect=["WS_004", "12", "q"]) as input_mock, \
                redirect_stdout(output):
            _interactive_mode(operator, timeout=5)
        rendered = output.getvalue()
        self.assertLess(rendered.index("WS_004"), rendered.index("West Orchard"))
        self.assertEqual(input_mock.call_args_list[0].args[0], "Enter station ID (or q to quit): ")
        self.assertIn("West Orchard", rendered)
        self.assertIn("No Watering Stations are currently available", rendered)
        operator.request_irrigation.assert_called_once_with("WS_004", 12, None)

    def test_menu_starts_a_second_station_while_the_first_irrigation_runs(self) -> None:
        release = threading.Event()
        requests: list[tuple[str, int]] = []
        stations = [{"station_id": "WS_001", "location": "North"},
                    {"station_id": "WS_002", "location": "South"}]

        class Operator:
            def request_available_stations(self, _timeout):
                return stations

            def request_irrigation(self, ws_id, duration, target):
                requests.append((ws_id, duration))
                return f"request-{ws_id}"

            def wait_for_decision(self, _request_id, _timeout):
                return True

            def wait_for_result(self, _request_id, _timeout):
                release.wait(5.0)  # the irrigation is still running
                return True

        output = io.StringIO()
        try:
            with mock.patch("builtins.input", side_effect=["WS_001", "30", "WS_002", "30", "q"]), \
                    redirect_stdout(output):
                _interactive_mode(Operator(), timeout=5)
        finally:
            release.set()
        self.assertEqual(requests, [("WS_001", 30), ("WS_002", 30)])
        self.assertIn("2 irrigation(s) still running", output.getvalue())

    def test_discovery_timeout_has_controlled_retry_prompt(self) -> None:
        operator = mock.Mock()
        operator.request_available_stations.side_effect = [TimeoutError("Central timeout")]
        with mock.patch("builtins.input", side_effect=["q"]), redirect_stdout(io.StringIO()) as output:
            _interactive_mode(operator, timeout=0.01)
        self.assertIn("Could not load available stations: Central timeout", output.getvalue())
        operator.request_irrigation.assert_not_called()

    def test_no_ws_id_q_stops_poll_thread_before_closing_consumer(self) -> None:
        publisher = mock.Mock()
        poll_entered = threading.Event()
        worker = mock.Mock()
        worker.wait_until_assigned.return_value = True
        worker.poll_in_progress = False

        def poll_once(timeout):
            if timeout == 0:
                return False
            worker.poll_in_progress = True
            poll_entered.set()
            time.sleep(0.15)
            worker.poll_in_progress = False
            return False

        def close_worker():
            worker.thread_alive_at_close = any(thread.is_alive() for thread in TrackingThread.started)

        worker.poll_once.side_effect = poll_once
        worker.close.side_effect = close_worker
        operator = mock.Mock()
        operator.worker.return_value = worker
        operator.request_available_stations.return_value = [
            {"station_id": "WS_001", "location": "North Garden"},
        ]

        class TrackingThread(REAL_THREAD):
            started = []

            def start(self):
                super().start()
                self.started.append(self)

        TrackingThread.started = []

        def enter_q(prompt):
            self.assertEqual(prompt, "Enter station ID (or q to quit): ")
            self.assertTrue(poll_entered.wait(timeout=1))
            self.assertTrue(worker.poll_in_progress)
            return "q"

        with mock.patch("WM_FO.WM_FO.KafkaPublisher", return_value=publisher), \
                mock.patch("WM_FO.WM_FO.FieldOperator", return_value=operator), \
                mock.patch("WM_FO.WM_FO.threading.Thread", TrackingThread), \
                mock.patch("builtins.input", side_effect=enter_q) as input_mock, \
                redirect_stdout(io.StringIO()):
            self.assertEqual(main(["--kafka-host", "kafka", "--kafka-port", "29092",
                                   "--operator-id", "OP_001"]), 0)

        input_mock.assert_called_once_with("Enter station ID (or q to quit): ")
        operator.request_irrigation.assert_not_called()
        worker.close.assert_called_once()
        self.assertFalse(worker.thread_alive_at_close)
        publisher.close.assert_called_once()

    def test_keyboard_interrupt_stops_poller_before_closing_consumer(self) -> None:
        publisher = mock.Mock()
        worker, operator = self._ready_mocks()
        threads = []

        class TrackingThread(REAL_THREAD):
            def start(self):
                super().start()
                threads.append(self)

        def close_worker():
            self.assertTrue(threads)
            self.assertFalse(any(thread.is_alive() for thread in threads))

        worker.close.side_effect = close_worker
        with mock.patch("WM_FO.WM_FO.KafkaPublisher", return_value=publisher), \
                mock.patch("WM_FO.WM_FO.FieldOperator", return_value=operator), \
                mock.patch("WM_FO.WM_FO.threading.Thread", TrackingThread), \
                mock.patch("builtins.input", side_effect=KeyboardInterrupt), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(self._main(publisher, operator), 0)
        worker.close.assert_called_once()
        publisher.close.assert_called_once()

    def test_assignment_startup_failure_closes_consumer_and_publisher(self) -> None:
        publisher = mock.Mock()
        worker, operator = self._ready_mocks()
        worker.wait_until_assigned.return_value = False
        with mock.patch("WM_FO.WM_FO.KafkaPublisher", return_value=publisher), \
                mock.patch("WM_FO.WM_FO.FieldOperator", return_value=operator), \
                mock.patch("WM_FO.WM_FO.threading.Thread") as thread_factory, \
                redirect_stdout(io.StringIO()):
            self.assertEqual(self._main(publisher, operator), 1)
        thread_factory.assert_not_called()
        worker.close.assert_called_once()
        publisher.close.assert_called_once()

    def test_poll_thread_start_failure_still_closes_resources(self) -> None:
        publisher = mock.Mock()
        worker, operator = self._ready_mocks()

        class NeverStartedThread:
            ident = None

            def __init__(self, **_kwargs):
                pass

            def start(self):
                raise RuntimeError("thread start failed")

        with mock.patch("WM_FO.WM_FO.KafkaPublisher", return_value=publisher), \
                mock.patch("WM_FO.WM_FO.FieldOperator", return_value=operator), \
                mock.patch("WM_FO.WM_FO.threading.Thread", NeverStartedThread), \
                redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError, "thread start failed"):
                self._main(publisher, operator)
        worker.close.assert_called_once()
        publisher.close.assert_called_once()

    def test_poller_timeout_warns_and_does_not_close_active_consumer(self) -> None:
        publisher = mock.Mock()
        worker, operator = self._ready_mocks()
        poll_entered, release_poll = threading.Event(), threading.Event()
        threads = []

        class TrackingThread(REAL_THREAD):
            def start(self):
                super().start()
                threads.append(self)

        def poll_once(timeout):
            if timeout == 0:
                return False
            poll_entered.set()
            release_poll.wait()
            return False

        worker.poll_once.side_effect = poll_once

        def enter_q(_prompt):
            self.assertTrue(poll_entered.wait(timeout=1))
            return "q"

        with mock.patch("WM_FO.WM_FO.KafkaPublisher", return_value=publisher), \
                mock.patch("WM_FO.WM_FO.FieldOperator", return_value=operator), \
                mock.patch("WM_FO.WM_FO.threading.Thread", TrackingThread), \
                mock.patch("WM_FO.WM_FO.POLL_SHUTDOWN_TIMEOUT_SECONDS", 0.01), \
                mock.patch("WM_FO.WM_FO.LOGGER.warning") as warning, \
                mock.patch("builtins.input", side_effect=enter_q), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(self._main(publisher, operator), 1)

        warning.assert_called_once()
        worker.close.assert_not_called()
        publisher.close.assert_called_once()
        release_poll.set()
        threads[0].join(timeout=1)
        self.assertFalse(threads[0].is_alive())

    def test_readiness_failure_joins_thread_before_closing_resources(self) -> None:
        publisher = mock.Mock()
        worker, operator = self._ready_mocks()
        threads = []

        class TrackingThread(REAL_THREAD):
            def start(self):
                super().start()
                threads.append(self)

        def fail_initial_poll(_timeout):
            raise RuntimeError("poll failed during startup")

        def close_worker():
            self.assertTrue(threads)
            self.assertFalse(any(thread.is_alive() for thread in threads))

        worker.poll_once.side_effect = fail_initial_poll
        worker.close.side_effect = close_worker
        with mock.patch("WM_FO.WM_FO.KafkaPublisher", return_value=publisher), \
                mock.patch("WM_FO.WM_FO.FieldOperator", return_value=operator), \
                mock.patch("WM_FO.WM_FO.threading.Thread", TrackingThread), \
                mock.patch("WM_FO.WM_FO.LOGGER.exception"), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(self._main(publisher, operator), 1)
        worker.close.assert_called_once()
        publisher.close.assert_called_once()

    @staticmethod
    def _ready_mocks():
        worker = mock.Mock()
        worker.wait_until_assigned.return_value = True
        operator = mock.Mock()
        operator.worker.return_value = worker
        operator.request_available_stations.return_value = [
            {"station_id": "WS_001", "location": "North Garden"},
        ]
        return worker, operator

    @staticmethod
    def _main(_publisher, _operator):
        return main(["--kafka-host", "kafka", "--kafka-port", "29092",
                     "--operator-id", "OP_001"])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import io
import threading
import time
import unittest
from contextlib import redirect_stdout
from unittest import mock

from WM_FO.main import _file_mode, _interactive_mode, main

REAL_THREAD = threading.Thread


class FieldOperatorInteractiveTests(unittest.TestCase):
    def test_file_mode_waits_sequentially_and_waits_four_seconds_after_denial(self) -> None:
        events = []

        class Operator:
            count = 0
            terminal_results = iter(["ACTIVATION_DENIED", "IRRIGATION_COMPLETED", "IRRIGATION_COMPLETED"])

            def request_irrigation(self, ws_id, duration=None, target=None):
                self.count += 1
                request_id = f"r{self.count}"
                events.append(("request", ws_id, duration, target, request_id))
                return request_id

            def wait_for_result(self, request_id, timeout):
                result = next(self.terminal_results)
                events.append(("terminal", request_id, timeout, result))
                return True

        records = [
            {"ws_id": "WS_001", "duration_seconds": 5},
            {"ws_id": "WS_002", "target_volume_liters": 2.0},
            {"ws_id": "WS_003"},
        ]
        with mock.patch("WM_FO.main.load_requests", return_value=records), \
                mock.patch("WM_FO.main.time.sleep", side_effect=lambda seconds: events.append(("sleep", seconds))):
            _file_mode(Operator(), "unused.jsonl", timeout=1)

        self.assertEqual(events, [
            ("request", "WS_001", 5, None, "r1"), ("terminal", "r1", None, "ACTIVATION_DENIED"), ("sleep", 4.0),
            ("request", "WS_002", None, 2.0, "r2"), ("terminal", "r2", None, "IRRIGATION_COMPLETED"), ("sleep", 4.0),
            ("request", "WS_003", None, None, "r3"), ("terminal", "r3", None, "IRRIGATION_COMPLETED"),
        ])

    def test_file_mode_does_not_advance_if_terminal_waiter_is_lost(self) -> None:
        operator = mock.Mock()
        operator.request_irrigation.return_value = "r1"
        operator.wait_for_result.return_value = False
        records = [{"ws_id": "WS_001"}, {"ws_id": "WS_002", "duration_seconds": 2}]
        with mock.patch("WM_FO.main.load_requests", return_value=records), \
                mock.patch("WM_FO.main.time.sleep") as sleep:
            with self.assertRaisesRegex(RuntimeError, "lost its terminal-result waiter"):
                _file_mode(operator, "unused.jsonl", timeout=1)
        operator.request_irrigation.assert_called_once_with("WS_001", None, None)
        sleep.assert_not_called()

    def test_q_at_station_prompt_exits_without_submitting(self) -> None:
        operator = mock.Mock()
        with mock.patch("builtins.input", side_effect=["  Q  "]):
            with redirect_stdout(io.StringIO()):
                _interactive_mode(operator, timeout=5)
        operator.request_irrigation.assert_not_called()

    def test_q_at_duration_prompt_exits_without_submitting(self) -> None:
        operator = mock.Mock()
        with mock.patch("builtins.input", side_effect=["WS_001", "q"]):
            with redirect_stdout(io.StringIO()):
                _interactive_mode(operator, timeout=5)
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

        class TrackingThread(REAL_THREAD):
            started = []

            def start(self):
                super().start()
                self.started.append(self)

        TrackingThread.started = []

        def enter_q(prompt):
            self.assertEqual(prompt, "WS ID: ")
            self.assertTrue(poll_entered.wait(timeout=1))
            self.assertTrue(worker.poll_in_progress)
            return "q"

        with mock.patch("WM_FO.main.KafkaPublisher", return_value=publisher), \
                mock.patch("WM_FO.main.FieldOperator", return_value=operator), \
                mock.patch("WM_FO.main.threading.Thread", TrackingThread), \
                mock.patch("builtins.input", side_effect=enter_q) as input_mock, \
                redirect_stdout(io.StringIO()):
            # Omit --ws-id exactly as in the interactive Docker invocation.
            self.assertEqual(main(["--kafka-host", "kafka", "--kafka-port", "29092",
                                   "--operator-id", "OP_001"]), 0)

        input_mock.assert_called_once_with("WS ID: ")
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
        with mock.patch("WM_FO.main.KafkaPublisher", return_value=publisher), \
                mock.patch("WM_FO.main.FieldOperator", return_value=operator), \
                mock.patch("WM_FO.main.threading.Thread", TrackingThread), \
                mock.patch("builtins.input", side_effect=KeyboardInterrupt), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(self._main(publisher, operator), 0)
        worker.close.assert_called_once()
        publisher.close.assert_called_once()

    def test_assignment_startup_failure_closes_consumer_and_publisher(self) -> None:
        publisher = mock.Mock()
        worker, operator = self._ready_mocks()
        worker.wait_until_assigned.return_value = False
        with mock.patch("WM_FO.main.KafkaPublisher", return_value=publisher), \
                mock.patch("WM_FO.main.FieldOperator", return_value=operator), \
                mock.patch("WM_FO.main.threading.Thread") as thread_factory, \
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

        with mock.patch("WM_FO.main.KafkaPublisher", return_value=publisher), \
                mock.patch("WM_FO.main.FieldOperator", return_value=operator), \
                mock.patch("WM_FO.main.threading.Thread", NeverStartedThread), \
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

        with mock.patch("WM_FO.main.KafkaPublisher", return_value=publisher), \
                mock.patch("WM_FO.main.FieldOperator", return_value=operator), \
                mock.patch("WM_FO.main.threading.Thread", TrackingThread), \
                mock.patch("WM_FO.main.POLL_SHUTDOWN_TIMEOUT_SECONDS", 0.01), \
                mock.patch("WM_FO.main.LOGGER.warning") as warning, \
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
        with mock.patch("WM_FO.main.KafkaPublisher", return_value=publisher), \
                mock.patch("WM_FO.main.FieldOperator", return_value=operator), \
                mock.patch("WM_FO.main.threading.Thread", TrackingThread), \
                mock.patch("WM_FO.main.LOGGER.exception"), \
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
        return worker, operator

    @staticmethod
    def _main(_publisher, _operator):
        return main(["--kafka-host", "kafka", "--kafka-port", "29092",
                     "--operator-id", "OP_001"])


if __name__ == "__main__":
    unittest.main()

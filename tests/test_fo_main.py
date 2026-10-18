from __future__ import annotations

import io
import threading
import time
import unittest
from contextlib import redirect_stdout
from unittest import mock

from WM_FO.main import _interactive_mode, main

REAL_THREAD = threading.Thread


class FieldOperatorInteractiveTests(unittest.TestCase):
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

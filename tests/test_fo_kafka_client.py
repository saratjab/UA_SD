from __future__ import annotations

import json
import io
import threading
import unittest
from contextlib import redirect_stdout
from unittest import mock

from common.kafka_contract import encode, envelope
from WM_FO.kafka_client import FieldOperator
from WM_FO.main import _poll_worker


class FieldOperatorKafkaTests(unittest.TestCase):
    class Publisher:
        def __init__(self): self.records = []
        def publish(self, *_args, **_kwargs):
            self.records.append((_args, _kwargs))

    def test_same_operator_uses_stable_group_and_routes_live_events_by_request(self) -> None:
        publisher = self.Publisher()
        first = FieldOperator("OP_001", publisher)  # type: ignore[arg-type]
        second = FieldOperator("OP_001", publisher)  # type: ignore[arg-type]
        worker_groups = []

        def worker_factory(_bootstrap, group_id, _topics, _handler, consumer, **kwargs):
            worker_groups.append(group_id)
            self.assertEqual(kwargs["auto_offset_reset"], "latest")
            return object()

        with mock.patch("WM_FO.kafka_client.KafkaWorker", side_effect=worker_factory):
            first.worker("localhost", consumer=object())
            second.worker("localhost", consumer=object())
        self.assertEqual(worker_groups[0], worker_groups[1])

        first_request = first.request_irrigation("WS_001", 20)
        second_request = second.request_irrigation("WS_002", 20)
        event = envelope("IRRIGATION_PROGRESS",
                         {"duration_seconds": 1.0, "accumulated_volume_liters": 0.1,
                          "flow_rate_lpm": 6.0},
                         operator_id="OP_001", request_id=second_request)
        with mock.patch("WM_FO.kafka_client.show_event") as display:
            first.handle_bytes(json.dumps(event).encode())
            self.assertFalse(first._pending[first_request].is_set())
            self.assertEqual(display.call_count, 0)
            second.handle_bytes(json.dumps(event).encode())
            self.assertFalse(second._pending[second_request].is_set())
            self.assertEqual(display.call_count, 1)

    def test_target_volume_activation_uses_station_key(self) -> None:
        publisher = self.Publisher()
        operator = FieldOperator("OP_001", publisher)  # type: ignore[arg-type]
        operator.request_irrigation("WS_009", target_volume_liters=2.5)
        args, kwargs = publisher.records[0]
        self.assertEqual(args[0], "wm.activation.requests")
        self.assertEqual(args[1]["payload"], {"target_volume_liters": 2.5})
        self.assertEqual(kwargs["key"], "WS_009")

    def test_manual_activation_publishes_empty_payload_with_unique_request_id(self) -> None:
        publisher = self.Publisher()
        operator = FieldOperator("OP_002", publisher)  # type: ignore[arg-type]
        first = operator.request_irrigation("WS_001")
        second = operator.request_irrigation("WS_001")
        self.assertNotEqual(first, second)
        topic, message = publisher.records[0][0]
        self.assertEqual(topic, "wm.activation.requests")
        self.assertEqual(message["ws_id"], "WS_001")
        self.assertEqual(message["operator_id"], "OP_002")
        self.assertEqual(message["request_id"], first)
        self.assertEqual(message["payload"], {})

    def test_station_discovery_publishes_request_and_correlates_matching_response(self) -> None:
        publisher = self.Publisher()
        operator = FieldOperator("OP_002", publisher)  # type: ignore[arg-type]

        def publish(_topic, request, *, key=None):
            self.assertEqual(_topic, "wm.activation.requests")
            self.assertEqual(key, "OP_002")
            self.assertEqual(request["event_type"], "AVAILABLE_STATIONS_REQUEST")
            self.assertEqual(request["operator_id"], "OP_002")
            self.assertTrue(request["request_id"])
            self.assertEqual(request["payload"], {})
            response = envelope(
                "AVAILABLE_STATIONS_RESPONSE",
                {"stations": [{"station_id": "WS_009", "location": "Orchard"}], "error": None},
                operator_id="OP_002", request_id=request["request_id"],
            )
            operator.handle_bytes(encode(response))

        publisher.publish = publish
        self.assertEqual(operator.request_available_stations(timeout=0.1), [
            {"station_id": "WS_009", "location": "Orchard"},
        ])

    def test_other_operator_and_irrigation_events_do_not_complete_discovery(self) -> None:
        publisher = self.Publisher()
        operator = FieldOperator("OP_002", publisher)  # type: ignore[arg-type]
        result = []
        failures = []

        def discover():
            try:
                result.extend(operator.request_available_stations(timeout=2.0))
            except Exception as exc:  # captured for assertion in the test thread
                failures.append(exc)

        thread = threading.Thread(target=discover)
        thread.start()
        while not publisher.records:
            threading.Event().wait(0.001)
        request = publisher.records[0][0][1]
        unrelated = envelope("IRRIGATION_COMPLETED", {}, operator_id="OP_002",
                             ws_id="WS_001", request_id="irrigation-request")
        wrong_operator = envelope("AVAILABLE_STATIONS_RESPONSE", {"stations": [], "error": None},
                                  operator_id="OP_003", request_id=request["request_id"])
        response = envelope("AVAILABLE_STATIONS_RESPONSE",
                            {"stations": [], "error": None},
                            operator_id="OP_002", request_id=request["request_id"])
        with mock.patch("WM_FO.kafka_client.show_event") as show_event:
            operator.handle_bytes(encode(unrelated))
            operator.handle_bytes(encode(wrong_operator))
            self.assertTrue(thread.is_alive())
            operator.handle_bytes(encode(response))
            show_event.assert_called_once_with(unrelated)
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(result, [])

    def test_station_discovery_timeout_is_controlled_and_cleans_pending_state(self) -> None:
        operator = FieldOperator("OP_002", self.Publisher())  # type: ignore[arg-type]
        with self.assertRaisesRegex(TimeoutError, "did not return"):
            operator.request_available_stations(timeout=0)
        self.assertEqual(operator._station_discoveries, {})

    def test_station_discovery_publish_failure_cleans_pending_state(self) -> None:
        class BrokenPublisher:
            def publish(self, *_args, **_kwargs):
                raise OSError("broker unavailable")

        operator = FieldOperator("OP_002", BrokenPublisher())  # type: ignore[arg-type]
        with self.assertRaisesRegex(OSError, "broker unavailable"):
            operator.request_available_stations()
        self.assertEqual(operator._station_discoveries, {})

    def test_local_event_consumer_filters_by_station_and_operator(self) -> None:
        operator = FieldOperator("OP_002", self.Publisher(), station_id="WS_001")  # type: ignore[arg-type]
        request_id = operator.request_irrigation("WS_001")
        event = envelope("IRRIGATION_TELEMETRY", {"sequence": 1, "flow_rate_lpm": 6.0,
                            "accumulated_volume_liters": 0.1, "duration_seconds": 1.0},
                         ws_id="WS_002", operator_id="OP_002", request_id=request_id)
        with mock.patch("WM_FO.kafka_client.show_event") as display:
            operator.handle_bytes(encode(event))
        display.assert_not_called()
        self.assertFalse(operator._pending[request_id].is_set())

    def test_local_consumer_group_can_be_station_specific(self) -> None:
        operator = FieldOperator("OP_002", self.Publisher(),
                                 consumer_group_id="wm-ws-local-WS_001-v1",
                                 station_id="WS_001")  # type: ignore[arg-type]
        seen = {}
        def factory(_bootstrap, group_id, _topics, _handler, consumer, **_kwargs):
            seen["group"] = group_id
            return object()
        with mock.patch("WM_FO.kafka_client.KafkaWorker", side_effect=factory):
            operator.worker("localhost", consumer=object())
        self.assertEqual(seen["group"], "wm-ws-local-WS_001-v1")

    def test_field_operator_rejects_mixed_modes(self) -> None:
        operator = FieldOperator("OP_001", self.Publisher())  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "not both"):
            operator.request_irrigation("WS_001", 2, 1.0)

    def test_restarted_operator_resumes_and_displays_completion_once(self) -> None:
        publisher = self.Publisher()
        previous_process = FieldOperator("OP_001", publisher)  # type: ignore[arg-type]
        request_id = previous_process.request_irrigation("WS_002", 30)
        progress = envelope(
            "IRRIGATION_PROGRESS",
            {"duration_seconds": 14.0, "accumulated_volume_liters": 1.4,
             "flow_rate_lpm": 6.0},
            ws_id="WS_002", operator_id="OP_001", request_id=request_id,
        )
        previous_process.handle_bytes(encode(progress))

        restarted_process = FieldOperator("OP_001", publisher)  # type: ignore[arg-type]
        groups = []

        def worker_factory(_bootstrap, group_id, _topics, _handler, _consumer, **kwargs):
            groups.append(group_id)
            self.assertEqual(kwargs["auto_offset_reset"], "latest")
            return object()

        with mock.patch("WM_FO.kafka_client.KafkaWorker", side_effect=worker_factory):
            restarted_process.worker("localhost", consumer=object())

        completion = envelope(
            "IRRIGATION_COMPLETED",
            {"duration_seconds": 30.0, "accumulated_volume_liters": 3.0,
             "end_reason": "TIME_LIMIT"},
            ws_id="WS_002", operator_id="OP_001", request_id=request_id,
        )
        output = io.StringIO()
        with redirect_stdout(output):
            restarted_process.handle_bytes(encode(completion))
            restarted_process.handle_bytes(encode(completion))  # replay before commit

        self.assertEqual(groups, [previous_process._consumer_group_id])
        self.assertIn(request_id, output.getvalue())
        self.assertIn("elapsed 30.0 s, total volume 3.0 L, reason TIME_LIMIT", output.getvalue())
        self.assertEqual(output.getvalue().count("completed:"), 1)

    def test_fresh_operator_starts_at_latest_and_filters_other_operators(self) -> None:
        operator = FieldOperator("OP_NEW", self.Publisher())  # type: ignore[arg-type]
        self.assertNotEqual(operator._consumer_group_id,
                            FieldOperator("OP_001", self.Publisher())._consumer_group_id)  # type: ignore[arg-type]
        kwargs_seen = {}

        def worker_factory(_bootstrap, group_id, _topics, _handler, _consumer, **kwargs):
            kwargs_seen.update(group_id=group_id, **kwargs)
            return object()

        with mock.patch("WM_FO.kafka_client.KafkaWorker", side_effect=worker_factory):
            operator.worker("localhost", consumer=object())
        self.assertEqual(kwargs_seen["auto_offset_reset"], "latest")

        old_result = envelope(
            "IRRIGATION_COMPLETED",
            {"duration_seconds": 30.0, "accumulated_volume_liters": 3.0,
             "end_reason": "TIME_LIMIT"},
            ws_id="WS_002", operator_id="OP_001", request_id="old-request",
        )
        with mock.patch("WM_FO.kafka_client.show_event") as display:
            operator.handle_bytes(encode(old_result))
        display.assert_not_called()

    def test_activation_denied_wakes_matching_request_and_is_displayed(self) -> None:
        operator = FieldOperator("OP_001", self.Publisher())  # type: ignore[arg-type]
        request_id = operator.request_irrigation("WS_001", 10)
        denial = envelope("ACTIVATION_DENIED", {"reason": "WS_OUT_OF_SERVICE"},
                          ws_id="WS_001", operator_id="OP_001", request_id=request_id)
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertTrue(operator.handle_bytes(encode(denial)))
            self.assertTrue(operator.wait_for_result(request_id, timeout=0.01))
        self.assertIn("[WS_001] Irrigation denied: WS_OUT_OF_SERVICE.", output.getvalue())

    def test_irrigation_completed_remains_a_terminal_result(self) -> None:
        operator = FieldOperator("OP_001", self.Publisher())  # type: ignore[arg-type]
        request_id = operator.request_irrigation("WS_001", 10)
        completed = envelope("IRRIGATION_COMPLETED",
                             {"duration_seconds": 10, "accumulated_volume_liters": 3.5,
                              "end_reason": "COMPLETED"},
                             ws_id="WS_001", operator_id="OP_001", request_id=request_id)
        output = io.StringIO()
        with redirect_stdout(output):
            progress = envelope("IRRIGATION_PROGRESS",
                                {"duration_seconds": 9, "accumulated_volume_liters": 3.0,
                                 "flow_rate_lpm": 20.0},
                                ws_id="WS_001", operator_id="OP_001", request_id=request_id)
            self.assertTrue(operator.handle_bytes(encode(progress)))
            self.assertTrue(operator.handle_bytes(encode(completed)))
            self.assertTrue(operator.handle_bytes(encode(completed)))
            self.assertTrue(operator.wait_for_result(request_id, timeout=0.01))
        self.assertIn(f"[WS_001] Request {request_id} completed: elapsed 10 s, total volume 3.5 L", output.getvalue())
        self.assertEqual(output.getvalue().count("completed:"), 1)
        self.assertIn("progress:", output.getvalue())

    def test_poll_worker_signals_ready_only_after_its_first_poll(self) -> None:
        class Worker:
            def __init__(self):
                self.first_poll = threading.Event()
                self.calls = 0

            def poll_once(self, _timeout):
                self.calls += 1
                if self.calls == 1:
                    self.first_poll.set()
                else:
                    raise RuntimeError("stop test thread")

        worker = Worker()
        ready, failed = threading.Event(), threading.Event()
        with mock.patch("WM_FO.main.LOGGER.exception"):
            thread = threading.Thread(target=_poll_worker, args=(worker, ready, failed), daemon=True)
            thread.start()
            self.assertTrue(ready.wait(timeout=1))
            thread.join(timeout=1)
        self.assertTrue(worker.first_poll.is_set())
        self.assertTrue(failed.is_set())


if __name__ == "__main__":
    unittest.main()

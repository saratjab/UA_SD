from __future__ import annotations

import socket
import threading
import unittest
from unittest import mock

from WM_WS.WM_WS_M import protocol
from WM_WS.WM_WS_E.config import EngineConfig
from WM_WS.WM_WS_E.engine import WateringStationEngine
from WM_WS.WM_WS_E.flow_meter import FlowMeter
from WM_WS.WM_WS_E.irrigation import IrrigationController, IrrigationRequest, IrrigationState
from WM_WS.WM_WS_E.valve import SolenoidValve


def engine_config() -> EngineConfig:
    return EngineConfig(
        ws_id="WS_001",
        kafka_host="127.0.0.1",
        kafka_port=9092,
        monitor_host="127.0.0.1",
        monitor_port=6000,
        flow_rate_lpm=12.0,
        default_irrigation_duration=2.0,
        socket_timeout=0.05,
        telemetry_interval=1.0,
    )


class EngineTests(unittest.TestCase):
    def test_engine_starts_with_valid_configuration(self) -> None:
        engine = WateringStationEngine(engine_config())
        self.assertEqual(engine.config.ws_id, "WS_001")
        self.assertEqual(engine.state, IrrigationState.IDLE)

    def test_engine_connects_to_monitor(self) -> None:
        client, server = socket.socketpair()
        with mock.patch("socket.create_connection", return_value=client) as create_connection:
            engine = WateringStationEngine(engine_config())
            engine.connect_to_monitor()
            self.assertIs(engine.monitor_socket, client)
            create_connection.assert_called_once_with(("127.0.0.1", 6000), timeout=0.05)
        engine.close()
        server.close()

    def test_hello_ws_e_is_accepted_by_monitor(self) -> None:
        engine_sock, monitor_sock = socket.socketpair()
        engine = WateringStationEngine(engine_config())

        def monitor() -> None:
            self.assertEqual(
                protocol.receive_message(monitor_sock),
                {"type": "HELLO_WS_E", "ws_id": "WS_001", "component": "WM_WS_E"},
            )
            protocol.send_message(monitor_sock, {"type": "HELLO_ACK", "ws_id": "WS_001", "status": "OK"})

        thread = threading.Thread(target=monitor)
        thread.start()
        try:
            self.assertTrue(engine.send_hello(engine_sock))
        finally:
            thread.join(timeout=2)
            engine_sock.close()
            monitor_sock.close()

    def test_health_check_produces_health_ok(self) -> None:
        engine = WateringStationEngine(engine_config())
        response = engine.handle_monitor_message({"type": "HEALTH_CHECK", "ws_id": "WS_001", "sequence": 4})
        self.assertEqual(response, {"type": "HEALTH_OK", "ws_id": "WS_001", "sequence": 4, "status": "OK"})

    def test_sequence_numbers_are_handled_correctly(self) -> None:
        engine = WateringStationEngine(engine_config())
        response = engine.handle_monitor_message({"type": "HEALTH_CHECK", "ws_id": "WS_001", "sequence": 99})
        self.assertEqual(response["sequence"], 99)  # type: ignore[index]

    def test_ko_simulation_causes_health_ko(self) -> None:
        engine = WateringStationEngine(engine_config())
        engine.simulate_failure()
        response = engine.handle_monitor_message({"type": "HEALTH_CHECK", "ws_id": "WS_001", "sequence": 5})
        self.assertEqual(response["type"], "HEALTH_KO")  # type: ignore[index]
        self.assertEqual(response["reason"], "simulated_failure")  # type: ignore[index]

    def test_health_checks_work_while_irrigation_is_running(self) -> None:
        valve = SolenoidValve()
        controller = IrrigationController("WS_001", valve, FlowMeter(12.0), telemetry_interval=0.05)
        engine = WateringStationEngine(engine_config(), irrigation_controller=controller)
        self.assertTrue(engine.request_irrigation("operator-1", duration_seconds=0.2))
        try:
            response = engine.handle_monitor_message({"type": "HEALTH_CHECK", "ws_id": "WS_001", "sequence": 6})
            self.assertEqual(response["type"], "HEALTH_OK")  # type: ignore[index]
        finally:
            engine.stop_irrigation()
            controller.wait_until_finished(timeout=2)

    def test_monitor_disconnection_is_handled_safely(self) -> None:
        engine_sock, monitor_sock = socket.socketpair()
        engine = WateringStationEngine(engine_config())
        engine.monitor_socket = engine_sock
        monitor_sock.close()
        engine.handle_monitor_messages()
        engine.close()

    def test_invalid_monitor_message_is_rejected(self) -> None:
        engine = WateringStationEngine(engine_config())
        response = engine.handle_monitor_message({"type": "UNKNOWN", "ws_id": "WS_001"})
        self.assertEqual(response, {"type": "NACK", "reason": "unexpected_message"})

    def test_malformed_protocol_message_is_handled_safely(self) -> None:
        engine_sock, monitor_sock = socket.socketpair()
        engine = WateringStationEngine(engine_config())
        engine.monitor_socket = engine_sock
        monitor_sock.sendall(b"\x02bad-json\x03\x00")
        try:
            engine.handle_monitor_messages()
        finally:
            engine.close()
            monitor_sock.close()


class IrrigationTests(unittest.TestCase):
    def test_irrigation_starts_and_opens_valve(self) -> None:
        valve = SolenoidValve()
        controller = IrrigationController("WS_001", valve, FlowMeter(12.0), telemetry_interval=0.01)
        self.assertTrue(controller.start(IrrigationRequest("operator-1", 0.02)))
        self.assertTrue(valve.is_open)
        controller.wait_until_finished(timeout=2)

    def test_flow_meter_produces_configured_flow_rate(self) -> None:
        flow_meter = FlowMeter(12.0)
        self.assertEqual(flow_meter.reading().flow_rate_lpm, 12.0)

    def test_accumulated_volume_increases_correctly(self) -> None:
        flow_meter = FlowMeter(12.0)
        reading = flow_meter.advance(30.0)
        self.assertEqual(reading.accumulated_volume_liters, 6.0)

    def test_telemetry_is_produced_while_watering(self) -> None:
        controller = IrrigationController("WS_001", SolenoidValve(), FlowMeter(60.0), telemetry_interval=0.01)
        self.assertTrue(controller.start(IrrigationRequest("operator-1", 0.02)))
        self.assertTrue(controller.wait_until_finished(timeout=2))
        self.assertGreaterEqual(len(controller.telemetry), 2)
        self.assertEqual(controller.telemetry[0].ws_id, "WS_001")

    def test_irrigation_stops_when_duration_is_reached(self) -> None:
        valve = SolenoidValve()
        controller = IrrigationController("WS_001", valve, FlowMeter(60.0), telemetry_interval=0.01)
        self.assertTrue(controller.start(IrrigationRequest("operator-1", 0.02)))
        self.assertTrue(controller.wait_until_finished(timeout=2))
        self.assertEqual(controller.state, IrrigationState.IDLE)
        self.assertFalse(valve.is_open)
        self.assertEqual(controller.last_result.stopped_by, "duration")  # type: ignore[union-attr]

    def test_manual_stop_closes_valve(self) -> None:
        valve = SolenoidValve()
        controller = IrrigationController("WS_001", valve, FlowMeter(60.0), telemetry_interval=0.05)
        self.assertTrue(controller.start(IrrigationRequest("operator-1", 1.0)))
        self.assertTrue(controller.stop())
        self.assertTrue(controller.wait_until_finished(timeout=2))
        self.assertFalse(valve.is_open)
        self.assertEqual(controller.last_result.stopped_by, "manual")  # type: ignore[union-attr]

    def test_final_volume_and_duration_are_preserved(self) -> None:
        controller = IrrigationController("WS_001", SolenoidValve(), FlowMeter(60.0), telemetry_interval=0.01)
        self.assertTrue(controller.start(IrrigationRequest("operator-1", 0.02)))
        self.assertTrue(controller.wait_until_finished(timeout=2))
        assert controller.last_result is not None
        self.assertGreater(controller.last_result.accumulated_volume_liters, 0)
        self.assertGreater(controller.last_result.duration_seconds, 0)

    def test_invalid_irrigation_request_is_rejected(self) -> None:
        controller = IrrigationController("WS_001", SolenoidValve(), FlowMeter(60.0), telemetry_interval=0.01)
        with self.assertRaises(ValueError):
            controller.start(IrrigationRequest("operator-1", 0))

    def test_stop_when_not_watering_returns_false(self) -> None:
        controller = IrrigationController("WS_001", SolenoidValve(), FlowMeter(60.0), telemetry_interval=0.01)
        self.assertFalse(controller.stop())


if __name__ == "__main__":
    unittest.main()


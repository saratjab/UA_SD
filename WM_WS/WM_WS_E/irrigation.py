from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import logging
import math
import threading
import time
from typing import Callable

from .flow_meter import FlowMeter, FlowReading
from .valve import SolenoidValve


LOGGER = logging.getLogger(__name__)


class IrrigationState(str, Enum):
    IDLE = "IDLE"
    WATERING = "WATERING"
    FAILED = "FAILED"


@dataclass(frozen=True)
class IrrigationRequest:
    operator_id: str
    duration_seconds: float | None = None
    target_volume_liters: float | None = None
    request_id: str = ""


@dataclass(frozen=True)
class IrrigationTelemetry:
    ws_id: str
    state: str
    operator_id: str
    flow_rate_lpm: float
    accumulated_volume_liters: float
    duration_seconds: float
    final: bool = False


@dataclass(frozen=True)
class IrrigationResult:
    ws_id: str
    operator_id: str
    duration_seconds: float
    accumulated_volume_liters: float
    stopped_by: str


class IrrigationController:
    def __init__(
        self,
        ws_id: str,
        valve: SolenoidValve,
        flow_meter: FlowMeter,
        telemetry_interval: float,
        sleep_func: Callable[[float], None] = time.sleep,
        telemetry_callback: Callable[[IrrigationTelemetry], None] | None = None,
        result_callback: Callable[[IrrigationResult, str], None] | None = None,
    ) -> None:
        self.ws_id = ws_id
        self.valve = valve
        self.flow_meter = flow_meter
        self.telemetry_interval = telemetry_interval
        self.sleep_func = sleep_func
        self.telemetry_callback = telemetry_callback
        self.result_callback = result_callback
        self.state = IrrigationState.IDLE
        self.telemetry: list[IrrigationTelemetry] = []
        self.last_result: IrrigationResult | None = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._stop_reason = "manual"
        self._thread: threading.Thread | None = None

    def start(self, request: IrrigationRequest) -> bool:
        if request.duration_seconds is not None and request.target_volume_liters is not None:
            raise ValueError("choose duration or target volume, not both")
        if request.duration_seconds is not None and (not math.isfinite(request.duration_seconds) or request.duration_seconds <= 0):
            raise ValueError("irrigation duration must be positive")
        if request.target_volume_liters is not None and (not math.isfinite(request.target_volume_liters) or request.target_volume_liters <= 0):
            raise ValueError("irrigation target volume must be positive")

        with self._lock:
            if self.state != IrrigationState.IDLE:
                return False
            self.state = IrrigationState.WATERING
            self.last_result = None
            self.telemetry = []
            self.flow_meter.reset()
            self._stop_event.clear()
            self._stop_reason = "manual"
            self.valve.open()
            mode = (f"duration_seconds={request.duration_seconds}" if request.duration_seconds is not None else
                    f"target_volume_liters={request.target_volume_liters}" if request.target_volume_liters is not None else
                    "manual_stop")
            LOGGER.info("Irrigation started operator_id=%s mode=%s", request.operator_id, mode)
            self._thread = threading.Thread(
                target=self._run,
                args=(request,),
                name="wm-ws-e-irrigation",
                daemon=True,
            )
            self._thread.start()
            return True

    def stop(self) -> bool:
        with self._lock:
            if self.state != IrrigationState.WATERING:
                return False
            self._stop_event.set()
            return True

    def emergency_stop(self, reason: str = "manual") -> bool:
        """Immediately make the actuator safe and stop the active cycle."""
        with self._lock:
            if self.state != IrrigationState.WATERING:
                self.valve.close()
                return False
            if not self._stop_event.is_set():
                self._stop_reason = reason
            self._stop_event.set()
            self.valve.close()
            return True

    def wait_until_finished(self, timeout: float | None = None) -> bool:
        thread = self._thread
        if thread is None:
            return True
        thread.join(timeout=timeout)
        return not thread.is_alive()

    def current_telemetry(self, operator_id: str = "") -> IrrigationTelemetry:
        reading = self.flow_meter.reading()
        with self._lock:
            state = self.state.value
        return self._build_telemetry(operator_id, reading, state, final=False)

    def _run(self, request: IrrigationRequest) -> None:
        stopped_by = "manual"
        while True:
            if self._stop_event.is_set():
                stopped_by = self._stop_reason
                break
            if request.duration_seconds is not None and self.flow_meter.duration_seconds >= request.duration_seconds:
                stopped_by = "duration"
                break
            if request.target_volume_liters is not None and self.flow_meter.accumulated_volume_liters >= request.target_volume_liters:
                stopped_by = "target_volume"
                break
            step = self.telemetry_interval
            if request.duration_seconds is not None:
                step = min(step, request.duration_seconds - self.flow_meter.duration_seconds)
            if request.target_volume_liters is not None and self.flow_meter.flow_rate_lpm > 0:
                liters_left = request.target_volume_liters - self.flow_meter.accumulated_volume_liters
                step = min(step, liters_left * 60.0 / self.flow_meter.flow_rate_lpm)
            self.sleep_func(step)
            if self._stop_event.is_set():
                stopped_by = self._stop_reason
                break
            reading = self.flow_meter.advance(step)
            telemetry = self._build_telemetry(request.operator_id, reading, IrrigationState.WATERING.value)
            self.telemetry.append(telemetry)
            if self.telemetry_callback:
                self.telemetry_callback(telemetry)
            LOGGER.info(
                "Flow rate = %.2f L/min, accumulated volume = %.2f L",
                telemetry.flow_rate_lpm,
                telemetry.accumulated_volume_liters,
            )
        self._finish(request.operator_id, stopped_by)

    def _finish(self, operator_id: str, stopped_by: str) -> None:
        self.valve.close()
        reading = self.flow_meter.reading()
        final_telemetry = self._build_telemetry(operator_id, reading, IrrigationState.IDLE.value, final=True)
        with self._lock:
            self.state = IrrigationState.IDLE
            self.telemetry.append(final_telemetry)
            self.last_result = IrrigationResult(
                ws_id=self.ws_id,
                operator_id=operator_id,
                duration_seconds=reading.duration_seconds,
                accumulated_volume_liters=reading.accumulated_volume_liters,
                stopped_by=stopped_by,
            )
            result = self.last_result
        if self.telemetry_callback:
            self.telemetry_callback(final_telemetry)
        if self.result_callback and result is not None:
            self.result_callback(result, stopped_by)
        LOGGER.info("Irrigation stopped")

    def _build_telemetry(
        self,
        operator_id: str,
        reading: FlowReading,
        state: str,
        final: bool = False,
    ) -> IrrigationTelemetry:
        return IrrigationTelemetry(
            ws_id=self.ws_id,
            state=state,
            operator_id=operator_id,
            flow_rate_lpm=reading.flow_rate_lpm,
            accumulated_volume_liters=reading.accumulated_volume_liters,
            duration_seconds=reading.duration_seconds,
            final=final,
        )

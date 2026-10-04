from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FlowReading:
    flow_rate_lpm: float
    accumulated_volume_liters: float
    duration_seconds: float


class FlowMeter:
    def __init__(self, flow_rate_lpm: float) -> None:
        if flow_rate_lpm < 0:
            raise ValueError("flow rate must be non-negative")
        self.flow_rate_lpm = flow_rate_lpm
        self.accumulated_volume_liters = 0.0
        self.duration_seconds = 0.0

    def reset(self) -> None:
        self.accumulated_volume_liters = 0.0
        self.duration_seconds = 0.0

    def advance(self, elapsed_seconds: float) -> FlowReading:
        if elapsed_seconds < 0:
            raise ValueError("elapsed time must be non-negative")
        self.duration_seconds += elapsed_seconds
        self.accumulated_volume_liters += self.flow_rate_lpm * (elapsed_seconds / 60.0)
        return self.reading()

    def reading(self) -> FlowReading:
        return FlowReading(
            flow_rate_lpm=self.flow_rate_lpm,
            accumulated_volume_liters=self.accumulated_volume_liters,
            duration_seconds=self.duration_seconds,
        )


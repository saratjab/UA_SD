from types import SimpleNamespace

from WM_Central.state_manager import StateManager


def test_restores_latest_persisted_volume_per_station() -> None:
    class FakeDatabase:
        def list_irrigations(self):
            return [
                SimpleNamespace(ws_id="WS_001", total_volume_l=0.5),
                SimpleNamespace(ws_id="WS_002", total_volume_l=0.5),
                SimpleNamespace(ws_id="WS_001", total_volume_l=7.0),
                # A later request without a final volume must not erase 7.0.
                SimpleNamespace(ws_id="WS_001", total_volume_l=None),
            ]

    state = StateManager(FakeDatabase())  # type: ignore[arg-type]

    assert state._telemetry == {
        "WS_001": (None, 7.0),
        "WS_002": (None, 0.5),
    }
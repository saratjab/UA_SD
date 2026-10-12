## Project handoff: WaterManagement

This is a distributed water-management system with a central controller and watering-station components.

- WM_Central/ is the control center: configuration, socket server, state management, SQLite persistence, and monitoring.
- WM_WS/WM_WS_M/ is the watering-station monitor: station registration with Central, engine health checks, and fault reporting/resolution.
- WM_WS/WM_WS_E/ is the watering-station engine: irrigation control, flow metering, solenoid valve control, and monitor-message handling.
- common/protocol.py defines the shared framed socket protocol used across Central, monitor, and engine components.

### Core architecture

CentralSocketServer receives station traffic and delegates business decisions to StateManager; StateManager persists durable decisions through Database.
The Monitor keeps the station registered with Central while it checks the Engine, and the Engine adopts its station ID from the Monitor handshake when it has no legacy ID configured.

Key entry points:

- Central bootstrap (WM_Central/main.py:34)
- Socket server (WM_Central/socket_server.py:63)
- State manager (WM_Central/state_manager.py:85)
- SQLite database layer (WM_Central/database.py:58)
- Station engine (WM_WS/WM_WS_E/engine.py:18)
- Irrigation controller (WM_WS/WM_WS_E/irrigation.py:49)
- Station monitor (WM_WS/WM_WS_M/monitor.py:19)

### Behavioral constraints

ADR-001 is the most important design reference: state model and schema (docs/adr/ADR-001-state-model-and-schema.md:1).

- Persist Central-owned durable state; treat connectivity and telemetry as runtime-only.
- Stations self-enroll with ID and location; known stations can reconnect and refresh last-seen data.
- Database constraints enforce one active fault / live irrigation where applicable and replay-safe request IDs.
- Irrigation requests are recorded for auditability and idempotency.
- On Central restart, unfinished irrigation is closed as a Central-restart outcome; blocked status persists while live connections do not.
- The Monitor re-registers with Central during health checks so a healthy station recovers its live connection after Central restarts.
- Engine shutdown emergency-stops irrigation and closes the valve before its monitor connection is closed.
- Status is derived from precedence rules—avoid introducing a competing “single status column” model.

### Current implementation boundaries

- Kafka host/port arguments are accepted by Central and Engine entry points, but Kafka message consumption/production is not implemented yet.
- Central has state-side block/activate and irrigation persistence APIs, but there is not yet a Kafka command path or operator-facing UI wired to them.
- The runtime irrigation controller currently simulates the actuator and flow meter; Central's irrigation database lifecycle is tested separately from a live engine request path.

### Tests

Tests are concentrated around protocol framing, Central socket behavior, state management/database operations, and engine irrigation/health behavior.

- Socket-server tests (WM_Central/tests/test_socket_server.py:1)
- State-manager tests (WM_Central/tests/test_state_manager.py:1)
- Engine tests (tests/test_ws_e_engine.py:1)
- Protocol tests (tests/test_protocol.py:1)
- End-to-end Central/Monitor/Engine tests (tests/test_wm_ws_integration.py:1)

### Suggested first pass for a new Codex

1. Read ADR-001, then models.py, state_manager.py, and database.py.
2. Follow registration, fault, disconnect, and irrigation flows through socket_server.py.
3. Use the corresponding tests before changing any state-transition behavior.
4. For shutdown behavior, inspect WateringStationEngine.close() and IrrigationController.emergency_stop().
5. For Central restart recovery, inspect WateringStationMonitor._ensure_central_registration() and its integration test.
6. Consult the graph report (graphify-out/GRAPH_REPORT.md) or interactive graph (graphify-out/graph.html) for cross-component dependencies.

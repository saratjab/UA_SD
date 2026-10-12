# Graph Report - UA_SD  (2026-10-12)

## Corpus Check
- Corpus is ~13,941 words - fits in a single context window. You may not need a graph.

## Summary
- 551 nodes · 1403 edges · 24 communities (13 shown, 11 thin omitted)
- Extraction: 85% EXTRACTED · 15% INFERRED · 0% AMBIGUOUS · INFERRED: 207 edges (avg confidence: 0.88)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `7eddf571`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- WateringStationMonitor
- WateringStationEngine
- IrrigationTests
- EngineServer
- WM_WS_E/main.py
- SocketTests
- StateManager
- test_socket_server.py
- protocol.py
- database.py
- CentralSocketServer
- Database
- CentralClient
- Effective Status Function
- Graphify Workflow
- ADR-001: WM_Central State Model and SQLite Schema
- Ahmad Bassam Oqaili
- Sarat Mu'taz Jaabari
- Project handoff: WaterManagement

## God Nodes (most connected - your core abstractions)
1. `Database` - 49 edges
2. `WateringStationEngine` - 42 edges
3. `StateManager` - 34 edges
4. `IrrigationController` - 32 edges
5. `FlowMeter` - 29 edges
6. `SolenoidValve` - 26 edges
7. `StateManagerTests` - 26 edges
8. `CentralSocketServer` - 25 edges
9. `CentralClient` - 23 edges
10. `WateringStationMonitor` - 21 edges

## Surprising Connections (you probably didn't know these)
- `WMWSIntegrationTests` --uses--> `MonitorConfig`  [INFERRED]
  tests/test_wm_ws_integration.py → WM_WS/WM_WS_M/config.py
- `WMWSIntegrationTests` --uses--> `WateringStationMonitor`  [INFERRED]
  tests/test_wm_ws_integration.py → WM_WS/WM_WS_M/monitor.py
- `WMWSIntegrationTests` --uses--> `EngineConfig`  [INFERRED]
  tests/test_wm_ws_integration.py → WM_WS/WM_WS_E/config.py
- `WMWSIntegrationTests` --uses--> `WateringStationEngine`  [INFERRED]
  tests/test_wm_ws_integration.py → WM_WS/WM_WS_E/engine.py
- `WMWSIntegrationTests` --uses--> `FlowMeter`  [INFERRED]
  tests/test_wm_ws_integration.py → WM_WS/WM_WS_E/flow_meter.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **WM_Central State Model** — docs_adr_adr_001_state_model_and_schema_durable_dimensions, docs_adr_adr_001_state_model_and_schema_effective_status, docs_adr_adr_001_state_model_and_schema_status_precedence, docs_adr_adr_001_state_model_and_schema_state_manager [EXTRACTED 1.00]
- **SQLite Data Integrity Model** — docs_adr_adr_001_state_model_and_schema_database_invariants, docs_adr_adr_001_state_model_and_schema_request_audit, docs_adr_adr_001_state_model_and_schema_end_reason_model [INFERRED 0.85]

## Communities (24 total, 11 thin omitted)

### Community 0 - "WateringStationMonitor"
Cohesion: 0.09
Nodes (9): FakeCentralClient, MonitorTests, MonitorConfig, WateringStationMonitor, build_parser(), parse_args(), configure_logging(), main() (+1 more)

### Community 1 - "WateringStationEngine"
Cohesion: 0.09
Nodes (15): EngineTests, IrrigationTests, EngineConfig, WateringStationEngine, FlowMeter, FlowReading, IrrigationController, IrrigationRequest (+7 more)

### Community 11 - "EngineServer"
Cohesion: 0.06
Nodes (6): BuilderTests, FramingTests, MessageFactoryTests, ProtocolTests, EngineServerTests, EngineServer

### Community 14 - "WM_WS_E/main.py"
Cohesion: 0.18
Nodes (8): CentralConfig, build_parser(), parse_args(), build_parser(), parse_args(), configure_logging(), main(), start_command_thread()

### Community 2 - "StateManager"
Cohesion: 0.11
Nodes (6): CommandOutcome, DisconnectOutcome, FaultOutcome, RegisterOutcome, StateManager, StateManagerTests

### Community 3 - "test_socket_server.py"
Cohesion: 0.16
Nodes (7): BadInputTests, DisconnectTests, FaultTests, ManyStationsTests, RegistrationTests, SocketServerTestCase, wait_until()

### Community 4 - "protocol.py"
Cohesion: 0.10
Nodes (15): ConnectionClosedError, InvalidLRCError, MalformedFrameError, ProtocolError, calculate_lrc(), create_frame(), fault_resolved_message(), health_check_message() (+7 more)

### Community 5 - "database.py"
Cohesion: 0.05
Nodes (18): AdminState, DenyReason, EndReason, IrrigationStatus, RegistrationResult, StationStatus, StationView, DatabaseTestCase (+10 more)

### Community 6 - "CentralSocketServer"
Cohesion: 0.12
Nodes (4): WMWSIntegrationTests, CentralSocketServer, MonitorConnection, _enable_keepalive()

### Community 7 - "Database"
Cohesion: 0.07
Nodes (11): Database, Fault, IrrigationRecord, Operator, Station, FakeClock, _fault(), _irrigation() (+3 more)

### Community 8 - "CentralClient"
Cohesion: 0.15
Nodes (3): CentralClientTests, CentralClient, server()

### Community 18 - "Effective Status Function"
Cohesion: 0.29
Nodes (5): Effective Status Function, Fault Resolution Message, Single Station Status Column, State Manager, Displayed Status Precedence

### Community 9 - "Project handoff: WaterManagement"
Cohesion: 0.21
Nodes (6): Project handoff: WaterManagement, Core architecture, Behavioral constraints, Current implementation boundaries, Tests, Suggested first pass for a new Codex

## Knowledge Gaps
- **13 isolated node(s):** `Fault Resolution Message`, `Single Station Status Column`, `State Manager`, `Displayed Status Precedence`, `Graphify Workflow` (+8 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 166 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **11 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Database` connect `Database` to `WateringStationEngine`, `StateManager`, `database.py`, `CentralSocketServer`?**
  _High betweenness centrality (0.145) - this node is a cross-community bridge._
- **Are the 12 inferred relationships involving `Database` (e.g. with `WMWSIntegrationTests` and `AdminState`) actually correct?**
  _`Database` has 12 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Fault Resolution Message`, `Single Station Status Column`, `State Manager` to the rest of the system?**
  _13 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `WateringStationMonitor` be split into smaller, more focused modules?**
  _Cohesion score 0.09191583610188261 - nodes in this community are weakly interconnected._
- **Why does `StateManager` connect `StateManager` to `WateringStationEngine`, `test_socket_server.py`, `database.py`, `CentralSocketServer`, `Database`?**
  _High betweenness centrality (0.120) - this node is a cross-community bridge._
- **Are the 9 inferred relationships involving `WateringStationEngine` (e.g. with `WMWSIntegrationTests` and `EngineTests`) actually correct?**
  _`WateringStationEngine` has 9 INFERRED edges - model-reasoned connections that need verification._
- **Should `WateringStationEngine` be split into smaller, more focused modules?**
  _Cohesion score 0.08575949367088608 - nodes in this community are weakly interconnected._
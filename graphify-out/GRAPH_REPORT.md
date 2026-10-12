# Graph Report - UA_SD  (2026-10-11)

## Corpus Check
- Corpus is ~12,801 words - fits in a single context window. You may not need a graph.

## Summary
- 542 nodes · 1303 edges · 31 communities (14 shown, 17 thin omitted)
- Extraction: 87% EXTRACTED · 13% INFERRED · 0% AMBIGUOUS · INFERRED: 168 edges (avg confidence: 0.88)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- Client Integration Tests
- Engine Health Tests
- State Manager Operations
- Socket Server Tests
- Shared Wire Protocol
- Domain Models
- Central Socket Server
- SQLite Database Core
- Monitor Wire Protocol
- Database Fault Tests
- Irrigation Database Tests
- Protocol Framing Tests
- Database Utilities
- Database Query Mapping
- Central Configuration
- Station Database Tests
- Socket Transport Tests
- Station Registration
- ADR State Model
- Message Factory Tests
- Test Clock Utility
- Graphify Workflow
- ADR Document
- Project Contributor
- Project Contributor

## God Nodes (most connected - your core abstractions)
1. `Database` - 46 edges
2. `WateringStationEngine` - 35 edges
3. `StateManager` - 31 edges
4. `StateManagerTests` - 26 edges
5. `IrrigationController` - 26 edges
6. `FlowMeter` - 24 edges
7. `CentralSocketServer` - 21 edges
8. `SolenoidValve` - 21 edges
9. `CentralClient` - 19 edges
10. `IrrigationTests` - 18 edges

## Surprising Connections (you probably didn't know these)
- `EngineTests` --uses--> `WateringStationEngine`  [INFERRED]
  tests/test_ws_e_engine.py → WM_WS/WM_WS_E/engine.py
- `EngineTests` --uses--> `FlowMeter`  [INFERRED]
  tests/test_ws_e_engine.py → WM_WS/WM_WS_E/flow_meter.py
- `IrrigationTests` --uses--> `FlowMeter`  [INFERRED]
  tests/test_ws_e_engine.py → WM_WS/WM_WS_E/flow_meter.py
- `EngineTests` --uses--> `IrrigationState`  [INFERRED]
  tests/test_ws_e_engine.py → WM_WS/WM_WS_E/irrigation.py
- `IrrigationTests` --uses--> `IrrigationState`  [INFERRED]
  tests/test_ws_e_engine.py → WM_WS/WM_WS_E/irrigation.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **WM_Central State Model** — docs_adr_adr_001_state_model_and_schema_durable_dimensions, docs_adr_adr_001_state_model_and_schema_effective_status, docs_adr_adr_001_state_model_and_schema_status_precedence, docs_adr_adr_001_state_model_and_schema_state_manager [EXTRACTED 1.00]
- **SQLite Data Integrity Model** — docs_adr_adr_001_state_model_and_schema_database_invariants, docs_adr_adr_001_state_model_and_schema_request_audit, docs_adr_adr_001_state_model_and_schema_end_reason_model [INFERRED 0.85]

## Communities (31 total, 17 thin omitted)

### Community 0 - "Client Integration Tests"
Cohesion: 0.05
Nodes (15): BuilderTests, CentralClientTests, server(), FakeCentralClient, MonitorTests, EngineServerTests, CentralClient, build_parser() (+7 more)

### Community 1 - "Engine Health Tests"
Cohesion: 0.08
Nodes (17): engine_config(), EngineTests, IrrigationTests, EngineConfig, parse_args(), WateringStationEngine, FlowMeter, FlowReading (+9 more)

### Community 2 - "State Manager Operations"
Cohesion: 0.12
Nodes (3): CommandOutcome, StateManager, StateManagerTests

### Community 3 - "Socket Server Tests"
Cohesion: 0.15
Nodes (7): wait_until(), BadInputTests, DisconnectTests, FaultTests, ManyStationsTests, RegistrationTests, SocketServerTestCase

### Community 4 - "Shared Wire Protocol"
Cohesion: 0.10
Nodes (14): calculate_lrc(), ConnectionClosedError, create_frame(), fault_resolved_message(), health_check_message(), InvalidLRCError, MalformedFrameError, parse_frame() (+6 more)

### Community 5 - "Domain Models"
Cohesion: 0.14
Nodes (13): AdminState, DenyReason, EndReason, IrrigationStatus, StationStatus, StationView, format_station_table(), log_station_table() (+5 more)

### Community 6 - "Central Socket Server"
Cohesion: 0.14
Nodes (4): CentralSocketServer, _enable_keepalive(), MonitorConnection, _protocol_reason()

### Community 8 - "Monitor Wire Protocol"
Cohesion: 0.15
Nodes (12): calculate_lrc(), ConnectionClosedError, create_frame(), health_check_message(), InvalidLRCError, MalformedFrameError, parse_frame(), ProtocolError (+4 more)

### Community 9 - "Database Fault Tests"
Cohesion: 0.12
Nodes (4): DatabaseTestCase, FaultTests, OperatorTests, SchemaTests

### Community 12 - "Database Utilities"
Cohesion: 0.18
Nodes (3): _fault(), utc_now(), Fault

### Community 13 - "Database Query Mapping"
Cohesion: 0.25
Nodes (4): _irrigation(), _operator(), IrrigationRecord, Operator

### Community 14 - "Central Configuration"
Cohesion: 0.23
Nodes (5): build_parser(), CentralConfig, parse_args(), configure_logging(), build_parser()

### Community 17 - "Station Registration"
Cohesion: 0.32
Nodes (3): _station(), RegistrationResult, Station

### Community 18 - "ADR State Model"
Cohesion: 0.29
Nodes (5): Effective Status Function, Fault Resolution Message, Single Station Status Column, State Manager, Displayed Status Precedence

## Knowledge Gaps
- **8 isolated node(s):** `Graphify Workflow`, `Sarat Mu'taz Jaabari`, `Ahmad Bassam Oqaili`, `ADR-001: WM_Central State Model and SQLite Schema`, `Displayed Status Precedence` (+3 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 166 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **17 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Database` connect `SQLite Database Core` to `State Manager Operations`, `Domain Models`, `Database Fault Tests`, `Database Utilities`, `Database Query Mapping`, `Central Configuration`, `Station Registration`?**
  _High betweenness centrality (0.112) - this node is a cross-community bridge._
- **Are the 11 inferred relationships involving `Database` (e.g. with `AdminState` and `DenyReason`) actually correct?**
  _`Database` has 11 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Graphify Workflow`, `Sarat Mu'taz Jaabari`, `Ahmad Bassam Oqaili` to the rest of the system?**
  _8 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Client Integration Tests` be split into smaller, more focused modules?**
  _Cohesion score 0.05209274314965372 - nodes in this community are weakly interconnected._
- **Why does `StateManager` connect `State Manager Operations` to `Socket Server Tests`, `Domain Models`, `Central Socket Server`, `SQLite Database Core`, `Central Configuration`?**
  _High betweenness centrality (0.096) - this node is a cross-community bridge._
- **Are the 8 inferred relationships involving `WateringStationEngine` (e.g. with `EngineTests` and `EngineConfig`) actually correct?**
  _`WateringStationEngine` has 8 INFERRED edges - model-reasoned connections that need verification._
- **Should `Engine Health Tests` be split into smaller, more focused modules?**
  _Cohesion score 0.07943037974683544 - nodes in this community are weakly interconnected._
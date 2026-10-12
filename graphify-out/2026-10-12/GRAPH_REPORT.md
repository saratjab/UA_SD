# Graph Report - UA_SD  (2026-10-12)

## Corpus Check
- 55 files · ~16,845 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 2 file(s) not represented in the graph (top: (none) 1, .db-shm 1)

## Summary
- 644 nodes · 1750 edges · 39 communities (15 shown, 24 thin omitted)
- Extraction: 85% EXTRACTED · 15% INFERRED · 0% AMBIGUOUS · INFERRED: 260 edges (avg confidence: 0.89)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `10e508f0`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- logging
- WateringStationEngine
- StateManagerTests
- .call
- protocol.py
- state_manager.py
- StateManager
- Database
- CentralClient
- Project handoff: WaterManagement
- IrrigationTests
- unittest
- test_wm_ws_integration.py
- .perform_health_check
- CentralKafkaService
- ._one
- SocketTests
- DatabaseTestCase
- Effective Status Function
- database.py
- EngineServer
- StationTests
- Graphify Workflow
- ADR-001: WM_Central State Model and SQLite Schema
- Ahmad Bassam Oqaili
- Sarat Mu'taz Jaabari
- _irrigation
- FramingTests
- ProtocolTests
- FakeCentralClient
- MessageFactoryTests
- WaterManagement Kafka message contract
- kafka-irrigation-lifecycle.md

## God Nodes (most connected - your core abstractions)
1. `Database` - 56 edges
2. `WateringStationEngine` - 49 edges
3. `StateManager` - 43 edges
4. `IrrigationController` - 36 edges
5. `FlowMeter` - 33 edges
6. `CentralKafkaService` - 31 edges
7. `SolenoidValve` - 30 edges
8. `StateManagerTests` - 26 edges
9. `CentralSocketServer` - 25 edges
10. `CentralClient` - 23 edges

## Surprising Connections (you probably didn't know these)
- `RealKafkaLifecycleTests` --uses--> `Database`  [INFERRED]
  tests/test_kafka_real_integration.py → WM_Central/database.py
- `WMWSIntegrationTests` --uses--> `Database`  [INFERRED]
  tests/test_wm_ws_integration.py → WM_Central/database.py
- `RealKafkaLifecycleTests` --uses--> `CentralKafkaService`  [INFERRED]
  tests/test_kafka_real_integration.py → WM_Central/kafka_service.py
- `main()` --uses--> `KafkaUnavailable`  [INFERRED]
  WM_Central/main.py → common/kafka_transport.py
- `WMWSIntegrationTests` --uses--> `StationStatus`  [INFERRED]
  tests/test_wm_ws_integration.py → WM_Central/models.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **WM_Central State Model** — docs_adr_adr_001_state_model_and_schema_durable_dimensions, docs_adr_adr_001_state_model_and_schema_effective_status, docs_adr_adr_001_state_model_and_schema_status_precedence, docs_adr_adr_001_state_model_and_schema_state_manager [EXTRACTED 1.00]
- **SQLite Data Integrity Model** — docs_adr_adr_001_state_model_and_schema_database_invariants, docs_adr_adr_001_state_model_and_schema_request_audit, docs_adr_adr_001_state_model_and_schema_end_reason_model [INFERRED 0.85]

## Communities (39 total, 24 thin omitted)

### Community 0 - "logging"
Cohesion: 0.21
Nodes (7): build_parser(), MonitorConfig, parse_args(), configure_logging(), main(), utc_timestamp(), WateringStationMonitor

### Community 1 - "WateringStationEngine"
Cohesion: 0.08
Nodes (12): EngineKafkaTests, FakePublisher, engine_config(), EngineTests, monitor(), IrrigationTests, WateringStationEngine, FlowMeter (+4 more)

### Community 2 - "StateManagerTests"
Cohesion: 0.11
Nodes (5): CommandOutcome, DisconnectOutcome, FaultOutcome, RegisterOutcome, StateManagerTests

### Community 3 - ".call"
Cohesion: 0.16
Nodes (7): wait_until(), BadInputTests, DisconnectTests, FaultTests, ManyStationsTests, RegistrationTests, SocketServerTestCase

### Community 4 - "protocol.py"
Cohesion: 0.10
Nodes (15): calculate_lrc(), ConnectionClosedError, create_frame(), fault_resolved_message(), health_check_message(), InvalidLRCError, MalformedFrameError, parse_frame() (+7 more)

### Community 5 - "state_manager.py"
Cohesion: 0.21
Nodes (7): AdminState, DenyReason, EndReason, IrrigationStatus, StationStatus, effective_status(), EffectiveStatusTests

### Community 6 - "StateManager"
Cohesion: 0.07
Nodes (14): WMWSIntegrationTests, build_parser(), CentralConfig, parse_args(), configure_logging(), main(), _poll_worker(), StationView (+6 more)

### Community 8 - "CentralClient"
Cohesion: 0.16
Nodes (3): CentralClientTests, server(), CentralClient

### Community 9 - "Project handoff: WaterManagement"
Cohesion: 0.29
Nodes (6): Behavioral constraints, Core architecture, Current implementation boundaries, Project handoff: WaterManagement, Suggested first pass for a new Codex, Tests

### Community 12 - "test_wm_ws_integration.py"
Cohesion: 0.21
Nodes (6): RealKafkaLifecycleTests, wait_until(), build_parser(), EngineConfig, parse_args(), IrrigationState

### Community 14 - "CentralKafkaService"
Cohesion: 0.06
Nodes (21): decode(), encode(), envelope(), MessageValidationError, utc_timestamp(), validate(), KafkaPublisher, KafkaUnavailable (+13 more)

### Community 15 - "._one"
Cohesion: 0.23
Nodes (6): _fault(), _operator(), _station(), Fault, Operator, Station

### Community 17 - "DatabaseTestCase"
Cohesion: 0.12
Nodes (4): DatabaseTestCase, FaultTests, OperatorTests, SchemaTests

### Community 18 - "Effective Status Function"
Cohesion: 0.29
Nodes (5): Effective Status Function, Fault Resolution Message, Single Station Status Column, State Manager, Displayed Status Precedence

### Community 36 - "WaterManagement Kafka message contract"
Cohesion: 0.50
Nodes (3): FO request and response interface, Local development, WaterManagement Kafka message contract

## Knowledge Gaps
- **16 isolated node(s):** `Core architecture`, `Behavioral constraints`, `Current implementation boundaries`, `Tests`, `Suggested first pass for a new Codex` (+11 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 192 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **24 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Database` connect `Database` to `state_manager.py`, `StateManager`, `test_wm_ws_integration.py`, `CentralKafkaService`, `._one`, `DatabaseTestCase`, `database.py`, `_irrigation`?**
  _High betweenness centrality (0.134) - this node is a cross-community bridge._
- **Are the 14 inferred relationships involving `Database` (e.g. with `RealKafkaLifecycleTests` and `WMWSIntegrationTests`) actually correct?**
  _`Database` has 14 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Core architecture`, `Behavioral constraints`, `Current implementation boundaries` to the rest of the system?**
  _16 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `WateringStationEngine` be split into smaller, more focused modules?**
  _Cohesion score 0.08485540334855403 - nodes in this community are weakly interconnected._
- **Why does `StateManager` connect `StateManager` to `StateManagerTests`, `.call`, `state_manager.py`, `Database`, `test_wm_ws_integration.py`, `CentralKafkaService`?**
  _High betweenness centrality (0.124) - this node is a cross-community bridge._
- **Are the 11 inferred relationships involving `WateringStationEngine` (e.g. with `EngineKafkaTests` and `RealKafkaLifecycleTests`) actually correct?**
  _`WateringStationEngine` has 11 INFERRED edges - model-reasoned connections that need verification._
- **Should `StateManagerTests` be split into smaller, more focused modules?**
  _Cohesion score 0.1091753774680604 - nodes in this community are weakly interconnected._
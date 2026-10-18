# Graph Report - UA_SD  (2026-10-18)

## Corpus Check
- 70 files · ~33,613 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 3 file(s) not represented in the graph (top: (none) 3)

## Summary
- 967 nodes · 2474 edges · 61 communities (35 shown, 26 thin omitted)
- Extraction: 84% EXTRACTED · 16% INFERRED · 0% AMBIGUOUS · INFERRED: 390 edges (avg confidence: 0.88)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `271364e8`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- WateringStationMonitor
- WateringStationEngine
- StateManagerTests
- test_socket_server.py
- protocol.py
- database.py
- CentralSocketServer
- Database
- CentralClient
- Project handoff: WaterManagement
- IrrigationTests
- common/__init__.py
- StateManager
- MonitorTests
- envelope
- _irrigation
- SocketTests
- WM_FO/main.py
- Effective Status Function
- .register_station
- EngineServer
- StationTests
- Graphify Workflow
- test_wm_ws_integration.py
- DatabaseTestCase
- execute_control_line
- WaterManagement_Project_Plan_Complete.md
- FramingTests
- ProtocolTests
- FakeCentralClient
- MessageFactoryTests
- WaterManagement Release 1 — implementation and verification report
- .block_station
- WM_Central/config.py
- logging
- 33. Testing Strategy
- 37. Development Timeline
- 21. CENTRAL Internal Responsibilities
- WM_Central/main.py
- 19. Kafka Event Examples
- KafkaWorker
- 4. Required Components
- 15. CENTRAL Commands
- 17. Socket Communication Design
- 34. Team Responsibilities
- 14. Fault / Leak Flow
- 20. Database Design
- 3. Source-of-Truth Rules
- 5. Official Conceptual Architecture
- 6. Why Both Sockets and Kafka Exist
- Operator
- 7. Complete System Flow
- FakeClock
- EffectiveStatusTests
- utc_now

## God Nodes (most connected - your core abstractions)
1. `Database` - 62 edges
2. `StateManager` - 54 edges
3. `WateringStationEngine` - 54 edges
4. `IrrigationController` - 43 edges
5. `envelope()` - 43 edges
6. `FlowMeter` - 39 edges
7. `CentralKafkaService` - 36 edges
8. `SolenoidValve` - 36 edges
9. `EngineKafkaService` - 29 edges
10. `KafkaWorker` - 28 edges

## Surprising Connections (you probably didn't know these)
- `Central service` --references--> `main()`  [INFERRED]
  docs/railway-deployment.md → WM_Central/main.py
- `WMWSIntegrationTests` --uses--> `Database`  [INFERRED]
  tests/test_wm_ws_integration.py → WM_Central/database.py
- `RealKafkaLifecycleTests` --uses--> `CentralKafkaService`  [INFERRED]
  tests/test_kafka_real_integration.py → WM_Central/kafka_service.py
- `CentralKafkaService` --uses--> `KafkaPublisher`  [INFERRED]
  WM_Central/kafka_service.py → common/kafka_transport.py
- `CentralKafkaService` --uses--> `KafkaWorker`  [INFERRED]
  WM_Central/kafka_service.py → common/kafka_transport.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **WM_Central State Model** — docs_adr_adr_001_state_model_and_schema_durable_dimensions, docs_adr_adr_001_state_model_and_schema_effective_status, docs_adr_adr_001_state_model_and_schema_status_precedence, docs_adr_adr_001_state_model_and_schema_state_manager [EXTRACTED 1.00]
- **SQLite Data Integrity Model** — docs_adr_adr_001_state_model_and_schema_database_invariants, docs_adr_adr_001_state_model_and_schema_request_audit, docs_adr_adr_001_state_model_and_schema_end_reason_model [INFERRED 0.85]

## Communities (61 total, 26 thin omitted)

### Community 0 - "WateringStationMonitor"
Cohesion: 0.18
Nodes (7): build_parser(), MonitorConfig, parse_args(), configure_logging(), main(), utc_timestamp(), WateringStationMonitor

### Community 1 - "WateringStationEngine"
Cohesion: 0.07
Nodes (16): EngineKafkaTests, FakePublisher, engine_config(), EngineTests, monitor(), IrrigationTests, EngineConfig, WateringStationEngine (+8 more)

### Community 3 - "test_socket_server.py"
Cohesion: 0.16
Nodes (7): wait_until(), BadInputTests, DisconnectTests, FaultTests, ManyStationsTests, RegistrationTests, SocketServerTestCase

### Community 4 - "protocol.py"
Cohesion: 0.10
Nodes (15): calculate_lrc(), ConnectionClosedError, create_frame(), fault_resolved_message(), health_check_message(), InvalidLRCError, MalformedFrameError, parse_frame() (+7 more)

### Community 5 - "database.py"
Cohesion: 0.24
Nodes (6): AdminState, DenyReason, EndReason, IrrigationStatus, StationStatus, effective_status()

### Community 6 - "CentralSocketServer"
Cohesion: 0.13
Nodes (4): WMWSIntegrationTests, CentralSocketServer, _enable_keepalive(), MonitorConnection

### Community 8 - "CentralClient"
Cohesion: 0.15
Nodes (3): CentralClientTests, server(), CentralClient

### Community 9 - "Project handoff: WaterManagement"
Cohesion: 0.29
Nodes (6): Behavioral constraints, Core architecture, Current implementation boundaries, Project handoff: WaterManagement, Suggested first pass for a new Codex, Tests

### Community 12 - "StateManager"
Cohesion: 0.11
Nodes (4): RegisterOutcome, StateManager, Publisher, FakePublisher

### Community 14 - "envelope"
Cohesion: 0.09
Nodes (16): decode(), encode(), envelope(), MessageValidationError, utc_timestamp(), validate(), KafkaContractTests, FieldOperatorKafkaTests (+8 more)

### Community 15 - "_irrigation"
Cohesion: 0.19
Nodes (4): _fault(), _irrigation(), Fault, IrrigationRecord

### Community 17 - "WM_FO/main.py"
Cohesion: 0.05
Nodes (21): KafkaPublisher, KafkaUnavailable, FieldOperatorInteractiveTests, start(), close_worker(), enter_q(), poll_once(), start() (+13 more)

### Community 18 - "Effective Status Function"
Cohesion: 0.29
Nodes (5): Effective Status Function, Fault Resolution Message, Single Station Status Column, State Manager, Displayed Status Precedence

### Community 19 - ".register_station"
Cohesion: 0.32
Nodes (3): _station(), RegistrationResult, Station

### Community 29 - "DatabaseTestCase"
Cohesion: 0.12
Nodes (4): DatabaseTestCase, FaultTests, OperatorTests, SchemaTests

### Community 30 - "execute_control_line"
Cohesion: 0.19
Nodes (4): execute_control_line(), select_station_ids(), DisconnectOutcome, ControlAndMonitoringTests

### Community 31 - "WaterManagement_Project_Plan_Complete.md"
Cohesion: 0.06
Nodes (30): 10. Operator Irrigation Request Flow, 11. Authorization and Start-Irrigation Flow, 12. Irrigation Telemetry Flow, 13. Irrigation Completion Flow, 16. Watering Station States, 1. Project Identity, 22. WM_WS_M Internal Structure, 23. WM_WS_E Internal Structure (+22 more)

### Community 36 - "WaterManagement Release 1 — implementation and verification report"
Cohesion: 0.06
Nodes (31): ADR-001: WM_Central State Model and SQLite Schema, FO request and response interface, Local development, WaterManagement Kafka message contract, Docker deployment, Endpoint variables, Local Docker applications with Railway Central and Kafka, Local Docker mode (+23 more)

### Community 38 - "WM_Central/config.py"
Cohesion: 0.47
Nodes (3): build_parser(), CentralConfig, parse_args()

### Community 39 - "logging"
Cohesion: 0.16
Nodes (6): build_parser(), parse_args(), configure_logging(), main(), _poll_worker(), start_command_thread()

### Community 40 - "33. Testing Strategy"
Cohesion: 0.18
Nodes (11): 33.1 Unit tests, 33.2 Socket protocol tests, 33.3 Integration tests, 33.4 End-to-end test, 33.5 Fault test, 33.6 Deployment test, 33. Testing Strategy, CENTRAL (+3 more)

### Community 41 - "37. Development Timeline"
Cohesion: 0.22
Nodes (9): 19–22 October, 23–24 October, 25 October, 37. Development Timeline, Week of 12–18 October, Week of 21–27 September, Week of 26 October, Week of 28 September–4 October (+1 more)

### Community 42 - "21. CENTRAL Internal Responsibilities"
Cohesion: 0.22
Nodes (9): 21. CENTRAL Internal Responsibilities, `authorization.py`, `database.py`, `irrigation_service.py`, `kafka_client.py`, `monitoring.py`, `protocol.py`, `socket_server.py` (+1 more)

### Community 43 - "WM_Central/main.py"
Cohesion: 0.20
Nodes (8): configure_logging(), main(), _poll_worker(), StationView, control_loop(), format_station_table(), log_station_table(), monitor_loop()

### Community 44 - "19. Kafka Event Examples"
Cohesion: 0.33
Nodes (6): 19.1 Operator request, 19.2 Central authorization, 19.3 Start command, 19.4 Telemetry, 19.5 Completion, 19. Kafka Event Examples

### Community 45 - "KafkaWorker"
Cohesion: 0.06
Nodes (12): KafkaWorker, TopicPartition, OneMessage, OneMessageConsumer, FakeConsumer, FakePartition, KafkaWorkerTests, 18.1 Recommended topics (+4 more)

### Community 46 - "4. Required Components"
Cohesion: 0.40
Nodes (5): 4.1 `WM_Central`, 4.2 `WM_WS_M` — Watering Station Monitor, 4.3 `WM_WS_E` — Watering Station Engine, 4.4 `WM_FO` — Field Operator Application, 4. Required Components

### Community 47 - "15. CENTRAL Commands"
Cohesion: 0.50
Nodes (4): 15.1 Start irrigation, 15.2 Block WS, 15.3 Activate WS, 15. CENTRAL Commands

### Community 48 - "17. Socket Communication Design"
Cohesion: 0.50
Nodes (4): 17.1 Recommended framing used by this project, 17.2 Recommended message representation, 17.3 Current socket message set, 17. Socket Communication Design

### Community 49 - "34. Team Responsibilities"
Cohesion: 0.50
Nodes (4): 34. Team Responsibilities, Shared responsibility, Student 1 — WS side, Student 2 — CENTRAL/FO side

### Community 50 - "14. Fault / Leak Flow"
Cohesion: 0.67
Nodes (3): 14. Fault / Leak Flow, Fault while idle, Fault while watering

### Community 51 - "20. Database Design"
Cohesion: 0.67
Nodes (3): 20.1 Recommended schema, 20.2 Persistence responsibilities, 20. Database Design

### Community 52 - "3. Source-of-Truth Rules"
Cohesion: 0.67
Nodes (3): 3.1 What is fixed by the assignment, 3.2 What the PDF leaves to the students, 3. Source-of-Truth Rules

### Community 53 - "5. Official Conceptual Architecture"
Cohesion: 0.67
Nodes (3): 5. Official Conceptual Architecture, Socket interfaces, Streaming interfaces

### Community 54 - "6. Why Both Sockets and Kafka Exist"
Cohesion: 0.67
Nodes (3): 6. Why Both Sockets and Kafka Exist, Kafka path, Socket path

## Knowledge Gaps
- **120 isolated node(s):** `Core architecture`, `Behavioral constraints`, `Current implementation boundaries`, `Tests`, `Suggested first pass for a new Codex` (+115 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 353 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **26 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `18. Kafka / Event-Streaming Design` connect `KafkaWorker` to `WaterManagement_Project_Plan_Complete.md`?**
  _High betweenness centrality (0.185) - this node is a cross-community bridge._
- **Are the 15 inferred relationships involving `Database` (e.g. with `RealKafkaLifecycleTests` and `WMWSIntegrationTests`) actually correct?**
  _`Database` has 15 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Core architecture`, `Behavioral constraints`, `Current implementation boundaries` to the rest of the system?**
  _120 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `WateringStationEngine` be split into smaller, more focused modules?**
  _Cohesion score 0.06529602132114982 - nodes in this community are weakly interconnected._
- **Why does `StateManager` connect `StateManager` to `WateringStationEngine`, `StateManagerTests`, `test_socket_server.py`, `database.py`, `CentralSocketServer`, `Database`, `logging`, `.block_station`, `WM_Central/main.py`, `envelope`, `WM_FO/main.py`, `test_wm_ws_integration.py`, `execute_control_line`?**
  _High betweenness centrality (0.121) - this node is a cross-community bridge._
- **Are the 18 inferred relationships involving `StateManager` (e.g. with `RealKafkaLifecycleTests` and `WMWSIntegrationTests`) actually correct?**
  _`StateManager` has 18 INFERRED edges - model-reasoned connections that need verification._
- **Should `protocol.py` be split into smaller, more focused modules?**
  _Cohesion score 0.0962566844919786 - nodes in this community are weakly interconnected._
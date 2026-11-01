# Graph Report - UA_SD  (2026-10-30)

## Corpus Check
- 83 files · ~44,395 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 9 file(s) not represented in the graph (top: (none) 3, .example 2, .properties 2)

## Summary
- 1156 nodes · 3096 edges · 66 communities (25 shown, 41 thin omitted)
- Extraction: 85% EXTRACTED · 15% INFERRED · 0% AMBIGUOUS · INFERRED: 464 edges (avg confidence: 0.88)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `a5a1cc37`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- _irrigation
- IrrigationController
- StateManagerTests
- .call
- protocol.py
- database.py
- CentralSocketServer
- Database
- CentralClient
- Project handoff: WaterManagement
- IrrigationTests
- KafkaTopicInitTests
- envelope
- WateringStationMonitor
- FieldOperator
- kafka-railway-listeners.sh
- SocketTests
- WateringStationEngine
- Effective Status Function
- VerifyKafkaCertificate.java
- StateManager
- StationTests
- Graphify Workflow
- test_admin_api.py
- SchemaTests
- ControlAndMonitoringTests
- CentralAdminApiTests
- FramingTests
- ProtocolTests
- FakeConsumer
- _handler_type
- First Railway deployment runbook
- KafkaWorker
- parse_args
- kafka_security_config
- _BoundedHTTPServer
- KafkaPublisher
- .test_authenticated_tls_publish_consume_and_invalid_credentials
- WM_Central/main.py
- admin_api.py
- OneMessage
- CentralAdminServer
- test_kafka_transport.py
- test_wm_ws_integration.py
- MessageFactoryTests
- kafka-init-topics.sh
- .block_station
- implement_field_operator.md
- implement_kafka_integration.md
- .register_station
- KafkaUnavailable
- FakeClock
- RealKafkaPartitioningTests
- setup-railway-local.sh
- utc_now
- OperatorTests

## God Nodes (most connected - your core abstractions)
1. `Database` - 68 edges
2. `StateManager` - 61 edges
3. `envelope()` - 58 edges
4. `WateringStationEngine` - 54 edges
5. `IrrigationController` - 52 edges
6. `FlowMeter` - 48 edges
7. `EngineKafkaService` - 48 edges
8. `SolenoidValve` - 45 edges
9. `CentralKafkaService` - 44 edges
10. `CentralAdminApiTests` - 31 edges

## Surprising Connections (you probably didn't know these)
- `RealKafkaLifecycleTests` --uses--> `Database`  [INFERRED]
  tests/test_kafka_real_integration.py → WM_Central/database.py
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

## Communities (66 total, 41 thin omitted)

### Community 0 - "_irrigation"
Cohesion: 0.17
Nodes (6): _fault(), _irrigation(), _operator(), Fault, IrrigationRecord, Operator

### Community 1 - "IrrigationController"
Cohesion: 0.09
Nodes (10): EngineKafkaTests, FakePublisher, IrrigationTests, FlowMeter, FlowReading, IrrigationController, IrrigationRequest, IrrigationTelemetry (+2 more)

### Community 3 - ".call"
Cohesion: 0.16
Nodes (7): wait_until(), BadInputTests, DisconnectTests, FaultTests, ManyStationsTests, RegistrationTests, SocketServerTestCase

### Community 4 - "protocol.py"
Cohesion: 0.10
Nodes (15): calculate_lrc(), ConnectionClosedError, create_frame(), fault_resolved_message(), health_check_message(), InvalidLRCError, MalformedFrameError, parse_frame() (+7 more)

### Community 5 - "database.py"
Cohesion: 0.11
Nodes (12): message_key(), AdminState, DenyReason, EndReason, IrrigationStatus, StationStatus, StationView, format_station_table() (+4 more)

### Community 6 - "CentralSocketServer"
Cohesion: 0.13
Nodes (4): CentralSocketServer, _enable_keepalive(), MonitorConnection, SocketFailureVisibilityTests

### Community 8 - "CentralClient"
Cohesion: 0.14
Nodes (3): CentralClientTests, server(), CentralClient

### Community 9 - "Project handoff: WaterManagement"
Cohesion: 0.29
Nodes (6): Behavioral constraints, Core architecture, Current implementation boundaries, Project handoff: WaterManagement, Suggested first pass for a new Codex, Tests

### Community 12 - "envelope"
Cohesion: 0.11
Nodes (10): decode(), envelope(), MessageValidationError, utc_timestamp(), validate(), KafkaContractTests, CentralKafkaService, LatestTelemetry (+2 more)

### Community 13 - "WateringStationMonitor"
Cohesion: 0.09
Nodes (4): FakeCentralClient, MonitorTests, utc_timestamp(), WateringStationMonitor

### Community 14 - "FieldOperator"
Cohesion: 0.06
Nodes (23): encode(), FieldOperatorKafkaTests, worker_factory(), Publisher, FieldOperatorInteractiveTests, start(), close_worker(), enter_q() (+15 more)

### Community 15 - "kafka-railway-listeners.sh"
Cohesion: 0.09
Nodes (25): fail(), KAFKA_OPTS, KAFKA_SSL_CLIENT_AUTH, KAFKA_SSL_KEY_PASSWORD, KAFKA_SSL_KEYSTORE_LOCATION, KAFKA_SSL_KEYSTORE_PASSWORD, KAFKA_SSL_KEYSTORE_TYPE, KAFKA_SSL_TRUSTSTORE_LOCATION (+17 more)

### Community 17 - "WateringStationEngine"
Cohesion: 0.07
Nodes (15): RealKafkaLifecycleTests, WMWSIntegrationTests, engine_config(), EngineTests, monitor(), EngineServerTests, EngineConfig, WateringStationEngine (+7 more)

### Community 18 - "Effective Status Function"
Cohesion: 0.29
Nodes (5): Effective Status Function, Fault Resolution Message, Single Station Status Column, State Manager, Displayed Status Precedence

### Community 20 - "StateManager"
Cohesion: 0.12
Nodes (4): RegisterOutcome, StateManager, Publisher, test_restores_latest_persisted_volume_per_station()

### Community 28 - "test_admin_api.py"
Cohesion: 0.18
Nodes (6): AdminClientError, CentralAdminClient, _is_loopback(), main(), _menu_command(), run_menu()

### Community 30 - "ControlAndMonitoringTests"
Cohesion: 0.15
Nodes (5): execute_control_line(), execute_menu_choice(), select_station_ids(), DisconnectOutcome, ControlAndMonitoringTests

### Community 35 - "_handler_type"
Cohesion: 0.42
Nodes (8): _handler_type(), _authorize(), do_GET(), do_POST(), _idempotency_key(), _read_command(), _reply(), setup()

### Community 36 - "First Railway deployment runbook"
Cohesion: 0.06
Nodes (30): ADR-001: WM_Central State Model and SQLite Schema, Assignment minimum: Railway and two lab PCs, Docker deployment, Endpoint variables, Local Docker mode, 1. Create the project and Kafka service, 2. Initialize the four topics, 3. Create Central from the application Dockerfile (+22 more)

### Community 38 - "parse_args"
Cohesion: 0.32
Nodes (3): build_parser(), CentralConfig, parse_args()

### Community 43 - "WM_Central/main.py"
Cohesion: 0.12
Nodes (10): configure_logging(), main(), _outbox_loop(), _poll_worker(), control_loop(), log_station_table(), monitor_loop(), show_control_menu() (+2 more)

### Community 44 - "admin_api.py"
Cohesion: 0.15
Nodes (3): AdminConflict, _execute_command(), format_status_response()

### Community 45 - "OneMessage"
Cohesion: 0.12
Nodes (6): Automated Field Operator request file, FO request and response interface, Local development, WaterManagement Kafka message contract, OneMessage, OneMessageConsumer

### Community 48 - "test_wm_ws_integration.py"
Cohesion: 0.06
Nodes (16): BuilderTests, RequestFileTests, wait_until(), _print(), show_event(), load_requests(), _validate(), build_parser() (+8 more)

### Community 54 - ".register_station"
Cohesion: 0.32
Nodes (3): _station(), RegistrationResult, Station

## Knowledge Gaps
- **58 isolated node(s):** `KAFKA_SSL_CLIENT_AUTH`, `KAFKA_SSL_KEYSTORE_LOCATION`, `KAFKA_SSL_KEYSTORE_TYPE`, `KAFKA_SSL_KEYSTORE_PASSWORD`, `KAFKA_SSL_KEY_PASSWORD` (+53 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 367 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **41 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `StateManager` connect `StateManager` to `StateManagerTests`, `.call`, `database.py`, `CentralSocketServer`, `Database`, `WM_Central/main.py`, `envelope`, `CentralAdminServer`, `test_wm_ws_integration.py`, `WateringStationEngine`, `.block_station`, `.snapshot`, `test_admin_api.py`, `ControlAndMonitoringTests`, `CentralAdminApiTests`?**
  _High betweenness centrality (0.109) - this node is a cross-community bridge._
- **Are the 15 inferred relationships involving `Database` (e.g. with `RealKafkaLifecycleTests` and `WMWSIntegrationTests`) actually correct?**
  _`Database` has 15 INFERRED edges - model-reasoned connections that need verification._
- **What connects `KAFKA_SSL_CLIENT_AUTH`, `KAFKA_SSL_KEYSTORE_LOCATION`, `KAFKA_SSL_KEYSTORE_TYPE` to the rest of the system?**
  _58 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `IrrigationController` be split into smaller, more focused modules?**
  _Cohesion score 0.08571428571428572 - nodes in this community are weakly interconnected._
- **Why does `Database` connect `Database` to `_irrigation`, `database.py`, `WM_Central/main.py`, `envelope`, `test_wm_ws_integration.py`, `WateringStationEngine`, `StateManager`, `.register_station`, `.load_seed_file`, `SchemaTests`?**
  _High betweenness centrality (0.085) - this node is a cross-community bridge._
- **Are the 21 inferred relationships involving `StateManager` (e.g. with `RealKafkaLifecycleTests` and `WMWSIntegrationTests`) actually correct?**
  _`StateManager` has 21 INFERRED edges - model-reasoned connections that need verification._
- **Should `protocol.py` be split into smaller, more focused modules?**
  _Cohesion score 0.0962566844919786 - nodes in this community are weakly interconnected._
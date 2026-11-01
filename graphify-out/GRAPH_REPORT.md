# Graph Report - UA_SD  (2026-10-31)

## Corpus Check
- 83 files · ~46,989 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 9 file(s) not represented in the graph (top: (none) 3, .example 2, .properties 2)

## Summary
- 1180 nodes · 3176 edges · 73 communities (36 shown, 37 thin omitted)
- Extraction: 85% EXTRACTED · 15% INFERRED · 0% AMBIGUOUS · INFERRED: 485 edges (avg confidence: 0.88)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `a755320b`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- ._all
- envelope
- StateManagerTests
- CentralSocketServer
- protocol.py
- database.py
- FieldOperator
- Database
- CentralClient
- Project handoff: WaterManagement
- IrrigationTests
- KafkaTopicInitTests
- CentralKafkaService
- WateringStationMonitor
- FieldOperatorInteractiveTests
- kafka-railway-listeners.sh
- SocketTests
- WateringStationEngine
- Effective Status Function
- VerifyKafkaCertificate.java
- StateManager
- StationTests
- Graphify Workflow
- test_admin_api.py
- DatabaseTestCase
- ControlAndMonitoringTests
- CentralAdminApiTests
- FramingTests
- ProtocolTests
- FakeConsumer
- _handler_type
- report.md
- KafkaWorker
- argparse
- KafkaPublisher
- test_engine_kafka_service.py
- IrrigationController
- .test_authenticated_tls_publish_consume_and_invalid_credentials
- WM_Central.py
- admin_api.py
- OneMessage
- CentralAdminServer
- kafka_transport.py
- test_wm_ws_integration.py
- MessageFactoryTests
- kafka-init-topics.sh
- .block_station
- implement_field_operator.md
- implement_kafka_integration.md
- .register_station
- WM_FO.py
- .snapshot
- EngineServer
- RealKafkaPartitioningTests
- WaterManagement Release 1 — implementation and verification report
- setup-railway-local.sh
- utc_now
- First Railway deployment runbook
- load_requests
- FakeCentralClient
- _irrigation
- WaterManagement Kafka message contract
- Docker deployment
- AI prompt record

## God Nodes (most connected - your core abstractions)
1. `Database` - 68 edges
2. `envelope()` - 66 edges
3. `StateManager` - 61 edges
4. `WateringStationEngine` - 54 edges
5. `IrrigationController` - 52 edges
6. `FlowMeter` - 48 edges
7. `EngineKafkaService` - 48 edges
8. `CentralKafkaService` - 45 edges
9. `SolenoidValve` - 45 edges
10. `FieldOperator` - 34 edges

## Surprising Connections (you probably didn't know these)
- `main()` --uses--> `KafkaUnavailable`  [INFERRED]
  WM_Central/WM_Central.py → common/kafka_transport.py
- `RealKafkaLifecycleTests` --uses--> `Database`  [INFERRED]
  tests/test_kafka_real_integration.py → WM_Central/database.py
- `WMWSIntegrationTests` --uses--> `Database`  [INFERRED]
  tests/test_wm_ws_integration.py → WM_Central/database.py
- `RealKafkaLifecycleTests` --uses--> `CentralKafkaService`  [INFERRED]
  tests/test_kafka_real_integration.py → WM_Central/kafka_service.py
- `CentralKafkaService` --uses--> `KafkaPublisher`  [INFERRED]
  WM_Central/kafka_service.py → common/kafka_transport.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **WM_Central State Model** — docs_adr_adr_001_state_model_and_schema_durable_dimensions, docs_adr_adr_001_state_model_and_schema_effective_status, docs_adr_adr_001_state_model_and_schema_status_precedence, docs_adr_adr_001_state_model_and_schema_state_manager [EXTRACTED 1.00]
- **SQLite Data Integrity Model** — docs_adr_adr_001_state_model_and_schema_database_invariants, docs_adr_adr_001_state_model_and_schema_request_audit, docs_adr_adr_001_state_model_and_schema_end_reason_model [INFERRED 0.85]

## Communities (73 total, 37 thin omitted)

### Community 0 - "._all"
Cohesion: 0.21
Nodes (6): _fault(), _operator(), _station(), Fault, Operator, Station

### Community 1 - "envelope"
Cohesion: 0.18
Nodes (6): envelope(), utc_timestamp(), EngineKafkaTests, FakePublisher, EngineKafkaService, SolenoidValve

### Community 3 - "CentralSocketServer"
Cohesion: 0.07
Nodes (12): wait_until(), CentralSocketServer, _enable_keepalive(), MonitorConnection, wait_until(), BadInputTests, DisconnectTests, FaultTests (+4 more)

### Community 4 - "protocol.py"
Cohesion: 0.10
Nodes (14): calculate_lrc(), ConnectionClosedError, create_frame(), fault_resolved_message(), health_check_message(), InvalidLRCError, MalformedFrameError, parse_frame() (+6 more)

### Community 5 - "database.py"
Cohesion: 0.18
Nodes (7): AdminState, DenyReason, EndReason, IrrigationStatus, StationStatus, effective_status(), EffectiveStatusTests

### Community 6 - "FieldOperator"
Cohesion: 0.17
Nodes (6): encode(), FieldOperatorKafkaTests, worker_factory(), publish(), Publisher, FieldOperator

### Community 8 - "CentralClient"
Cohesion: 0.14
Nodes (3): CentralClientTests, server(), CentralClient

### Community 9 - "Project handoff: WaterManagement"
Cohesion: 0.29
Nodes (6): Behavioral constraints, Core architecture, Current implementation boundaries, Project handoff: WaterManagement, Suggested first pass for a new Codex, Tests

### Community 12 - "CentralKafkaService"
Cohesion: 0.11
Nodes (8): decode(), message_key(), MessageValidationError, validate(), KafkaContractTests, CentralKafkaService, LatestTelemetry, CentralKafkaTests

### Community 13 - "WateringStationMonitor"
Cohesion: 0.13
Nodes (4): MonitorTests, utc_timestamp(), WateringStationMonitor, main()

### Community 14 - "FieldOperatorInteractiveTests"
Cohesion: 0.10
Nodes (8): FieldOperatorInteractiveTests, start(), close_worker(), enter_q(), poll_once(), start(), start(), start()

### Community 15 - "kafka-railway-listeners.sh"
Cohesion: 0.09
Nodes (25): fail(), KAFKA_OPTS, KAFKA_SSL_CLIENT_AUTH, KAFKA_SSL_KEY_PASSWORD, KAFKA_SSL_KEYSTORE_LOCATION, KAFKA_SSL_KEYSTORE_PASSWORD, KAFKA_SSL_KEYSTORE_TYPE, KAFKA_SSL_TRUSTSTORE_LOCATION (+17 more)

### Community 17 - "WateringStationEngine"
Cohesion: 0.12
Nodes (5): RealKafkaLifecycleTests, engine_config(), EngineTests, monitor(), WateringStationEngine

### Community 18 - "Effective Status Function"
Cohesion: 0.29
Nodes (5): Effective Status Function, Fault Resolution Message, Single Station Status Column, State Manager, Displayed Status Precedence

### Community 20 - "StateManager"
Cohesion: 0.10
Nodes (5): RegisterOutcome, StateManager, Publisher, FakePublisher, test_restores_latest_persisted_volume_per_station()

### Community 28 - "test_admin_api.py"
Cohesion: 0.15
Nodes (7): format_status_response(), AdminClientError, CentralAdminClient, _is_loopback(), main(), _menu_command(), run_menu()

### Community 29 - "DatabaseTestCase"
Cohesion: 0.10
Nodes (5): DatabaseTestCase, FakeClock, FaultTests, OperatorTests, SchemaTests

### Community 30 - "ControlAndMonitoringTests"
Cohesion: 0.15
Nodes (5): execute_control_line(), execute_menu_choice(), select_station_ids(), DisconnectOutcome, ControlAndMonitoringTests

### Community 35 - "_handler_type"
Cohesion: 0.31
Nodes (8): _handler_type(), _authorize(), do_GET(), do_POST(), _idempotency_key(), _read_command(), _reply(), setup()

### Community 36 - "report.md"
Cohesion: 0.25
Nodes (4): ADR-001: WM_Central State Model and SQLite Schema, AI development record — concise summary, not a verbatim transcript, Connect a new lab PC to Railway Kafka, WaterManagement Distributed System

### Community 37 - "KafkaWorker"
Cohesion: 0.15
Nodes (4): KafkaWorker, TopicPartition, _print(), show_event()

### Community 38 - "argparse"
Cohesion: 0.19
Nodes (6): build_parser(), CentralConfig, parse_args(), build_parser(), FieldOperatorConfig, parse_args()

### Community 39 - "KafkaPublisher"
Cohesion: 0.14
Nodes (5): kafka_security_config(), KafkaPublisher, KafkaUnavailable, FakeProducer, KafkaWorkerTests

### Community 40 - "test_engine_kafka_service.py"
Cohesion: 0.13
Nodes (11): build_parser(), EngineConfig, parse_args(), FlowReading, IrrigationState, configure_logging(), main(), _poll_worker() (+3 more)

### Community 41 - "IrrigationController"
Cohesion: 0.18
Nodes (6): IrrigationTests, FlowMeter, IrrigationController, IrrigationRequest, IrrigationResult, IrrigationTelemetry

### Community 43 - "WM_Central.py"
Cohesion: 0.13
Nodes (10): control_loop(), log_station_table(), monitor_loop(), show_control_menu(), start_control_menu(), _stdin_is_interactive(), configure_logging(), main() (+2 more)

### Community 46 - "CentralAdminServer"
Cohesion: 0.20
Nodes (3): CentralAdminServer, _execute_command(), FakePublisher

### Community 48 - "test_wm_ws_integration.py"
Cohesion: 0.15
Nodes (6): BuilderTests, _protocol_reason(), build_parser(), MonitorConfig, parse_args(), configure_logging()

### Community 55 - "WM_FO.py"
Cohesion: 0.19
Nodes (7): _file_mode(), _interactive_mode(), _is_quit(), main(), _poll_worker(), _stop_poll_worker(), _submit_and_wait()

### Community 59 - "EngineServer"
Cohesion: 0.23
Nodes (3): WMWSIntegrationTests, EngineServerTests, EngineServer

### Community 61 - "WaterManagement Release 1 — implementation and verification report"
Cohesion: 0.15
Nodes (13): 1. Problem and requirements, 2. Component responsibilities and topology, 3. Irrigation message flow, 4. Station state, controls, and persistence, 5. Reliability and recovery behavior, 6. Deployment and verification, 7. Final verification matrix, 8. Remaining limitations and evidence gaps (+5 more)

### Community 65 - "First Railway deployment runbook"
Cohesion: 0.18
Nodes (11): 1. Create the project and Kafka service, 2. Initialize the four topics, 3. Create Central from the application Dockerfile, 4. Run lab clients, 5. Validate, observe, and recover, Deployment limits, First Railway deployment runbook, Operator PC: remote Central menu (+3 more)

### Community 66 - "load_requests"
Cohesion: 0.27
Nodes (3): RequestFileTests, load_requests(), _validate()

### Community 70 - "WaterManagement Kafka message contract"
Cohesion: 0.40
Nodes (4): Automated Field Operator request file, FO request and response interface, Local development, WaterManagement Kafka message contract

### Community 71 - "Docker deployment"
Cohesion: 0.50
Nodes (4): Assignment minimum: Railway and two lab PCs, Docker deployment, Endpoint variables, Local Docker mode

### Community 72 - "AI prompt record"
Cohesion: 0.67
Nodes (3): AI prompt record, Existing prompt record, Main prompt themes

## Knowledge Gaps
- **61 isolated node(s):** `KAFKA_SSL_CLIENT_AUTH`, `KAFKA_SSL_KEYSTORE_LOCATION`, `KAFKA_SSL_KEYSTORE_TYPE`, `KAFKA_SSL_KEYSTORE_PASSWORD`, `KAFKA_SSL_KEY_PASSWORD` (+56 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 373 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **37 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `StateManager` connect `StateManager` to `StateManagerTests`, `CentralSocketServer`, `database.py`, `Database`, `WM_Central.py`, `CentralKafkaService`, `CentralAdminServer`, `test_wm_ws_integration.py`, `WateringStationEngine`, `.block_station`, `.snapshot`, `EngineServer`, `test_admin_api.py`, `ControlAndMonitoringTests`, `CentralAdminApiTests`?**
  _High betweenness centrality (0.139) - this node is a cross-community bridge._
- **Are the 15 inferred relationships involving `Database` (e.g. with `RealKafkaLifecycleTests` and `WMWSIntegrationTests`) actually correct?**
  _`Database` has 15 INFERRED edges - model-reasoned connections that need verification._
- **What connects `KAFKA_SSL_CLIENT_AUTH`, `KAFKA_SSL_KEYSTORE_LOCATION`, `KAFKA_SSL_KEYSTORE_TYPE` to the rest of the system?**
  _61 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `CentralSocketServer` be split into smaller, more focused modules?**
  _Cohesion score 0.0687719298245614 - nodes in this community are weakly interconnected._
- **Why does `Database` connect `Database` to `._all`, `database.py`, `_irrigation`, `WM_Central.py`, `CentralKafkaService`, `test_wm_ws_integration.py`, `WateringStationEngine`, `StateManager`, `.register_station`, `.load_seed_file`, `EngineServer`, `DatabaseTestCase`?**
  _High betweenness centrality (0.088) - this node is a cross-community bridge._
- **Are the 21 inferred relationships involving `StateManager` (e.g. with `RealKafkaLifecycleTests` and `WMWSIntegrationTests`) actually correct?**
  _`StateManager` has 21 INFERRED edges - model-reasoned connections that need verification._
- **Should `protocol.py` be split into smaller, more focused modules?**
  _Cohesion score 0.10037878787878787 - nodes in this community are weakly interconnected._
# Graph Report - UA_SD  (2026-11-01)

## Corpus Check
- 79 files · ~40,675 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 9 file(s) not represented in the graph (top: (none) 3, .example 2, .properties 2)

## Summary
- 1178 nodes · 3172 edges · 60 communities (28 shown, 32 thin omitted)
- Extraction: 85% EXTRACTED · 15% INFERRED · 0% AMBIGUOUS · INFERRED: 478 edges (avg confidence: 0.88)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `7ba70945`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- WaterManagement
- EngineKafkaService
- StateManagerTests
- CentralSocketServer
- protocol.py
- database.py
- FieldOperator
- Database
- CentralClient
- WateringStationMonitor
- IrrigationTests
- KafkaTopicInitTests
- envelope
- MonitorTests
- Detailed Implementation Prompt — WM_WS_M
- kafka-railway-listeners.sh
- SocketTests
- WateringStationEngine
- KafkaPublisher
- VerifyKafkaCertificate.java
- test_state_manager_persistence.py
- project_context_and_monitor_impl.md
- StationTests
- Graphify Workflow
- test_admin_api.py
- SchemaTests
- Task: Connect WM_Central and WM_FO Through Kafka
- CentralAdminApiTests
- FramingTests
- ProtocolTests
- FakeConsumer
- Task: Implement the Field Operator (WM_FO)
- prompts/README.md
- KafkaWorker
- kafka_security_config
- test_engine_kafka_service.py
- IrrigationController
- .test_authenticated_tls_publish_consume_and_invalid_credentials
- StateManager
- admin_api.py
- OneMessage
- Task: Complete the Kafka Irrigation Lifecycle
- kafka_transport.py
- test_wm_ws_integration.py
- MessageFactoryTests
- kafka-init-topics.sh
- KafkaUnavailable
- FaultTests
- kafka_client.py
- EngineServer
- setup-railway-local.sh
- load_requests
- FakeCentralClient

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

## Communities (60 total, 32 thin omitted)

### Community 0 - "WaterManagement"
Cohesion: 0.15
Nodes (12): Configuration and implementation entry points, Functional checks, One-time setup on each PC, Repository layout, Requirements, Run Dockerized FO and WS instances against Railway, Run the complete system locally with Docker, Start a Field Operator (+4 more)

### Community 1 - "EngineKafkaService"
Cohesion: 0.18
Nodes (4): EngineKafkaTests, FakePublisher, EngineKafkaService, SolenoidValve

### Community 2 - "StateManagerTests"
Cohesion: 0.11
Nodes (4): CommandOutcome, DisconnectOutcome, FaultOutcome, StateManagerTests

### Community 3 - "CentralSocketServer"
Cohesion: 0.08
Nodes (11): CentralSocketServer, _enable_keepalive(), MonitorConnection, wait_until(), BadInputTests, DisconnectTests, FaultTests, ManyStationsTests (+3 more)

### Community 4 - "protocol.py"
Cohesion: 0.10
Nodes (15): calculate_lrc(), ConnectionClosedError, create_frame(), fault_resolved_message(), health_check_message(), InvalidLRCError, MalformedFrameError, parse_frame() (+7 more)

### Community 5 - "database.py"
Cohesion: 0.14
Nodes (10): utc_timestamp(), AdminState, DenyReason, EndReason, IrrigationStatus, StationStatus, effective_status(), DatabaseTestCase (+2 more)

### Community 6 - "FieldOperator"
Cohesion: 0.05
Nodes (25): encode(), FieldOperatorKafkaTests, worker_factory(), publish(), Publisher, FieldOperatorInteractiveTests, start(), close_worker() (+17 more)

### Community 7 - "Database"
Cohesion: 0.05
Nodes (15): RealKafkaLifecycleTests, Database, _fault(), _irrigation(), _operator(), _station(), utc_now(), Fault (+7 more)

### Community 8 - "CentralClient"
Cohesion: 0.15
Nodes (3): CentralClientTests, server(), CentralClient

### Community 9 - "WateringStationMonitor"
Cohesion: 0.23
Nodes (4): utc_timestamp(), WateringStationMonitor, configure_logging(), main()

### Community 12 - "envelope"
Cohesion: 0.09
Nodes (13): decode(), envelope(), message_key(), MessageValidationError, validate(), KafkaContractTests, CentralKafkaService, LatestTelemetry (+5 more)

### Community 14 - "Detailed Implementation Prompt — WM_WS_M"
Cohesion: 0.17
Nodes (12): Approved message contract, Central client, Concurrency, logging, and errors, Configuration, Detailed Implementation Prompt — WM_WS_M, Engine server and health monitoring, Framing and JSON messages, Inspect before editing (+4 more)

### Community 15 - "kafka-railway-listeners.sh"
Cohesion: 0.09
Nodes (25): fail(), KAFKA_OPTS, KAFKA_SSL_CLIENT_AUTH, KAFKA_SSL_KEY_PASSWORD, KAFKA_SSL_KEYSTORE_LOCATION, KAFKA_SSL_KEYSTORE_PASSWORD, KAFKA_SSL_KEYSTORE_TYPE, KAFKA_SSL_TRUSTSTORE_LOCATION (+17 more)

### Community 17 - "WateringStationEngine"
Cohesion: 0.14
Nodes (4): engine_config(), EngineTests, monitor(), WateringStationEngine

### Community 21 - "project_context_and_monitor_impl.md"
Cohesion: 0.18
Nodes (10): Assignment requirements for WM_WS_M, Communication boundaries, Completion report, Design and code quality, Project behavior for integration context, Project overview, Protocol and interface discipline, Repository-first workflow (+2 more)

### Community 28 - "test_admin_api.py"
Cohesion: 0.12
Nodes (8): format_status_response(), AdminClientError, CentralAdminClient, _is_loopback(), main(), _menu_command(), run_menu(), build_parser()

### Community 30 - "Task: Connect WM_Central and WM_FO Through Kafka"
Cohesion: 0.25
Nodes (8): Completion report, Operator responses, Reliability and configuration, Request flow, Review the relevant context, Scope and boundaries, Task: Connect WM_Central and WM_FO Through Kafka, Tests and documentation

### Community 31 - "CentralAdminApiTests"
Cohesion: 0.06
Nodes (15): _BoundedHTTPServer, CentralAdminServer, _handler_type(), _authorize(), do_GET(), do_POST(), _idempotency_key(), _read_command() (+7 more)

### Community 35 - "Task: Implement the Field Operator (WM_FO)"
Cohesion: 0.29
Nodes (7): Implementation guidance, Kafka and configuration, Required behavior, Review the project first, Scope, Task: Implement the Field Operator (WM_FO), Tests and completion

### Community 36 - "prompts/README.md"
Cohesion: 0.22
Nodes (4): How these prompts were prepared, Prompt files, Suggested reading order, WaterManagement Development Prompts

### Community 39 - "kafka_security_config"
Cohesion: 0.24
Nodes (3): kafka_security_config(), RealKafkaPartitioningTests, KafkaWorkerTests

### Community 40 - "test_engine_kafka_service.py"
Cohesion: 0.15
Nodes (10): build_parser(), EngineConfig, parse_args(), IrrigationState, configure_logging(), main(), _poll_worker(), start_command_thread() (+2 more)

### Community 41 - "IrrigationController"
Cohesion: 0.18
Nodes (7): IrrigationTests, FlowMeter, FlowReading, IrrigationController, IrrigationRequest, IrrigationResult, IrrigationTelemetry

### Community 43 - "StateManager"
Cohesion: 0.09
Nodes (14): StationView, control_loop(), execute_control_line(), execute_menu_choice(), format_station_table(), log_station_table(), monitor_loop(), select_station_ids() (+6 more)

### Community 46 - "Task: Complete the Kafka Irrigation Lifecycle"
Cohesion: 0.29
Nodes (7): Before coding, Central integration, Configuration, tests, and delivery, Engine integration and reliability, Scope and architecture, Task: Complete the Kafka Irrigation Lifecycle, Topics and message contract

### Community 48 - "test_wm_ws_integration.py"
Cohesion: 0.16
Nodes (4): BuilderTests, build_parser(), MonitorConfig, parse_args()

### Community 59 - "EngineServer"
Cohesion: 0.24
Nodes (3): WMWSIntegrationTests, EngineServerTests, EngineServer

### Community 66 - "load_requests"
Cohesion: 0.27
Nodes (3): RequestFileTests, load_requests(), _validate()

## Knowledge Gaps
- **73 isolated node(s):** `KAFKA_SSL_CLIENT_AUTH`, `KAFKA_SSL_KEYSTORE_LOCATION`, `KAFKA_SSL_KEYSTORE_TYPE`, `KAFKA_SSL_KEYSTORE_PASSWORD`, `KAFKA_SSL_KEY_PASSWORD` (+68 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 382 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **32 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `StateManager` connect `StateManager` to `StateManagerTests`, `CentralSocketServer`, `database.py`, `Database`, `envelope`, `test_wm_ws_integration.py`, `test_state_manager_persistence.py`, `EngineServer`, `test_admin_api.py`, `CentralAdminApiTests`?**
  _High betweenness centrality (0.112) - this node is a cross-community bridge._
- **Are the 15 inferred relationships involving `Database` (e.g. with `RealKafkaLifecycleTests` and `WMWSIntegrationTests`) actually correct?**
  _`Database` has 15 INFERRED edges - model-reasoned connections that need verification._
- **What connects `KAFKA_SSL_CLIENT_AUTH`, `KAFKA_SSL_KEYSTORE_LOCATION`, `KAFKA_SSL_KEYSTORE_TYPE` to the rest of the system?**
  _73 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `StateManagerTests` be split into smaller, more focused modules?**
  _Cohesion score 0.11153846153846154 - nodes in this community are weakly interconnected._
- **Why does `Database` connect `Database` to `database.py`, `StateManager`, `envelope`, `test_wm_ws_integration.py`, `EngineServer`, `SchemaTests`?**
  _High betweenness centrality (0.085) - this node is a cross-community bridge._
- **Are the 21 inferred relationships involving `StateManager` (e.g. with `RealKafkaLifecycleTests` and `WMWSIntegrationTests`) actually correct?**
  _`StateManager` has 21 INFERRED edges - model-reasoned connections that need verification._
- **Should `CentralSocketServer` be split into smaller, more focused modules?**
  _Cohesion score 0.07511737089201878 - nodes in this community are weakly interconnected._
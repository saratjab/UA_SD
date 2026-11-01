# Task: Complete the Kafka Irrigation Lifecycle
Work in the existing Python WaterManagement project for Distributed Systems, Release 1.
Implement the Kafka flow between WM_Central and the Watering Station Engine (WM_WS_E).
Keep the existing socket communication among Central, WM_WS_M, and WM_WS_E working.
This is an integration task; inspect and extend the current code instead of replacing it.
Treat the official assignment PDF as authoritative and distinguish its requirements from team choices.
Use the project plan as the shared design unless existing coordinated code requires a change.

## Before coding
Check the current branch, working tree, staged changes, and repository instructions.
Work only on feature/kafka-irrigation-lifecycle.
If the branch is missing, create it from the integrated main only when doing so is safe.
If switching could affect uncommitted work, stop and report the current state.
Do not stash, reset, clean, overwrite, commit, merge, or push changes.
Read the project plan, assignment PDF, state-model ADR, and relevant Central, Monitor, Engine, database, and test code.
Adjust paths to the actual repository and inspect implementations, not filenames alone.
Summarize the architecture, public interfaces, persistence model, and implementation plan.
After the review, continue with implementation.

## Scope and architecture
Integrate Kafka request handling and command/event flows in Central and WM_WS_E.
Add shared Kafka configuration, message validation, tests, and documentation where needed.
Add local Docker Compose Kafka setup only if the repository lacks one.
Do not implement WM_FO, replace SQLite, redesign the system, or deploy to Railway.
Central must expose the agreed Kafka contract so the teammate building FO can integrate.
Preserve sockets for WM_WS_M ↔ Central registration/status/faults and WM_WS_M ↔ Engine health checks.
Keep Monitor-to-Engine health checks at one-second intervals.
Keep the Engine responsive to Monitor health checks during watering and Kafka activity.
Use Kafka for asynchronous activation requests, commands, station events, telemetry, and operator events.
Do not swap Kafka and socket responsibilities.
Use the existing threaded design where practical; do not add an async framework without need.

## Topics and message contract
Use these team topics without renaming them:
- wm.activation.requests: FO produces; Central consumes.
- wm.ws.commands: Central produces; Engine consumes.
- wm.ws.events: Engine produces; Central consumes.
- wm.operator.events: Central produces; FO consumes.
Key station commands/events by ws_id and operator notifications by operator_id where applicable.
Use separate Central groups for activation requests and WS events.
Use a stable station-specific Engine group and a stable operator-specific FO group; document routing and partition behavior.
Use JSON values with event_type, message_id, UTC ISO-8601 timestamp, applicable ws_id/operator_id/request_id, and payload.
Validate message shape and field types; malformed messages must not crash consumers.
Preserve the Engine measurement names flow_rate_lpm, accumulated_volume_liters, and duration_seconds.
Do not rename existing Python model fields unnecessarily.

An IRRIGATION_REQUEST has message_id, request_id, operator_id, ws_id, timestamp, and payload.duration_seconds.
Central validates the request and publishes one station-targeted START_IRRIGATION only after authorization.
Preserve request_id through the full lifecycle and include applicable station/operator IDs.
The Engine publishes IRRIGATION_STARTED only after the valve opens and watering begins.
While watering, publish IRRIGATION_TELEMETRY about once per second with IDs, sequence, timestamp, and the existing measurement fields.
Use current flow-meter/controller values; accumulated_volume_liters is cumulative for that irrigation.
On completion, stop the controller, close the valve, finalize values, and publish one final event.
Cover time limit, manual stop, Central stop/block, and fault interruption; include final duration, volume, and a consistent end_reason.
Central publishes correlated authorization/denial, progress, and final result events for FO to consume.
Document concrete FO request and response examples; do not implement FO.

## Central integration
Use the existing Central services, StateManager, SQLite schema, and effective-state precedence.
Validate operator authorization, station registration/availability, identifiers, and duration.
Reject disconnected, watering, faulty/LEAK, blocked/OUT OF SERVICE, or otherwise ineligible stations.
Deduplicate by request_id and prevent concurrent irrigation starts at one station.
Record requests through existing database methods where possible; make only necessary schema changes.
Do not mark WATERING until the Engine confirms IRRIGATION_STARTED.
Update telemetry safely and persist final results without adding cumulative samples together.
Do not let late events override a current fault or Central block.
Handle command-publication failure without assuming the Engine ran it.
Integrate START_IRRIGATION, BLOCK_WS, and ACTIVATE_WS; target stations individually.
Blocking must stop active watering; activation is allowed only when the station is healthy and eligible.

## Engine integration and reliability
Integrate Kafka in a focused Engine service and reuse the existing irrigation interfaces.
Validate/filter commands by ws_id; reject invalid commands and prevent duplicate starts.
Keep local controls compatible and publish their final results.
Keep health replies and local safety actions independent of Kafka availability.
A fault must stop watering and close the valve immediately, even if event publishing fails.
Avoid duplicate fault transitions when both the Monitor socket and Kafka report the same incident.
Log broker and delivery failures; close Kafka clients cleanly at shutdown.
Assume records can be duplicated; use stable IDs to deduplicate requests, commands, telemetry, and results.
Ignore stale samples and prevent an old completion from ending a newer irrigation.
Choose and document offset commits for state-changing records; explain remaining crash windows.
Use persistent deduplication when needed across restarts, reusing SQLite where practical.
Configure producer acknowledgments and make failures visible.
Use locks for shared state but do not hold them during network I/O.
Avoid duplicate consumer loops during recovery.

## Configuration, tests, and delivery
Inspect existing CLI/configuration first; preserve required arguments and add compatible Kafka settings.
Support configurable bootstrap servers and one Kafka client library; do not hard-code localhost in business logic.
Add an example environment file without secrets if useful.
Use pinned broker configuration in Docker Compose only if local Kafka setup is missing.
Preserve existing tests and add focused Central, Engine, contract, and lifecycle coverage.
Test authorization/denial, duplicates, station isolation, telemetry, completion, stop/block, faults, valve closure, and health replies during Kafka work.
Add a real-broker integration test; clearly distinguish it from mocked tests.
Run relevant tests and report actual results, including tests blocked by unavailable Kafka.
Document topics, groups, keys, schemas, lifecycle, idempotency, errors, local broker setup, and test commands in docs/kafka-message-contract.md.
Store the prompt under prompts/ as required by the assignment.
Do not commit, merge, push, or change another branch.
Report existing architecture, changed files, implemented flow, tests, broker status, limitations, local run commands, and FO integration requirements.
Do not claim unverified items are complete; keep the result understandable for the lab defense.
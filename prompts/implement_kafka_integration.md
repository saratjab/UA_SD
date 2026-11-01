# Task: Connect WM_Central and WM_FO Through Kafka

The WM_FO application is implemented; the next feature is its Kafka integration with WM_Central.
Work in the existing Python WaterManagement project and follow its current architecture.
Focus on the request and response flow between the operator application and Central.

## Review the relevant context

Check the current branch, working tree, and repository instructions before editing.
Review the project plan, timeline, PROJECT_CONTEXT.md, Git history, or code graph as needed.
You do not need to reread every project document; use the material relevant to this integration.
Verify important decisions in the actual source code and current Kafka contract.
Inspect WM_FO's request publisher and event consumer, Central's Kafka service, authorization path, SQLite/state handling, configuration, and related tests.
Summarize the existing interfaces and the small set of files you plan to change.
Then implement the integration; do not stop at a plan.

## Scope and boundaries

Implement the Kafka request and operator-notification flow between WM_FO and WM_Central.
Keep the existing project topics, event names, envelope, field names, and Kafka client library.
Do not invent a second contract or change shared message names without identifying a real incompatibility.
Reuse the existing Central business logic, SQLite database, and StateManager.
Keep the existing Central-to-Engine and Engine-to-Central Kafka flow working.
Do not implement or rewrite WM_WS_M, WM_WS_E, or unrelated deployment features.
Make only focused changes to WM_FO or Central where the existing integration requires them.

## Request flow

Use the agreed activation-request topic, wm.activation.requests.
Central must consume WM_FO's IRRIGATION_REQUEST messages using the established envelope and fields.
Validate message IDs, request IDs, operator ID, target WS ID, timestamp, and requested duration.
Check the operator against the existing database and authorization rules.
Check station registration, connectivity, effective state, and availability using existing Central logic.
Reject requests for invalid operators, unknown or unavailable stations, or invalid durations.
Prevent duplicate requests and concurrent requests from starting duplicate irrigation.
Record accepted or rejected requests using existing persistence methods where appropriate.
For an authorized request, continue through the existing Central-to-Engine command path.
Do not report that watering has started until the Engine's start-confirmation event is received.

## Operator responses

Use the agreed operator-event topic, wm.operator.events.
Publish correlated authorization or denial decisions, progress, and final results for WM_FO.
Include the request ID, operator ID, WS ID, event type, timestamp, and reason/result fields required by the shared contract.
Ensure each FO instance receives only notifications for its operator using the established consumer-group and filtering strategy.
Provide useful, accurate feedback; distinguish authorization from actual start and final completion.
Handle Engine rejection, command publication failure, station faults, and completion through the existing lifecycle.
Do not publish duplicate or contradictory results when Central receives repeated events.

## Reliability and configuration

Use the current Kafka transport and configurable bootstrap settings.
Keep the Central socket server responsive while Kafka messages are processed.
Validate incoming records so malformed messages do not crash Central.
Use the established offset-commit and idempotency approach for state-changing requests.
Handle duplicate delivery and restarts without authorizing the same request twice.
Log connection, validation, authorization, publication, and processing failures clearly.
Close Kafka workers and clients cleanly during shutdown.
Do not add a new framework, broker, database, or broad retry system.

## Tests and documentation

Preserve existing tests and use their conventions.
Add focused tests for request validation, authorization success and denial, unknown operator/station, unavailable station, duplicate requests, command publication, operator-event correlation, and final-result delivery.
Use mocks for unit tests and the existing local broker setup for integration coverage where available.
Clearly identify which tests require a running Kafka broker.
Update the Kafka contract documentation only if the implemented interface changes or examples are missing.
Document the exact request and response fields WM_FO and its developer must use.
Run relevant tests and report the actual results; do not claim checks that were not run.

## Completion report

List the files changed and explain their roles.
Describe the request-to-Central-to-operator-result flow that now works.
Give the commands to run Central and WM_FO and a basic request example.
Report tests run, broker requirements, assumptions, and any remaining integration decisions.
Do not commit, push, or merge automatically.
Keep the change focused and understandable to the students maintaining the project.

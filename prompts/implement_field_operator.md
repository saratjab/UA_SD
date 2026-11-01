# Task: Implement the Field Operator (WM_FO)

Work in the existing Python WaterManagement project for Distributed Systems, Release 1.
Implement the Field Operator application, named WM_FO, using the repository's current architecture.
This is an existing project; extend its code and follow the agreed interfaces.

## Review the project first

Before editing, check the current branch, working tree, and repository instructions.
Read the project plan, project timeline, PROJECT_CONTEXT.md, relevant Git history, and the available code graph.
Use the graph and Git history to understand the project, then verify details in the actual source files.
Inspect the current Kafka message contract, Central Kafka service, configuration, examples, and tests.
Identify existing WM_FO code and reuse its conventions where practical.
If project documents and implemented interfaces disagree, report the difference and follow the coordinated current contract unless it conflicts with the official assignment.
Before coding, briefly summarize the current state and the files you intend to change.
Then implement the feature; do not stop after producing a plan.

## Scope

Build only WM_FO and the integration points it needs to communicate with Central.
Do not implement or rewrite WM_Central, WM_WS_M, WM_WS_E, Kafka infrastructure, or unrelated features.
Use Kafka for operator requests and operator-specific notifications as defined by the existing project contract.
Keep topic names, event names, field names, and message envelopes consistent with the contract.
Do not invent a parallel protocol or change shared interfaces without explaining the conflict first.

## Required behavior

WM_FO must identify itself with the configured operator ID.
Support a one-off irrigation request for a selected watering station.
Support reading a sequence of irrigation requests from a file for automated runs.
Use the project's agreed file format and validation rules; if the format is not documented, inspect existing examples and state the smallest assumption.
Each request must include the agreed operator ID, target WS ID, requested duration, stable request ID, message ID, timestamp, and payload fields.
Publish requests to the agreed activation-request topic.
Use request IDs to match Central's authorization, progress, and final-result events to the correct request.
Consume only notifications intended for this operator.
Display authorization or denial, useful progress updates, and the final outcome clearly.
For file-driven requests, wait four seconds after each request finishes, whether it succeeds or fails, before sending the next request.
Do not send the next file request while the previous request is still pending.
Handle invalid input, unavailable Central/Kafka, malformed events, and interrupted execution with clear messages and safe cleanup.

## Kafka and configuration

Inspect existing configuration and use the project's current Kafka client library and transport helpers.
Accept the Kafka bootstrap address and port and the operator ID through the existing configuration style, or argparse/environment variables where appropriate.
Do not hard-code host addresses, ports, operator IDs, or topic names.
Use a stable operator-specific consumer group so one operator's application receives its own events.
Validate incoming and outgoing messages with the shared contract.
Use the shared message key and timestamp conventions.
Log useful connection and failure information without flooding the console.
Close Kafka producers, consumers, and worker threads cleanly on shutdown.

## Implementation guidance

Keep the code straightforward and easy to explain during the project defense.
Separate request creation, file parsing, Kafka communication, and display logic according to the repository's existing structure.
Avoid unnecessary dependencies, frameworks, and broad refactors.
Preserve unrelated changes in the working tree.
Do not commit, push, merge, or switch branches automatically.
If the current branch or uncommitted state makes the work unsafe, stop and explain before changing files.

## Tests and completion

Use the repository's existing test framework and conventions.
Add tests for request validation, file parsing, message construction, request correlation, operator-event filtering, and the four-second file sequence.
Test authorization/denial and final-result handling with mocks or the existing test setup.
Run relevant tests and report the actual results; do not claim unrun checks passed.
Update only the documentation needed to describe WM_FO usage and its Kafka contract.
When finished, report the files changed, how to run WM_FO, how to submit a one-off request, how to run file-driven requests, tests executed, assumptions, and any Central integration details that must remain aligned.
Keep the implementation focused on WM_FO and understandable to the student maintaining it.

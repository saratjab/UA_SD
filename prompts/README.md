# WaterManagement Development Prompts

This folder contains the development prompts prepared for the WaterManagement Distributed Systems project (Release 1).

## How these prompts were prepared

The prompts were designed using the WaterManagement project plan as the shared guide for scope, milestones, component responsibilities, and integration.
Their wording and organization were refined with GPT Prompt Enhance to make each task clearer and easier to follow.
The official assignment remains the authority for course requirements; the project plan records the team's design choices.
These files are focused task briefs, not complete chat transcripts, and they do not claim that every requested implementation or test has been completed.
Any implementation or test status should be verified in the source code, test results, and project documentation.

## Prompt files

| File | Purpose |
|---|---|
| [project_context_and_monitor_impl.md](project_context_and_monitor_impl.md) | Introduces the full WaterManagement system and defines the scope and integration context for WM_WS_M. |
| [implement_detailed_ws_m.md](implement_detailed_ws_m.md) | Gives the detailed implementation requirements for the Monitor, including its TCP protocol, registration, Engine handshake, health checks, fault reporting, and tests. |
| [implement_field_operator.md](implement_field_operator.md) | Defines the WM_FO application, its one-off and file-driven irrigation requests, Kafka notifications, and tests. |
| [implement_kafka_integration.md](implement_kafka_integration.md) | Defines the Kafka request and operator-response integration between WM_FO and WM_Central, without implementing FO again. |
| [kafka-irrigation-lifecycle.md](kafka-irrigation-lifecycle.md) | Defines the wider Central–Engine Kafka irrigation lifecycle, including commands, telemetry, completion, faults, reliability, and integration tests. |

## Suggested reading order

1. Start with project_context_and_monitor_impl.md for the overall system and Monitor context.
2. Read implement_detailed_ws_m.md for the Monitor-specific implementation contract.
3. Read implement_field_operator.md for the operator application's behavior.
4. Read implement_kafka_integration.md for the FO-to-Central request and response path.
5. Read kafka-irrigation-lifecycle.md for the complete Central-to-Engine irrigation lifecycle.

The prompts focus on different implementation stages and can share architectural context, but each file has a distinct task scope.
When a prompt, project-plan decision, and official assignment appear to conflict, verify the issue against the assignment and the current implementation before changing a shared interface.

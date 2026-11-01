WaterManagement — WM_WS_M Development Prompt
Project context and implementation brief
This prompt is enhanced using GPT Prompt Enhance. It is enahnced to fit the tone delivered to codex for implementation purposes, core of the prompt is designed by the team.

## Project overview

This repository contains a Python implementation of WaterManagement, a Distributed Systems course project (26/27, Release 1).
The application simulates a network of irrigation stations serving parks and gardens.
The system supports station registration, operator irrigation requests, centralized authorization, irrigation telemetry, and fault handling.
The required software components are WM_Central, WM_WS_M, WM_WS_E, and WM_FO.
The official assignment PDF is the final authority for course requirements.
The shared WaterManagement project plan provides the team's agreed technical context and design decisions.
Keep official requirements distinct from team design choices and recommendations.
Do not describe a team choice as an official assignment requirement.

## Whole-system roles

WM_Central is the system authority: it registers stations, validates requests, manages station states, receives events, and coordinates commands.
CENTRAL persists station/operator information and other required records in SQLite.
WM_WS_M is the station monitor: it authenticates/registers its station with CENTRAL and watches the health of its Engine.
WM_WS_E is the station engine: it handles irrigation operation, simulated flow/volume, valve behavior, and station events.
WM_FO is the field-operator application: it requests irrigation and displays authorization and result messages.
Kafka is the event-streaming subsystem used for asynchronous system events and commands.
Docker is required for deployment, and GitHub is required for source management and sharing the development prompts.
The assignment's minimum deployment places CENTRAL, SQLite, and Kafka in the cloud, with multiple operator apps and stations on lab PCs.
Do not implement the complete system as part of this task; this prompt requests only WM_WS_M.

## Communication boundaries

The architecture uses both direct sockets and Kafka, each for a different purpose.
WM_WS_M communicates with WM_Central over a TCP socket for station authentication/registration and fault/status communication.
WM_WS_M communicates with its WM_WS_E over a TCP socket for the direct health-check path.
WM_WS_E, WM_FO, and WM_Central use Kafka for the agreed asynchronous event and command flows.
Do not replace either Monitor socket path with Kafka.
Do not move the system's Kafka event flows onto the Monitor sockets.
The Engine's runtime keypress that makes it return KO belongs to WM_WS_E; WM_WS_M only detects and reports the result.

## Assignment requirements for WM_WS_M

Implement the application named WM_WS_M in Python.
At startup, accept the port of the socket server offered to WM_WS_E.
Also accept the IP address and socket port of WM_Central and the unique WS ID.
Use the repository's established command-line conventions; otherwise use argparse.
Do not hard-code deployment-specific addresses, ports, or station IDs.
Connect to WM_Central and authenticate/register the station using its WS ID.
Use the existing Central exchange to confirm the station is operational and ready.
Start a socket server on the configured port and wait for this station's Engine to connect.
Once connected, send a health-check message to WM_WS_E every one second.
Read and validate each response using the protocol shared with WM_WS_E.
Continue monitoring during irrigation; health checks must not depend on whether watering is active.
Treat a missing response, timeout, unexpected close, or KO response as an Engine fault.
On fault, notify WM_Central using the agreed socket message and identify the WS.
The assignment describes this notification as simulating leak detection.
Do not add physical leak-sensor behavior to WM_WS_M.
Remain operational while the system is running and follow the repository's agreed behavior after reporting a fault.

## Project behavior for integration context

CENTRAL remains running, loads registered stations from SQLite, and shows stations that are not connected as DISCONNECTED.
The station states used by the system are AVAILABLE, WATERING, LEAK, OUT OF SERVICE, and DISCONNECTED.
CENTRAL checks that a station is available before authorizing an operator's irrigation request.
After authorization, CENTRAL notifies the operator and station, and the Engine starts irrigation.
During irrigation, the Engine sends flow rate and accumulated volume information to CENTRAL every second through the agreed event stream.
Irrigation ends at its configured time limit or when stopped at the station; the system then reports a final summary.
A station fault during irrigation must cause irrigation to stop immediately and the incident to be reported to the relevant components.
CENTRAL can asynchronously request start irrigation, block a station, or activate a station.
These flows belong to the wider system; implement only Monitor-side behavior needed for integration.

## Protocol and interface discipline

The assignment permits students to define socket protocols and recommends STX/DATA/ETX/LRC framing with ACK/NACK.
The project plan records a team design using this framing and JSON message data, with agreed message names and fields.
Treat these framing and message details as team conventions, not as requirements imposed by the assignment.
Inspect the repository and the current project plan before changing or adding protocol code.
Reuse existing framing, checksum, message names, schemas, and socket helpers where available.
Do not independently rename operations, change field meanings, reverse socket directions, or introduce a second protocol.
If the repository and project plan disagree, identify the mismatch and make the smallest compatible change.
If a necessary interface is missing, state the unresolved detail and coordinate it with the teammate/instructor rather than silently inventing a convention.
Keep socket transport, protocol encoding/decoding, and monitoring decisions separate where that fits the existing layout.

## Repository-first workflow

Before editing, inspect repository instructions, current branches/worktree state, component folders, dependencies, and run conventions.
Identify existing Central and Engine interfaces, shared protocol utilities, and any implementation already present for WM_WS_M.
The project plan describes the WS side as a completed/current baseline, so verify the actual repository state and preserve working behavior.
Summarize the relevant findings and the files you intend to create or modify before making changes.
Implement only the smallest changes needed for WM_WS_M and its direct integration points.
Do not rewrite unrelated components or add CENTRAL, FO, Kafka business logic, or a second Engine implementation.
Use minimal fakes or mocks only when needed to exercise Monitor behavior without the full system.

## Design and code quality

Follow the repository's Python style and existing concurrency model.
If no model is established, use simple threading for the socket listener and periodic checks.
Keep concurrency understandable and protect shared state explicitly when multiple threads use it.
Use socket timeouts so a missing Engine response can be detected.
Use type hints, focused functions, and standard-library facilities where practical.
Use Python logging for startup, registration results, Engine connection, fault detection, and fault-report outcome.
Avoid noisy success logging on every interval unless the project's existing behavior requires it.
Handle connection failures, disconnects, timeouts, malformed frames/messages, invalid checksums, and unexpected socket closure clearly.
Clean up sockets and worker activity on orderly shutdown where the current application structure allows it.
Avoid unnecessary dependencies, frameworks, broad refactors, or abstractions that make the component harder to explain.

## Verification

Use existing tests and test commands if present; do not claim that a check passed unless it was run.
Check that startup accepts the four required values: Engine server port, Central IP, Central port, and WS ID.
Verify registration/authentication behavior against the available Central interface or a suitable fake.
Verify Engine connection and a healthy response.
Verify that health checks repeat at one-second intervals.
Verify KO, no response/timeout, and Engine disconnect detection.
Verify that a detected fault is reported to CENTRAL using the agreed protocol.
Verify malformed or invalid protocol input is rejected or handled safely.
Do not claim full end-to-end compatibility when CENTRAL or WM_WS_E is unavailable for integration.

## Completion report

When the work is complete, list the files created or modified and their responsibilities.
State any dependency changes and provide the command used to run WM_WS_M.
List the verification actually performed and its outcome.
Document assumptions and unresolved interface decisions, especially those requiring coordination with CENTRAL or WM_WS_E.
Keep the result assignment-focused, minimal, and understandable enough for an individual oral defense.

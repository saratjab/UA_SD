# Detailed Implementation Prompt — WM_WS_M
## Purpose
Implement only the Watering Station Monitor (WM_WS_M) for the WaterManagement project.
The project is written in Python for a university Distributed Systems assignment.
Treat the assignment, repository interfaces, and approved decisions below as the source of truth.
Keep the implementation minimal, clear, and suitable for an individual oral defense.

## Scope and architecture
WM_WS_M authenticates/registers its station with WM_Central and monitors its own WM_WS_E.
WM_WS_M is a TCP client of WM_Central and a TCP server for WM_WS_E.
Use TCP sockets for both connections and the assignment-recommended STX/DATA/ETX/LRC framing.
Encode DATA as a UTF-8 JSON object.
Do not implement Kafka, WM_Central, WM_FO, or the Engine itself.
A small Engine test client/mock is allowed only when needed for verification.
The runtime keypress that makes the Engine return KO belongs to WM_WS_E.
The Monitor detects and reports KO; it must not generate KO or simulate the keypress.
Do not add advanced retry or reconnection logic.

## Inspect before editing
Read repository instructions and inspect the current layout, Monitor code, tests, and shared protocol.
Check the actual WM_Central and WM_WS_E interfaces; preserve compatible conventions already in use.
If an existing interface conflicts with this prompt, report the mismatch before changing it.
Keep changes in WM_WS/WM_WS_M/ and its tests, except for a necessary, explained integration fix.
Before editing, summarize what exists and which files you plan to touch.

## Suggested package
Adapt this structure to existing repository conventions:
WM_WS/WM_WS_M/
    __init__.py, main.py, config.py, protocol.py, central_client.py, ws_e_server.py, monitor.py
Put tests in the repository's established test location.
Avoid unnecessary files, dependencies, and abstractions.

## Configuration
Accept WS ID, Central host and port, Engine-listening host and port, health-check interval, and timeout.
Use the existing configuration style or argparse when no style exists.
Do not hard-code network addresses, ports, or WS IDs.
Default the health-check interval to one second, as required by the assignment.
Keep the interval configurable and validate all configuration values.
Provide useful command-line help.

## Framing and JSON messages
Use STX=0x02 and ETX=0x03.
A frame is STX byte + UTF-8 JSON DATA bytes + ETX byte + one LRC byte.
Calculate LRC as XOR of all DATA bytes and validate it before decoding JSON.
Implement reusable LRC, frame-encoding, and frame-parsing functions.
Handle TCP partial reads and multiple frames correctly.
Reject malformed framing, invalid UTF-8/JSON, missing fields, and invalid LRC safely.
Keep protocol code independent from socket and monitoring logic.
Every message uses this JSON envelope:
{"type":"MESSAGE_TYPE","ws_id":"WS-001","payload":{}}

## Approved message contract
Monitor -> Central: REGISTER_WS, payload {}.
Central -> Monitor: REGISTER_ACK, payload {}.
Central -> Monitor: REGISTER_NACK, payload {"reason":"..."}.
Engine -> Monitor: HELLO_WS_E, payload {}.
Monitor -> Engine: HELLO_ACK, payload {}.
Monitor -> Engine: HELLO_NACK, payload {"reason":"..."}.
Monitor -> Engine: HEALTH_CHECK, payload {"sequence":1}.
Engine -> Monitor: HEALTH_OK or HEALTH_KO, payload {"sequence":1}.
Monitor -> Central: WS_E_FAULT, payload {"fault":"ko|timeout|connection_lost"}.
Central -> Monitor: FAULT_ACK, payload {}.
Central -> Monitor: FAULT_NACK, payload {"reason":"..."}.
Require the expected response type, matching WS ID, and matching health-check sequence.
Send HELLO_NACK when possible and close an Engine connection with the wrong WS ID.
Treat unknown message types or responses for another station as invalid.

## Central client
Connect to Central and send REGISTER_WS with the configured WS ID.
Wait for REGISTER_ACK or REGISTER_NACK before continuing.
On ACK, proceed; on NACK, log its reason and exit cleanly.
Keep the Central socket available for fault reporting.
For a fault, send one WS_E_FAULT with the WS ID and the appropriate fault value.
Wait for FAULT_ACK or FAULT_NACK and log the result.
Do not add retry loops or complicated Central recovery.

## Engine server and health monitoring
Bind the server to the configured Engine-listening host and port.
Accept one Engine connection and wait for HELLO_WS_E.
Validate its WS ID; send HELLO_ACK if it matches, otherwise reject and close it.
After handshake, send HEALTH_CHECK at the configured interval, defaulting to one second.
Increase the sequence number for each check and await its matching response.
On HEALTH_OK, continue monitoring.
On HEALTH_KO, report WS_E_FAULT with fault=ko.
On timeout, report WS_E_FAULT with fault=timeout.
On unexpected Engine disconnect, report WS_E_FAULT with fault=connection_lost.
Report at most one fault for the same connection failure; avoid duplicate notifications.
Release sockets cleanly after failure; do not implement automatic reconnection unless required by existing code.

## Concurrency, logging, and errors
Use the repository's concurrency model; otherwise prefer straightforward Python threading.
Keep Central communication, Engine acceptance, and health checks responsive.
Avoid concurrent unsynchronized writes to the same socket.
Use type hints and Python logging.
Log startup, Central connection/registration, Engine connection, and fault outcomes.
Do not log every successful one-second health check.
Handle connection errors, timeouts, disconnects, invalid frames, and unexpected exceptions safely.
Close sockets and stop worker activity cleanly on failure or requested shutdown.
Keep the code easy to explain; do not add frameworks or broad refactors.

## Tests and delivery
Use existing test conventions and add focused tests for LRC, frame creation/parsing, invalid LRC, and malformed frames.
Test REGISTER_WS, ACK/NACK, Engine handshake and wrong WS ID.
Test HEALTH_CHECK/HEALTH_OK, sequence matching, KO, timeout, disconnect, and fault notification/ACK handling.
Use mocks or local sockets where practical; do not require the full deployment for protocol tests.
Run the relevant existing tests after implementation and report the actual outcome.
Show changed files, final WM_WS_M tree, run command, and a basic Engine test-client procedure.
State assumptions and any Central/Engine details teammates must match.
Do not claim unverified compatibility or tests.
Do not modify unrelated components.

# WaterManagement — Release 1
# Project Context, Architecture, Development Plan, and AI Coding Specification

> **Purpose**
>
> This document is the shared technical context for:
> 1. both team members, so everybody understands the complete system and message flow;
> 2. AI coding tools, so code is developed against one agreed architecture instead of inventing different interfaces.
>
> **Primary source of truth:** the official `Practica_SD2627_WaterManagement_Vfinal_EN.pdf` (WaterManagement, Course 26/27, Release 1).
>
> This document deliberately separates:
> - **ASSIGNMENT REQUIREMENT** = explicitly required or stated by the PDF.
> - **TEAM DESIGN** = our chosen implementation detail where the PDF gives freedom.
> - **RECOMMENDATION** = a practical implementation choice suggested for consistency/simplicity, but not an assignment requirement.
>
> An AI tool must never present a TEAM DESIGN or RECOMMENDATION as if it were an official requirement.

---

# 1. Project Identity

| Item | Value |
|---|---|
| Course | Distributed Systems (SD) 26/27 |
| Assignment | WaterManagement — Release 1 |
| Language | Python |
| Team size | Up to 2 students |
| Submission | Week of 26 October 2026; exact date set by the lab instructor |
| Main components | `WM_Central`, `WM_WS_M`, `WM_WS_E`, `WM_FO` |
| Mandatory middleware | Kafka |
| Mandatory database | SQLite |
| Mandatory deployment technology | Docker |
| Mandatory repository | GitHub |
| Required cloud platform in the assignment | Railway (or AWS Learner Lab as the alternative CI/CD route stated by the PDF) |

---

# 2. Assignment Goal

The project is a real-time distributed simulation for centralized management of irrigation stations (`WS`) located in different parks/gardens.

The system must allow:

- centralized registration and supervision of multiple irrigation stations;
- operator requests for irrigation;
- centralized authorization/denial;
- irrigation control;
- continuous flow and accumulated-volume monitoring;
- health/fault detection;
- asynchronous commands from CENTRAL;
- operation of multiple operators and multiple watering stations;
- distributed deployment;
- socket communication;
- Kafka-based event streaming;
- SQLite persistence.

The official architecture is intentionally hybrid:

```text
DIRECT SOCKET COMMUNICATION
    Monitor <-> CENTRAL
    Monitor <-> Engine

KAFKA / EVENT STREAMING
    CENTRAL <-> Kafka/Event Manager
    Engine  <-> Kafka/Event Manager
    FO      <-> Kafka/Event Manager
```

The important distinction is:

```text
Sockets = direct communication for Monitor/status/authentication/health
Kafka   = asynchronous event/command/streaming communication
```

The Monitor is NOT supposed to replace Kafka, and Kafka is NOT supposed to replace the Monitor-Engine health socket.

---

# 3. Source-of-Truth Rules

## 3.1 What is fixed by the assignment

The following are mandatory according to the PDF:

- software components and mandatory application names;
- sockets as one of the communication mechanisms;
- Kafka as the event-streaming subsystem;
- SQLite;
- required WS states;
- registration/authentication behavior;
- health checks from Monitor to Engine every second;
- fault notification;
- irrigation lifecycle;
- asynchronous commands from CENTRAL;
- required command-line inputs;
- deployment topology;
- Docker;
- GitHub;
- lab report;
- deployment guide;
- screenshots;
- AI prompts stored/shared with teaching staff.

## 3.2 What the PDF leaves to the students

The PDF explicitly allows students to define socket communication protocols at their discretion and gives freedom for implementation details.

Therefore the following are TEAM DESIGN decisions, not official requirements:

- exact socket message names;
- exact JSON schema;
- exact topic names;
- exact Kafka partition count;
- exact Kafka consumer-group strategy;
- exact SQLite table schema;
- exact UI technology;
- exact internal Python module structure;
- exact logging framework;
- exact retry/backoff values, except where the PDF explicitly specifies timing such as the one-second health check and four-second file-request delay.

---

# 4. Required Components

## 4.1 `WM_Central`

**ASSIGNMENT REQUIREMENT**

CENTRAL is the control center and implements the core system logic and governance.

Responsibilities:

- start and remain running;
- listen for Monitor socket connections;
- authenticate/register WSs;
- maintain WS information/state;
- receive irrigation requests;
- validate whether a WS can irrigate;
- authorize or deny irrigation;
- receive WS statuses and irrigation events;
- receive fault notifications;
- send commands to one or all WSs;
- provide the monitoring/control panel;
- persist required data in SQLite.

Required application name:

```text
WM_Central
```

Required command-line configuration includes:

```text
socket listening port
Kafka Broker/Bootstrap IP
Kafka Broker/Bootstrap port
```

---

## 4.2 `WM_WS_M` — Watering Station Monitor

**ASSIGNMENT REQUIREMENT**

The Monitor represents the monitoring/safety side of the WS.

Responsibilities:

- connect to CENTRAL;
- authenticate/register the WS;
- provide a socket server for `WM_WS_E`;
- wait for its Engine connection;
- send a health-check every second;
- detect no response/timeout;
- detect `KO` response;
- report a fault to CENTRAL, simulating a leak;
- remain operational while the Engine is watering.

Required application name:

```text
WM_WS_M
```

Required command-line configuration includes:

```text
socket server port offered to WM_WS_E
CENTRAL IP
CENTRAL socket port
WS ID
```

Important:

```text
WM_WS_M <-> WM_Central = SOCKET
WM_WS_M <-> WM_WS_E   = SOCKET
```

The Monitor is the component responsible for the Engine health-check path.

---

## 4.3 `WM_WS_E` — Watering Station Engine

**ASSIGNMENT REQUIREMENT**

The Engine is the operational part of the WS.

Responsibilities:

- wait for service/irrigation requests from the central system;
- receive/use flow-meter information;
- activate the solenoid valve;
- stop irrigation;
- simulate watering;
- calculate/report flow and accumulated volume;
- allow the required runtime KO simulation;
- remain responsive to Monitor health checks while watering.

Required application name:

```text
WM_WS_E
```

Required command-line configuration includes:

```text
Kafka Broker/Bootstrap IP
Kafka Broker/Bootstrap port
Monitor IP
Monitor socket port
```

The PDF states that the WS ID comes from the Monitor's command-line configuration and that CENTRAL validates the ID during authentication.

Communication:

```text
WM_WS_E <-> WM_WS_M      = SOCKET
WM_WS_E <-> Kafka/Event Manager = STREAMING
```

---

## 4.4 `WM_FO` — Field Operator Application

**ASSIGNMENT REQUIREMENT**

The Operator application is used to request irrigation.

Responsibilities:

- identify the operator;
- request irrigation at a WS;
- receive authorization/denial/result messages;
- show the status of the WS serving the operator;
- allow a one-off request;
- read requests from a file for automated testing;
- after each file-driven request finishes (successfully or unsuccessfully), wait 4 seconds before the next request.

Required application name:

```text
WM_FO
```

Required command-line configuration includes:

```text
Kafka Broker/Bootstrap IP
Kafka Broker/Bootstrap port
Operator ID
```

Communication:

```text
WM_FO <-> Kafka/Event Manager = STREAMING
```

---

# 5. Official Conceptual Architecture

Figure 3 in the assignment defines the high-level communication model.

```text
                                  ┌──────────────────────┐
                                  │      WM_Central      │
                                  │      System Logic    │
                                  │        + DB          │
                                  └───────┬──────┬───────┘
                                          │      │
                            SOCKETS       │      │ STREAMING / QM
                         status/auth     │      │
                                          │      ▼
                                          │  ┌────────────────────┐
                                          │  │ Kafka / Queue and  │
                                          │  │ Event Manager      │
                                          │  └──────┬─────┬──────┘
                                          │         │     │
                                          │       STREAM STREAM
                                          │         │     │
                                          ▼         ▼     ▼
                                  ┌────────────┐  ┌───────┐  ┌────────────┐
                                  │ WM_WS_M    │  │WM_FO  │  │ WM_WS_E    │
                                  │ Monitor    │  │  App  │  │ Engine     │
                                  └─────┬──────┘  └───────┘  └────────────┘
                                        │
                                      SOCKET
                                        │
                                        ▼
                                  ┌────────────┐
                                  │ WM_WS_E    │
                                  │ Engine     │
                                  └────────────┘
```

More precisely, per Figure 3:

### Socket interfaces

```text
WM_Central <----socket----> WM_WS_M
WM_WS_M    <----socket----> WM_WS_E
```

The CENTRAL↔Monitor socket is used for status/authentication.

The Monitor↔Engine socket is used for health monitoring and the local WS safety path.

### Streaming interfaces

```text
WM_Central <----Kafka/Event Manager----> WM_WS_E
WM_Central <----Kafka/Event Manager----> WM_FO
```

The figure also represents the WS Engines and Operator Apps as clients of the queue/event-management subsystem.

There is no Kafka requirement between the Monitor and Engine for the health-check mechanism.

---

# 6. Why Both Sockets and Kafka Exist

This distinction must be understood before implementing the system.

## Socket path

The Monitor needs direct communication with:

```text
Engine
CENTRAL
```

because it must perform immediate health checks and report faults.

Example:

```text
Monitor -> HEALTH_CHECK
Engine  -> HEALTH_OK
```

This happens every second.

If:

```text
Engine -> HEALTH_KO
```

or the Engine does not answer, then:

```text
Monitor -> CENTRAL : fault notification
```

## Kafka path

Kafka is used for asynchronous event/command communication between the central system, engines, and operators.

Examples include:

```text
FO -> irrigation request -> Kafka -> CENTRAL

CENTRAL -> irrigation command -> Kafka -> Engine

Engine -> telemetry/status/event -> Kafka -> CENTRAL

CENTRAL -> result/notification -> Kafka -> FO
```

This separation matches Figure 3 and the assignment's description of sockets versus the event-streaming subsystem.

---

# 7. Complete System Flow

## 7.1 CENTRAL startup

**ASSIGNMENT REQUIREMENT**

```text
Start WM_Central
      |
      v
Open SQLite
      |
      v
Load registered WSs
      |
      v
Show registered WSs
      |
      v
Previously registered WS but not connected?
      |
      +----> show DISCONNECTED
      |
      v
Start socket listener
      |
      v
Connect to / start Kafka client
      |
      v
Remain running indefinitely
```

Important:

CENTRAL cannot know the live status of a WS that is not connected, so a registered-but-not-currently-connected WS must be shown as `DISCONNECTED`.

---

# 8. WS Registration and Authentication Flow

```text
WM_WS_M starts
      |
      v
Read:
- port for WM_WS_E
- CENTRAL IP/port
- WS_ID
      |
      v
Connect to CENTRAL using SOCKET
      |
      v
Send WS registration/authentication information
      |
      v
CENTRAL validates WS_ID / registration
      |
      +---- invalid ----> reject / registration failure
      |
      +---- valid -----> confirm operational WS
                           |
                           v
                       WS becomes available
```

The exact authentication data is not specified by the PDF.

**TEAM DESIGN:** use the WS ID and registration information as the initial identity mechanism. Do not invent passwords, JWT, OAuth, TLS, or another security system unless the team deliberately adds and understands it.

---

# 9. Monitor-Engine Connection and Health Flow

```text
WM_WS_M starts socket server
      |
      v
WM_WS_E connects to WM_WS_M
      |
      v
HELLO / HELLO_ACK
      |
      v
Continuous loop:
      |
      +--> Monitor sends HEALTH_CHECK every 1 second
      |
      +--> Engine answers HEALTH_OK
      |
      +--> continue
```

Failure cases:

```text
HEALTH_KO
   OR
no response / timeout
   OR
Engine disconnects
        |
        v
Monitor detects fault
        |
        v
Monitor -> CENTRAL : fault/leak notification
        |
        v
CENTRAL -> WS/FO : incident information
```

The assignment explicitly requires a runtime mechanism in the Engine to trigger a KO simulation.

---

# 10. Operator Irrigation Request Flow

The operator can submit the request manually or through the file-driven test mode.

```text
WM_FO
   |
   | irrigation request
   v
Kafka
   |
   v
WM_Central
   |
   | validate:
   | - WS exists?
   | - WS connected?
   | - WS available?
   | - no fault/leak?
   | - not out of service?
   v
Authorize OR Deny
```

The assignment states that CENTRAL must check station availability before authorizing irrigation.

The operator must receive visible information about the progress/result of the request.

---

# 11. Authorization and Start-Irrigation Flow

Once CENTRAL authorizes the request:

```text
WM_Central
   |
   +----> notify WM_FO
   |
   +----> notify target WS
              |
              v
          Kafka/Event Manager
              |
              v
          WM_WS_E
              |
              v
        Open solenoid valve
              |
              v
         Start irrigation
              |
              v
         WS = WATERING
```

The PDF says that once CENTRAL has completed its checks and notified both the WS and operator to proceed, the system opens the solenoid valve.

---

# 12. Irrigation Telemetry Flow

During watering, the WS must continuously report information every second.

```text
WM_WS_E
   |
   | every second
   | flow_rate
   | accumulated_volume
   v
Kafka / Event Manager
   |
   v
WM_Central
   |
   +--> update station state
   +--> update monitoring panel
   +--> keep irrigation information
   |
   +--> notify the relevant operator application
```

The monitoring panel must show, during watering:

```text
WS ID
Location
WATERING
Flow rate (liters/minute)
Accumulated volume (liters)
Operator ID
```

---

# 13. Irrigation Completion Flow

Irrigation continues until one of the following occurs:

1. the preset time limit is reached; or
2. a menu option is used on the WS to stop the supply.

Then:

```text
WM_WS_E
   |
   v
Stop watering
Close valve
   |
   v
Send final irrigation event
   |
   v
Kafka
   |
   v
WM_Central
   |
   +--> persist final result
   +--> update WS status
   +--> notify operator
   |
   v
WS returns to AVAILABLE / waiting state
```

The final result should include the required summary information such as:

```text
total volume
duration
completion status
operator ID
WS ID
```

---

# 14. Fault / Leak Flow

The assignment requires continuous station self-monitoring.

## Fault while idle

```text
Engine KO / health failure
        |
        v
Monitor detects fault
        |
        v
Monitor -> CENTRAL
        |
        v
CENTRAL: WS = LEAK
        |
        v
Irrigation denied until resolved
```

## Fault while watering

```text
WATERING
   |
   v
Fault detected
   |
   v
Irrigation must end immediately
   |
   v
Engine stops / valve closes
   |
   v
Monitor reports fault
   |
   v
CENTRAL updates station to LEAK
   |
   +--> inform relevant operator
   +--> show incident on CENTRAL
   +--> show incident on station Monitor
```

After the contingency is resolved:

```text
WS -> notify CENTRAL
CENTRAL -> WS becomes available again
```

The exact physical/manual mechanism for resolving a simulated leak is a TEAM DESIGN item.

---

# 15. CENTRAL Commands

The assignment requires CENTRAL to be able to asynchronously send these commands to one or all WSs:

```text
START IRRIGATION
BLOCK WS
ACTIVATE WS
```

## 15.1 Start irrigation

Behaves like an operator-triggered irrigation request.

```text
CENTRAL
  |
  v
Kafka command
  |
  v
Target WS Engine
  |
  v
Start irrigation
```

## 15.2 Block WS

```text
CENTRAL -> BLOCK WS
      |
      v
WS ends active irrigation if any
      |
      v
WS = OUT OF SERVICE
```

Display:

```text
ORANGE
"Out of Service"
```

## 15.3 Activate WS

```text
CENTRAL -> ACTIVATE WS
      |
      v
WS becomes AVAILABLE / ACTIVATED
      |
      v
GREEN
```

All these actions are asynchronous: there are no "turns"; operators, CENTRAL, and WSs can send requests/messages at any time.

---

# 16. Watering Station States

**ASSIGNMENT REQUIREMENT**

```text
AVAILABLE
WATERING
LEAK
OUT OF SERVICE
DISCONNECTED
```

Meaning:

| State | Meaning |
|---|---|
| AVAILABLE | WS is functioning correctly and waiting for an irrigation request |
| WATERING | WS is currently supplying irrigation |
| LEAK | WS is connected but a fault/leak is detected; irrigation must not occur |
| OUT OF SERVICE | WS is deliberately blocked by CENTRAL |
| DISCONNECTED | WS is not connected to CENTRAL and cannot be monitored |

Display mapping required by the PDF:

```text
AVAILABLE      = GREEN
WATERING       = GREEN BLINKING
LEAK           = RED
OUT OF SERVICE = ORANGE
DISCONNECTED   = GRAY
```

Useful implementation transition model:

```text
DISCONNECTED
      |
      | registration/authentication + healthy connection
      v
  AVAILABLE
   |      |
   |      +---- block ----> OUT OF SERVICE
   |                           |
   |                           +---- activate ----> AVAILABLE
   |
   +---- start irrigation ----> WATERING
                                  |
                                  +---- normal end ---> AVAILABLE
                                  |
                                  +---- fault --------> LEAK
                                                        |
                                                        +-- resolved --> AVAILABLE
```

A network loss during any state must be reflected appropriately by CENTRAL; exact precedence between `DISCONNECTED` and another persisted state is an implementation rule that must be kept consistent.

---

# 17. Socket Communication Design

The PDF allows the team to define the socket protocol.

## 17.1 Recommended framing used by this project

**TEAM DESIGN / based on the PDF's recommended framing**

```text
<STX><DATA><ETX><LRC>
```

Definitions:

```text
STX = 0x02
ETX = 0x03
DATA = encoded message
LRC = byte-by-byte XOR of the message/data bytes
```

The receiver validates the frame and answers with:

```text
ACK
```

or

```text
NACK
```

The PDF's example concept is an operation code plus fields. JSON is a TEAM DESIGN choice.

## 17.2 Recommended message representation

Use JSON inside DATA:

```json
{
  "type": "HEALTH_CHECK",
  "ws_id": "WS_001",
  "message_id": "uuid",
  "timestamp": "2026-10-05T12:00:00Z",
  "payload": {}
}
```

All message types must be versioned/managed consistently.

## 17.3 Current socket message set

**TEAM DESIGN**

Monitor <-> Engine:

```text
HELLO_WS_E
HELLO_ACK
HEALTH_CHECK
HEALTH_OK
HEALTH_KO
```

Monitor <-> CENTRAL:

```text
REGISTER_WS
REGISTER_ACK
REGISTER_NACK
WS_FAULT
FAULT_ACK
FAULT_NACK
```

The exact names can change, but both students and all AI tools must use one shared contract.

---

# 18. Kafka / Event-Streaming Design

The assignment requires Kafka, but it does not prescribe exact topic names.

Therefore the following is a **TEAM DESIGN**.

## 18.1 Recommended topics

```text
wm.activation.requests
wm.ws.commands
wm.ws.events
wm.operator.events
```

Purpose:

| Topic | Producer | Consumer | Purpose |
|---|---|---|---|
| `wm.activation.requests` | WM_FO | WM_Central | Operator irrigation requests |
| `wm.ws.commands` | WM_Central | WM_WS_E | Start/block/activate and related WS commands |
| `wm.ws.events` | WM_WS_E | WM_Central | Telemetry, completion, WS status, irrigation events |
| `wm.operator.events` | WM_Central | WM_FO | Authorization, progress, final result, incident messages |

The Monitor does not use Kafka for its health-check socket.

## 18.2 Recommended event envelope

Use one common envelope:

```json
{
  "event_type": "IRRIGATION_REQUEST",
  "message_id": "uuid",
  "timestamp": "2026-10-05T12:00:00Z",
  "ws_id": "WS_001",
  "operator_id": "OP_001",
  "request_id": "uuid",
  "payload": {}
}
```

Fields:

- `event_type` = operation/event name;
- `message_id` = unique message identifier;
- `timestamp` = event creation time;
- `ws_id` = target/source WS where applicable;
- `operator_id` = operator where applicable;
- `request_id` = correlation identifier for one logical irrigation request;
- `payload` = event-specific data.

## 18.3 Kafka keys

**RECOMMENDATION**

Use stable keys to preserve ordering for the same logical entity:

```text
wm.activation.requests -> key = ws_id
wm.ws.commands         -> key = ws_id
wm.ws.events           -> key = ws_id
wm.operator.events     -> key = operator_id
```

This is an implementation choice, not an assignment requirement.

## 18.4 Consumer-group rule

For the first working version, do not invent a complex routing topology.

**RECOMMENDATION**

- `WM_Central` uses a dedicated consumer group for station events.
- `WM_Central` uses a dedicated consumer group for operator activation requests.
- Each `WM_WS_E` can use a station-specific consumer group and filter commands using `ws_id`.
- Each `WM_FO` can use an operator-specific consumer group and filter events using `operator_id`.

This approach is simple to reason about for a university project and avoids accidentally consuming another station's command as if it were its own.

The exact partition count and optimization strategy can be changed later if required.

---

# 19. Kafka Event Examples

## 19.1 Operator request

```json
{
  "event_type": "IRRIGATION_REQUEST",
  "message_id": "m1",
  "request_id": "r1",
  "operator_id": "OP_001",
  "ws_id": "WS_001",
  "payload": {
    "duration_seconds": 60
  }
}
```

## 19.2 Central authorization

```json
{
  "event_type": "IRRIGATION_AUTHORIZED",
  "message_id": "m2",
  "request_id": "r1",
  "operator_id": "OP_001",
  "ws_id": "WS_001",
  "payload": {}
}
```

## 19.3 Start command

```json
{
  "event_type": "START_IRRIGATION",
  "message_id": "m3",
  "request_id": "r1",
  "ws_id": "WS_001",
  "operator_id": "OP_001",
  "payload": {
    "duration_seconds": 60
  }
}
```

## 19.4 Telemetry

```json
{
  "event_type": "IRRIGATION_TELEMETRY",
  "message_id": "m4",
  "request_id": "r1",
  "ws_id": "WS_001",
  "operator_id": "OP_001",
  "payload": {
    "flow_lpm": 12.4,
    "accumulated_volume_l": 4.1
  }
}
```

## 19.5 Completion

```json
{
  "event_type": "IRRIGATION_COMPLETED",
  "message_id": "m5",
  "request_id": "r1",
  "ws_id": "WS_001",
  "operator_id": "OP_001",
  "payload": {
    "duration_seconds": 60,
    "total_volume_l": 12.7,
    "reason": "TIME_LIMIT"
  }
}
```

---

# 20. Database Design

**ASSIGNMENT REQUIREMENT:** SQLite.

The PDF requires the database to contain operator and WS data such as ID/status and any additional information required by the implementation.

## 20.1 Recommended schema

```text
operators
---------
operator_id PK
name NULLABLE
active

watering_stations
-----------------
ws_id PK
location
status
registered_at
last_seen_at
is_registered

irrigations
-----------
irrigation_id PK
request_id UNIQUE
ws_id
operator_id
start_time
end_time
duration_seconds
total_volume_l
result
```

Optional tables can be added only when there is a clear reason.

## 20.2 Persistence responsibilities

CENTRAL should persist at least:

```text
WS ID
WS location
WS state/persistent information
operator identity
irrigation results
```

On restart:

```text
CENTRAL
  |
  v
Load DB
  |
  v
Load known WSs
  |
  v
If WS is not currently connected -> DISCONNECTED
```

---

# 21. CENTRAL Internal Responsibilities

A clean separation is recommended:

```text
WM_Central/
├── main.py
├── config.py
├── socket_server.py
├── kafka_client.py
├── database.py
├── models.py
├── state_manager.py
├── authorization.py
├── irrigation_service.py
├── command_service.py
├── monitoring.py
├── protocol.py
└── tests/
```

Suggested responsibilities:

### `socket_server.py`
Only socket transport and Monitor connections.

### `protocol.py`
Frame parsing/encoding, STX/ETX/LRC, ACK/NACK.

### `kafka_client.py`
Kafka producer/consumer setup and message transport.

### `state_manager.py`
WS state transitions and in-memory live state.

### `authorization.py`
Validate irrigation requests.

### `irrigation_service.py`
Application-level orchestration of irrigation requests/results.

### `database.py`
SQLite connection and CRUD operations.

### `monitoring.py`
Dashboard/panel state and display preparation.

AI tools must not put all of CENTRAL into one huge file.

---

# 22. WM_WS_M Internal Structure

Recommended:

```text
WM_WS_M/
├── main.py
├── config.py
├── central_client.py
├── engine_server.py
├── protocol.py
├── monitor.py
└── tests/
```

Responsibilities:

```text
central_client.py
    CENTRAL connection
    registration/authentication
    fault reporting

engine_server.py
    accept Engine socket
    read Engine messages
    send health checks

monitor.py
    health timing
    timeout/KO detection
    state transitions
```

The health check must continue while the Engine performs irrigation.

---

# 23. WM_WS_E Internal Structure

Recommended:

```text
WM_WS_E/
├── main.py
├── config.py
├── engine.py
├── kafka_client.py
├── monitor_client.py
├── irrigation.py
├── flow_meter.py
├── valve.py
└── tests/
```

Responsibilities:

```text
monitor_client.py
    Monitor socket communication
    HELLO/HELLO_ACK
    HEALTH_OK / HEALTH_KO

kafka_client.py
    receive central commands
    publish WS events

irrigation.py
    lifecycle
    timing
    start/stop
    totals

flow_meter.py
    simulated flow

valve.py
    open/close state
```

The Engine must not block its Monitor health response while watering.

---

# 24. WM_FO Structure

Recommended:

```text
WM_FO/
├── main.py
├── config.py
├── kafka_client.py
├── operator.py
├── request_file.py
├── display.py
└── tests/
```

Responsibilities:

```text
operator.py
    create/send irrigation requests

request_file.py
    read file-driven requests
    wait 4 seconds after each completed request

kafka_client.py
    send requests
    receive operator notifications

display.py
    show statuses/results
```

---

# 25. Concurrency Model

The application is asynchronous. Design every component so that one activity does not block unrelated activities.

Minimum scenarios:

```text
Multiple WSs active
Multiple FOs active
Multiple telemetry streams
Health checks during irrigation
Fault during irrigation
CENTRAL command during irrigation
WS disconnect during operation
```

Recommended Python approach:

- use `threading` or `concurrent.futures` for socket/background tasks;
- keep Kafka producer/consumer work separate from the UI/control loop;
- use thread-safe state management (`Lock` or another explicit synchronization mechanism) where multiple threads update shared state;
- never hold a global lock while performing slow network operations;
- use timeouts on socket operations;
- use clean shutdown handling.

Do not introduce `asyncio` unless the team deliberately chooses it and both students understand it. Simplicity is preferred.

---

# 26. Message Correlation and Idempotency

**RECOMMENDATION**

Every logical irrigation request should have:

```text
request_id
```

Every individual message should have:

```text
message_id
```

This makes it possible to correlate:

```text
REQUEST
AUTHORIZED/DENIED
START
TELEMETRY
COMPLETED
FAULT
```

The implementation should avoid performing the same state-changing operation twice when the same logical command is accidentally received more than once.

This is a design recommendation for robustness, not an explicit PDF requirement.

---

# 27. Error Handling

Required/expected failure cases:

```text
Engine sends HEALTH_KO
Engine fails to answer
Engine disconnects
Monitor loses CENTRAL connection
Malformed socket frame
Invalid LRC
Invalid JSON / invalid fields
Unknown WS ID
Unknown operator ID
Irrigation request for unavailable WS
WS is LEAK
WS is OUT OF SERVICE
WS is DISCONNECTED
Fault during watering
CENTRAL command during watering
Kafka connection problem
```

For every failure, define:

```text
1. who detects it;
2. what local state changes;
3. what remote message is sent;
4. what CENTRAL stores/displays;
5. whether the operation must stop.
```

---

# 28. Deployment Topology

**ASSIGNMENT REQUIREMENT**

The required minimum physical deployment is:

```text
                         RAILWAY / CLOUD
              ┌─────────────────────────────────┐
              │ WM_Central                      │
              │ SQLite DB                       │
              │ Kafka                           │
              └────────────────┬────────────────┘
                               │
                          network
                               │
               ┌───────────────┴───────────────┐
               │                               │
          LAB PC 1                        LAB PC 2
               │                               │
         several WM_FO                 several WM_WS
                                             │
                                      ┌──────┴──────┐
                                      │             │
                                   WM_WS_M       WM_WS_E
```

The PDF requires:

- cloud: `WM_Central`, DB, Kafka;
- lab PC 1: several `WM_FO`;
- lab PC 2: several `WM_WS`;
- Docker for deploying modules;
- GitHub for source management;
- instructor access to the repository;
- CI/CD path using VS Code -> GitHub -> Railway, or the AWS Learner Lab alternative described by the PDF.

---

# 29. Configuration and Environment

Do not hard-code environment-specific addresses in source code.

Use environment variables and/or CLI arguments.

Example configuration:

```text
CENTRAL_HOST
CENTRAL_PORT
WS_ID
MONITOR_HOST
MONITOR_PORT
KAFKA_BOOTSTRAP_SERVERS
KAFKA_GROUP_ID
OPERATOR_ID
DB_PATH
```

The exact variable names are a TEAM DESIGN choice.

The mandatory CLI arguments stated by the PDF must still be supported by each application.

---

# 30. Docker

**ASSIGNMENT REQUIREMENT:** Docker must be used for deployment of all modules.

Recommended:

```text
Dockerfile
docker/
README deployment instructions
.env.example
```

Each application should have:

```text
container starts
  |
  v
read configuration
  |
  v
start application
  |
  v
connect to required services
```

Do not bake secrets or machine-specific IP addresses into Docker images.

---

# 31. GitHub

**ASSIGNMENT REQUIREMENT**

GitHub is mandatory.

Repository should contain at minimum:

```text
WM_Central/
WM_WS/
WM_FO/
tests/
docs/
prompts/
docker/
README.md
.gitignore
requirements.txt
```

The teaching staff must be given access to the repository.

---

# 32. AI Development Rules

AI is explicitly allowed by the assignment as a support tool, but the students remain responsible for understanding and being able to modify the code.

Every AI tool working on this repository must follow these rules:

1. Read this file before changing code.
2. Treat the official PDF as the final authority.
3. Never invent an assignment requirement.
4. Distinguish assignment requirements from team design decisions.
5. Inspect existing code before proposing changes.
6. Preserve working interfaces unless a coordinated interface change is explicitly requested.
7. Never replace a required socket path with Kafka.
8. Never replace the Kafka event-streaming path with direct socket communication.
9. Keep Monitor health checks on the Monitor-Engine socket.
10. Keep Monitor-CENTRAL authentication/status/fault communication on the socket.
11. Keep Engine/CENTRAL/FO event flows on Kafka.
12. Keep network transport separate from business logic.
13. Do not create unnecessary frameworks or dependencies.
14. Prefer standard-library Python where practical.
15. Use type hints and clear function boundaries.
16. Add or update automated tests for non-trivial changes.
17. Run the existing test suite after changes.
18. Do not rewrite unrelated files.
19. Do not silently rename message types, topics, states, or CLI arguments.
20. Report any proposed interface change before making it.
21. Keep logs useful for debugging distributed behavior.
22. Make concurrency explicit and safe.
23. Use timeouts and reconnection handling for network communication.
24. Do not commit automatically.
25. Do not claim a test/deployment passed unless it was actually executed.
26. Keep the code understandable enough for an individual oral defense.
27. Store the prompts used for development in the repository as required by the assignment.

---

# 33. Testing Strategy

## 33.1 Unit tests

### WM_WS_M

```text
frame encoding
frame decoding
LRC validation
HELLO handling
HEALTH_CHECK
HEALTH_OK
HEALTH_KO
timeout detection
fault notification
```

### WM_WS_E

```text
valve open/close
flow simulation
volume accumulation
start irrigation
stop irrigation
time-limit completion
HEALTH_OK while watering
HEALTH_KO simulation
Kafka event handling
```

### CENTRAL

```text
WS registration
WS state transitions
availability checks
authorization/denial
database CRUD
commands
event processing
fault processing
```

### WM_FO

```text
request creation
file parsing
4-second sequencing
operator filtering
result handling
```

## 33.2 Socket protocol tests

```text
valid frame
invalid STX
invalid ETX
invalid LRC
malformed DATA
missing fields
unknown operation
ACK
NACK
disconnect
timeout
```

## 33.3 Integration tests

```text
Monitor <-> Engine
Monitor <-> CENTRAL
FO <-> Kafka <-> CENTRAL
CENTRAL <-> Kafka <-> Engine
```

## 33.4 End-to-end test

```text
Start CENTRAL
    |
Start WS Monitor
    |
Register WS
    |
Start WS Engine
    |
Connect Engine to Monitor
    |
Health checks OK
    |
Start FO
    |
FO requests irrigation
    |
CENTRAL authorizes
    |
Engine starts watering
    |
Telemetry every second
    |
Stop/time-limit
    |
Final summary
    |
WS AVAILABLE
```

## 33.5 Fault test

```text
Start watering
    |
Trigger HEALTH_KO
    |
Monitor detects
    |
CENTRAL receives fault
    |
Irrigation stops immediately
    |
WS = LEAK
    |
Operator/CENTRAL see fault
```

## 33.6 Deployment test

Run the exact Railway + lab-PC topology required for grading before evaluation.

---

# 34. Team Responsibilities

## Student 1 — WS side

Primary ownership:

```text
WM_WS_M
WM_WS_E
socket protocol
health monitoring
fault simulation
flow/valve simulation
WS tests
WS-side Docker
```

## Student 2 — CENTRAL/FO side

Primary ownership:

```text
WM_Central
SQLite
central socket server
WS registration/state
WM_FO
operator-side behavior
```

## Shared responsibility

```text
Kafka design and integration
end-to-end integration
concurrency
Docker deployment
Railway
cloud/lab networking
final testing
documentation
AI prompts
```

Both students must be able to explain all modules during evaluation.

---

# 35. Branch and Integration Workflow

Recommended:

```text
main
 |
 +-- feature/ws-monitor
 +-- feature/ws-engine
 +-- feature/central
 +-- feature/field-operator
 +-- feature/kafka
 +-- feature/integration
 +-- feature/docker-deployment
```

Workflow:

```text
pull latest main
      |
      v
create feature branch
      |
      v
implement
      |
      v
run tests
      |
      v
review diff
      |
      v
commit
      |
      v
push
      |
      v
Pull Request
      |
      v
review
      |
      v
merge
```

Never continue major development on an obsolete branch after its work has been merged.

---

# 36. Integration Order

Use this order to reduce debugging complexity:

```text
1. WM_WS_M <-> WM_WS_E
2. WM_WS_M <-> WM_Central
3. CENTRAL + SQLite + live WS state
4. WM_FO
5. Kafka transport
6. FO -> CENTRAL request flow
7. CENTRAL -> Engine irrigation command
8. Engine -> CENTRAL telemetry
9. Completion + operator result
10. Fault handling
11. CENTRAL block/activate/start commands
12. Multiple WS
13. Multiple FO
14. Concurrency stress
15. Docker
16. Railway
17. Final distributed deployment test
```

---

# 37. Development Timeline

## Week of 21–27 September

```text
Architecture
Assignment analysis
Sockets
Kafka concepts
SQLite
Docker
Railway
```

Status:

```text
DONE
```

## Week of 28 September–4 October

```text
WM_WS_M
WM_WS_E
WS integration
WS tests
CENTRAL foundation
SQLite foundation
```

Status:

```text
WS side = completed/current baseline
CENTRAL/DB = in progress
```

## Week of 5–11 October

```text
Finish CENTRAL
Finish SQLite
Integrate Monitor <-> CENTRAL
Finish WM_FO
Start Kafka integration
Complete first end-to-end irrigation flow
```

Target:

```text
FO -> CENTRAL -> WS
```

working for one WS.

## Week of 12–18 October

```text
Complete Kafka flows
Telemetry
Monitoring panel
Fault handling
Multiple WS
Multiple FO
Concurrency
Complete local end-to-end tests
```

Target:

```text
complete local system
```

## 19–22 October

```text
Docker
Railway
Cloud configuration
Lab configuration
Cloud <-> lab connectivity
```

## 23–24 October

```text
Run exact grading topology
Test several FOs
Test several WSs
Test failure scenarios
```

## 25 October

```text
Bug fixing
Screenshots
README
Deployment guide
Report
AI prompt collection
No major new features
```

## Week of 26 October

```text
Submission / evaluation
```

The PDF states the submission deadline falls in the week of October 26, 2026. The exact deadline is set by the lab instructor.

---

# 38. Definition of Done

The implementation is complete when all of the following are true:

```text
[ ] WM_Central works
[ ] WM_WS_M works
[ ] WM_WS_E works
[ ] WM_FO works

[ ] Monitor <-> Engine socket works
[ ] Monitor <-> CENTRAL socket works
[ ] Kafka event streaming works

[ ] WS registration/authentication works
[ ] DISCONNECTED works
[ ] AVAILABLE works
[ ] WATERING works
[ ] LEAK works
[ ] OUT OF SERVICE works

[ ] irrigation authorization works
[ ] irrigation start works
[ ] telemetry every second works
[ ] irrigation stop works
[ ] final summary works
[ ] operator notifications work

[ ] HEALTH_KO works
[ ] timeout detection works
[ ] Engine disconnect detection works
[ ] fault during watering stops irrigation immediately

[ ] CENTRAL START works
[ ] CENTRAL BLOCK works
[ ] CENTRAL ACTIVATE works

[ ] multiple WS work
[ ] multiple FO work
[ ] concurrent activity works

[ ] SQLite persistence works
[ ] restart state handling works

[ ] Docker deployment works
[ ] Railway/cloud deployment works
[ ] lab <-> cloud network path works
[ ] GitHub repository ready
[ ] AI prompts stored

[ ] tests pass
[ ] deployment guide completed
[ ] screenshots completed
[ ] report completed
```

---

# 39. Open Decisions That Must Be Agreed by Both Students

These are not fully fixed by the PDF and must be decided once, documented, and then kept stable:

```text
1. Exact Kafka topic names
2. Kafka partition count
3. Kafka consumer-group IDs
4. Exact JSON schemas
5. Exact socket message names
6. Exact authentication mechanism
7. Exact SQLite schema
8. Exact mechanism for resolving a simulated LEAK
9. Exact monitoring-panel technology
10. Exact logging format
```

Once agreed, update this file and treat the updated values as the team contract.

---

# 40. Important Non-Goals

Unless required to solve an integration problem, do NOT add:

```text
OAuth
JWT
microservice frameworks
Kubernetes
Redis
PostgreSQL
MongoDB
complex web frameworks
service meshes
advanced observability platforms
extra cloud services
```

The assignment can be solved with:

```text
Python
TCP sockets
Kafka
SQLite
Docker
GitHub
Railway
```

Keep the design understandable and defensible.

---

# 41. Final Mental Model for the Team

Remember the whole project as four roles:

```text
WM_Central
= "authority and system brain"

WM_WS_M
= "watchdog / safety monitor"

WM_WS_E
= "station worker / irrigation engine"

WM_FO
= "operator requesting irrigation"
```

And two communication styles:

```text
SOCKET
= direct, endpoint-to-endpoint communication

KAFKA
= asynchronous event/command communication
```

The most important communication picture is:

```text
                    ┌─────────────────┐
                    │   WM_Central    │
                    │ logic + SQLite  │
                    └───────┬─────────┘
                            │
                  SOCKET    │    KAFKA
                status/auth │   events/commands
                            │
                      ┌─────┴─────┐
                      │           │
                 WM_WS_M       Kafka/Event
                    │            Manager
                  SOCKET        /         \
                    │          /           \
                 WM_WS_E      WM_FO       WM_WS_E
```

Therefore:

```text
Monitor health:
    WM_WS_M <-> WM_WS_E over socket

Monitor registration/fault:
    WM_WS_M <-> WM_Central over socket

Operator request:
    WM_FO -> Kafka -> WM_Central

Central command:
    WM_Central -> Kafka -> WM_WS_E

Telemetry/event:
    WM_WS_E -> Kafka -> WM_Central

Operator result:
    WM_Central -> Kafka -> WM_FO
```

This is the core architecture that all implementation work must preserve.

---

# 42. Official Deliverables Checklist

The assignment requires a lab report/document package containing, among other items:

- cover page with required student information;
- description of the software components;
- source code for the components;
- step-by-step deployment guide;
- screenshots of the applications working while connected.

The assignment also requires the AI prompts used during development to be included in GitHub and shared with the teaching staff.

Follow the instructor's exact submission procedure and deadline.

---

# 43. Source Reference Map

Use these PDF sections when resolving disagreements:

- Pages 3–6: scope, CENTRAL, states, FO, WS.
- Pages 7–9: solution mechanics and Figure 3 architecture.
- Pages 10–12: technical design, mandatory component names, command-line arguments, Kafka, Monitor behavior, FO file-driven behavior, socket protocol.
- Pages 13–14: deployment, Docker, GitHub, deliverables, AI use.

The official PDF remains the authority over this plan.

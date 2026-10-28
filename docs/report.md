# WaterManagement Release 1 — implementation and verification report

**Prepared:** 18 October 2026  
**Scope:** the current repository implementation and reported local
verification. This report does not claim a Railway deployment or completion of
the final two-PC lab acceptance test.

## 1. Problem and requirements

WaterManagement coordinates irrigation requests among operators and remotely
monitored watering stations. The system needs to authorize a request against
the current operator/station state, operate the Engine's valve safely, provide
progress and final measurements, and keep durable station and irrigation
records across Central restarts. Multiple stations operate concurrently, so
each station needs its own Monitor–Engine connection and independent irrigation
state.

The implementation retains the assignment's two communication styles for
different purposes. The Monitor's registration and health protocol remains
framed TCP. Kafka carries asynchronous irrigation commands and events. SQLite
is Central's durable record; transient connectivity and per-second telemetry
remain runtime state.

## 2. Component responsibilities and topology

```mermaid
flowchart LR
    FO[WM_FO]
    C[WM_Central]
    DB[(SQLite)]
    K[(Kafka)]
    M1[WM_WS_M · station 1]
    E1[WM_WS_E · station 1]
    M2[WM_WS_M · station 2]
    E2[WM_WS_E · station 2]

    FO -->|activation request| K
    K -->|request| C
    C <--> DB
    C -->|station command| K
    K -->|command| E1
    K -->|command| E2
    E1 -->|events and measurements| K
    E2 -->|events and measurements| K
    K -->|Engine events| C
    C -->|operator results| K
    K -->|filtered events| FO
    M1 <-->|HELLO / health checks| E1
    M2 <-->|HELLO / health checks| E2
    M1 <-->|registration / faults| C
    M2 <-->|registration / faults| C
```

- **WM_Central** validates operator and station requests, applies station
  policy, owns the status/control terminal, consumes Engine events, and stores
  station, fault, request, and final-result records.
- **WM_WS_M (Monitor)** enrolls/authenticates its configured station ID and
  location with Central. It maintains the Engine socket session, sends health
  checks at the configured one-second default, and reports fault and recovery
  events.
- **WM_WS_E (Engine)** connects to its matching Monitor and requires the
  `HELLO_ACK` session before accepting irrigation. It operates the valve and
  flow meter, consumes commands, and publishes start, telemetry, rejection,
  and completion events.
- **WM_FO (Field Operator)** submits duration, target-volume, or manual-stop
  requests, including sequential JSONL request files, and displays progress and
  final results with request and station IDs.
- **Kafka** connects the asynchronous request/command/event path. Topic names,
  fields, correlation IDs, and event types are specified in the
  [Kafka contract](kafka-message-contract.md).
- **SQLite** stores durable facts. The local Docker deployment puts the
  database under `/data` on the named `central-data` volume; restarting Central
  does not remove that volume.

## 3. Irrigation message flow

1. FO publishes an `ACTIVATION_REQUEST` containing `ws_id`, `operator_id`, one
   optional mode (`duration_seconds` or `target_volume_liters`), and a unique
   `request_id`. Omitting both limits means run until manually stopped.
2. Central checks that the operator and station are registered and that the
   station is available. It commits the request and stable-keyed outgoing
   command/operator messages together in its SQLite outbox. Delivery retries
   after restart until Kafka confirms each record.
3. The station Engine consumes the command. A matching station ID and an
   established Monitor session are required before it starts. An Engine that is
   blocked, failed, or disconnected from its Monitor emits a rejection rather
   than opening the valve.
4. After starting, the Engine publishes `IRRIGATION_STARTED` and cumulative
   `IRRIGATION_TELEMETRY` (flow rate, volume, and elapsed duration). When the
   cycle ends, it publishes `IRRIGATION_COMPLETED` with final measurements and
   an end reason.
5. Central updates SQLite and forwards progress and completion to FO. The
   request ID keeps the messages attached to the same irrigation throughout
   the workflow.

The socket protocol uses `<STX><DATA><ETX><LRC>`, where DATA is a UTF-8 JSON
object and LRC is the XOR of its bytes. Both peers use the shared framing and
LRC validation; this is the Monitor registration/control and Monitor–Engine
handshake/health protocol, separate from Kafka envelopes. Central applies a
10-second registration read timeout and a bounded 5-second response write
timeout, then permits idle registered sockets while TCP keepalive detects dead
peers. Monitors continue the configured one-second health-check cadence and
use a bounded health response timeout; on connection loss they report the
failure and reconnect/re-register with Central.

## 4. Station state, controls, and persistence

Central does not store the visible station status as one mutable field. It
derives it from durable administrative state, open faults, active irrigation,
and current Monitor connectivity. The precedence is:

1. No Monitor connection: `DISCONNECTED` (grey).
2. Open fault: `LEAK` (red).
3. Administratively blocked: `OUT_OF_SERVICE` (orange, displayed as “Out of
   Service”).
4. Active irrigation: `WATERING` (blinking green in the terminal display).
5. Otherwise: `AVAILABLE` (green).

The precedence makes a safety fault visible over an administrative block, and
keeps a block or fault durable while the station is disconnected. The
[ADR-001 state-model decision](adr/ADR-001-state-model-and-schema.md) describes
the rationale and schema invariants.

Central's optional interactive terminal presents a numbered menu for status,
single/all eligible irrigation, block, and activate actions. It is isolated on
its own daemon thread; absent/non-interactive stdin disables only the menu and
stdin EOF never shuts down the service. BLOCK persists the administrative
state and stops active irrigation; ACTIVATE refuses stations that are
disconnected or faulted. START issues irrigation through the same transactional
outbox path. Headless Central also provides an authenticated HTTP admin API;
public use requires HTTPS through Railway's TLS-terminating domain and a
configured token. Railway resources and cross-network access still need setup
and verification; see the [deployment runbook](railway-deployment.md).

SQLite uses constraints and unique indexes to enforce one live irrigation per
station, one open fault per station, and one row per request ID. Those checks
make duplicate request delivery safe and prevent competing Central handlers
from authorizing two simultaneous cycles for one station. Central persists
final measurements and administrative/fault state, while live connectivity
and per-second telemetry are derived/runtime data. The schema migration adds
the supported `MONITOR_UNAVAILABLE` Engine rejection reason without discarding
existing irrigation rows. SQLite enables foreign keys and WAL mode; one shared
connection is protected by a reentrant lock, and state-changing transactions
use `BEGIN IMMEDIATE` so validation and insertion cannot race another Central
handler.

## 5. Reliability and recovery behavior

- **Engine–Monitor loss:** the Engine closes the valve, reports the final
  available measurement when possible, clears its ready state, and retries
  connection with bounded backoff. Irrigation cannot start until the
  Monitor handshake succeeds again.
- **Engine health failure:** Monitor reports the fault to Central. Central's
  derived station state becomes `LEAK`; a later healthy response resolves the
  fault and restores availability, subject to connectivity and administrative
  state.
- **Monitor reconnect:** registration is once per successful Central session.
  A real disconnection leads to a new registration on reconnection; routine
  one-second health checks do not create repeated registrations.
- **Central restart during watering:** the SQLite database and Kafka offsets
  persist. Central preserves a `WATERING` record at startup so a queued Engine
  completion can add the actual duration and volume after Central returns.
  Replay is guarded by request IDs and idempotent state transitions.
- **Engine process restart:** its local SQLite recovery store preserves the
  request and latest measurements. Recovery closes the valve and publishes a
  final interrupted result instead of restarting the cycle; an unpublished
  completion remains queued with a stable message ID.
- **FO process restart:** an operator's stable Kafka group resumes from its
  last committed offset. A completion left unprocessed can be displayed when
  that operator reconnects, without replaying all topic history to every new
  FO process. Run one active FO consumer per operator; members of one Kafka
  group divide partitions instead of receiving broadcast copies.
- **Concurrent stations:** each station has a separate Engine consumer group,
  Monitor socket, Engine socket, and irrigation controller. Local concurrent
  watering has been exercised for two stations.

Kafka consumers use manual commits after their handlers succeed. This gives
at-least-once processing, not exactly-once side effects in every crash window;
the database's unique request IDs and guarded transitions provide the
application-level idempotency. The local single-node Kafka broker has no broker
redundancy. Topics use three partitions by default. Activation requests,
commands, and station events key by `ws_id`; operator notifications key by
`operator_id`. This retains order per entity within each topic and allows
partition-level parallelism; there is no global ordering across keys or topics.

## 6. Local deployment and verification

`docker-compose.local.yml` starts Kafka, Central, two Monitor/Engine station
pairs, and an on-demand FO profile. Kafka advertises `kafka:29092` to Compose
clients and `localhost:9092` to host clients. Central's SQLite file is under
`/data` on `central-data`; Kafka uses a separate `kafka-data` volume. The
[local deployment guide](local-docker-deployment.md) documents build, startup,
operator launch, status/log checks, and safe restart commands.

Reported verification for this submission:

- Latest full automated run: **266 tests passed, no skips**, with
  `RUN_KAFKA_INTEGRATION=1 RUN_KAFKA_SECURE_INTEGRATION=1
  KAFKA_BOOTSTRAP=localhost:9092`. This includes the real-broker lifecycle,
  partition-ordering, and temporary SASL_SSL broker tests. Railway has not been
  deployed or tested.
- Local Docker smoke checks, station/Engine recovery, Central restart recovery,
  and overlapping irrigation on both local stations have been exercised.
- The checks verified request authorization, irrigation start, telemetry,
  completion measurements, and persistence/reconnection behavior. The Docker
  guide contains the repeatable commands; no captured screenshots or raw
  acceptance-test transcript are currently kept in the repository.

These results describe local testing only. A container build or local test run
does not establish cloud operation or the laboratory network topology.

## 7. Decisions and limits

The principal design decision is to keep durable administrative/fault facts
separate from fast-changing connectivity and derived display status. This
avoids restoring stale `AVAILABLE` or `WATERING` labels after a Central restart.
Kafka is used where producer and consumer lifetimes are independent; the socket
protocol remains for the existing Monitor relationships and one-second
health-check exchange. SQLite is sufficient for the single Central process in
Release 1 and is protected by schema constraints and a persistent local volume.

Still pending:

- **Railway:** deploy Central, Kafka, and persistent SQLite storage, then verify
  that the cloud services remain available. No Railway deployment is claimed.
- **Two-PC acceptance:** run multiple FO instances from one laboratory PC and
  multiple Monitor/Engine station instances from another, using the actual
  cloud-to-lab network addresses. This final acceptance test is pending.
- **Operational evidence:** capture and retain the final assessment logs or
  screenshots, and confirm the instructor can access the repository.
- **Assessment readiness:** both students should be prepared to explain the
  state model, concurrency, socket framing, Kafka correlation, and the recovery
  paths described here.

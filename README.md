# WaterManagement — Distributed Systems Release 1

WaterManagement coordinates irrigation across independently running watering
stations. It accepts operator requests, checks station and operator state,
starts irrigation, reports live measurements, and records the final result.
The design keeps the existing Monitor socket protocols, uses Kafka for
asynchronous requests and events, and uses SQLite for Central's durable state.

## Applications and infrastructure

| Component | Responsibility |
| --- | --- |
| `WM_Central` | Registers stations, authenticates operators, authorizes requests, maintains the monitoring/control terminal, and records irrigation results. |
| `WM_WS_M` (Monitor) | Registers its station ID and location with Central; performs one-second Engine health checks; reports faults and reconnects. |
| `WM_WS_E` (Engine) | Requires a successful Monitor handshake before irrigation, operates the valve and flow meter, consumes Central commands, and publishes progress and completion. |
| `WM_FO` (Field Operator) | Submits station/duration requests and displays authorization, progress, denial, and final results for its operator ID. |
| Kafka | Carries activation requests, station commands, Engine events, and operator-facing events using the contracts in [`docs/kafka-message-contract.md`](docs/kafka-message-contract.md). |
| SQLite | Stores station registration and administrative state, operators, faults, and irrigation requests/results. Central uses a persistent Compose volume locally. |

Monitor–Central registration/fault messages and Monitor–Engine handshakes and
health checks use the existing framed TCP protocol. Kafka carries the
asynchronous irrigation workflow:

1. FO publishes `ACTIVATION_REQUEST` with station, operator, duration, and a
   request ID.
2. Central checks the operator and the station's derived state, records the
   request, publishes `START_IRRIGATION`, and informs FO of authorization or
   denial.
3. Engine accepts a start only while its Monitor session is established. It
   publishes `IRRIGATION_STARTED`, cumulative telemetry, and a final result.
4. Central persists the final duration and volume and forwards progress and
   completion to the matching operator.

Central derives station status from connectivity, faults, administrative
blocking, and active irrigation. The visible states are `DISCONNECTED`,
`LEAK`, `OUT_OF_SERVICE` (shown as “Out of Service”), `WATERING`, and
`AVAILABLE`. BLOCK and ACTIVATE persist administrative state; START requests
irrigation through Central's terminal controls. SQLite constraints and request
IDs support idempotent processing. Kafka consumers manually commit after
successful handling.

The recovery paths include Engine valve closure and reconnect backoff when its
Monitor connection is lost, Monitor re-registration after a real Central
reconnect, Central recovery of pending Kafka station events after a restart,
and FO resumption of operator results from its committed Kafka offset. These
behaviors do not make the local single-broker setup highly available.

## Run locally with Docker

This is a **local development deployment**, not a Railway deployment. It starts
Central, Kafka, and two Monitor/Engine pairs. FO runs on demand in a terminal.
Prerequisites are Docker Desktop (or Docker Engine) and Compose v2. Run from the
repository root:

```sh
docker compose -f docker-compose.local.yml config
docker compose -f docker-compose.local.yml build
docker compose -f docker-compose.local.yml up -d
docker compose -f docker-compose.local.yml ps
docker compose -f docker-compose.local.yml logs --tail=100 central kafka ws1-monitor ws1-engine ws2-monitor ws2-engine
```

Check that Central and Kafka are healthy and both Monitors register and exchange
health checks. To run an interactive operator (the default development operator
is `OP_001`):

```sh
docker compose -f docker-compose.local.yml run --no-deps --rm -it fo \
  --kafka-host kafka --kafka-port 29092 --operator-id OP_001
```

The container uses Kafka at `kafka:29092`; host-based tools use
`localhost:9092`. Central's Monitor socket is `central:5000` inside Compose and
is published on host port `5000` by default. If that host port is occupied, set
`CENTRAL_PUBLISHED_PORT=15000` before Compose commands. Station IDs, locations,
ports, database path, and other local settings can be overridden with the
variables listed in [`docs/local-docker-deployment.md`](docs/local-docker-deployment.md).

For a quick Kafka/topic and persisted-result check:

```sh
docker compose -f docker-compose.local.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
docker compose -f docker-compose.local.yml exec central python -c \
  "import sqlite3; c=sqlite3.connect('/data/wm_central.db'); print(c.execute('SELECT request_id, ws_id, status, duration_s, total_volume_l, end_reason FROM irrigations ORDER BY requested_at DESC LIMIT 10').fetchall())"
```

Stop the stack without removing its persisted Kafka or SQLite data:

```sh
docker compose -f docker-compose.local.yml stop
```

Use `down` only when removing the containers and network is desired. Do not use
`down --volumes` if you need to preserve the named volumes.

The complete startup, verification, restart, port, and persistence procedure is
in the [local Docker deployment guide](docs/local-docker-deployment.md).

## Automated tests

Install the application dependency with `python3 -m pip install -r requirements.txt`.
The unit suite can be run with:

```sh
python3 -m unittest discover -v
```

The real-broker integration test is opt-in and requires the local Kafka broker
to be running with its host listener on port 9092:

```sh
RUN_KAFKA_INTEGRATION=1 python3 -m unittest tests.test_kafka_real_integration -v
```

The test also accepts `KAFKA_BOOTSTRAP` when Kafka is at another address. The
suite covers Central persistence/state transitions, socket framing, Monitor and
Engine behavior, Kafka transport and integration, and FO request/event handling.

## Submission documents

- [Implementation and verification report](docs/report.md)
- [Local Docker deployment guide](docs/local-docker-deployment.md)
- [Kafka message contract](docs/kafka-message-contract.md)
- [Central state and SQLite ADR](docs/adr/ADR-001-state-model-and-schema.md)
- [AI prompt index and reconstructed summaries](prompts/README.md)
- [Student names](Nombres.txt)

The full Railway topology and the final two-PC lab acceptance test are separate
from this local Compose setup. Neither is claimed as completed here; see the
report for the current verification status and remaining work.

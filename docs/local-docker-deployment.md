# Local Docker deployment

This Compose deployment runs Kafka, Central with SQLite, and two independent
watering stations (each with its own Monitor and Engine). FO containers are
started on demand. It uses the repository's existing Python module entry
points and does not alter the Kafka message or socket contracts.

## Requirements

- Docker Desktop (or a Docker Engine with Compose v2).
- Free host ports `5000` and `9092` by default.

The shared app image is built once from the root `Dockerfile`. It contains the
application packages and Python dependency only; it excludes local databases,
virtual environments, Git metadata, and graph output. SQLite is written to the
named `central-data` volume at `/data/wm_central.db`. Kafka has a separate named
`kafka-data` volume.

On the machine used for this verification, host port 5000 was already occupied.
To use the tested setup, prefix each Compose command below with
`CENTRAL_PUBLISHED_PORT=15000`. This only changes Central's host-published port;
Monitors continue to connect to `central:5000` inside Compose. On a machine
where host port 5000 is free, omit that prefix.

## Build and validate

Run from the repository root:

```sh
docker compose -f docker-compose.local.yml config
docker compose -f docker-compose.local.yml build
```

Optional environment overrides can be placed in the shell or a local `.env`
file (the `.env` file is excluded from the image build context):

| Variable | Default | Purpose |
|---|---:|---|
| `CENTRAL_PUBLISHED_PORT` | `5000` | Host port mapped to Central's socket port |
| `CENTRAL_SOCKET_PORT` | `5000` | Central socket port inside the Compose network |
| `KAFKA_PUBLISHED_PORT` | `9092` | Kafka host-client port |
| `DB_PATH` | `/data/wm_central.db` | SQLite path; keep it under `/data` for persistence |
| `WS1_ID`, `WS1_LOCATION` | `WS_001`, `North Garden` | First station identity and location |
| `WS2_ID`, `WS2_LOCATION` | `WS_002`, `South Garden` | Second station identity and location |
| `WS1_ENGINE_PORT`, `WS2_ENGINE_PORT` | `6001`, `6002` | Monitor listener ports inside their separate containers |
| `MONITOR_HEALTH_INTERVAL` | `1.0` | Seconds between Monitor-to-Engine health checks |
| `FO_OPERATOR_ID` | `OP_001` | Default seeded operator for the on-demand FO service |

The sample Central command seeds development operators `OP_001`–`OP_003` on
startup. Stations enroll themselves with their configured IDs and locations.

## Start and inspect the stack

```sh
docker compose -f docker-compose.local.yml up -d
docker compose -f docker-compose.local.yml ps
docker compose -f docker-compose.local.yml logs --tail=100 central kafka ws1-monitor ws1-engine ws2-monitor ws2-engine
```

Wait until `kafka` and `central` report healthy. Check station registration and
Engine health-check logs:

```sh
docker compose -f docker-compose.local.yml logs --since=2m ws1-monitor ws1-engine ws2-monitor ws2-engine
```

The Monitor defaults to one-second health checks. Central binds to `0.0.0.0`
inside the container. Monitors use the Compose DNS name `central`; Engines use
their matching Monitor service names (`ws1-monitor` and `ws2-monitor`). Their
ports `6001` and `6002` are internal only and do not collide because they live
in separate containers.

Before the application containers start, the one-shot `kafka-init` service
creates the four contract topics with `--if-not-exists`. This is safe to rerun;
it does not delete topics or messages.

## Start Field Operators

An interactive FO is a separate, on-demand container. Start one per terminal;
each process has an independent Kafka consumer group:

```sh
docker compose -f docker-compose.local.yml run --no-deps --rm -it fo \
  --kafka-host kafka --kafka-port 29092 --operator-id OP_001
```

Start another FO in another terminal, changing the operator ID as needed:

```sh
docker compose -f docker-compose.local.yml run --no-deps --rm -it fo \
  --kafka-host kafka --kafka-port 29092 --operator-id OP_002
```

For a single request, add the station/duration flags to the interactive command:

```sh
docker compose -f docker-compose.local.yml run --no-deps --rm -it fo \
  --kafka-host kafka --kafka-port 29092 --operator-id OP_001 \
  --ws-id WS_001 --duration-seconds 4
```

After the final result, type `q` and press Enter to exit the FO. The default
request timeout is 300 seconds. `--no-deps` avoids re-running the one-shot topic
initializer; use it only after the complete stack has been started.

## Kafka and ports

Kafka advertises two endpoints:

- Containers use `kafka:29092` on the Compose network (`INTERNAL` listener).
- Host tools use `localhost:${KAFKA_PUBLISHED_PORT:-9092}` (`HOST` listener).

The Compose file publishes host port 9092 to Kafka's container port 9092; the
internal listener on 29092 is not published. Do not configure container clients
with `localhost`: inside a container, that points back to that same container.
Central's Monitor socket is published at
`${CENTRAL_PUBLISHED_PORT:-5000}` on the host and is reached by Monitors as
`central:${CENTRAL_SOCKET_PORT:-5000}` inside Compose.

For example, a host-based FO process can use
`--kafka-host localhost --kafka-port 9092`; an FO container must use
`--kafka-host kafka --kafka-port 29092`.

## Restart, logs, and persistence

```sh
docker compose -f docker-compose.local.yml restart central
docker compose -f docker-compose.local.yml logs -f central ws1-monitor ws2-monitor
docker compose -f docker-compose.local.yml stop
docker compose -f docker-compose.local.yml start
docker compose -f docker-compose.local.yml down
```

`restart`, `stop`, `start`, and `down` preserve named volumes. Do not add
`--volumes`/`-v` to `down` if you need to retain SQLite or Kafka data. Central
loads registered station IDs and locations from SQLite and initially shows
them disconnected; the two Monitors should reconnect and register again.

## Verification and limitations

Compose validation and image builds only establish configuration/build
success. Before relying on the deployment, verify both registrations, matching
Engine-to-Monitor connections, health checks, FO authorization, start and
telemetry events, completion measurements in SQLite, overlapping requests on
both stations, and station reconnection after restarting Central without
removing `central-data`.

The local broker is a single-node development broker with no redundancy. This
file does not deploy anything to Railway. Railway will need separate service
configuration, persistent storage suitable for its runtime, external Kafka
addresses/security settings, and lab-to-cloud network access. Do not reuse the
local-only advertised `localhost` Kafka listener for Railway clients.


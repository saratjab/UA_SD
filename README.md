# WaterManagement

WaterManagement is a distributed irrigation simulator for the Distributed Systems practical (course 2026/27). It manages watering stations (WS), accepts irrigation requests from field operators (FO), and tracks station health and irrigation telemetry.

## System overview

The application has three components:

- **WM_Central** keeps the SQLite station/operator state, authorizes irrigation, handles station control, and monitors station connections. It communicates with station monitors over TCP and exchanges request, command, and event messages through Kafka.
- **WM_WS_M (Monitor)** registers its station with Central, supervises its Engine over TCP, and reports health and status changes.
- **WM_WS_E (Engine)** simulates the valve and flow meter, processes authorized irrigation, and publishes progress and completion events through Kafka. Its SQLite state file is persisted in a Docker volume so in-flight work can be recovered after a restart.
- **WM_FO (Field Operator)** lists available stations, submits duration or volume requests, and displays their results. An Engine can also use its local menu to start/stop irrigation and simulate or clear a fault.

Kafka is the event bus for operator requests and irrigation lifecycle events. TCP connects Central to station monitors and each monitor to its Engine. SQLite stores Central's registry and state, plus Engine recovery state.

## Requirements

- Docker Engine or Docker Desktop with the Docker Compose v2 plugin
- Git

No Python installation or IDE is needed to run the application instances. The application image is built from the repository's `Dockerfile` and includes the pinned runtime dependency range from `requirements.txt`.

## Run the complete system locally with Docker

This starts Kafka, topic initialization, Central, two stations (each with a Monitor and Engine), and an optional FO. All containers join the `watermanagement-local` network.

From the repository root:

```bash
docker compose -f docker-compose.local.yml up -d --build
docker compose -f docker-compose.local.yml ps
docker compose -f docker-compose.local.yml logs -f central ws1-monitor ws1-engine
```

The local compose file seeds development operators (`OP_001`–`OP_003`) into the persistent Central SQLite database. Default stations are `WS_001` and `WS_002`; their Engine state is stored in named Docker volumes. To change IDs, locations, or ports, set the corresponding `WS1_*` / `WS2_*` variables before starting Compose. Kafka is published on `localhost:9092`, Central's station socket on `localhost:5000`, and its local admin API on `localhost:8001`.

Start an interactive FO in a terminal:

```bash
docker compose --profile operator -f docker-compose.local.yml run --rm -it fo
```

The default operator is `OP_001`. To use a different seeded operator, prefix the command with `FO_OPERATOR_ID=OP_002` (or `OP_003`). The FO lists available stations; enter a station ID and a positive duration in seconds. Type `q` to exit.

Several FOs can run and send requests at the same time, but **each FO must use its own operator ID**. If two operators request the same station at once, Central authorizes exactly one; the other receives `ACTIVATION_DENIED WS_BUSY`. An operator ID belongs to one FO process at a time: the FO's Kafka consumer group is named after the ID so that a restarted FO can still show the results of requests it sent before the restart, and two processes sharing one ID would split that group and one of them would miss its replies.

The Engine menu can be opened with:

```bash
docker attach watermanagement-local-ws1-engine-1
```

Use `m` to request manual irrigation, `s` to stop it, `f` to simulate an Engine fault, `c` to clear the fault, and `q` to quit the Engine. Detach while leaving the Engine running with `Ctrl+P`, then `Ctrl+Q`.

Stop the local stack and preserve its SQLite data with:

```bash
docker compose -f docker-compose.local.yml down
```

`down -v` removes Central and Engine state volumes as well as Kafka data.

## Run Dockerized FO and WS instances against Railway

This layout keeps WM_Central, its SQLite database, and Kafka in Railway and runs every FO and WS container on local PCs. Do not start a second local Central for this setup. Each FO container and each WS container is created by Docker Compose; the WS consists of separate Monitor and Engine containers.

### One-time setup on each PC

1. Clone the repository and check out the deployment branch that contains the corrected Compose entry points:

   ```bash
   git clone https://github.com/saratjab/UA_SD.git
   cd UA_SD
   ```

2. Place the deployment-provided `.env.railway` in the repository root and the Kafka CA certificate at `secrets/railway-kafka-ca.pem`. Set `KAFKA_CA_CERT_PATH` in `.env.railway` to the certificate's actual absolute path on that PC. The env file must contain the Railway Central host/port, Kafka host/port, Kafka SASL credentials, and CA path used by the Compose files.

3. Build the application image once on each PC:

   ```bash
   docker build -t watermanagement-app:local .
   ```

Keep `.env.railway`, Kafka credentials, and the CA certificate private; do not commit them.

### Start a Field Operator

Use an operator ID that exists and is active in the deployed Central database. `OP_003` was verified in the deployment runbook; IDs vary by Central database.

```bash
docker compose \
  --profile operator \
  --env-file .env.railway \
  -f docker-compose.local.yml \
  -f docker-compose.railway.yml \
  run --rm --no-deps -it \
  --entrypoint python fo -m WM_FO.WM_FO \
  --kafka-host <KAFKA_TCP_PROXY_HOST> \
  --kafka-port <KAFKA_TCP_PROXY_PORT> \
  --operator-id OP_003
```

Replace the Kafka host and port with the external bootstrap endpoint from the deployment env file. At the prompt, choose an available station and enter an irrigation duration. Type `q` to stop the FO; Compose removes its one-off container.

### Start a Watering Station

Run each station in its own terminal. Use an authorized, unique station ID and an active Central operator ID for the local Engine menu.

```bash
export WS_ID=WS_004
export WS_LOCATION="West Garden"
export LOCAL_OPERATOR_ID=OP_003

docker compose \
  --project-name wm-ws-ws-004 \
  --env-file .env.railway \
  -f docker-compose.station.yml \
  up -d --no-build monitor engine
```

The station Compose file starts both the Monitor and Engine. It connects the Monitor to Railway Central over TCP and the Engine to Railway Kafka using SASL_SSL and the mounted CA certificate. The Engine state volume persists across `stop` and `up` operations.

Check and follow the instance:

```bash
docker compose --project-name wm-ws-ws-004 --env-file .env.railway -f docker-compose.station.yml ps
docker compose --project-name wm-ws-ws-004 --env-file .env.railway -f docker-compose.station.yml logs -f --tail=100
```

Both services should remain `Up`. Logs should show station registration, Engine connection/health checks, and `HEALTH_OK` reports. For another station, choose a new `WS_ID`, set its location, and use a distinct Compose project name (for example `wm-ws-ws-005`). Export the three variables again in each new terminal before using the station Compose file.

### Stop and administer instances

Stop a station while retaining its Engine state, then start it again with the same project name and IDs:

```bash
docker compose --project-name wm-ws-ws-004 --env-file .env.railway -f docker-compose.station.yml stop
docker compose --project-name wm-ws-ws-004 --env-file .env.railway -f docker-compose.station.yml up -d --no-build monitor engine
```

Open the Engine menu with `docker attach wm-ws-ws-004-engine-1`. Detach with `Ctrl+P`, then `Ctrl+Q`. Exit an FO by typing `q`.

The Central admin menu is a local client and does not start another Central server:

```bash
python3 -m WM_Central.admin_client --url https://<CENTRAL_ADMIN_HOST>
```

It prompts for the admin token. The menu can display station status, start/stop irrigation, and block or activate stations. Keep the token private.

## Functional checks

With a WS and FO running, submit a short duration request (for example, five seconds). A successful flow is submitted → authorized → started → telemetry/progress → completed, after which Central reports the station as available. The simulated default flow rate is 6 L/min.

The Engine menu also supports these checks:

- `m`, then `s`: manual irrigation followed by a manual stop; completion should report `MANUAL_STOP`.
- `f`: simulate an Engine fault; the Monitor reports it and Central marks the station as leaking/unavailable.
- `c`: clear the fault; healthy monitoring resumes and the station becomes available again.
- Central admin choices `4` and `6`: block and reactivate one station, respectively.

Use a known station and active operator. A unique local ID is not automatically authorized by Central.

## Configuration and implementation entry points

| Component | Python module | Main configuration |
|---|---|---|
| Central | `WM_Central.WM_Central` | TCP socket, Kafka, SQLite path, optional admin API |
| Field Operator | `WM_FO.WM_FO` | Kafka endpoint, registered operator ID, optional request file or one-off request |
| Station Monitor | `WM_WS.WM_WS_M.WM_WS_M` | Station ID/location, Central endpoint, Engine listener and health-check timing |
| Station Engine | `WM_WS.WM_WS_E.WM_WS_E` | Kafka endpoint, Monitor endpoint, station state path, local operator ID |

`docker-compose.local.yml` defines the local broker, Central, two local stations, and the FO profile. `docker-compose.station.yml` is the standalone Railway-connected WS deployment. `docker-compose.railway.yml` overlays Railway networking/security settings onto the local services for an FO container. `docker-compose.kafka.yml` provides a standalone local Kafka broker and topic initializer.

## Repository layout

```text
WM_Central/       Central state, TCP server, Kafka service, SQLite schema and admin API
WM_FO/            Field Operator CLI and Kafka request/result handling
WM_WS/WM_WS_M/    Station Monitor and Central/Engine TCP coordination
WM_WS/WM_WS_E/    Station Engine, simulated irrigation, telemetry and recovery state
common/           Shared socket protocol and Kafka contracts/transport
docker-compose*.yml, Dockerfile   Container build and deployment configurations
examples/         Example automated irrigation request data
tests/            Unit and integration tests
```

# Docker deployment

The Compose setup runs Central, two Watering Station Monitor/Engine pairs, and
an on-demand Field Operator. Kafka and Central endpoints are configurable at
startup; the application protocols and default station/operator IDs are
unchanged. Run the commands below from the repository root with Docker Compose
v2.24 or later.

## Local Docker mode

With no endpoint variables set, the existing local setup is used:

- Kafka: `kafka:29092`
- Central for Monitors: `central:5000`

Build and start the local stack:

```sh
docker compose -f docker-compose.local.yml up -d --build
```

The one-shot `kafka-init` service creates `wm.activation.requests`,
`wm.ws.commands`, `wm.ws.events`, and `wm.operator.events` with three partitions
by default and replication factor one for the single-broker development setup.
Set `KAFKA_PARTITIONS` or `KAFKA_REPLICATION_FACTOR` to override those creation
defaults. On startup it safely increases an existing topic's partition count
when needed; it never reduces partitions, deletes topics/records, or resets
consumer offsets. Partition expansion changes key-to-partition mapping, so
ordering across the expansion is not guaranteed for affected keys.

Both Compose files mount [`config/kafka/server.properties`](../config/kafka/server.properties)
at `/mnt/shared/config`, the file-input location supported by the pinned
`apache/kafka:3.8.0` image. Compose `KAFKA_*` environment variables override
the file for listener addresses and the replication setting. `KAFKA_BROKER_NAME`
defaults to `kafka`; if it is changed, use that same value for the local client
bootstrap host (`KAFKA_HOST`) and broker quorum/listeners. The Compose service
is still named `kafka` for dependencies.

Local container clients use `kafka:29092`; host processes use
`localhost:9092` (or `KAFKA_PUBLISHED_PORT`). These are local Compose listener
addresses. Railway services must use the broker's private hostname and internal
listener; lab PCs must use a TCP Proxy or other externally reachable Kafka
endpoint. A Compose hostname such as `kafka` does not resolve across PCs.

Central's numbered terminal menu remains optional; non-TTY startup and stdin
EOF do not stop Central. The authenticated HTTP admin API is disabled by
default (`CENTRAL_ADMIN_PORT=0`) and has no default token. To enable it for a
local demonstration, the Compose host mapping is loopback-only at
`127.0.0.1:8001`. In a shell, enter a private token without putting it in shell
history, then recreate Central:

```sh
export CENTRAL_ADMIN_PORT=8001
export CENTRAL_ADMIN_REQUIRE_HTTPS=false
read -rsp 'Local admin token: ' CENTRAL_ADMIN_TOKEN; echo
export CENTRAL_ADMIN_TOKEN
docker compose -f docker-compose.local.yml up -d --build central
```

Run the menu client on that PC:

```sh
python -m WM_Central.admin_client --url http://127.0.0.1:8001
```

The client prompts for the token if `CENTRAL_ADMIN_TOKEN` is not set. The
local exception for HTTP is limited to loopback; use HTTPS for any remote
host. Never expose the admin port through a public TCP proxy. Do not use the
Monitor socket or Kafka as an unauthenticated admin control channel.

Run a Field Operator when needed:

```sh
docker compose -f docker-compose.local.yml run --no-deps --rm -it fo
```

The defaults are stations `WS_001` and `WS_002`, and operator `OP_001`.

## Assignment minimum: Railway and two lab PCs

The assignment's minimum deployment has **Central, its SQLite database, and
Kafka in Railway**. One lab PC runs Field Operators; a second lab PC runs each
Watering Station's Monitor and Engine. Docker is used on both PCs. The Railway
overlay below disables the local Central and Kafka services so there is one
Central instance and one Kafka system.

After creating the Railway services, attach Central's persistent volume
at `/data`, and expose TCP proxies for Central's internal port `5000` and Kafka's
client listener port `9092`. For the Railway service settings and Central start
configuration, see
[`railway-deployment.md`](railway-deployment.md). The values below are the
placeholders for generated proxy endpoints; fill them from your own Railway
environment after creating its TCP proxies:

```sh
export KAFKA_HOST='<KAFKA_TCP_PROXY_HOST>'
export KAFKA_PORT='<KAFKA_TCP_PROXY_PUBLIC_PORT>'
export CENTRAL_HOST='<CENTRAL_TCP_PROXY_HOST>'
export CENTRAL_PORT='<CENTRAL_TCP_PROXY_PUBLIC_PORT>'
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_REQUIRE_SECURITY=true
export KAFKA_SASL_MECHANISM=PLAIN
export KAFKA_SASL_USERNAME='<GENERATED_KAFKA_USERNAME>'
read -rsp 'Kafka password: ' KAFKA_SASL_PASSWORD; echo
export KAFKA_SASL_PASSWORD
export KAFKA_SSL_CA_PEM="$(cat /secure/path/wm-kafka-ca.pem)"
```

Use the same Kafka settings on both PCs. The broker certificate must have SANs
for both the private Railway hostname and the public TCP proxy hostname. The
clients verify the CA chain and endpoint hostname; do not disable verification.
The public hostname used here must be the exact public DNS SAN on the Kafka
certificate (use a Railway custom TCP proxy domain if necessary).
Keep the Kafka password in Railway secrets and enter it at the prompt. Do not
commit it or put it in a shared `.env` file.

On **each lab PC**, from the repository root, build the app image:

```sh
docker build -t watermanagement-app:local .
```

On the **Field Operator PC**, run one command per operator (use a separate
terminal for each interactive operator):

```sh
docker compose -f docker-compose.local.yml -f docker-compose.railway.yml \
  run --no-deps --rm -it fo
```

On the **Watering Station PC**, start both station pairs:

```sh
docker compose -f docker-compose.local.yml -f docker-compose.railway.yml \
  up -d ws1-monitor ws1-engine ws2-monitor ws2-engine
```

The Monitor connects to Railway Central using `CENTRAL_HOST` and
`CENTRAL_PORT`. Each Engine connects to Railway Kafka using `KAFKA_HOST` and
`KAFKA_PORT`, and connects to its matching Monitor over the local Docker
network. Field Operators also connect to Railway Kafka. Keep these four
environment variables set in the terminal that runs each Compose command (or
put them in a `.env` file in the repository root on each PC).

Create or expand the required topics on the external broker. Set the desired
partition and replication values (defaults shown); use a replication factor no
greater than the broker count. This safe-to-repeat command leaves existing
records and consumer offsets untouched and never reduces partition counts:

```sh
export KAFKA_PARTITIONS=3
export KAFKA_REPLICATION_FACTOR=1
docker run --rm \
  -v "$PWD/scripts/kafka-init-topics.sh:/kafka-init-topics.sh:ro" \
  -e KAFKA_BOOTSTRAP_SERVERS="$KAFKA_HOST:$KAFKA_PORT" \
  -e KAFKA_PARTITIONS -e KAFKA_REPLICATION_FACTOR \
  apache/kafka:3.8.0 /bin/bash -ec /kafka-init-topics.sh
```

TCP Proxy addresses are for clients outside Railway. Services running inside
Railway should use Kafka's private service hostname and internal listener port,
and Central's private hostname with port `5000`.

For a single-machine **local development** run, use only
`docker-compose.local.yml`; it starts local Kafka and Central with defaults
`kafka:29092` and `central:5000`.

## Endpoint variables

| Variable | Local default | Used by |
|---|---|---|
| `KAFKA_HOST` | `kafka` | Central, Engines, Field Operator |
| `KAFKA_BROKER_NAME` | `kafka` | Broker hostname/quorum and network alias |
| `KAFKA_PORT` | `29092` | Central, Engines, Field Operator |
| `CENTRAL_HOST` | `central` | Both Monitors |
| `CENTRAL_PORT` | `CENTRAL_SOCKET_PORT` (default `5000`) | Both Monitors |
| `CENTRAL_SOCKET_PORT` | `5000` | Central's local listener |
| `CENTRAL_PUBLISHED_PORT` | `5000` | Host port mapped to local Central |
| `KAFKA_PUBLISHED_PORT` | `9092` | Host port mapped to the local Kafka broker |
| `DB_PATH` | `/data/wm_central.db` | Central's SQLite database |
| `ENGINE_STATE_PATH` | unset (legacy per-station filename) | Optional override for one Engine's recovery SQLite file |

Other station IDs, locations, and Engine ports remain configurable with
`WS1_ID`, `WS1_LOCATION`, `WS2_ID`, `WS2_LOCATION`, `WS1_ENGINE_PORT`, and
`WS2_ENGINE_PORT`. Compose stores Central's SQLite data and local Kafka data in
named volumes.

The local Compose file gives `ws1-engine` and `ws2-engine` independent named
volumes mounted at `/engine-state`, with distinct per-station SQLite filenames.
These volumes preserve interrupted-irrigation recovery data across Engine
container recreation. Keep them when cleaning up the local stack; do not use
`docker compose down -v` if recovery state must be retained. For Railway Kafka
from either lab PC, set `KAFKA_REQUIRE_SECURITY=true` together with
`KAFKA_SECURITY_PROTOCOL=SASL_SSL`, credentials, and the trusted CA. Local
Compose defaults the guard to false and PLAINTEXT for the local-only broker.

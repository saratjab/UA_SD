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

After creating the Railway services, attach Central's persistent volume at
`/data` and expose TCP proxies for Central's internal port `5000` and Kafka's
client listener port `9092`. For Railway service settings and Central startup,
see [`railway-deployment.md`](railway-deployment.md).

On **each lab PC**, copy `.env.railway.example` to `.env.railway`, enter the
allocated Kafka and Central TCP proxy hosts/ports and generated Kafka SASL
credentials, then place the public Kafka CA certificate at the configured
`KAFKA_CA_CERT_PATH` (default `secrets/railway-kafka-ca.pem`, which is ignored
by Git; custom paths should be outside the repository or under `secrets/`).
Keep the same Railway endpoint and credentials on both PCs. The file sets the
container CA path to `/run/secrets/kafka/ca.pem`; the Railway Compose overlay
mounts the CA read-only in Central, both Engines, the topic initializer, and
the Field Operator. The certificate must validate the exact public Kafka TCP
proxy hostname. Keep the credentials in the ignored `.env.railway` file and
never commit it.

Run the one-time configuration and certificate check on both PCs:

```sh
./scripts/setup-railway-local.sh
```

On **PC1 (Field Operator)**, run an interactive operator when needed:

```sh
docker compose --env-file .env.railway \
  -f docker-compose.local.yml -f docker-compose.railway.yml \
  --profile operator run --build --rm -it fo
```

On **PC2 (Watering Stations)**, start both station pairs with the standard
command:

```sh
docker compose --env-file .env.railway \
  -f docker-compose.local.yml -f docker-compose.railway.yml \
  up -d --build ws1-monitor ws1-engine ws2-monitor ws2-engine
```

Each Monitor reaches Railway Central using `CENTRAL_HOST` and `CENTRAL_PORT`.
Engines and Field Operators reach Railway Kafka using `KAFKA_HOST` and
`KAFKA_PORT`. The overlay disables the local Kafka and Central containers;
using `docker-compose.local.yml` alone still runs the local PLAINTEXT mode.
The TCP proxy placeholders are `<KAFKA_TCP_PROXY_HOST>`,
`<KAFKA_TCP_PROXY_PUBLIC_PORT>`, `<CENTRAL_TCP_PROXY_HOST>`, and
`<CENTRAL_TCP_PROXY_PUBLIC_PORT>`; replace them with the endpoints allocated
in your Railway project.

Create or expand the required topics on the external broker when needed. The
Railway overlay supplies the same SASL_SSL settings and read-only CA mount to
the topic initializer. The command is safe to repeat; it leaves existing
records and consumer offsets untouched and never reduces partition counts:

```sh
docker compose --env-file .env.railway \
  -f docker-compose.local.yml -f docker-compose.railway.yml \
  --profile railway-disabled run --no-deps --rm kafka-init
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
from either lab PC, use `.env.railway` and `docker-compose.railway.yml`. Local
Compose defaults the guard to false and PLAINTEXT for the local-only broker.

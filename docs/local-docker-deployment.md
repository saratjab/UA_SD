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
`wm.ws.commands`, `wm.ws.events`, and `wm.operator.events` with
`--if-not-exists`. Re-running it does not delete topics or messages.

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

First deploy Central and Kafka in Railway, attach Central's persistent volume
at `/data`, and expose TCP proxies for Central's internal port `5000` and for
Kafka. For the Railway service settings and Central startup command, see
[`railway-deployment.md`](railway-deployment.md). The values below are the
provided proxy endpoints for clients outside Railway:

```sh
export KAFKA_HOST=yamanote.proxy.rlwy.net
export KAFKA_PORT=44500
export CENTRAL_HOST=iriguchi.proxy.rlwy.net
export CENTRAL_PORT=20893
```

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

Create the required topics on the external broker if they are not already
present. Run from either PC; this safe-to-repeat command creates missing topics
and does not delete topics or messages:

```sh
for topic in wm.activation.requests wm.ws.commands wm.ws.events wm.operator.events; do
  docker run --rm apache/kafka:3.8.0 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server yamanote.proxy.rlwy.net:44500 \
    --create --if-not-exists --topic "$topic"
done
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
| `KAFKA_PORT` | `29092` | Central, Engines, Field Operator |
| `CENTRAL_HOST` | `central` | Both Monitors |
| `CENTRAL_PORT` | `CENTRAL_SOCKET_PORT` (default `5000`) | Both Monitors |
| `CENTRAL_SOCKET_PORT` | `5000` | Central's local listener |
| `CENTRAL_PUBLISHED_PORT` | `5000` | Host port mapped to local Central |
| `KAFKA_PUBLISHED_PORT` | `9092` | Host port mapped to the local Kafka broker |
| `DB_PATH` | `/data/wm_central.db` | Central's SQLite database |

Other station IDs, locations, and Engine ports remain configurable with
`WS1_ID`, `WS1_LOCATION`, `WS2_ID`, `WS2_LOCATION`, `WS1_ENGINE_PORT`, and
`WS2_ENGINE_PORT`. Compose stores Central's SQLite data and local Kafka data in
named volumes.

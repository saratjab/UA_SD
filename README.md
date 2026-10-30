# WaterManagement Distributed System

Release 1 contains Central, the Watering Station Monitor and Engine, and the
Field Operator application. For the complete local Docker deployment, including
two independently configured stations, Kafka listeners, persistent SQLite,
verification steps, and Railway preparation notes, see
[`docs/local-docker-deployment.md`](docs/local-docker-deployment.md).

## Connect a new lab PC to Railway Kafka

Use this setup on each lab PC that will run a Field Operator or Watering
Station. Railway Central and Kafka stay hosted; the lab PCs need the Railway
Kafka TCP proxy endpoint, Central TCP proxy endpoint, SASL credentials, and the
public CA certificate that signs the broker certificate.

From the repository root on each PC:

```sh
cp .env.railway.example .env.railway
mkdir -p secrets
```

Edit `.env.railway` with the allocated TCP proxy hostnames and ports, and the
generated Kafka username and password. Copy the public Kafka CA certificate to
`secrets/railway-kafka-ca.pem` (or change `KAFKA_CA_CERT_PATH` to a path outside
the repository). The file sets `KAFKA_SSL_CA_LOCATION` to the mounted
container path `/run/secrets/kafka/ca.pem`. `.env.railway` and certificate
files are ignored by Git.
Keep the same Railway endpoint and Kafka credentials on both PCs; operator and
station IDs can be configured independently in that file.

Run the one-time checker on each PC. It checks required settings, validates the
CA certificate, and verifies both local and Railway Compose configurations
without displaying credential values:

```sh
./scripts/setup-railway-local.sh
```

For **PC1**, running a Field Operator, start the operator when needed:

```sh
docker compose --env-file .env.railway \
  -f docker-compose.local.yml -f docker-compose.railway.yml \
  --profile operator run --build --rm -it fo
```

For **PC2**, running both local watering stations, the standard start command
is:

```sh
docker compose --env-file .env.railway \
  -f docker-compose.local.yml -f docker-compose.railway.yml \
  up -d --build ws1-monitor ws1-engine ws2-monitor ws2-engine
```

The Engine menus are available by attaching to a station Engine, for example
`docker compose ... attach ws1-engine`. Railway mode mounts the CA read-only in
Central, both Engines, and the Field Operator container. The local-only Compose
command remains unchanged and uses the local PLAINTEXT Kafka broker.

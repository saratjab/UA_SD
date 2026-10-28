# First Railway deployment runbook

This guide is a preparation checklist; no Railway project, service, domain,
TCP proxy, secret, or volume has been created by this repository change. Railway
does not deploy a Compose file as one Docker Compose runtime. Create one Railway
service per Compose service and enter service variables, volumes, health checks,
and networking in the Railway project. Railway's current Compose guide describes
this translation and the settings that must be applied manually:
[Deploy a Docker Compose app](https://docs.railway.com/guides/docker-compose).

The examples below use placeholders only. Replace every `<...>` value after
Railway allocates it. Do not reuse endpoints from another project or environment.
The `fix/transaction` changes must be pushed to a reviewed repository revision
before Railway can build them; this runbook does not push or deploy anything.

## 1. Create the project and Kafka service

1. In Railway, create an **Empty Project** and select the environment for the
   demo. Use the same project and environment for Kafka and Central.
2. Add a service from this repository. Set its source root to the repository
   root and set its Dockerfile path to `Dockerfile.kafka`. The production image
   uses `apache/kafka:3.8.0`, the separate
   `config/kafka/server.railway.properties`, and a fail-closed startup script.
   The local Compose broker remains a distinct PLAINTEXT-only development
   setup bound to the host loopback interface.
3. Name the service `Kafka` if you want to use the example Railway reference
   `${{Kafka.RAILWAY_PRIVATE_DOMAIN}}`; otherwise enter that service's actual
   private domain as `<KAFKA_PRIVATE_HOST>`.
4. Set up Kafka for an initial **private-only** deployment. Use the actual
   `<KAFKA_PRIVATE_HOST>` from Railway's private networking settings, set
   `KAFKA_PUBLIC_LISTENER_ENABLED=false`, and omit `KAFKA_PUBLIC_HOST` and
   `KAFKA_PUBLIC_PORT`. Generate a server certificate with that private name
   in its SAN. The startup script enables only the authenticated `INTERNAL`
   SASL_SSL listener in this phase. This avoids a bootstrap dependency on the
   public TCP proxy, which Railway allocates after the service exists.
5. Add the following variables, based on
   [`config/kafka/railway.env.example`](../config/kafka/railway.env.example):

   ```text
   KAFKA_NODE_ID=1
   KAFKA_PROCESS_ROLES=broker,controller
   KAFKA_LISTENERS=INTERNAL://:29092,CONTROLLER://127.0.0.1:9093
   KAFKA_PRIVATE_HOST=<KAFKA_PRIVATE_HOST>
   KAFKA_PUBLIC_LISTENER_ENABLED=false
   KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:SASL_SSL,CONTROLLER:PLAINTEXT
   KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL
   KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER
   KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093
   KAFKA_LOG_DIRS=/var/lib/kafka/data
   KAFKA_SASL_MECHANISM=PLAIN
   KAFKA_SASL_USERNAME=<GENERATED_KAFKA_USERNAME>
   KAFKA_SASL_PASSWORD=<GENERATED_URLSAFE_32_CHAR_SECRET>
   KAFKA_TLS_KEYSTORE_B64=<BASE64_PKCS12_KEYSTORE_SECRET>
   KAFKA_TLS_KEYSTORE_PASSWORD=<KEYSTORE_PASSWORD_SECRET>
   KAFKA_TLS_CA_CERT_B64=<BASE64_PUBLIC_CA_CERTIFICATE>
   KAFKA_NUM_PARTITIONS=3
   KAFKA_DEFAULT_REPLICATION_FACTOR=1
   KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
   KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1
   KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1
   KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0
   ```

   After Kafka is running, create the TCP proxy on internal port `9092`, then
   set `KAFKA_PUBLIC_LISTENER_ENABLED=true`, `KAFKA_PUBLIC_HOST` to the
   allocated proxy hostname, and `KAFKA_PUBLIC_PORT` to its allocated port.
   Startup constructs the listener and advertised addresses itself and rejects
   plaintext client listeners. Kafka 3.8 supports SASL/PLAIN with SASL_SSL in this combined
   single-node KRaft configuration. The broker's JAAS user is provisioned from
   these environment secrets at startup; this is a single application user,
   not Kafka SCRAM provisioning or per-topic ACL management.
6. Generate a private CA and a server certificate. Put the CA private key in offline protected storage;
   upload only the server PKCS#12 keystore, its password, the public CA
   certificate, and the Kafka username/password as Railway secrets. Example
   commands (run on a secure admin workstation; never commit generated files):

   ```sh
   openssl genrsa -out wm-kafka-ca.key 4096
   openssl req -x509 -new -key wm-kafka-ca.key -sha256 -days 3650 \
     -subj '/CN=WaterManagement Kafka private CA' \
     -addext 'basicConstraints=critical,CA:TRUE' \
     -addext 'keyUsage=critical,keyCertSign,cRLSign' -out wm-kafka-ca.pem
   openssl genrsa -out wm-kafka-server.key 3072
   openssl req -new -key wm-kafka-server.key -subj '/CN=<KAFKA_PUBLIC_HOST>' \
     -out wm-kafka-server.csr
   cat > wm-kafka-san.ext <<'EOF'
   subjectAltName=DNS:<KAFKA_PRIVATE_HOST>
   extendedKeyUsage=serverAuth
   keyUsage=digitalSignature,keyEncipherment
   EOF
   openssl x509 -req -in wm-kafka-server.csr -CA wm-kafka-ca.pem \
     -CAkey wm-kafka-ca.key -CAcreateserial -days 825 -sha256 \
     -extfile wm-kafka-san.ext -out wm-kafka-server.pem
   openssl pkcs12 -export -name kafka -inkey wm-kafka-server.key \
     -in wm-kafka-server.pem -certfile wm-kafka-ca.pem \
     -out wm-kafka-server.p12
   openssl rand -hex 32  # use as KAFKA_SASL_PASSWORD
   openssl rand -hex 24  # use as KAFKA_TLS_KEYSTORE_PASSWORD
   base64 < wm-kafka-server.p12 | tr -d '\n'  # set KAFKA_TLS_KEYSTORE_B64 as a secret
   base64 < wm-kafka-ca.pem | tr -d '\n'     # set KAFKA_TLS_CA_CERT_B64
   ```

   For the initial private-only deployment, use the exact private Railway DNS
   name in the SAN (no scheme or port). The startup verifier checks validity,
   the PKIX chain to the configured CA, serverAuth usage, and an exact
   case-insensitive `dNSName` SAN for every enabled listener; wildcards, CN-only
   names, URI SANs, ports, and partial hostname matches are rejected. The CA
   certificate must be a valid CA with `CA:TRUE` and `keyCertSign`. After
   creating the TCP proxy, update the SAN to
   `DNS:<KAFKA_PRIVATE_HOST>,DNS:<KAFKA_TCP_PROXY_HOST>`, regenerate the signed
   server certificate and PKCS#12 keystore, and replace the Railway secrets.
   Enable the external listener and redeploy. Startup rejects a keystore
   without the SAN for every enabled listener hostname.
   If Railway's assigned TCP proxy hostname cannot be represented in a
   certificate you can provision, configure a custom TCP proxy domain first,
   use that exact DNS name for `KAFKA_PUBLIC_HOST`, and include it in the SAN.
   PCs must connect using that same hostname (and Railway's allocated public
   port); never disable certificate or hostname verification.
   Generate a URL-safe random password of at least 32 characters for
   `KAFKA_SASL_PASSWORD`, a separate strong keystore password, and store each
   in Railway's secret-variable UI. `KAFKA_TLS_CA_CERT_B64` is the public CA
   certificate and may be copied to clients; its private signing key must not
   be uploaded to Railway or either lab PC.
7. Attach a Railway volume to Kafka at `/var/lib/kafka/data`. This is a separate
   manual volume from Central's SQLite volume. The repository does not create
   either Railway volume. If Railway's volume permissions prevent this image
   from writing, set `RAILWAY_RUN_UID=0` on that service.
8. Deploy the private-only Kafka service and check its KRaft logs. Confirm
   Central can connect to `<KAFKA_PRIVATE_HOST>:29092`. Then create the Kafka
   TCP Proxy for internal port `9092`, record its hostname and public port,
   update the certificate and public listener variables, and redeploy. Check
   the final logs for both client listeners. The public TCP proxy transports raw Kafka TCP; TLS and client
   authentication are enforced by Kafka itself. A bad/missing endpoint,
   keystore, SAN, CA certificate, or credential stops broker startup.
   Bootstrap Central at `<KAFKA_PRIVATE_HOST>:29092`; PCs bootstrap at the
   public TCP proxy. Kafka metadata advertises the corresponding listener to
   each client. The controller listener binds to loopback and remains
   PLAINTEXT for the single-node KRaft quorum only.

## 2. Initialize the four topics

Run the existing topic initializer from a machine with Docker after Kafka is
healthy and the external listener has been advertised. Use the allocated TCP
proxy endpoint below; do not substitute a remembered hostname or port:

```sh
export KAFKA_BOOTSTRAP_SERVERS='<KAFKA_TCP_PROXY_HOST>:<KAFKA_TCP_PROXY_PUBLIC_PORT>'
export KAFKA_PARTITIONS=3
export KAFKA_REPLICATION_FACTOR=1
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_REQUIRE_SECURITY=true
export KAFKA_SASL_MECHANISM=PLAIN
export KAFKA_SASL_USERNAME='<GENERATED_KAFKA_USERNAME>'
read -rsp 'Kafka password: ' KAFKA_SASL_PASSWORD; echo
export KAFKA_SASL_PASSWORD
export KAFKA_SSL_CA_PEM="$(cat wm-kafka-ca.pem)"
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS \
  -e KAFKA_PARTITIONS \
  -e KAFKA_REPLICATION_FACTOR \
  -e KAFKA_REQUIRE_SECURITY -e KAFKA_SECURITY_PROTOCOL -e KAFKA_SASL_MECHANISM \
  -e KAFKA_SASL_USERNAME -e KAFKA_SASL_PASSWORD -e KAFKA_SSL_CA_PEM \
  -v "$PWD/scripts/kafka-init-topics.sh:/kafka-init-topics.sh:ro" \
  --entrypoint /bin/bash apache/kafka:3.8.0 -ec /kafka-init-topics.sh
```

The script creates the four required topics if missing and increases a topic's
partition count only when it is below the configured count. It does not delete
topics or records, reduce partition counts, or reset consumer offsets. Existing
topics with more than three partitions stay unchanged. Confirm all four topic
names and partition counts in the script output or with Kafka's topic CLI before
starting clients. A single broker requires replication factor `1` and is a
single point of failure; three partitions permit partition-level parallelism,
not broker redundancy.

## 3. Create Central from the application Dockerfile

1. Add a second service from the same reviewed repository revision. Set the
   root directory to the repository root and use the root `Dockerfile`. It now
   starts headless Central with `python -m WM_Central.main`; do not set an
   interactive start command.
2. Create a Central volume and attach it with mount path `/data`. This is a
   manual dashboard step. Until you create and attach it, SQLite is ephemeral;
   do not assume a volume exists because the image creates the directory.
3. Configure the following Central variables. Set `CENTRAL_ADMIN_TOKEN` as a
   Railway secret using a password manager or a cryptographically secure
   generator. Never place the secret in source, logs, a URL, or a start command.

   | Variable | Value |
   |---|---|
   | `CENTRAL_SOCKET_HOST` | `0.0.0.0` |
   | `CENTRAL_SOCKET_PORT` | `5000` |
   | `KAFKA_HOST` | `<KAFKA_PRIVATE_HOST>` |
   | `KAFKA_PORT` | `29092` |
   | `KAFKA_SECURITY_PROTOCOL` | `SASL_SSL` |
   | `KAFKA_REQUIRE_SECURITY` | `true` (fails closed if secure settings are missing or downgraded) |
   | `KAFKA_SASL_MECHANISM` | `PLAIN` |
   | `KAFKA_SASL_USERNAME` | Same generated username as Kafka service |
   | `KAFKA_SASL_PASSWORD` | Same value as Kafka service, stored as Railway secret |
   | `KAFKA_SSL_CA_PEM` | Public CA certificate PEM; trust anchor, not a private key |
   | `DB_PATH` | `/data/wm_central.db` |
   | `PORT` | `8080` (or another free internal HTTP port) |
   | `CENTRAL_ADMIN_HOST` | `0.0.0.0` |
   | `CENTRAL_ADMIN_REQUIRE_HTTPS` | `true` |
   | `CENTRAL_ADMIN_TOKEN` | Secret value; no default |
   | `RAILWAY_RUN_UID` | `0` for the root-mounted volume with this image's non-root user |

   Leave `CENTRAL_ADMIN_PORT` unset so Central takes its admin HTTP port from
   Railway's `PORT`. If you set `CENTRAL_ADMIN_PORT`, it takes precedence and
   must equal `PORT`. The station TCP listener defaults to `5000` and can be
   overridden by `CENTRAL_SOCKET_PORT`.
4. Generate a Railway HTTP domain for Central and ensure it routes to the same
   internal port as `PORT`. Railway terminates TLS at the public edge and sets
   `X-Forwarded-Proto: https`; Central requires that marker when HTTPS
   enforcement is enabled. Railway redirects public plain HTTP to HTTPS, and
   Central rejects requests without the HTTPS marker. Verify the behavior using
   the public HTTPS domain and the supplied admin client.
5. Separately create a TCP Proxy for Central's internal socket port
   `CENTRAL_SOCKET_PORT` (`5000` by default). Record its allocated
   `<CENTRAL_TCP_PROXY_HOST>` and `<CENTRAL_TCP_PROXY_PUBLIC_PORT>` for the
   Monitor on PC 2. Do **not** create a TCP Proxy for the admin HTTP port.
   The admin client must use the public HTTPS domain, not the TCP proxy.

The listeners are separate: the station protocol binds `CENTRAL_SOCKET_HOST` /
`CENTRAL_SOCKET_PORT`; the authenticated admin HTTP server binds
`CENTRAL_ADMIN_HOST` / `PORT`. The admin API has no default token. Invalid ports,
missing token with admin enabled, failed binds, and failed listener startup are
reported and stop startup rather than silently selecting another interface.
Central does not require an interactive terminal and continues if stdin is
closed.

## 4. Run lab clients

Use the generated Railway endpoints from the same environment. Do not put the
secret into a checked-in `.env` file.

### PC 1: Field Operator

Build the repository application image locally, then launch only the operator
service with the external Kafka endpoint:

```sh
docker compose -f docker-compose.local.yml build central
export KAFKA_HOST='<KAFKA_TCP_PROXY_HOST>'
export KAFKA_PORT='<KAFKA_TCP_PROXY_PUBLIC_PORT>'
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_REQUIRE_SECURITY=true
export KAFKA_SASL_MECHANISM=PLAIN
export KAFKA_SASL_USERNAME='<GENERATED_KAFKA_USERNAME>'
read -rsp 'Kafka password: ' KAFKA_SASL_PASSWORD; echo
export KAFKA_SASL_PASSWORD
export KAFKA_SSL_CA_PEM="$(cat wm-kafka-ca.pem)"
export FO_OPERATOR_ID='OP_001'
docker compose -f docker-compose.local.yml --profile operator run --rm fo
```

The FO uses the same Kafka topics and its configured consumer group. Use the
interactive FO prompt or the documented JSONL request file under `examples/`.

### PC 2: Monitor and Engine pairs

Build the same image, set Central's generated TCP proxy endpoint and Kafka's
generated public TCP endpoint, then start the Monitor/Engine pairs. The overlay
removes Central/Kafka as local services and directs the lab station stack to
Railway:

```sh
docker compose -f docker-compose.local.yml build central
export CENTRAL_HOST='<CENTRAL_TCP_PROXY_HOST>'
export CENTRAL_PORT='<CENTRAL_TCP_PROXY_PUBLIC_PORT>'
export KAFKA_HOST='<KAFKA_TCP_PROXY_HOST>'
export KAFKA_PORT='<KAFKA_TCP_PROXY_PUBLIC_PORT>'
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_REQUIRE_SECURITY=true
export KAFKA_SASL_MECHANISM=PLAIN
export KAFKA_SASL_USERNAME='<GENERATED_KAFKA_USERNAME>'
read -rsp 'Kafka password: ' KAFKA_SASL_PASSWORD; echo
export KAFKA_SASL_PASSWORD
export KAFKA_SSL_CA_PEM="$(cat wm-kafka-ca.pem)"
docker compose -f docker-compose.local.yml -f docker-compose.railway.yml \
  up --build ws1-monitor ws1-engine ws2-monitor ws2-engine
```

The overlay mounts separate named volumes `ws1-engine-state` and
`ws2-engine-state` at `/engine-state`; each Engine writes its own SQLite
recovery file there (the filename includes its station ID). Preserve those
volumes when replacing/restarting Engine containers. Back them up with the
normal Docker volume backup procedure; do not use `docker compose down -v`.
`ENGINE_STATE_PATH` can override the path for a single Engine process when
running outside Compose. Without it, the legacy per-station working-directory
filename remains in effect for backward compatibility.

The Compose defaults identify the two demo stations as `WS_001` and `WS_002`;
override the `WS1_*` / `WS2_*` variables when the Central database uses other
station IDs or locations. Keep the one-second Monitor health-check interval.

### Operator PC: remote Central menu

From the repository root, run:

```sh
export CENTRAL_ADMIN_URL='https://<CENTRAL_HTTPS_DOMAIN>'
read -rsp 'Central admin token: ' CENTRAL_ADMIN_TOKEN; echo
export CENTRAL_ADMIN_TOKEN
python -m WM_Central.admin_client
```

The client hides token input when the variable is not set. All admin operations,
including status reads, require the token. The client refuses public `http://`
URLs. Use only the HTTPS domain over public networks.

## 5. Validate, observe, and recover

1. In Railway's deployment view, confirm Kafka and Central are active. Inspect
   each service's build and deploy logs. Kafka should report KRaft startup and
   listener setup; Central should log its station socket listener on `5000`,
   admin HTTP listener on `PORT`, database initialization, and Kafka consumer
   assignments. Do not configure the authenticated admin API as a public
   unauthenticated healthcheck.
2. From PC 1 or PC 2, verify Kafka protocol connectivity with Kafka 3.8.0's
   `kafka-broker-api-versions.sh --bootstrap-server
   <KAFKA_TCP_PROXY_HOST>:<KAFKA_TCP_PROXY_PUBLIC_PORT>`. Verify Central TCP
   connectivity with `nc -vz <CENTRAL_TCP_PROXY_HOST>
   <CENTRAL_TCP_PROXY_PUBLIC_PORT>`.
3. From the operator PC, run `python -m WM_Central.admin_client` against the
   HTTPS domain. Confirm it lists both stations. Submit a short test request and
   confirm its final result, Central logs, station logs, and operator result.
4. Confirm the four topics have at least three partitions. Confirm Central
   startup has recovered its SQLite outbox and any interrupted irrigation. Do
   not delete Kafka topics, records, or consumer offsets during recovery.
5. Before a risky release, use Railway's volume backup feature for Central's
   `/data` volume and retain the backup. Roll back the Central service to the
   previous known-good deployment through Railway's deployment history; keep
   the volume attached and do not restore an older database over newer data
   without a deliberate recovery decision. A rollback of code does not reverse
   SQLite schema/data changes or Kafka topic partition increases.
6. If Kafka is unavailable, keep Central's SQLite volume intact and inspect
   Kafka/Central logs. Restarting Central retries pending outbox records; the
   topic initializer can be rerun safely. Do not reset offsets to troubleshoot
   the outbox. A single-broker outage still interrupts Kafka traffic until that
   broker recovers.

## Settings that must be created manually in Railway

Repository files cannot allocate these project resources or discover their
generated public addresses. Complete each in Railway and record the results:

- Create the project, environment, Kafka and Central services, service names,
  source branch/revision, and Dockerfile paths.
- Add Railway variables and secret variables; reference the Kafka private
  hostname from Central.
- Create and attach Kafka's `/var/lib/kafka/data` and Central's `/data` volumes.
- Create Kafka TCP Proxy on internal `9092`; copy its allocated host/port into
  `KAFKA_PUBLIC_HOST`, `KAFKA_PUBLIC_PORT`, and lab client variables. Regenerate
  the Kafka certificate with both endpoint SANs, enable the public listener,
  and redeploy Kafka.
- Create Central TCP Proxy on internal `5000`; set lab `CENTRAL_HOST` and
  `CENTRAL_PORT` from its allocated host/port.
- Generate Central's HTTPS domain on the `PORT` listener; do not create an
  admin TCP proxy.
- Initialize topics by running the repository script after Kafka is reachable.
- Select the monitoring/health approach and inspect deployment logs. Railway's
  service healthcheck settings are dashboard configuration; this app does not
  expose an unauthenticated HTTP health route.
- Review backups, deployment rollback target, replica count, and region. Keep
  Central at one replica while using a local SQLite volume.

## Deployment limits

Kafka is a single broker and Central uses a single SQLite file, so neither has
high availability. SASL/PLAIN authenticates clients over TLS, but Kafka
authorization ACLs are not configured; every authenticated Kafka client shares
the single broker application identity. Railway configuration and cross-network
behavior must be verified after resources are created; the successful local
secure broker test does not validate Railway DNS, proxy allocation, volume
permissions, TLS routing, or project settings. Do not claim the Railway broker
is secured until a real connection through both the private listener and the
public TCP proxy has been tested.

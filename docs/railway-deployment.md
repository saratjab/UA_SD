# Railway deployment preparation

This guide configures the Railway side for the assignment: Central, a Kafka
broker, and persistent SQLite storage. It does not claim a cloud deployment is
validated. Validation requires pushing this branch, deploying it from Railway,
and connecting lab clients to the deployed services.

## Central service

Create a Railway service from this repository. Configure it to build from the
repository root using the existing `Dockerfile`. Select the `feature/docker_and_railway`
branch after it has been pushed; do not deploy the older `main` branch.

Set the service's custom start command to:

```sh
/bin/sh -c 'exec python -m WM_Central.main --socket-host 0.0.0.0 --socket-port "$CENTRAL_SOCKET_PORT" --kafka-host "$KAFKA_HOST" --kafka-port "$KAFKA_PORT" --db-path "$DB_PATH"'
```

The shell wrapper expands environment variables in the command. These flags
match `WM_Central.main`: socket port, Kafka host and port, and database path.
Central binds to `0.0.0.0` so Railway can route the configured TCP port to it.

Add these service variables:

| Variable | Value |
|---|---|
| `CENTRAL_SOCKET_PORT` | `5000` |
| `KAFKA_HOST` | The Kafka service's private hostname, such as `${{Kafka.RAILWAY_PRIVATE_DOMAIN}}` |
| `KAFKA_PORT` | The actual Kafka listener port reachable over Railway private networking |
| `DB_PATH` | `/data/wm_central.db` |
| `RAILWAY_RUN_UID` | `0` so the process can write to Railway's root-mounted volume with this image's non-root default user |

The port is a project configuration value, not a Railway-assigned external
port. The source requires `--socket-port`, `--kafka-host`, and `--kafka-port`;
it only reads `DB_PATH` directly from the environment when no CLI override is
passed. `WM_Central/config.py` sets the socket host default to `0.0.0.0`.

## Persistent SQLite volume

Attach a Railway volume to the Central service with mount path:

```text
/data
```

Keep `DB_PATH=/data/wm_central.db` so the SQLite file and its WAL sidecar files
are on the persistent volume. The app's local default is `wm_central.db`, which
would be inside the container filesystem and would not persist across deploys.
Railway mounts volumes as root; `RAILWAY_RUN_UID=0` is set above because the
existing Docker image declares a non-root `app` user.

## Kafka connectivity

Provision Kafka in the same Railway project and environment as Central. Set
`KAFKA_HOST` to its private hostname and `KAFKA_PORT` to its actual internal
listener port. Central's Python code constructs the bootstrap address from
these two values, so do not pass a combined `host:port` string as `KAFKA_HOST`.

The lab Field Operator and Watering Station Engine also connect directly to
Kafka using `--kafka-host` and `--kafka-port`. If those clients run outside
Railway, they also need a reachable Kafka endpoint. Configure a TCP Proxy for
Kafka or use a broker endpoint reachable from the lab; Central's private
network address alone will not work from the lab PCs.

## Central socket TCP Proxy

In the Central service's Railway networking settings, create a TCP Proxy for
the internal port `5000` (the value of `CENTRAL_SOCKET_PORT`). Railway supplies
an external hostname and proxy port. Give that generated pair to the lab
Monitors as their `--central-host` and `--central-port` values. Use the
generated external port for clients; do not substitute it for Central's
internal listen port.

Railway services in the same project/environment can use private networking
for internal connections. Lab machines outside Railway must use the TCP Proxy
address for Central.

## Deploy and validate

1. Push `feature/docker_and_railway` to GitHub.
2. In Railway, select that branch as the Central service source and deploy it.
3. Confirm the deployment uses this repository's root `Dockerfile` and the
   start command above.
4. Confirm Central logs show its socket listener on port `5000` and successful
   Kafka startup. This app has no HTTP health endpoint; do not configure an
   HTTP healthcheck path for it.
5. Confirm the Railway volume is attached at `/data`, then restart/redeploy
   Central and verify the SQLite file and registered station records persist.
6. From a lab machine, connect a Monitor to the TCP Proxy's generated host and
   port. Verify station enrollment and health checks in Central logs.
7. Verify FO and Engine can reach Kafka from the lab, then run an irrigation
   request through to its completion summary.

Only mark the cloud deployment validated after the pushed branch is deployed
and these cross-network checks pass. The deployment branch and service settings
must correspond to the commit being demonstrated.

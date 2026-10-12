# WaterManagement Kafka message contract

All records are UTF-8 JSON envelopes with `event_type`, UUID `message_id`, UTC
ISO-8601 `timestamp`, object `payload`, and the applicable `ws_id`,
`operator_id`, and `request_id`. Consumers reject malformed envelopes. All
durations are seconds and all volumes are litres.

| Topic | Producer -> consumer | Event types |
| --- | --- | --- |
| `wm.activation.requests` | FO -> Central | `ACTIVATION_REQUEST` |
| `wm.ws.commands` | Central -> Engine | `START_IRRIGATION`, `STOP_IRRIGATION`, `BLOCK_WS`, `ACTIVATE_WS` |
| `wm.ws.events` | Engine -> Central | `IRRIGATION_STARTED`, `IRRIGATION_TELEMETRY`, `IRRIGATION_COMPLETED`, `COMMAND_REJECTED` |
| `wm.operator.events` | Central -> FO | `ACTIVATION_AUTHORIZED`, `ACTIVATION_DENIED`, `ACTIVATION_STATUS`, `IRRIGATION_STARTED`, `IRRIGATION_PROGRESS`, `IRRIGATION_COMPLETED`, `COMMAND_REJECTED` |

## FO request and response interface

FO publishes an `ACTIVATION_REQUEST` to `wm.activation.requests`:

```json
{"event_type":"ACTIVATION_REQUEST","message_id":"uuid","timestamp":"2026-10-11T12:00:00Z","ws_id":"WS_001","operator_id":"OP_001","request_id":"uuid","payload":{"duration_seconds":30}}
```

`request_id` must be a stable UUID for retries. `duration_seconds` is a positive
whole number. Central validates that operator and station exist, the operator is
active, and that the connected station is AVAILABLE. A duplicate request gets an
`ACTIVATION_STATUS`; it never starts a second irrigation. FO consumes
`wm.operator.events` with an operator-specific group and filters `operator_id`.

For `START_IRRIGATION`, the Engine receives the same IDs and a payload of
`{"duration_seconds": N}`. Engine emits `IRRIGATION_STARTED` only after its
valve/controller starts. Each `IRRIGATION_TELEMETRY` payload is:

```json
{"sequence":1,"flow_rate_lpm":6.0,"accumulated_volume_liters":0.1,"duration_seconds":1.0}
```

`accumulated_volume_liters` is cumulative from this irrigation; Central replaces
the prior reading only for a newer sequence. Completion payloads contain
`end_reason`, `duration_seconds`, and `accumulated_volume_liters`. Supported wire
reasons: `TIME_LIMIT`, `MANUAL_STOP`, `CENTRAL_STOP`, `STATION_BLOCKED`,
`LEAK_DETECTED`, `ENGINE_FAULT`.

Central groups are `wm-central-activation-v1` and `wm-central-events-v1`; an
Engine uses `wm-ws-engine-<ws_id>-v1`. Consumers disable auto commit and commit
only after successful handling. SQLite request uniqueness and terminal updates
provide idempotency, but a process crash after a broker publish and before offset
commit can produce a duplicate notification/command; consumers must remain
idempotent. This is at-least-once delivery, not a claim of exactly-once execution.

## Local development

Install dependencies with `python3 -m pip install -r requirements.txt`, then run
`docker compose -f docker-compose.kafka.yml up -d`. Start Central, Monitor, and
Engine respectively:

```sh
python3 -m WM_Central.main --socket-port 5000 --kafka-host localhost --kafka-port 9092 --seed-dev
python3 -m WM_WS.WM_WS_M.main --ws-id WS_001 --location field-a --central-host localhost --central-port 5000 --engine-host 127.0.0.1 --engine-port 5001
python3 -m WM_WS.WM_WS_E.main --ws-id WS_001 --kafka-host localhost --kafka-port 9092 --monitor-host 127.0.0.1 --monitor-port 5001
```

Run unit tests with `python3 -m unittest discover -v`. Real-broker tests are
opt-in: `RUN_KAFKA_INTEGRATION=1 python3 -m unittest discover -v`; they require
the compose broker and `confluent-kafka`.

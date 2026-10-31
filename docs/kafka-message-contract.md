# WaterManagement Kafka message contract

All records are UTF-8 JSON envelopes with `event_type`, UUID `message_id`, UTC
ISO-8601 `timestamp`, object `payload`, and the applicable `ws_id`,
`operator_id`, and `request_id`. Consumers reject malformed envelopes. All
durations are seconds and all volumes are litres.

All four descriptive topic names are retained. Kafka keys follow one policy:

| Topic | Message key |
| --- | --- |
| `wm.activation.requests` | `ws_id` |
| `wm.ws.commands` | `ws_id` |
| `wm.ws.events` | `ws_id` |
| `wm.operator.events` | `operator_id` |

The shared publisher applies this policy, including Engine events and Central
outbox retries. Central stores the key alongside each outbox record. A key
routes that entity's records to one partition, where Kafka preserves append
order. Different keys may be processed in parallel and have no ordering
relationship, and Kafka does not provide a total order across topics. Consumer
groups distribute partitions among members; they do not
broadcast a partition to every member. Three partitions are configured by
default using `KAFKA_PARTITIONS`; `KAFKA_REPLICATION_FACTOR` defaults to `1` for
single-broker development. Partitioning supports consumer parallelism, but a
single broker remains a single point of failure.

Increasing partition count is additive: it preserves records and consumer
offsets and does not delete topics. Existing higher counts are left untouched.
However, the key-to-partition mapping can change after expansion, so ordering
across records written before and after the increase is not guaranteed for
affected keys.

| Topic | Producer -> consumer | Event types |
| --- | --- | --- |
| `wm.activation.requests` | FO -> Central | `ACTIVATION_REQUEST` |
| `wm.ws.commands` | Central -> Engine | `START_IRRIGATION`, `STOP_IRRIGATION`, `BLOCK_WS`, `ACTIVATE_WS` |
| `wm.ws.events` | Engine -> Central | `IRRIGATION_STARTED`, `IRRIGATION_TELEMETRY`, `IRRIGATION_COMPLETED`, `COMMAND_REJECTED` |
| `wm.operator.events` | Central -> FO | `ACTIVATION_AUTHORIZED`, `ACTIVATION_DENIED`, `ACTIVATION_STATUS`, `IRRIGATION_STARTED`, `IRRIGATION_PROGRESS`, `IRRIGATION_COMPLETED`, `COMMAND_REJECTED` |

## FO request and response interface

FO publishes an `ACTIVATION_REQUEST` to `wm.activation.requests`:

Use exactly one of positive `duration_seconds`, positive `target_volume_liters`,
or neither field for watering until manually stopped. The two target modes
cannot be combined.

```json
{"event_type":"ACTIVATION_REQUEST","message_id":"uuid","timestamp":"2026-10-11T12:00:00Z","ws_id":"WS_001","operator_id":"OP_001","request_id":"uuid","payload":{"duration_seconds":30}}
```

`request_id` must be a stable UUID for retries. `duration_seconds`, when present,
is a positive whole number. Central validates that operator and station exist, the operator is
active, and that the connected station is AVAILABLE. A duplicate request gets an
`ACTIVATION_STATUS`; it never starts a second irrigation. FO consumes
`wm.operator.events`, filters by `operator_id`, and correlates each event to a
pending request.

For `START_IRRIGATION`, the Engine receives the same IDs and a payload of
`{"duration_seconds": N}`, `{"target_volume_liters": N}`, or `{}` for manual-stop
mode. Target-volume runs end once measured cumulative volume reaches or exceeds
the target. Engine emits `IRRIGATION_STARTED` only after its valve/controller
starts. Each `IRRIGATION_TELEMETRY` payload is:

```json
{"sequence":1,"flow_rate_lpm":6.0,"accumulated_volume_liters":0.1,"duration_seconds":1.0}
```

`accumulated_volume_liters` is cumulative from this irrigation; Central replaces
the prior reading only for a newer sequence. Completion payloads contain
`end_reason`, `duration_seconds`, and `accumulated_volume_liters`. Supported wire
reasons: `TIME_LIMIT`, `TARGET_VOLUME`, `MANUAL_STOP`, `CENTRAL_STOP`, `STATION_BLOCKED`,
`LEAK_DETECTED`, `ENGINE_FAULT`, `MONITOR_DISCONNECTED`. The latter reports
that the Engine safely stopped watering after losing its Monitor connection;
Central records `DISCONNECTED` as the end reason. If Central's socket-side
disconnect transition wins the race, the later completion contributes final
measurements and the actual valve-stop timestamp while preserving Central's
recorded end reason.

Engine `COMMAND_REJECTED` uses reason `ENGINE_BLOCKED` or `ENGINE_FAILED`.
Central records the irrigation as `DENIED` with that exact reason and forwards
it to FO.
`ACTIVATION_AUTHORIZED` means Central accepted the request; an Engine rejection
can still follow if Engine state changed before command delivery.

Central groups are `wm-central-activation-v1` and `wm-central-events-v1`; an
Engine uses `wm-ws-engine-<ws_id>-v1`. FO uses a stable
`wm-field-operator-<operator_id>-v1` group. A returning operator resumes at the
group's last committed offset and can receive results produced while its FO
process was down. A new operator group starts at the latest offset, so it does
not replay old topic history. Different operators have separate groups and FO
filters each event by `operator_id`. Run one active FO consumer per operator at
a time: Kafka group members load-balance partitions rather than broadcasting
each event to every same-operator terminal. FO displays a completed event even
when its original in-memory request waiter was lost to a process crash, and
deduplicates terminal request IDs within the process. Consumers disable auto
commit and commit only after successful handling. This remains at-least-once
delivery; a crash after display but before offset commit can result in a
repeated display after restart.

## Local development

Both compose files run the same topic initializer. New topics use the configured
partition and replication counts. Existing topics with fewer partitions are
expanded with Kafka's partition increase operation; topics with higher counts
are left alone. The initializer does not lower partition counts, reset consumer
offsets, or delete topics/records.

### Automated Field Operator request file

Use `--request-file examples/automated_irrigation_requests.jsonl`. The UTF-8
JSONL file contains one object per line. Each record has required non-empty
`ws_id` and optionally one of `duration_seconds` (positive whole seconds) or
`target_volume_liters` (positive number). Omit both for manual-stop mode. Unknown
fields, malformed lines, empty files, invalid values, and mixed modes are errors;
records are not silently skipped. The FO waits for each denial or completion,
then waits four seconds before submitting the next request. An indefinite-mode
record therefore holds the sequence until the station is stopped through the
supported station/Central control interface. Progress and completion output
includes station/request IDs, elapsed time, total volume, and completion reason.

The sample demonstrates all three modes and places indefinite watering last.

```sh
python3 -m WM_FO.main --kafka-host localhost --kafka-port 9092 \
  --operator-id OP_001 --request-file examples/automated_irrigation_requests.jsonl
```

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

CREATE TABLE IF NOT EXISTS operators (
    operator_id TEXT    PRIMARY KEY CHECK (length(trim(operator_id)) > 0),
    name        TEXT,
    active      INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1))
);

CREATE TABLE IF NOT EXISTS kafka_outbox (
    message_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    message_key TEXT,
    message_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    delivered_at TEXT
);

-- The flush loop reads undelivered rows (delivered_at IS NULL) every second
-- and pruning deletes by delivered_at. Equal keys are stored in rowid order,
-- so "ORDER BY rowid" needs no extra sort.
CREATE INDEX IF NOT EXISTS ix_outbox_pending ON kafka_outbox (delivered_at);

CREATE TABLE IF NOT EXISTS watering_stations (
    ws_id         TEXT PRIMARY KEY CHECK (length(trim(ws_id)) > 0),
    location      TEXT NOT NULL    CHECK (length(trim(location)) > 0),
    admin_state   TEXT NOT NULL DEFAULT 'ACTIVE'
                  CHECK (admin_state IN ('ACTIVE', 'BLOCKED')),
    registered_at TEXT NOT NULL,
    last_seen_at  TEXT
);

CREATE TABLE IF NOT EXISTS faults (
    fault_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    ws_id       TEXT NOT NULL REFERENCES watering_stations (ws_id),
    fault_type  TEXT NOT NULL,
    details     TEXT,
    detected_at TEXT NOT NULL,
    resolved_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_faults_one_open_per_ws
    ON faults (ws_id) WHERE resolved_at IS NULL;


CREATE TABLE IF NOT EXISTS irrigations (
    irrigation_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id           TEXT NOT NULL UNIQUE,
    ws_id                TEXT NOT NULL REFERENCES watering_stations (ws_id),
    operator_id          TEXT          REFERENCES operators (operator_id),
    status               TEXT NOT NULL
                         CHECK (status IN ('DENIED', 'AUTHORIZED', 'WATERING', 'ENDED')),
    deny_reason          TEXT
                         CHECK (deny_reason IN ('WS_DISCONNECTED', 'WS_LEAK',
                                                'WS_OUT_OF_SERVICE', 'WS_BUSY',
                                                'OPERATOR_INACTIVE', 'ENGINE_BLOCKED',
                                                'ENGINE_FAILED', 'MONITOR_UNAVAILABLE')),
    requested_duration_s INTEGER CHECK (requested_duration_s IS NULL OR requested_duration_s > 0),
    requested_volume_l   REAL CHECK (requested_volume_l IS NULL OR requested_volume_l > 0),
    requested_at         TEXT NOT NULL,
    started_at           TEXT,
    ended_at             TEXT,
    duration_s           REAL,
    total_volume_l       REAL CHECK (total_volume_l IS NULL OR total_volume_l >= 0),
    end_reason           TEXT
                         CHECK (end_reason IN ('DURATION', 'TARGET_VOLUME', 'MANUAL', 'LEAK', 'BLOCKED',
                                               'DISCONNECTED', 'CENTRAL_RESTART')),

    CHECK ((status = 'DENIED') = (deny_reason IS NOT NULL)),
    CHECK ((status = 'ENDED') = (end_reason IS NOT NULL)),
    CHECK (status <> 'ENDED' OR ended_at IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_irrigations_one_live_per_ws
    ON irrigations (ws_id) WHERE status IN ('AUTHORIZED', 'WATERING');
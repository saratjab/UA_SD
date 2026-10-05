-- =====================================================================
-- WM_Central SQLite schema          (see docs/adr/ADR-001-state-model-and-schema.md)
--
-- Apply with sqlite3.Connection.executescript(); every statement is idempotent.
--
-- PER-CONNECTION settings (SQLite does not store these in the file) - set them
-- in database.py right after every connect():
--     PRAGMA foreign_keys = ON;      -- FK enforcement is OFF by default!
--     PRAGMA journal_mode = WAL;     -- readers don't block the writer
--
-- Timestamps are ISO-8601 UTC text, e.g. 2026-10-05T12:00:00Z
-- (same format the Monitor already produces).
--
-- What is NOT stored here on purpose:
--   * connectivity (is the Monitor socket open?)  -> runtime only
--   * per-second telemetry (flow / volume ticks)  -> runtime only
--   * the displayed status (AVAILABLE, LEAK, ...) -> DERIVED, see ADR-001
-- =====================================================================


-- Operators are pre-registered in CENTRAL (PDF: "Operator ID ... registered
-- in CENTRAL"). Unknown operator_id => request denied before persisting.
CREATE TABLE IF NOT EXISTS operators (
    operator_id TEXT    PRIMARY KEY CHECK (length(trim(operator_id)) > 0),
    name        TEXT,
    active      INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1))
);


-- A row exists only after a WS has enrolled (REGISTER_WS with ws_id + location).
-- admin_state is the ONLY durable "state" of a station:
--   ACTIVE  -> can serve irrigation (shown AVAILABLE when connected & healthy)
--   BLOCKED -> deliberately blocked by CENTRAL (shown OUT OF SERVICE)
CREATE TABLE IF NOT EXISTS watering_stations (
    ws_id         TEXT PRIMARY KEY CHECK (length(trim(ws_id)) > 0),
    location      TEXT NOT NULL    CHECK (length(trim(location)) > 0),
    admin_state   TEXT NOT NULL DEFAULT 'ACTIVE'
                  CHECK (admin_state IN ('ACTIVE', 'BLOCKED')),
    registered_at TEXT NOT NULL,           -- first successful enrollment
    last_seen_at  TEXT                     -- last register / disconnect time
);


-- Incident log. A station is in LEAK exactly while it has an OPEN fault
-- (resolved_at IS NULL) -> one source of truth, no separate flag to desync.
CREATE TABLE IF NOT EXISTS faults (
    fault_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    ws_id       TEXT NOT NULL REFERENCES watering_stations (ws_id),
    fault_type  TEXT NOT NULL,             -- stored as reported by the Monitor
    details     TEXT,
    detected_at TEXT NOT NULL,             -- Monitor's timestamp
    resolved_at TEXT                       -- NULL = still open
);

-- At most ONE open fault per station: a repeated report is a no-op
-- (use INSERT OR IGNORE and still answer FAULT_ACK).
CREATE UNIQUE INDEX IF NOT EXISTS ux_faults_one_open_per_ws
    ON faults (ws_id) WHERE resolved_at IS NULL;


-- One row per irrigation REQUEST (denied ones included: audit + idempotency,
-- request_id is UNIQUE so a replayed Kafka message cannot act twice).
--   status: DENIED -> (terminal)
--           AUTHORIZED -> WATERING -> ENDED
--           AUTHORIZED -> ENDED     (Engine never started, e.g. DISCONNECTED)
-- operator_id is NULL when the irrigation was started by a CENTRAL command.
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
                                                'OPERATOR_INACTIVE')),
    requested_duration_s INTEGER CHECK (requested_duration_s IS NULL OR requested_duration_s > 0),
    requested_at         TEXT NOT NULL,
    started_at           TEXT,
    ended_at             TEXT,
    duration_s           REAL,             -- actual, reported by the Engine
    total_volume_l       REAL CHECK (total_volume_l IS NULL OR total_volume_l >= 0),
    end_reason           TEXT
                         CHECK (end_reason IN ('DURATION', 'MANUAL', 'LEAK', 'BLOCKED',
                                               'DISCONNECTED', 'CENTRAL_RESTART')),

    -- A row is DENIED  <=> it has a deny_reason.
    CHECK ((status = 'DENIED') = (deny_reason IS NOT NULL)),
    -- A row is ENDED   <=> it has an end_reason (and an end time).
    CHECK ((status = 'ENDED') = (end_reason IS NOT NULL)),
    CHECK (status <> 'ENDED' OR ended_at IS NOT NULL)
);

-- At most ONE live irrigation per station. The database enforces "WS_BUSY"
-- even if two requests race: the second INSERT fails, Central answers DENIED.
CREATE UNIQUE INDEX IF NOT EXISTS ux_irrigations_one_live_per_ws
    ON irrigations (ws_id) WHERE status IN ('AUTHORIZED', 'WATERING');

-- Startup recovery (run by Central on every start, before accepting traffic):
--   UPDATE irrigations
--      SET status = 'ENDED', end_reason = 'CENTRAL_RESTART', ended_at = :now
--    WHERE status IN ('AUTHORIZED', 'WATERING');

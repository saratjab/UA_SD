# ADR-001: WM_Central state model and SQLite schema

**Status:** Proposed
**Date:** 2026-10-05
**Deciders:** Both team members (WS side + CENTRAL side)

## Context

CENTRAL must show five station states (AVAILABLE, WATERING, LEAK, OUT OF SERVICE,
DISCONNECTED), survive restarts (PDF, "Solution mechanics" step 1: on startup it reads
its registered stations with their location from the DB and shows them DISCONNECTED
until they connect), and receive facts from two different channels:
socket events from Monitors (register, fault) and Kafka events from Engines
(telemetry, completion). SQLite is mandatory.

Facts that constrain the design:

- **PDF:** a WS registers by sending its **ID and location**; CENTRAL must accept
  "registration and enrollment requests for a new irrigation station" at any time.
  The WS ID comes from the Monitor's CLI and its validity is confirmed by CENTRAL.
- **PDF:** the FO only receives an Operator ID, "registered in CENTRAL" -> operators
  must exist before they request anything.
- **Monitor code today:** `REGISTER_WS` carries only `ws_id` (no location).
- **Plan, section 20.1:** a single `status` column on `watering_stations`.

## Decision

1. **Do not persist the displayed status.** Persist only what CENTRAL itself decides
   or must remember, and derive the rest:

   | Dimension | Where it lives | Why |
   |---|---|---|
   | Administrative state (ACTIVE / BLOCKED) | `watering_stations.admin_state` (durable) | A blocked station must stay blocked after a CENTRAL restart |
   | Fault | `faults` table: open row = LEAK | One source of truth, keeps incident history |
   | Live irrigation | `irrigations.status` in (AUTHORIZED, WATERING) | Same table that stores the result |
   | Connectivity | Runtime only (socket open?) | Unknowable after a restart; PDF requires DISCONNECTED then |
   | Telemetry ticks | Runtime only | 1 write/second/station is pointless; persist the final summary |

2. **Displayed status is computed, first match wins:**

   ```text
   1. not connected              -> DISCONNECTED   (gray)
   2. open fault exists          -> LEAK           (red)
   3. admin_state = BLOCKED      -> OUT_OF_SERVICE (orange)
   4. irrigation status WATERING -> WATERING       (green blinking)
   5. otherwise                  -> AVAILABLE      (green)
   ```

   LEAK outranks OUT_OF_SERVICE because it is a safety condition; a station that is
   both stays blocked after the leak is resolved. DISCONNECTED outranks everything
   for display, but a persisted block or open fault survives it.

3. **Stations self-enroll.** `REGISTER_WS` carries `ws_id` **and `location`**.
   Unknown ID -> enroll (INSERT). Known ID -> authenticate and refresh `last_seen_at`
   (and `location` if it changed). Reject (REGISTER_NACK) on empty/malformed ID or
   missing location for a new station. A second registration for an ID that already
   has a live connection replaces the old one. **Operators are pre-registered**
   (seeded / added from a CENTRAL option); unknown operator -> request denied.

4. **Let the database enforce the invariants** (partial unique indexes + CHECKs):
   one open fault per station, one live irrigation per station (this *is* the
   `WS_BUSY` rule, and it is race-safe), `request_id` UNIQUE (replayed Kafka
   messages cannot act twice), `status`/`deny_reason`/`end_reason` consistency.

5. **Every request is recorded**, including denied ones (audit + idempotency), except
   requests naming an unknown station or operator, which are rejected before
   persisting (the foreign keys would refuse them anyway).

6. **End reasons are split by who can observe them.** Engine-reported: `DURATION`,
   `MANUAL`, `LEAK`, `BLOCKED`. CENTRAL-inferred (no final event can arrive):
   `DISCONNECTED`, `CENTRAL_RESTART`. On every start CENTRAL closes any
   AUTHORIZED/WATERING row as `CENTRAL_RESTART`.

## Options Considered

### Option A: single `status` column per station (plan 20.1)

| Dimension | Assessment |
|---|---|
| Complexity | Low at first, grows with every special case |
| Correctness after restart | Poor: stored WATERING/AVAILABLE is false after a restart; DISCONNECTED would overwrite a persisted block |
| Team familiarity | High |

**Pros:** trivial queries. **Cons:** mixes durable and live facts; the plan itself
flags the DISCONNECTED-vs-other-state precedence as unresolved.

### Option B: separate durable dimensions + derived status (chosen)

| Dimension | Assessment |
|---|---|
| Complexity | Low-Medium: one pure function `effective_status()` |
| Correctness after restart | Good: only truly durable facts are stored |
| Team familiarity | Medium: partial indexes are new but small |

**Pros:** precedence is explicit and unit-testable; DB rejects impossible states.
**Cons:** the displayed status needs a function instead of a column read.

## Trade-off Analysis

Option A is faster for the first hour and slower for every bug afterwards. The
status is a function of facts that change at different speeds (admin decision: rarely;
fault: occasionally; connectivity: constantly). Storing the fast-changing ones is what
creates stale-state bugs. Option B moves the complexity into one tested function.

## Consequences

- **Easier:** restart handling, precedence rules, oral defense ("why is this state
  shown?" has a one-line answer), concurrency (DB blocks double irrigation).
- **Harder:** state is no longer a single column; `state_manager` must combine the
  DB with an in-memory connectivity map. Use one writer path for state changes.
- **Deviates from the plan:** `watering_stations.status` and `is_registered` are
  gone; `faults` is a new table (justified: incident history + single source of
  truth for LEAK); `irrigations` gained `status`, `deny_reason`, `requested_*`.
- **Revisit:** exact LEAK resolution mechanism (PDF: "a new message will be sent to
  CENTRAL"); whether "connected" also requires the Engine to be attached.

## Action Items

1. [ ] **Monitor change (WS side):** `REGISTER_WS` must send `location`
   (add `--location` to `WM_WS_M`; the PDF's argument list is "at least", so adding
   it is allowed). The current code sends only `ws_id`, so it does not meet the PDF.
2. [ ] Agree with the teammate that the Monitor stays alive after a fault, otherwise
   LEAK can never be a connected state (see review notes).
3. [ ] Decide: is a station "connected" when the Monitor is registered, or only once
   the Engine is attached? (Proposed: add an `ENGINE_CONNECTED` message.)
4. [ ] `database.py`: connect helper (`foreign_keys=ON`, WAL, one connection per
   thread), schema bootstrap, startup recovery, CRUD, `effective_status()`.
5. [ ] Unit tests for every transition in the table above.
6. [ ] Decide how operators are added (seed file, CENTRAL menu, or both).

# OPC UA Alarms & Conditions Pipeline — Design

**Date:** 2026-07-16
**Status:** Approved

## Overview

Two independent Rust binaries plus Dockerized persistence and exploration tools:

- **`ac-server`** — an OPC UA server that generates Alarms & Conditions events every
  second: plain `BaseEventType` events with randomized content, and
  `ExclusiveLevelAlarmType` condition events driven by a simulated sensor value
  (random walk). Mock data is seeded from entropy so it differs between runs.
- **`ac-client`** — an OPC UA client that connects to a server given by URL,
  browses the server's event type hierarchy, creates an event monitored item on
  the Server object, subscribes, and stores every received event in Postgres.
- **Postgres** — runs in a Docker container via `docker-compose.yml`; schema is
  applied automatically on first start.
- **Metabase** — runs in a Docker container via `docker-compose.yml` and can be
  pointed at the Postgres `ac_events` database for ad hoc exploration.

Only anonymous authentication and security policy `None` are supported, by design.

## Requirements

1. Client connects to an A&C server whose URL is passed as a CLI parameter.
2. Client browses the server's Event Types and prints the discovered hierarchy.
3. Client creates an event monitored item + subscription and receives events.
4. Server emits `BaseEventType` events every second with mock data that varies
   between runs.
5. Server emits a condition-type event (`ExclusiveLevelAlarmType`, a subtype of
   `AlarmConditionType`) with realistic Active/Cleared transitions.
6. Client persists received events into Postgres running in Docker.
7. Anonymous connection only; no certificates or user credentials.

## Architecture

### Repo layout

```
Alarms-and-Conditions-Rust/
├── Cargo.toml            # workspace members = ["ac-server", "ac-client"]; nothing else shared
├── ac-server/            # self-contained binary crate
│   └── src/{main.rs, simulation.rs, events.rs}
├── ac-client/            # self-contained binary crate
│   └── src/{main.rs, subscriber.rs, db.rs}
├── docker-compose.yml    # Postgres + Metabase
└── db/init.sql           # schema, auto-applied on first container start
```

**Separation constraint:** the two crates share no code. There is no common
library crate; the workspace exists only for build convenience. Either crate
could be moved to its own repository unchanged. The client learns about the
server's event types by browsing at runtime, never from shared definitions.

### Server (`ac-server`)

- **Stack:** `async-opcua` (server feature), `tokio`, `rand`.
- **Endpoint:** `opc.tcp://0.0.0.0:4855`, security policy `None`, message mode
  `None`, anonymous identity only.
- **Address space:** a `Simulation` folder containing a `Sensor1` object with a
  `Value` variable (Double). `Sensor1` and the standard `Server` object
  (`i=2253`) are event notifiers (`SubscribeToEvents`).
- **Simulation loop (1 s tokio interval):**
  - Emit one `BaseEventType` event with randomized message text and severity.
    RNG is seeded from OS entropy at startup, so output differs between runs.
  - Advance `Sensor1.Value` by a random walk. When the value crosses the high
    threshold, raise an `ExclusiveLevelAlarmType` event
    (`ActiveState = Active`, `Retain = true`, severity scaled by how far the
    value exceeds the threshold). When the value returns below the threshold,
    emit the cleared transition (`ActiveState = Inactive`, `Retain = false`).
- **Fallback:** if `async-opcua`'s generated condition types are incomplete,
  use a custom `#[derive(Event)]` struct whose `EventType` is the
  `ExclusiveLevelAlarmType` NodeId, carrying `ConditionName`, `ActiveState`,
  and `AckedState` fields — wire-compatible with the client's event filter.

### Client (`ac-client`)

- **Stack:** `async-opcua` (client feature), `sqlx` (postgres,
  runtime-tokio), `clap`, `tokio`.
- **CLI:** `ac-client <server-url>` (required positional). `--database-url`
  flag or `DATABASE_URL` env var, defaulting to the compose file's credentials
  (`postgres://ac:ac@localhost:5432/ac_events`).
- **Flow:**
  1. Connect with anonymous identity, security `None`.
  2. Browse `Types → EventTypes → BaseEventType` recursively; print the event
     type hierarchy to stdout.
  3. Create a subscription and an event monitored item on the `Server` object
     (`i=2253`) with an `EventFilter` whose select clauses are: `EventId`,
     `EventType`, `SourceName`, `Time`, `Severity`, `Message`,
     `ConditionName`, `ActiveState/Id`, `AckedState/Id`.
  4. Each event notification is decoded into an `EventRecord` struct and sent
     over an mpsc channel to a dedicated DB writer task, which inserts it into
     the `events` table (typed columns plus the full field set as JSONB).
  5. Run until Ctrl+C; on shutdown drain the channel, then close the session.

### Database & Docker

- `docker-compose.yml`: `postgres:17-alpine`, port 5432, credentials via
  environment variables, `db/init.sql` mounted into
  `/docker-entrypoint-initdb.d/`.
- `docker-compose.yml`: `metabase/metabase:latest`, published on host port
  3001, with Metabase's application state stored in a dedicated Postgres
  database (`metabase_app`). Metabase can connect to the alarm database by
  using `postgres` as the hostname on the default compose network.
- Schema:

```sql
CREATE TABLE events (
  id             BIGSERIAL PRIMARY KEY,
  event_id       BYTEA,
  event_type     TEXT,        -- e.g. BaseEventType, ExclusiveLevelAlarmType
  source_name    TEXT,
  event_time     TIMESTAMPTZ,
  severity       INT,
  message        TEXT,
  condition_name TEXT,        -- NULL for base events
  active         BOOLEAN,     -- NULL for base events
  acked          BOOLEAN,     -- NULL for base events
  raw            JSONB,       -- all select-clause fields as received
  received_at    TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX events_event_time_idx ON events (event_time);
CREATE INDEX events_event_type_idx ON events (event_type);
```

- `sqlx` is used with runtime query binding (no compile-time checked macros),
  so building the client never requires a live database.

## Error handling

- **Server:** fail fast with a clear message on startup errors (port in use,
  bad config). Errors inside the simulation loop are logged and the loop
  continues.
- **Client:** clear error and non-zero exit if the initial OPC UA connect or
  the DB connect fails. Mid-run session drops are handled by `async-opcua`'s
  built-in reconnect. Per-event decode failures and per-row insert failures
  are logged and skipped — the pipeline never crashes on a single bad event.

## Testing

- **Unit tests:**
  - Client: mapping of select-clause result order → `EventRecord` (including
    missing/null condition fields on base events).
  - Server: threshold-crossing logic (raise once on crossing, clear once on
    return, no re-raise while already active).
- **Integration:** one `#[ignore]`-marked end-to-end test that spawns the
  server, runs the client against it, and asserts rows appear in Postgres.
  Run explicitly when Docker is available.
- **Manual verification (documented in README):**
  1. `docker compose up -d`
  2. `cargo run -p ac-server`
  3. `cargo run -p ac-client -- opc.tcp://localhost:4855`
  4. `psql postgres://ac:ac@localhost:5432/ac_events -c "SELECT event_type, severity, message, active FROM events ORDER BY id DESC LIMIT 10;"`

## Out of scope

- Any authentication beyond anonymous; any security policy beyond `None`.
- Condition acknowledgement round-trip (client calls `Acknowledge`) — the
  `AckedState` field is stored but the client never acknowledges.
- Historical event access, alarm shelving, audit events.
- Containerizing the client/server binaries.

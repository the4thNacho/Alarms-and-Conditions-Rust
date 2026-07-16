# Alarms & Conditions in Rust (OPC UA)

Two independent Rust binaries demonstrating an OPC UA Alarms & Conditions
pipeline, built on [async-opcua](https://github.com/FreeOpcUa/async-opcua):

- **`ac-server`** — an OPC UA server (`opc.tcp://localhost:4855`, anonymous
  only, security `None`) that every second emits a randomized `BaseEventType`
  event and advances a simulated sensor. When the sensor crosses its high
  limit, it raises an `ExclusiveLevelAlarmType` alarm; when it returns below,
  it clears it. Data is seeded from OS entropy, so every run differs.
- **`ac-client`** — an OPC UA client that connects to a server URL given on
  the command line, prints the server's event type hierarchy (browsed live),
  subscribes to events on the Server object, and stores every event in
  Postgres (typed columns + raw JSONB).

Each crate has its own README with details: [`ac-server/`](ac-server/README.md),
[`ac-client/`](ac-client/README.md).

## Prerequisites

- Rust (stable), Docker with the compose plugin.

## Running

```bash
# 1. Start Postgres and Metabase
docker compose up -d

# 2. Start the server
cargo run -p ac-server

# 3. In another terminal, start the client
cargo run -p ac-client -- opc.tcp://localhost:4855

# 4. Inspect stored events
docker exec ac-postgres psql -U ac -d ac_events -c \
  "SELECT event_type, severity, condition_name, active, left(message, 60) AS message \
   FROM events ORDER BY id DESC LIMIT 10;"
```

## Metabase

The compose stack exposes Metabase through a small gateway on
http://localhost:3001. Opening that URL now redirects to the live dashboard
view (`/dashboard/2-ac-alarm-overview#refresh=1`) by default.

Direct Metabase access (without the default redirect) is still available on
http://localhost:3002.

Metabase stores its own application data in Postgres instead of the embedded
file DB, which is more reliable for this setup.

After the first Metabase login flow, add the alarm database as a PostgreSQL
data source with:

- Host: postgres
- Port: 5432
- Database: ac_events
- Username: ac
- Password: ac

If you already created the `pgdata` volume before this change, the new
`metabase_app` database will not be created automatically. In that case run:

```bash
docker exec ac-postgres psql -U ac -d postgres -c "CREATE DATABASE metabase_app;"
docker compose up -d metabase
```

Metabase and Postgres share the default Docker Compose network, so the service
name `postgres` is the correct hostname from inside the Metabase container.

The client's Postgres connection defaults to
`postgres://ac:ac@localhost:5432/ac_events` and can be overridden with
`--database-url` or the `DATABASE_URL` environment variable.

## Tests

```bash
cargo test                                            # unit tests
cargo test -p ac-client --test e2e -- --ignored       # e2e (needs docker compose up -d)
```

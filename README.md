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
# 1. Start Postgres (schema auto-applies on first start)
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

The client's Postgres connection defaults to
`postgres://ac:ac@localhost:5432/ac_events` and can be overridden with
`--database-url` or the `DATABASE_URL` environment variable.

## Tests

```bash
cargo test                                            # unit tests
cargo test -p ac-client --test e2e -- --ignored       # e2e (needs docker compose up -d)
```

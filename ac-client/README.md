# ac-client

OPC UA Alarms & Conditions client built on
[async-opcua](https://github.com/FreeOpcUa/async-opcua). Self-contained —
works against any A&C server offering an anonymous, security-`None` endpoint.

## What it does

1. Connects to the server URL given on the command line (anonymous,
   security `None` — the only supported mode).
2. Browses and prints the server's event type hierarchy
   (`Types → EventTypes → BaseEventType` subtree).
3. Creates a subscription and an event monitored item on the `Server`
   object (`ns=0;i=2253`) selecting: EventId, EventType, SourceName, Time,
   Severity, Message, ConditionName, ActiveState/Id, AckedState/Id, Retain.
4. Stores every received event in Postgres — typed columns plus the full
   field set as JSONB (see `../db/init.sql` for the schema).

## Prerequisites

Postgres must be reachable. From the workspace root:

```bash
docker compose up -d
```

## Running

```bash
cargo run -p ac-client -- opc.tcp://localhost:4855
```

| Option | Default | Purpose |
|---|---|---|
| `<url>` (positional, required) | — | OPC UA server endpoint |
| `--database-url` / `DATABASE_URL` | `postgres://ac:ac@localhost:5432/ac_events` | Postgres connection string |

Stop with Ctrl+C. Inspect stored events:

```bash
docker exec ac-postgres psql -U ac -d ac_events -c \
  "SELECT event_type, severity, condition_name, active, left(message, 60) AS message \
   FROM events ORDER BY id DESC LIMIT 10;"
```

## Testing

```bash
cargo test -p ac-client                          # unit tests
cargo test -p ac-client --test e2e -- --ignored  # e2e (needs server buildable + postgres up)
```

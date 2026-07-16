# ac-server

OPC UA Alarms & Conditions demo server built on
[async-opcua](https://github.com/FreeOpcUa/async-opcua). Self-contained —
no dependency on `ac-client` or the database.

## What it does

- Serves `opc.tcp://localhost:4855` with security policy `None` and
  **anonymous authentication only**.
- Exposes `Objects/Simulation/Sensor1` (an event notifier) with a `Value`
  variable (Double).
- Every second:
  - emits a `BaseEventType` event with a randomized message and severity
    (seeded from OS entropy — differs between runs);
  - advances `Sensor1.Value` with a mean-reverting random walk. Crossing the
    high limit (10.0) raises an `ExclusiveLevelAlarmType` alarm
    (`ActiveState=Active`, `Retain=true`); dropping back clears it.

## Running

```bash
cargo run -p ac-server        # from the workspace root
# or, from this directory:
cargo run
```

Log verbosity is controlled with `RUST_LOG` (default `info`), e.g.
`RUST_LOG=debug cargo run -p ac-server`. Stop with Ctrl+C.

## Testing

```bash
cargo test -p ac-server
```

Any OPC UA client supporting anonymous connections can consume the events —
subscribe to events on the `Server` object (`ns=0;i=2253`).

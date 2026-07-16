# OPC UA Alarms & Conditions Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build two independent Rust binaries — an OPC UA A&C server that emits `BaseEventType` and `ExclusiveLevelAlarmType` events every second, and an OPC UA client that browses event types, subscribes to events, and stores them in Dockerized Postgres.

**Architecture:** Cargo workspace with two fully self-contained crates (`ac-server`, `ac-client`) that share no code. The server simulates a sensor with a random walk and a high-limit alarm; the client connects anonymously to a URL given on the CLI, browses the event type hierarchy, creates an event monitored item on the Server object, and pipes decoded events through an mpsc channel into Postgres.

**Tech Stack:** `async-opcua` 0.18 (imported as `opcua`; lib name is `opcua` even though the package is `async-opcua`), tokio, sqlx 0.8 (postgres), clap 4, rand 0.9, postgres:17-alpine via docker compose.

**Spec:** `docs/superpowers/specs/2026-07-16-opcua-ac-pipeline-design.md`

**Verified API facts (from async-opcua 0.18 source — do not re-derive):**
- Dependency: `async-opcua = { version = "0.18", features = ["server"] }` (or `["client"]`); import path is `use opcua::...`.
- Server: `ServerBuilder::new_anonymous("name")` creates a `None`-security, anonymous-only endpoint at path `/`. `.host(...)`, `.port(...)`, `.with_node_manager(simple_node_manager(NamespaceMetadata { namespace_uri, ..Default::default() }, "id"))`, `.build()` → `(Server, ServerHandle)`.
- `handle.node_managers().get_of_type::<SimpleNodeManager>()`, `handle.get_namespace_index(uri)`, `handle.subscriptions()` (Arc\<SubscriptionCache\>), `handle.cancel()`.
- Emit events: `subscriptions.notify_events([(&evt as &dyn opcua::nodes::Event, &ObjectId::Server.into())].into_iter())`.
- `BaseEventType::new(type_id: impl Into<NodeId>, event_id: ByteString, message: impl Into<LocalizedText>, time: DateTime)` plus builder methods `.set_source_node(NodeId)`, `.set_source_name(UAString)`, `.set_severity(u16)`. Fields are `pub`.
- Generated ns0 event types live in `opcua::core_namespace::events::*` (available with the `server` feature via `generated-address-space`). `ExclusiveLevelAlarmType::new_event_now(type_id, event_id, message, &NamespaceMap)` constructs one; all non-base fields are `Default::default()` and `pub`. For ns0 types the `NamespaceMap` argument is ignored, so `&NamespaceMap::new()` is fine.
- Nesting: `ExclusiveLevelAlarmType.base` → `ExclusiveLimitAlarmType` (has `active_state: TwoStateVariableType`) `.base` → `LimitAlarmType` (has `high_limit: f64`) `.base` → `AlarmConditionType` (has `active_state`) `.base` → `AcknowledgeableConditionType` (has `acked_state`) `.base` → `ConditionType` (has `condition_name: UAString`, `retain: bool`) `.base` → `opcua::nodes::BaseEventType`.
- `TwoStateVariableType` fields: `id: bool`, `true_state: LocalizedText`, `false_state: LocalizedText`.
- Field lookup (`Event::get_field`) falls through `base` structs for unmatched names, and select clauses with `type_definition_id == BaseEventType` skip type-tree validation and resolve against the concrete event (per OPC UA spec) — so the client uses `ObjectTypeId::BaseEventType` for ALL select clauses, including condition fields. Missing fields return `Variant::Empty`.
- Client: `ClientBuilder::new().application_name(..).application_uri(..).product_uri(..).session_retry_limit(3).client().unwrap()`, then `client.connect_to_matching_endpoint((url, SecurityPolicy::None.to_str(), MessageSecurityMode::None, UserTokenPolicy::anonymous()), IdentityToken::Anonymous).await` → `(Arc<Session>, EventLoop)`; `let handle = event_loop.spawn(); session.wait_for_connection().await;`.
- `session.create_subscription(Duration, lifetime_count, max_keep_alive_count, max_notifications_per_publish, priority, publishing_enabled, EventCallback) -> Result<u32, StatusCode>`; `EventCallback::new(move |event: Option<Vec<Variant>>-like, item| ...)` (match the sample: `if let Some(ref values) = event`).
- Event monitored item: `let mut req: MonitoredItemCreateRequest = node_id.into(); req.item_to_monitor.attribute_id = AttributeId::EventNotifier as u32; req.requested_parameters.filter = ExtensionObject::from_message(EventFilter { where_clause: ContentFilter { elements: None }, select_clauses: Some(...) });` then `session.create_monitored_items(sub_id, TimestampsToReturn::Neither, vec![req]).await`.
- `SimpleAttributeOperand::new_value(ObjectTypeId::BaseEventType, "ActiveState/Id")` — splits `/` into a browse path, targets the Value attribute.
- `session.browse(&[BrowseDescription { ... }], max_refs, None).await -> Result<Vec<BrowseResult>, Error>`.
- `opcua::types::DateTime::as_chrono() -> chrono::DateTime<Utc>`. `Variant` variants of interest: `Boolean(bool)`, `UInt16(u16)`, `String(UAString)` (`.value() -> &Option<String>`), `DateTime(Box<DateTime>)`, `ByteString(ByteString)` (`.value: Option<Vec<u8>>`), `LocalizedText(Box<LocalizedText>)` (`.text: UAString`), `NodeId(Box<NodeId>)`, `Empty`.
- `NamespaceMap::new()` exists. `opcua::crypto::random::byte_string(n)` makes random ByteStrings.

---

### Task 1: Workspace scaffolding

**Files:**
- Create: `Cargo.toml` (workspace root)
- Create: `ac-server/Cargo.toml`, `ac-server/src/main.rs`
- Create: `ac-client/Cargo.toml`, `ac-client/src/main.rs`

- [ ] **Step 1: Create the workspace root `Cargo.toml`**

```toml
[workspace]
resolver = "2"
members = ["ac-server", "ac-client"]
```

- [ ] **Step 2: Create `ac-server/Cargo.toml`**

```toml
[package]
name = "ac-server"
version = "0.1.0"
edition = "2021"

[dependencies]
async-opcua = { version = "0.18", features = ["server"] }
tokio = { version = "1", features = ["full"] }
rand = "0.9"
log = "0.4"
env_logger = "0.11"
```

- [ ] **Step 3: Create `ac-server/src/main.rs` placeholder**

```rust
fn main() {
    println!("ac-server");
}
```

- [ ] **Step 4: Create `ac-client/Cargo.toml`**

```toml
[package]
name = "ac-client"
version = "0.1.0"
edition = "2021"

[dependencies]
async-opcua = { version = "0.18", features = ["client"] }
tokio = { version = "1", features = ["full"] }
clap = { version = "4", features = ["derive", "env"] }
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres", "chrono", "json"] }
serde_json = "1"
chrono = { version = "0.4", features = ["serde"] }
anyhow = "1"
log = "0.4"
env_logger = "0.11"
```

- [ ] **Step 5: Create `ac-client/src/main.rs` placeholder**

```rust
fn main() {
    println!("ac-client");
}
```

- [ ] **Step 6: Verify the workspace builds**

Run: `cargo build`
Expected: compiles both crates successfully (first build downloads async-opcua and takes a few minutes).

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock ac-server ac-client
git commit -m "chore: scaffold workspace with ac-server and ac-client crates"
```

---

### Task 2: Postgres in Docker

**Files:**
- Create: `db/init.sql`
- Create: `docker-compose.yml`

- [ ] **Step 1: Create `db/init.sql`**

```sql
CREATE TABLE events (
  id             BIGSERIAL PRIMARY KEY,
  event_id       BYTEA,
  event_type     TEXT,
  source_name    TEXT,
  event_time     TIMESTAMPTZ,
  severity       INT,
  message        TEXT,
  condition_name TEXT,
  active         BOOLEAN,
  acked          BOOLEAN,
  raw            JSONB,
  received_at    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX events_event_time_idx ON events (event_time);
CREATE INDEX events_event_type_idx ON events (event_type);
```

- [ ] **Step 2: Create `docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:17-alpine
    container_name: ac-postgres
    environment:
      POSTGRES_USER: ac
      POSTGRES_PASSWORD: ac
      POSTGRES_DB: ac_events
    ports:
      - "5432:5432"
    volumes:
      - ./db/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:
```

- [ ] **Step 3: Validate compose file and start the database**

Run: `docker compose config -q && docker compose up -d && sleep 5`
Expected: no config errors; container `ac-postgres` starts.

- [ ] **Step 4: Verify the schema was applied**

Run: `docker exec ac-postgres psql -U ac -d ac_events -c "\d events"`
Expected: table description showing the columns above. (If the volume already existed from a previous attempt, run `docker compose down -v` first — init.sql only runs on a fresh volume.)

- [ ] **Step 5: Commit**

```bash
git add db/init.sql docker-compose.yml
git commit -m "feat: add Dockerized Postgres with events schema"
```

---

### Task 3: Server simulation state machine (TDD)

**Files:**
- Create: `ac-server/src/simulation.rs`
- Modify: `ac-server/src/main.rs` (add `mod simulation;`)

- [ ] **Step 1: Write the failing tests**

Create `ac-server/src/simulation.rs` with only the test module first:

```rust
//! Simulated sensor value with a high-limit alarm state machine.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raises_once_when_crossing_high_limit() {
        let mut sim = Simulation::new(9.5, 10.0);
        assert_eq!(sim.step(1.0), AlarmTransition::Raised); // 10.5 > 10.0
        assert!(sim.alarm_active());
        assert_eq!(sim.step(1.0), AlarmTransition::None); // still active, no re-raise
    }

    #[test]
    fn clears_once_when_returning_below_limit() {
        let mut sim = Simulation::new(9.5, 10.0);
        sim.step(1.0); // raised at 10.5
        assert_eq!(sim.step(-1.0), AlarmTransition::Cleared); // 9.5 <= 10.0
        assert!(!sim.alarm_active());
        assert_eq!(sim.step(-1.0), AlarmTransition::None);
    }

    #[test]
    fn no_alarm_while_below_limit() {
        let mut sim = Simulation::new(0.0, 10.0);
        assert_eq!(sim.step(1.0), AlarmTransition::None);
        assert!(!sim.alarm_active());
    }

    #[test]
    fn severity_scales_with_excursion_and_caps_at_1000() {
        let mut sim = Simulation::new(10.0, 10.0);
        sim.step(0.5); // value 10.5, excess 0.5
        assert_eq!(sim.severity(), 525); // 500 + 0.5 * 50

        let mut sim2 = Simulation::new(0.0, 10.0);
        sim2.step(100.0); // far above the limit
        assert_eq!(sim2.severity(), 1000);
    }
}
```

Add `mod simulation;` at the top of `ac-server/src/main.rs`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ac-server`
Expected: FAIL to compile — `Simulation` and `AlarmTransition` not found.

- [ ] **Step 3: Implement the state machine (above the test module in the same file)**

```rust
/// What happened to the alarm state after a simulation step.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum AlarmTransition {
    /// No change in alarm state.
    None,
    /// The value crossed above the high limit; the alarm was raised.
    Raised,
    /// The value returned to or below the high limit; the alarm was cleared.
    Cleared,
}

/// A sensor value driven by external deltas, with a single high-limit alarm.
pub struct Simulation {
    value: f64,
    high_limit: f64,
    alarm_active: bool,
}

impl Simulation {
    pub fn new(start: f64, high_limit: f64) -> Self {
        Self {
            value: start,
            high_limit,
            alarm_active: false,
        }
    }

    /// Advance the sensor by `delta` and report any alarm transition.
    pub fn step(&mut self, delta: f64) -> AlarmTransition {
        self.value += delta;
        if !self.alarm_active && self.value > self.high_limit {
            self.alarm_active = true;
            AlarmTransition::Raised
        } else if self.alarm_active && self.value <= self.high_limit {
            self.alarm_active = false;
            AlarmTransition::Cleared
        } else {
            AlarmTransition::None
        }
    }

    pub fn value(&self) -> f64 {
        self.value
    }

    pub fn high_limit(&self) -> f64 {
        self.high_limit
    }

    pub fn alarm_active(&self) -> bool {
        self.alarm_active
    }

    /// Alarm severity scaled by excursion above the limit:
    /// 500 at the limit, +50 per unit above, capped at 1000.
    pub fn severity(&self) -> u16 {
        let excess = (self.value - self.high_limit).max(0.0);
        (500.0 + excess * 50.0).min(1000.0) as u16
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ac-server`
Expected: 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add ac-server/src/simulation.rs ac-server/src/main.rs
git commit -m "feat(server): add sensor simulation with high-limit alarm state machine"
```

---

### Task 4: Server event construction (TDD)

**Files:**
- Create: `ac-server/src/events.rs`
- Modify: `ac-server/src/main.rs` (add `mod events;`)

- [ ] **Step 1: Write the failing tests**

Create `ac-server/src/events.rs` with only the test module first (tests exercise `Event::get_field` exactly the way the server's filter evaluation will, proving the deeply nested alarm fields actually surface):

```rust
//! Construction of the OPC UA events emitted by the simulation.

#[cfg(test)]
mod tests {
    use super::*;
    use opcua::nodes::Event;
    use opcua::types::{
        AttributeId, NodeId, NumericRange, ObjectTypeId, QualifiedName, UAString, Variant,
    };

    fn get(evt: &dyn Event, path: &[&str]) -> Variant {
        let path: Vec<QualifiedName> = path.iter().map(|s| QualifiedName::from(*s)).collect();
        evt.get_field(
            &ObjectTypeId::BaseEventType.into(),
            AttributeId::Value,
            &NumericRange::None,
            &path,
        )
    }

    #[test]
    fn alarm_event_exposes_condition_fields() {
        let ns = opcua::types::NamespaceMap::new();
        let source = NodeId::new(1, "Sensor1");
        let evt = build_level_alarm(true, 12.5, 625, &source, "Sensor1", 10.0, &ns);

        assert_eq!(
            get(&evt, &["ConditionName"]),
            Variant::from(UAString::from("HighLevel"))
        );
        assert_eq!(get(&evt, &["ActiveState", "Id"]), Variant::from(true));
        assert_eq!(get(&evt, &["AckedState", "Id"]), Variant::from(false));
        assert_eq!(get(&evt, &["Retain"]), Variant::from(true));
        assert_eq!(get(&evt, &["Severity"]), Variant::from(625u16));
        assert_eq!(
            get(&evt, &["SourceName"]),
            Variant::from(UAString::from("Sensor1"))
        );
        assert_eq!(
            get(&evt, &["EventType"]),
            Variant::from(NodeId::from(ObjectTypeId::ExclusiveLevelAlarmType))
        );
    }

    #[test]
    fn cleared_alarm_is_inactive_and_not_retained() {
        let ns = opcua::types::NamespaceMap::new();
        let source = NodeId::new(1, "Sensor1");
        let evt = build_level_alarm(false, 8.0, 100, &source, "Sensor1", 10.0, &ns);

        assert_eq!(get(&evt, &["ActiveState", "Id"]), Variant::from(false));
        assert_eq!(get(&evt, &["Retain"]), Variant::from(false));
    }

    #[test]
    fn base_event_has_no_condition_fields() {
        let mut rng = rand::rng();
        let source = NodeId::new(1, "Sensor1");
        let evt = build_base_event(&mut rng, &source, "Sensor1");

        assert_eq!(get(&evt, &["ConditionName"]), Variant::Empty);
        assert_eq!(get(&evt, &["ActiveState", "Id"]), Variant::Empty);
        assert!(matches!(get(&evt, &["Severity"]), Variant::UInt16(_)));
        assert!(matches!(get(&evt, &["Message"]), Variant::LocalizedText(_)));
    }
}
```

Add `mod events;` at the top of `ac-server/src/main.rs`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ac-server`
Expected: FAIL to compile — `build_level_alarm` / `build_base_event` not found.

- [ ] **Step 3: Implement the event builders (above the test module)**

```rust
use opcua::core_namespace::events::ExclusiveLevelAlarmType;
use opcua::crypto::random;
use opcua::nodes::BaseEventType;
use opcua::types::{DateTime, NamespaceMap, NodeId, ObjectTypeId, UAString};
use rand::Rng;

/// Message pool for randomized BaseEventType events.
const BASE_EVENT_MESSAGES: &[&str] = &[
    "Routine heartbeat from simulation",
    "Sensor sweep completed",
    "Diagnostics cycle finished",
    "Telemetry snapshot recorded",
];

/// Build a randomized `BaseEventType` event from the given source.
pub fn build_base_event(
    rng: &mut impl Rng,
    source_node: &NodeId,
    source_name: &str,
) -> BaseEventType {
    let msg = BASE_EVENT_MESSAGES[rng.random_range(0..BASE_EVENT_MESSAGES.len())];
    BaseEventType::new(
        ObjectTypeId::BaseEventType,
        random::byte_string(16),
        format!("{msg} (#{})", rng.random_range(0..1_000_000u32)),
        DateTime::now(),
    )
    .set_source_node(source_node.clone())
    .set_source_name(UAString::from(source_name))
    .set_severity(rng.random_range(1..=200u16))
}

/// Build an `ExclusiveLevelAlarmType` event for an alarm raise (`active = true`)
/// or clear (`active = false`) transition.
pub fn build_level_alarm(
    active: bool,
    value: f64,
    severity: u16,
    source_node: &NodeId,
    source_name: &str,
    high_limit: f64,
    namespaces: &NamespaceMap,
) -> ExclusiveLevelAlarmType {
    let message = if active {
        format!("{source_name} value {value:.2} exceeded high limit {high_limit:.2}")
    } else {
        format!("{source_name} value {value:.2} returned below high limit {high_limit:.2}")
    };

    let mut evt = ExclusiveLevelAlarmType::new_event_now(
        ExclusiveLevelAlarmType::event_type_id(),
        random::byte_string(16),
        message,
        namespaces,
    );

    // Walk down the type hierarchy:
    //   evt.base                     ExclusiveLimitAlarmType (active_state)
    //   evt.base.base                LimitAlarmType (high_limit)
    //   evt.base.base.base           AlarmConditionType (active_state)
    //   evt.base.base.base.base      AcknowledgeableConditionType (acked_state)
    //   evt.base.base.base.base.base ConditionType (condition_name, retain)
    //   ...            .base.base    BaseEventType (severity, source, ...)
    evt.base.base.high_limit = high_limit;

    // ActiveState is defined on both ExclusiveLimitAlarmType and
    // AlarmConditionType; set both so any type-definition id resolves consistently.
    for state in [
        &mut evt.base.active_state,
        &mut evt.base.base.base.active_state,
    ] {
        state.id = active;
        state.true_state = "Active".into();
        state.false_state = "Inactive".into();
    }

    let acked = &mut evt.base.base.base.base.acked_state;
    acked.id = false; // this demo never acknowledges alarms
    acked.false_state = "Unacknowledged".into();

    let condition = &mut evt.base.base.base.base.base;
    condition.condition_name = UAString::from("HighLevel");
    condition.retain = active;

    let base = &mut condition.base;
    base.source_node = source_node.clone();
    base.source_name = UAString::from(source_name);
    base.severity = severity;

    evt
}
```

Note: if a field access fails to compile (e.g. `source_name` is `Option<UAString>` or a field has a slightly different name), inspect the structs with `cargo doc -p async-opcua-nodes --open` or read the source in `~/.cargo/registry/src/*/async-opcua-nodes-0.18*/src/events/event.rs` and `async-opcua-core-namespace-0.18*/src/events/generated.rs`, and adjust the field access — do NOT change the test expectations, which encode the wire behavior the client relies on.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ac-server`
Expected: all 7 tests PASS (4 simulation + 3 events).

- [ ] **Step 5: Commit**

```bash
git add ac-server/src/events.rs ac-server/src/main.rs
git commit -m "feat(server): add BaseEventType and ExclusiveLevelAlarmType event builders"
```

---

### Task 5: Server main — address space + 1 s event loop

**Files:**
- Modify: `ac-server/src/main.rs` (replace entirely)

- [ ] **Step 1: Write `ac-server/src/main.rs`**

```rust
//! OPC UA Alarms & Conditions demo server.
//!
//! Serves an anonymous-only, security-None endpoint on opc.tcp://localhost:4855.
//! Every second it emits a randomized BaseEventType event and advances a
//! simulated sensor; crossing the high limit raises/clears an
//! ExclusiveLevelAlarmType alarm.

mod events;
mod simulation;

use std::sync::Arc;
use std::time::Duration;

use log::{info, warn};
use opcua::nodes::Event;
use opcua::server::address_space::{EventNotifier, ObjectBuilder, VariableBuilder};
use opcua::server::diagnostics::NamespaceMetadata;
use opcua::server::node_manager::memory::{simple_node_manager, SimpleNodeManager};
use opcua::server::{ServerBuilder, SubscriptionCache};
use opcua::types::{
    DataTypeId, DataValue, NamespaceMap, NodeId, ObjectId, ObjectTypeId, VariableTypeId,
};
use rand::Rng;

use simulation::{AlarmTransition, Simulation};

const NAMESPACE_URI: &str = "urn:AcServer";
const HIGH_LIMIT: f64 = 10.0;
/// Severity of the "alarm cleared" notification.
const CLEARED_SEVERITY: u16 = 100;

#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let (server, handle) = ServerBuilder::new_anonymous("ac-server")
        .application_uri("urn:AcServer")
        .product_uri("urn:AcServer")
        .host("localhost")
        .port(4855)
        .with_node_manager(simple_node_manager(
            NamespaceMetadata {
                namespace_uri: NAMESPACE_URI.to_owned(),
                ..Default::default()
            },
            "simulation",
        ))
        .build()
        .unwrap_or_else(|e| {
            eprintln!("Failed to build server: {e}");
            std::process::exit(1);
        });

    let node_manager = handle
        .node_managers()
        .get_of_type::<SimpleNodeManager>()
        .expect("simple node manager was registered above");
    let ns = handle
        .get_namespace_index(NAMESPACE_URI)
        .expect("namespace was registered above");

    let (sensor_id, value_id) = build_address_space(ns, &node_manager);

    tokio::spawn(run_simulation(
        node_manager,
        handle.subscriptions().clone(),
        sensor_id,
        value_id,
    ));

    let handle_c = handle.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("shutting down");
            handle_c.cancel();
        }
    });

    info!("ac-server listening on opc.tcp://localhost:4855");
    if let Err(e) = server.run().await {
        eprintln!("Server failed: {e}");
        std::process::exit(1);
    }
}

/// Add Objects/Simulation/Sensor1 (an event notifier) with a Value property.
/// Returns (sensor node id, value node id).
fn build_address_space(ns: u16, manager: &Arc<SimpleNodeManager>) -> (NodeId, NodeId) {
    let address_space = manager.address_space();
    let mut address_space = address_space.write();

    let sim_folder = NodeId::new(ns, "Simulation");
    address_space.add_folder(
        &sim_folder,
        "Simulation",
        "Simulation",
        &NodeId::objects_folder_id(),
    );

    let sensor_id = NodeId::new(ns, "Sensor1");
    ObjectBuilder::new(&sensor_id, "Sensor1", "Sensor1")
        .organized_by(sim_folder)
        .event_notifier(EventNotifier::SUBSCRIBE_TO_EVENTS)
        .has_type_definition(ObjectTypeId::BaseObjectType)
        .insert(&mut *address_space);

    let value_id = NodeId::new(ns, "Sensor1.Value");
    VariableBuilder::new(&value_id, "Value", "Value")
        .data_type(DataTypeId::Double)
        .property_of(sensor_id.clone())
        .has_type_definition(VariableTypeId::PropertyType)
        .insert(&mut *address_space);

    (sensor_id, value_id)
}

/// Tick once per second: emit a base event, advance the sensor, and raise or
/// clear the level alarm on threshold crossings.
async fn run_simulation(
    manager: Arc<SimpleNodeManager>,
    subscriptions: Arc<SubscriptionCache>,
    sensor_id: NodeId,
    value_id: NodeId,
) {
    // Seeded from OS entropy: mock data differs between runs.
    let mut rng = rand::rng();
    let mut sim = Simulation::new(5.0, HIGH_LIMIT);
    // The alarm namespace argument is unused for ns0 event types.
    let namespaces = NamespaceMap::new();
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        interval.tick().await;

        // 1. A randomized BaseEventType event every tick.
        let base_evt = events::build_base_event(&mut rng, &sensor_id, "Sensor1");
        subscriptions.notify_events(
            [(&base_evt as &dyn Event, &ObjectId::Server.into())].into_iter(),
        );

        // 2. Mean-reverting random walk: noise plus a pull toward 8.0 keeps the
        //    value oscillating around the 10.0 limit, so alarms raise AND clear.
        let delta = rng.random_range(-2.0..2.0) + 0.2 * (8.0 - sim.value());
        let transition = sim.step(delta);

        if let Err(e) = manager.set_values(
            &subscriptions,
            [(&value_id, None, DataValue::new_now(sim.value()))].into_iter(),
        ) {
            warn!("failed to update sensor value: {e}");
        }

        match transition {
            AlarmTransition::Raised | AlarmTransition::Cleared => {
                let active = transition == AlarmTransition::Raised;
                let severity = if active { sim.severity() } else { CLEARED_SEVERITY };
                let alarm = events::build_level_alarm(
                    active,
                    sim.value(),
                    severity,
                    &sensor_id,
                    "Sensor1",
                    HIGH_LIMIT,
                    &namespaces,
                );
                subscriptions.notify_events(
                    [(&alarm as &dyn Event, &ObjectId::Server.into())].into_iter(),
                );
                info!(
                    "alarm {}: value={:.2} severity={}",
                    if active { "RAISED" } else { "CLEARED" },
                    sim.value(),
                    severity
                );
            }
            AlarmTransition::None => {}
        }
    }
}
```

- [ ] **Step 2: Build and run existing tests**

Run: `cargo test -p ac-server`
Expected: compiles; all 7 tests still PASS. (If an import path differs in 0.18 — e.g. `NamespaceMetadata`'s module — fix the `use`, guided by compiler suggestions; the samples in the async-opcua repo are the reference.)

- [ ] **Step 3: Smoke-test that the server starts and listens**

Run:
```bash
cargo run -p ac-server &
SERVER_PID=$!
sleep 3
(exec 3<>/dev/tcp/localhost/4855) && echo "PORT 4855 OPEN"
kill $SERVER_PID
```
Expected: `PORT 4855 OPEN` and log line `ac-server listening on opc.tcp://localhost:4855`.

- [ ] **Step 4: Commit**

```bash
git add ac-server/src/main.rs
git commit -m "feat(server): serve anonymous endpoint with 1s event simulation loop"
```

---

### Task 6: Client CLI, connection, and event type browsing

**Files:**
- Create: `ac-client/src/browse.rs`
- Modify: `ac-client/src/main.rs` (replace entirely)

- [ ] **Step 1: Create `ac-client/src/browse.rs`**

```rust
//! Browse and print the server's event type hierarchy.

use std::sync::Arc;

use anyhow::Context;
use opcua::client::Session;
use opcua::types::{
    BrowseDescription, BrowseDirection, BrowseResultMask, NodeClassMask, NodeId, ObjectTypeId,
    ReferenceTypeId,
};

/// Recursively print the event type hierarchy rooted at BaseEventType.
pub async fn print_event_types(session: &Arc<Session>) -> anyhow::Result<()> {
    println!("Server event type hierarchy:");
    print_subtree(session, ObjectTypeId::BaseEventType.into(), "BaseEventType".into(), 1).await
}

async fn print_subtree(
    session: &Arc<Session>,
    node: NodeId,
    name: String,
    depth: usize,
) -> anyhow::Result<()> {
    println!("{}{name}", "  ".repeat(depth));

    let results = session
        .browse(
            &[BrowseDescription {
                node_id: node,
                browse_direction: BrowseDirection::Forward,
                reference_type_id: ReferenceTypeId::HasSubtype.into(),
                include_subtypes: true,
                node_class_mask: NodeClassMask::OBJECT_TYPE.bits(),
                result_mask: BrowseResultMask::All as u32,
            }],
            1000,
            None,
        )
        .await
        .context("browsing event types")?;

    for result in results {
        for reference in result.references.unwrap_or_default() {
            // Async recursion requires boxing.
            Box::pin(print_subtree(
                session,
                reference.node_id.node_id.clone(),
                reference.display_name.text.to_string(),
                depth + 1,
            ))
            .await?;
        }
    }
    Ok(())
}
```

Note: if `session.browse`'s error type doesn't satisfy `anyhow`'s `Context` bound, use `.map_err(|e| anyhow::anyhow!("browsing event types: {e}"))` instead.

- [ ] **Step 2: Replace `ac-client/src/main.rs`**

```rust
//! OPC UA Alarms & Conditions client.
//!
//! Connects anonymously to the given server URL, prints the server's event
//! type hierarchy, subscribes to events on the Server object, and stores
//! every received event in Postgres.

mod browse;

use anyhow::Context;
use clap::Parser;
use opcua::client::{ClientBuilder, IdentityToken};
use opcua::crypto::SecurityPolicy;
use opcua::types::{MessageSecurityMode, UserTokenPolicy};

#[derive(Parser)]
#[command(about = "OPC UA A&C client: browses event types, subscribes, stores events in Postgres")]
struct Args {
    /// OPC UA server endpoint URL, e.g. opc.tcp://localhost:4855
    url: String,

    /// Postgres connection string
    #[arg(
        long,
        env = "DATABASE_URL",
        default_value = "postgres://ac:ac@localhost:5432/ac_events"
    )]
    database_url: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    let args = Args::parse();

    let mut client = ClientBuilder::new()
        .application_name("ac-client")
        .application_uri("urn:AcClient")
        .product_uri("urn:AcClient")
        .session_retry_limit(3)
        .client()
        .expect("default client config is valid");

    println!("Connecting to {} (anonymous, security None)...", args.url);
    let (session, event_loop) = client
        .connect_to_matching_endpoint(
            (
                args.url.as_str(),
                SecurityPolicy::None.to_str(),
                MessageSecurityMode::None,
                UserTokenPolicy::anonymous(),
            ),
            IdentityToken::Anonymous,
        )
        .await
        .with_context(|| format!("connecting to {}", args.url))?;

    let event_loop_handle = event_loop.spawn();
    session.wait_for_connection().await;
    println!("Connected.");

    browse::print_event_types(&session).await?;

    session.disconnect().await.ok();
    event_loop_handle.await.ok();
    Ok(())
}
```

(The subscription and DB pipeline are wired in Tasks 7–9; this task ends with connect + browse + disconnect.)

- [ ] **Step 3: Build**

Run: `cargo build -p ac-client`
Expected: compiles. Fix import paths per compiler suggestions if any differ.

- [ ] **Step 4: Verify against the live server**

Run:
```bash
cargo run -p ac-server &
SERVER_PID=$!
sleep 3
cargo run -p ac-client -- opc.tcp://localhost:4855
kill $SERVER_PID
```
Expected: client prints `Connected.` and an indented tree under `BaseEventType` that includes `ConditionType`, `AcknowledgeableConditionType`, `AlarmConditionType`, `LimitAlarmType`, `ExclusiveLimitAlarmType`, and `ExclusiveLevelAlarmType`, then exits 0.

- [ ] **Step 5: Commit**

```bash
git add ac-client/src/browse.rs ac-client/src/main.rs
git commit -m "feat(client): connect anonymously via CLI url and browse event type hierarchy"
```

---

### Task 7: Client event decoding (TDD)

**Files:**
- Create: `ac-client/src/subscriber.rs`
- Modify: `ac-client/src/main.rs` (add `mod subscriber;`)

- [ ] **Step 1: Write the failing tests**

Create `ac-client/src/subscriber.rs` with only the test module first:

```rust
//! Event filter field selection and decoding of received event notifications.

#[cfg(test)]
mod tests {
    use super::*;
    use opcua::types::{
        ByteString, DateTime, LocalizedText, NodeId, ObjectTypeId, UAString, Variant,
    };

    fn alarm_variants() -> Vec<Variant> {
        vec![
            Variant::from(ByteString::from(vec![1u8, 2, 3])),          // EventId
            Variant::from(NodeId::from(ObjectTypeId::ExclusiveLevelAlarmType)), // EventType
            Variant::from(UAString::from("Sensor1")),                  // SourceName
            Variant::from(DateTime::now()),                            // Time
            Variant::from(625u16),                                     // Severity
            Variant::from(LocalizedText::from("value exceeded limit")), // Message
            Variant::from(UAString::from("HighLevel")),                // ConditionName
            Variant::from(true),                                       // ActiveState/Id
            Variant::from(false),                                      // AckedState/Id
            Variant::from(true),                                       // Retain
        ]
    }

    #[test]
    fn select_clauses_match_event_fields() {
        let clauses = select_clauses();
        assert_eq!(clauses.len(), EVENT_FIELDS.len());
        // ActiveState/Id must become a two-element browse path.
        let active = &clauses[7];
        assert_eq!(active.browse_path.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn decodes_alarm_event() {
        let record = EventRecord::from_variants(&alarm_variants());
        assert_eq!(record.event_id.as_deref(), Some(&[1u8, 2, 3][..]));
        assert_eq!(record.event_type.as_deref(), Some("ExclusiveLevelAlarmType"));
        assert_eq!(record.source_name.as_deref(), Some("Sensor1"));
        assert!(record.event_time.is_some());
        assert_eq!(record.severity, Some(625));
        assert_eq!(record.message.as_deref(), Some("value exceeded limit"));
        assert_eq!(record.condition_name.as_deref(), Some("HighLevel"));
        assert_eq!(record.active, Some(true));
        assert_eq!(record.acked, Some(false));
        // Raw JSON keeps every field keyed by name.
        assert_eq!(record.raw["ConditionName"], "HighLevel");
    }

    #[test]
    fn decodes_base_event_with_empty_condition_fields() {
        let mut variants = alarm_variants();
        variants[1] = Variant::from(NodeId::from(ObjectTypeId::BaseEventType));
        variants[6] = Variant::Empty; // ConditionName
        variants[7] = Variant::Empty; // ActiveState/Id
        variants[8] = Variant::Empty; // AckedState/Id
        variants[9] = Variant::Empty; // Retain

        let record = EventRecord::from_variants(&variants);
        assert_eq!(record.event_type.as_deref(), Some("BaseEventType"));
        assert_eq!(record.condition_name, None);
        assert_eq!(record.active, None);
        assert_eq!(record.acked, None);
    }

    #[test]
    fn tolerates_short_field_list() {
        let record = EventRecord::from_variants(&[Variant::from(ByteString::from(vec![9u8]))]);
        assert_eq!(record.event_id.as_deref(), Some(&[9u8][..]));
        assert_eq!(record.severity, None);
    }
}
```

Add `mod subscriber;` to `ac-client/src/main.rs`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p ac-client`
Expected: FAIL to compile — `EVENT_FIELDS`, `select_clauses`, `EventRecord` not found.

- [ ] **Step 3: Implement (above the test module)**

```rust
use chrono::{DateTime as ChronoDateTime, Utc};
use opcua::types::{ObjectTypeId, SimpleAttributeOperand, Variant};

/// Field paths requested in the event filter select clauses, in this order.
/// `EventRecord::from_variants` decodes notification values by this order.
pub const EVENT_FIELDS: &[&str] = &[
    "EventId",
    "EventType",
    "SourceName",
    "Time",
    "Severity",
    "Message",
    "ConditionName",
    "ActiveState/Id",
    "AckedState/Id",
    "Retain",
];

/// Build the event filter select clauses, one per entry in [`EVENT_FIELDS`].
///
/// All clauses use BaseEventType as the type definition id: per OPC UA Part 4,
/// the server then resolves the browse path against the concrete event type,
/// returning null for fields the event doesn't have.
pub fn select_clauses() -> Vec<SimpleAttributeOperand> {
    EVENT_FIELDS
        .iter()
        .map(|f| SimpleAttributeOperand::new_value(ObjectTypeId::BaseEventType, f))
        .collect()
}

/// A decoded event notification, ready for insertion into the database.
#[derive(Debug, Clone, PartialEq)]
pub struct EventRecord {
    pub event_id: Option<Vec<u8>>,
    pub event_type: Option<String>,
    pub source_name: Option<String>,
    pub event_time: Option<ChronoDateTime<Utc>>,
    pub severity: Option<i32>,
    pub message: Option<String>,
    pub condition_name: Option<String>,
    pub active: Option<bool>,
    pub acked: Option<bool>,
    pub raw: serde_json::Value,
}

impl EventRecord {
    /// Decode a notification's field values; order matches [`EVENT_FIELDS`].
    /// Missing or unexpected variants become `None`.
    pub fn from_variants(fields: &[Variant]) -> Self {
        let get = |i: usize| fields.get(i).unwrap_or(&Variant::Empty);

        let raw: serde_json::Map<String, serde_json::Value> = EVENT_FIELDS
            .iter()
            .enumerate()
            .map(|(i, name)| (name.to_string(), serde_json::Value::from(format!("{}", get(i)))))
            .collect();

        Self {
            event_id: as_bytes(get(0)),
            event_type: event_type_name(get(1)),
            source_name: as_string(get(2)),
            event_time: as_time(get(3)),
            severity: as_severity(get(4)),
            message: as_string(get(5)),
            condition_name: as_string(get(6)),
            active: as_bool(get(7)),
            acked: as_bool(get(8)),
            raw: serde_json::Value::Object(raw),
        }
    }
}

fn as_bytes(v: &Variant) -> Option<Vec<u8>> {
    match v {
        Variant::ByteString(b) => b.value.clone(),
        _ => None,
    }
}

fn as_string(v: &Variant) -> Option<String> {
    match v {
        Variant::String(s) => s.value().clone(),
        Variant::LocalizedText(t) => Some(t.text.to_string()),
        _ => None,
    }
}

fn as_time(v: &Variant) -> Option<ChronoDateTime<Utc>> {
    match v {
        Variant::DateTime(dt) => Some(dt.as_chrono()),
        _ => None,
    }
}

fn as_severity(v: &Variant) -> Option<i32> {
    match v {
        Variant::UInt16(s) => Some(i32::from(*s)),
        _ => None,
    }
}

fn as_bool(v: &Variant) -> Option<bool> {
    match v {
        Variant::Boolean(b) => Some(*b),
        _ => None,
    }
}

/// Map well-known event type node ids to readable names; otherwise the node id string.
fn event_type_name(v: &Variant) -> Option<String> {
    let Variant::NodeId(id) = v else { return None };
    let name = if **id == ObjectTypeId::BaseEventType.into() {
        "BaseEventType".to_string()
    } else if **id == ObjectTypeId::ExclusiveLevelAlarmType.into() {
        "ExclusiveLevelAlarmType".to_string()
    } else {
        id.to_string()
    };
    Some(name)
}
```

Note: if `UAString::value()` returns something other than `&Option<String>`, adapt (`.as_ref().map(|s| s.to_string())` or `Some(s.to_string())` guarded by `s.is_null()`). Keep the tests as written.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p ac-client`
Expected: 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add ac-client/src/subscriber.rs ac-client/src/main.rs
git commit -m "feat(client): add event select clauses and notification decoding"
```

---

### Task 8: Client database writer

**Files:**
- Create: `ac-client/src/db.rs`
- Modify: `ac-client/src/main.rs` (add `mod db;`)

- [ ] **Step 1: Create `ac-client/src/db.rs`**

```rust
//! Postgres persistence for received events.

use log::{error, info};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tokio::sync::mpsc;

use crate::subscriber::EventRecord;

/// Connect to Postgres, failing fast if it is unreachable.
pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(4)
        .connect(database_url)
        .await
}

/// Insert one event row.
pub async fn insert_event(pool: &PgPool, record: &EventRecord) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO events \
         (event_id, event_type, source_name, event_time, severity, message, \
          condition_name, active, acked, raw) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind(&record.event_id)
    .bind(&record.event_type)
    .bind(&record.source_name)
    .bind(record.event_time)
    .bind(record.severity)
    .bind(&record.message)
    .bind(&record.condition_name)
    .bind(record.active)
    .bind(record.acked)
    .bind(&record.raw)
    .execute(pool)
    .await?;
    Ok(())
}

/// Consume decoded events from the channel and insert them until the channel
/// closes. Insert failures are logged and skipped — one bad row never stops
/// the pipeline.
pub async fn run_writer(pool: PgPool, mut rx: mpsc::UnboundedReceiver<EventRecord>) {
    while let Some(record) = rx.recv().await {
        match insert_event(&pool, &record).await {
            Ok(()) => info!(
                "stored event type={} severity={} message={:?}",
                record.event_type.as_deref().unwrap_or("?"),
                record.severity.unwrap_or(0),
                record.message.as_deref().unwrap_or("")
            ),
            Err(e) => error!("failed to insert event: {e}"),
        }
    }
}
```

Add `mod db;` to `ac-client/src/main.rs`.

- [ ] **Step 2: Build and run tests**

Run: `cargo test -p ac-client`
Expected: compiles; the 4 subscriber tests still PASS.

- [ ] **Step 3: Commit**

```bash
git add ac-client/src/db.rs ac-client/src/main.rs
git commit -m "feat(client): add Postgres writer task for event records"
```

---

### Task 9: Wire the client pipeline (subscribe → decode → store)

**Files:**
- Modify: `ac-client/src/main.rs`
- Modify: `ac-client/src/subscriber.rs` (add the subscribe function)

- [ ] **Step 1: Add the subscription function to `ac-client/src/subscriber.rs`**

Add these imports at the top and the function below them (above the `#[cfg(test)]` module):

```rust
use std::sync::Arc;
use std::time::Duration;

use opcua::client::{EventCallback, Session};
use opcua::types::{
    AttributeId, ContentFilter, EventFilter, ExtensionObject, MonitoredItemCreateRequest, NodeId,
    ObjectId, StatusCode, TimestampsToReturn,
};
use tokio::sync::mpsc;

/// Create a subscription and an event monitored item on the Server object.
/// Each received event is decoded into an [`EventRecord`] and sent to `tx`.
pub async fn subscribe_to_events(
    session: &Arc<Session>,
    tx: mpsc::UnboundedSender<EventRecord>,
) -> Result<(), StatusCode> {
    let callback = EventCallback::new(move |event, _item| {
        if let Some(ref fields) = event {
            // A send error just means we are shutting down.
            let _ = tx.send(EventRecord::from_variants(fields));
        }
    });

    let subscription_id = session
        .create_subscription(
            Duration::from_millis(500), // publishing interval
            12000,                      // lifetime count
            50,                         // max keep-alive count
            65535,                      // max notifications per publish
            0,                          // priority
            true,                       // publishing enabled
            callback,
        )
        .await?;

    let event_filter = EventFilter {
        where_clause: ContentFilter { elements: None },
        select_clauses: Some(select_clauses()),
    };

    let mut request: MonitoredItemCreateRequest = NodeId::from(ObjectId::Server).into();
    request.item_to_monitor.attribute_id = AttributeId::EventNotifier as u32;
    request.requested_parameters.sampling_interval = 100.0;
    request.requested_parameters.queue_size = 100;
    request.requested_parameters.filter = ExtensionObject::from_message(event_filter);

    let results = session
        .create_monitored_items(subscription_id, TimestampsToReturn::Neither, vec![request])
        .await?;
    for result in &results {
        if result.status_code.is_bad() {
            return Err(result.status_code);
        }
    }

    println!("Subscribed to events on the Server object (subscription {subscription_id}).");
    Ok(())
}
```

Note: check the actual element type of `create_monitored_items`' result — if it is `MonitoredItemCreateResult`, `result.status_code` is correct; adjust the field access if the client API wraps it differently.

- [ ] **Step 2: Wire the pipeline in `ac-client/src/main.rs`**

Replace the end of `main` (everything from `browse::print_event_types` onward) with:

```rust
    browse::print_event_types(&session).await?;

    // Fail fast if the database is unreachable.
    let pool = db::connect(&args.database_url)
        .await
        .with_context(|| format!("connecting to Postgres at {}", args.database_url))?;
    println!("Connected to Postgres.");

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let writer = tokio::spawn(db::run_writer(pool, rx));

    subscriber::subscribe_to_events(&session, tx)
        .await
        .map_err(|status| anyhow::anyhow!("creating event subscription failed: {status}"))?;

    println!("Receiving events — press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await.ok();

    println!("Shutting down...");
    // Disconnecting drops the subscription (and with it the callback holding
    // the channel sender), which lets the writer drain and exit.
    session.disconnect().await.ok();
    event_loop_handle.await.ok();
    writer.await.ok();
    Ok(())
```

- [ ] **Step 3: Build and run tests**

Run: `cargo test -p ac-client`
Expected: compiles; all tests PASS.

- [ ] **Step 4: End-to-end verification**

Run (from the workspace root, with the Postgres container up — `docker compose up -d`):
```bash
docker exec ac-postgres psql -U ac -d ac_events -c "TRUNCATE events;"
cargo run -p ac-server &
SERVER_PID=$!
sleep 3
timeout --signal=INT 20 cargo run -p ac-client -- opc.tcp://localhost:4855
kill $SERVER_PID
docker exec ac-postgres psql -U ac -d ac_events -c \
  "SELECT count(*), count(*) FILTER (WHERE event_type = 'BaseEventType') AS base FROM events;"
docker exec ac-postgres psql -U ac -d ac_events -c \
  "SELECT event_type, severity, condition_name, active, left(message, 60) FROM events ORDER BY id DESC LIMIT 10;"
```
Expected:
- Client prints the type hierarchy, `Connected to Postgres.`, `Subscribed to events...`, then `stored event ...` log lines.
- The count query shows roughly 10–20 rows, most of type `BaseEventType`.
- If an alarm transition occurred during the window, rows with `event_type = ExclusiveLevelAlarmType`, `condition_name = HighLevel`, and `active` true/false appear. (Alarm timing is random; absence within 20 s is not a failure — the e2e test in Task 10 covers alarms over a longer window if needed.)

- [ ] **Step 5: Commit**

```bash
git add ac-client/src/main.rs ac-client/src/subscriber.rs
git commit -m "feat(client): subscribe to A&C events and store them in Postgres"
```

---

### Task 10: Ignored e2e test + READMEs

**Files:**
- Create: `ac-client/tests/e2e.rs`
- Modify: `README.md`
- Create: `ac-server/README.md`
- Create: `ac-client/README.md`

- [ ] **Step 1: Create `ac-client/tests/e2e.rs`**

```rust
//! End-to-end test: spawns the real server and client binaries and asserts
//! events land in Postgres. Requires the Docker Postgres container to be up
//! (`docker compose up -d`), so it is ignored by default:
//!
//!   cargo test -p ac-client --test e2e -- --ignored

use std::process::{Child, Command};
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

const DATABASE_URL: &str = "postgres://ac:ac@localhost:5432/ac_events";

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
#[ignore = "requires docker compose postgres and builds/runs both binaries"]
async fn events_flow_from_server_to_postgres() {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(DATABASE_URL)
        .await
        .expect("postgres reachable — did you run `docker compose up -d`?");

    sqlx::query("TRUNCATE events").execute(&pool).await.unwrap();

    // Build first so `cargo run` startup below is fast and predictable.
    let status = Command::new("cargo")
        .args(["build", "-p", "ac-server", "-p", "ac-client"])
        .status()
        .unwrap();
    assert!(status.success(), "workspace build failed");

    let _server = KillOnDrop(
        Command::new("cargo")
            .args(["run", "-p", "ac-server"])
            .spawn()
            .unwrap(),
    );
    tokio::time::sleep(Duration::from_secs(3)).await;

    let _client = KillOnDrop(
        Command::new("cargo")
            .args(["run", "-p", "ac-client", "--", "opc.tcp://localhost:4855"])
            .spawn()
            .unwrap(),
    );

    // Give the pipeline time to deliver ~10 base events.
    tokio::time::sleep(Duration::from_secs(15)).await;

    let row = sqlx::query(
        "SELECT count(*) AS total, \
         count(*) FILTER (WHERE event_type = 'BaseEventType') AS base \
         FROM events",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let total: i64 = row.get("total");
    let base: i64 = row.get("base");
    assert!(total >= 5, "expected at least 5 events, got {total}");
    assert!(base >= 5, "expected at least 5 BaseEventType events, got {base}");
}
```

- [ ] **Step 2: Verify the test is skipped by default and passes when run**

Run: `cargo test -p ac-client`
Expected: e2e test listed as ignored; unit tests pass.

Run (with `docker compose up -d` done): `cargo test -p ac-client --test e2e -- --ignored --nocapture`
Expected: PASS in ~25 s.

- [ ] **Step 3: Write `README.md`** (replace the one-line stub)

````markdown
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
````

- [ ] **Step 4: Write `ac-server/README.md`**

````markdown
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
````

- [ ] **Step 5: Write `ac-client/README.md`**

````markdown
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
````

In the root `README.md` (from Step 3), add a line at the end of the intro
list pointing to the per-crate READMEs:

```markdown
Each crate has its own README with details: [`ac-server/`](ac-server/README.md),
[`ac-client/`](ac-client/README.md).
```

- [ ] **Step 6: Commit**

```bash
git add ac-client/tests/e2e.rs README.md ac-server/README.md ac-client/README.md
git commit -m "test: add ignored e2e pipeline test and usage READMEs"
```

---

## Self-review notes

- Spec coverage: URL parameter + anonymous-only (Task 6), browse event types (Task 6), monitored item + subscription (Task 9), 1 s `BaseEventType` with run-varying mock data (Tasks 4–5), `ExclusiveLevelAlarmType` raise/clear (Tasks 3–5), Postgres-in-Docker storage with typed columns + JSONB (Tasks 2, 8–9), error handling (fail fast on connect, log-and-skip per event — Tasks 5, 8, 9), unit + ignored e2e tests (Tasks 3, 4, 7, 10). No gaps.
- All async-opcua API shapes in this plan were verified against the 0.18 sources (see "Verified API facts"); where 0.18 crates.io releases might differ slightly from git master, tasks note the fallback (compiler-guided import fixes only — test expectations stay).

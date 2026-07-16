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
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

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
    // Seeded from OS entropy: mock data differs between runs. `StdRng` (unlike
    // the thread-local `ThreadRng`) is `Send`, so it can be held across the
    // `.await` points in this task.
    let mut rng = StdRng::from_os_rng();
    let mut sim = Simulation::new(5.0, HIGH_LIMIT);
    // The namespace argument is unused for ns0 event types.
    let namespaces = NamespaceMap::new();
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        interval.tick().await;

        // 1. A randomized BaseEventType event every tick.
        let base_evt = events::build_base_event(&mut rng, &sensor_id, "Sensor1");
        subscriptions
            .notify_events([(&base_evt as &dyn Event, &ObjectId::Server.into())].into_iter());

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
                // `sim.alarm_active()` reflects the state *after* `step()`, so it
                // agrees with the transition just observed.
                let active = sim.alarm_active();
                debug_assert_eq!(active, transition == AlarmTransition::Raised);
                let severity = if active {
                    sim.severity()
                } else {
                    CLEARED_SEVERITY
                };
                let alarm = events::build_level_alarm(
                    active,
                    sim.value(),
                    severity,
                    &sensor_id,
                    "Sensor1",
                    sim.high_limit(),
                    &namespaces,
                );
                subscriptions
                    .notify_events([(&alarm as &dyn Event, &ObjectId::Server.into())].into_iter());
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

//! Construction of the OPC UA events emitted by the simulation.

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

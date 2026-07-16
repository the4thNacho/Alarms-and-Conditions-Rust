//! Event filter field selection and decoding of received event notifications.

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
    /// Decode a notification's field values, looked up by name in [`EVENT_FIELDS`].
    /// Missing or unexpected variants become `None`.
    pub fn from_variants(fields: &[Variant]) -> Self {
        let by_name = |name: &str| -> &Variant {
            EVENT_FIELDS
                .iter()
                .position(|f| *f == name)
                .and_then(|i| fields.get(i))
                .unwrap_or(&Variant::Empty)
        };

        let raw: serde_json::Map<String, serde_json::Value> = EVENT_FIELDS
            .iter()
            .map(|name| (name.to_string(), raw_json_value(by_name(name))))
            .collect();

        Self {
            event_id: as_bytes(by_name("EventId")),
            event_type: event_type_name(by_name("EventType")),
            source_name: as_string(by_name("SourceName")),
            event_time: as_time(by_name("Time")),
            severity: as_severity(by_name("Severity")),
            message: as_string(by_name("Message")),
            condition_name: as_string(by_name("ConditionName")),
            active: as_bool(by_name("ActiveState/Id")),
            acked: as_bool(by_name("AckedState/Id")),
            raw: serde_json::Value::Object(raw),
        }
    }
}

/// Render a variant as a clean JSON value for the raw audit column.
fn raw_json_value(v: &Variant) -> serde_json::Value {
    match v {
        Variant::Empty => serde_json::Value::Null,
        Variant::Boolean(b) => (*b).into(),
        Variant::UInt16(n) => (*n).into(),
        Variant::String(s) => s.value().clone().unwrap_or_default().into(),
        Variant::LocalizedText(t) => t.text.to_string().into(),
        Variant::ByteString(b) => b
            .value
            .as_ref()
            .map(|bytes| bytes.iter().map(|x| format!("{x:02x}")).collect::<String>())
            .unwrap_or_default()
            .into(),
        other => format!("{other}").into(),
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
    // Well-known event types live in namespace 0 with numeric identifiers, so
    // compare the raw numeric id rather than allocating a NodeId to compare against.
    let name = match (id.namespace, id.as_u32()) {
        (0, Some(n)) if n == ObjectTypeId::BaseEventType as u32 => "BaseEventType".to_string(),
        (0, Some(n)) if n == ObjectTypeId::ExclusiveLevelAlarmType as u32 => {
            "ExclusiveLevelAlarmType".to_string()
        }
        _ => id.to_string(),
    };
    Some(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opcua::types::{
        ByteString, DateTime, LocalizedText, NodeId, ObjectTypeId, UAString, Variant,
    };

    fn alarm_variants() -> Vec<Variant> {
        vec![
            Variant::from(ByteString::from(vec![1u8, 2, 3])), // EventId
            Variant::from(NodeId::from(ObjectTypeId::ExclusiveLevelAlarmType)), // EventType
            Variant::from(UAString::from("Sensor1")),         // SourceName
            Variant::from(DateTime::now()),                   // Time
            Variant::from(625u16),                            // Severity
            Variant::from(LocalizedText::from("value exceeded limit")), // Message
            Variant::from(UAString::from("HighLevel")),       // ConditionName
            Variant::from(true),                              // ActiveState/Id
            Variant::from(false),                             // AckedState/Id
            Variant::from(true),                              // Retain
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
        assert_eq!(
            record.event_type.as_deref(),
            Some("ExclusiveLevelAlarmType")
        );
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

    #[test]
    fn raw_json_uses_clean_values() {
        let record = EventRecord::from_variants(&alarm_variants());
        assert_eq!(record.raw["Message"], "value exceeded limit"); // not a Debug dump
        assert_eq!(record.raw["EventId"], "010203"); // hex, not Debug
        assert_eq!(record.raw["Retain"], true);

        let record = EventRecord::from_variants(&[]);
        assert_eq!(record.raw["ConditionName"], serde_json::Value::Null); // Empty -> null
    }

    #[test]
    fn mismatched_variant_types_decode_to_none() {
        let mut variants = alarm_variants();
        variants[4] = Variant::from(UAString::from("not a number")); // Severity slot
        let record = EventRecord::from_variants(&variants);
        assert_eq!(record.severity, None);
    }

    #[test]
    fn unknown_event_type_falls_back_to_node_id_string() {
        let mut variants = alarm_variants();
        variants[1] = Variant::from(NodeId::new(2, 1234u32));
        let record = EventRecord::from_variants(&variants);
        assert_eq!(record.event_type.as_deref(), Some("ns=2;i=1234"));
    }
}

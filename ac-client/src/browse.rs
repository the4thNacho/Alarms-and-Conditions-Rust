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
    print_subtree(
        session,
        ObjectTypeId::BaseEventType.into(),
        "BaseEventType".into(),
        1,
    )
    .await
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
        if result.status_code.is_bad() {
            log::warn!("browse under {name} failed: {}", result.status_code);
        }

        if !result.continuation_point.is_null() {
            log::warn!("browse results truncated under {name}");
        }

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

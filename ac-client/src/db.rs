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

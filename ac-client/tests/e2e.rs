//! End-to-end test: spawns the real server and client binaries and asserts
//! events land in Postgres. Requires the Docker Postgres container to be up
//! (`docker compose up -d`), so it is ignored by default:
//!
//!   cargo test -p ac-client --test e2e -- --ignored

use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

const DATABASE_URL: &str = "postgres://ac:ac@localhost:5432/ac_events";

/// Cargo test binaries run with cwd set to the crate's manifest directory,
/// not the workspace root, so build binary paths from
/// `CARGO_MANIFEST_DIR` rather than assuming a relative `target/debug/...`.
fn workspace_target_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("ac-client has a parent (workspace root)")
        .join("target/debug")
}

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

    // Build first so the binaries below start fast and predictably.
    let status = Command::new("cargo")
        .args(["build", "-p", "ac-server", "-p", "ac-client"])
        .status()
        .unwrap();
    assert!(status.success(), "workspace build failed");

    let target_dir = workspace_target_dir();

    let _server = KillOnDrop(Command::new(target_dir.join("ac-server")).spawn().unwrap());
    tokio::time::sleep(Duration::from_secs(3)).await;

    let _client = KillOnDrop(
        Command::new(target_dir.join("ac-client"))
            .args(["opc.tcp://localhost:4855"])
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
    assert!(
        base >= 5,
        "expected at least 5 BaseEventType events, got {base}"
    );
}

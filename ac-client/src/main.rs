//! OPC UA Alarms & Conditions client.
//!
//! Connects anonymously to the given server URL, prints the server's event
//! type hierarchy, subscribes to events on the Server object, and stores
//! every received event in Postgres.

mod browse;
mod db;
mod subscriber;

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
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();
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
    session.disconnect().await.ok();
    event_loop_handle.await.ok();
    // Drop the last Session reference: the event callback (and the channel
    // sender it owns) lives inside the Session, so this closes the writer's
    // channel and lets it drain and exit.
    drop(session);
    writer.await.ok();
    Ok(())
}

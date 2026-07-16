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
    // used from Task 8 onwards
    let _ = &args.database_url;

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

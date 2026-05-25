//! Demonstrates leader election with a live Route53 backend.
//!
//! Usage:
//!   cargo run --example elect -- <shard_id> [hostname]
//!
//! Required environment variables:
//!   HOSTED_ZONE_ID   Route53 hosted zone ID (e.g. Z1234567890ABCDEF)
//!   BASE_DOMAIN      Base domain for lock records (e.g. locks.internal.example.com)
//!
//! Optional environment variables:
//!   LEASE_SECS       Lease duration in seconds (default: 15)
//!   RENEW_SECS       Renew period in seconds   (default: 5)
//!
//! AWS credentials are loaded from the standard chain:
//!   env vars → ~/.aws/config → IAM instance role
//!
//! Run multiple instances with the same shard_id to observe leader election.
//! Press Ctrl+C for a graceful shutdown that immediately releases the lock.

use shard_elect::{Config, Coordinator, Route53Locker};
use std::time::Duration;
use tracing::{error, info};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("shard_elect=debug".parse().unwrap())
                .add_directive("elect=info".parse().unwrap()),
        )
        .init();

    let (shard_id, hostname) = parse_args();

    let zone_id = require_env("HOSTED_ZONE_ID");
    let base_domain = require_env("BASE_DOMAIN");

    let lease_secs: u64 = env_u64("LEASE_SECS", 15);
    let renew_secs: u64 = env_u64("RENEW_SECS", 5);

    let owner_id = format!(
        "{}-{}-{}",
        hostname.replace(' ', "-"),
        std::process::id(),
        uuid::Uuid::new_v4(),
    );

    info!(shard_id, owner_id, "starting coordinator");

    let locker = Route53Locker::from_env(&zone_id, &base_domain).await;

    let config = Config::new(shard_id.clone(), locker)
        .owner_id(owner_id.clone())
        .lease_duration(Duration::from_secs(lease_secs))
        .renew_period(Duration::from_secs(renew_secs));

    let coordinator = Coordinator::start(config);

    // Spawn a task that prints a line every time leadership changes.
    let mut rx = coordinator.subscribe();
    tokio::spawn(async move {
        loop {
            match rx.changed().await {
                Ok(()) => match *rx.borrow_and_update() {
                    true  => info!(shard_id = %shard_id, owner_id = %owner_id, "*** acquired leadership ***"),
                    false => info!(shard_id = %shard_id, owner_id = %owner_id, "--- lost leadership ---"),
                },
                Err(_) => break, // coordinator shut down
            }
        }
    });

    // Wait for Ctrl+C then shut down gracefully.
    tokio::signal::ctrl_c().await.expect("failed to listen for Ctrl+C");
    info!("shutting down — releasing lock");

    if let Err(e) = coordinator.shutdown().await {
        error!(error = %e, "shutdown error");
        std::process::exit(1);
    }

    info!("done");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_args() -> (String, String) {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() || args.iter().any(|a| a == "-h" || a == "--help") {
        eprintln!(
            "Usage: elect <shard_id> [hostname]\n\
             \n\
             Required env vars:\n\
               HOSTED_ZONE_ID   Route53 hosted zone ID\n\
               BASE_DOMAIN      Base domain for lock records\n\
             \n\
             Optional env vars:\n\
               LEASE_SECS       Lease duration in seconds (default: 15)\n\
               RENEW_SECS       Renew period in seconds   (default: 5)\n\
             \n\
             Example:\n\
               HOSTED_ZONE_ID=Z123 BASE_DOMAIN=locks.example.com \\\n\
                 cargo run --example elect -- my-worker myhost"
        );
        std::process::exit(1);
    }

    let shard_id = args[0].clone();
    let hostname = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into()));

    (shard_id, hostname)
}

fn require_env(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| {
        eprintln!("error: environment variable {key} is required");
        std::process::exit(1);
    })
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

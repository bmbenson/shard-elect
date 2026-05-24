//! Distributed leader election using AWS Route53 TXT records.
//!
//! Leadership is stored as a TXT record at `{shard_id}.{base_domain}` with value
//! `"{owner_id} {expiry_unix_secs}"`.  Acquisition and renewal use Route53's
//! `ChangeResourceRecordSets` in a DELETE-old + CREATE-new atomic batch, giving
//! compare-and-swap semantics: if the stored value changed between our read and
//! our write, Route53 rejects the entire batch and we back off.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use shard_elect::{Coordinator, Config, Route53Locker};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() {
//!     let locker = Route53Locker::from_env(
//!         "Z1234567890ABCDEF",   // hosted zone ID
//!         "locks.internal.example.com",
//!     ).await;
//!
//!     let config = Config::new("my-worker", locker)
//!         .owner_id(format!("{}-{}", hostname(), std::process::id()))
//!         .lease_duration(Duration::from_secs(15))
//!         .renew_period(Duration::from_secs(5));
//!
//!     let coordinator = Coordinator::start(config);
//!
//!     // Poll the current state:
//!     if coordinator.is_leader() {
//!         println!("I am the leader");
//!     }
//!
//!     // Or react to changes via a watch channel:
//!     let mut rx = coordinator.subscribe();
//!     tokio::spawn(async move {
//!         loop {
//!             rx.changed().await.unwrap();
//!             println!("leadership changed: {}", *rx.borrow_and_update());
//!         }
//!     });
//!
//!     // Graceful shutdown releases the lock immediately.
//!     coordinator.shutdown().await.unwrap();
//! }
//!
//! fn hostname() -> String { "myhost".to_string() }
//! ```
//!
//! # Rate limits
//! Route53 allows ~5 `ChangeResourceRecordSets` calls/second per hosted zone.
//! With the default 5-second renew period a single coordinator makes ~1 call
//! every 5 seconds; you can safely run ~25 shards per zone at that rate.
//!

mod coordinator;
mod error;
mod locker;
mod route53;

pub use coordinator::{Config, Coordinator};
pub use error::Error;
pub use locker::Locker;
pub use route53::Route53Locker;

use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{interval, interval_at, MissedTickBehavior};
use tracing::{debug, info, warn};

use crate::{Error, Locker};

static DEFAULT_LEASE_DURATION_SEC: u64 = 15;
static DEFAULT_RENEW_PERIOD_SEC: u64 = 5;

/// Configuration for a [`Coordinator`].
///
/// # Defaults
/// - `lease_duration`: 15 seconds
/// - `renew_period`: 5 seconds (1/3 of lease — matches the Go library's recommendation)
/// - `owner_id`: a UUID (override with your hostname + PID for easier debugging)
///
/// # Invariant
/// `renew_period` must be less than `lease_duration`.  The coordinator will
/// panic at construction time if this is violated.
pub struct Config<L: Locker> {
    /// Logical name for the resource being coordinated (e.g. `"worker-shard-0"`).
    pub shard_id: String,
    /// Unique identifier for this process.  Defaults to a random UUID.
    pub owner_id: String,
    /// How long a successfully acquired lease is valid.
    pub lease_duration: Duration,
    /// How often to attempt renewal (and how often followers poll for an open slot).
    /// Followers attempt acquisition at `renew_period / 2` so they pick up expired
    /// leases faster than the leader renews.
    pub renew_period: Duration,
    /// The lock backend.
    pub locker: Arc<L>,
}

impl<L: Locker> Config<L> {
    /// Create a config with sensible defaults.
    pub fn new(shard_id: impl Into<String>, locker: L) -> Self {
        Self {
            shard_id: shard_id.into(),
            owner_id: uuid::Uuid::new_v4().to_string(),
            lease_duration: Duration::from_secs(DEFAULT_LEASE_DURATION_SEC),
            renew_period: Duration::from_secs(DEFAULT_RENEW_PERIOD_SEC),
            locker: Arc::new(locker),
        }
    }

    pub fn owner_id(mut self, id: impl Into<String>) -> Self {
        self.owner_id = id.into();
        self
    }

    pub fn lease_duration(mut self, d: Duration) -> Self {
        self.lease_duration = d;
        self
    }

    pub fn renew_period(mut self, d: Duration) -> Self {
        self.renew_period = d;
        self
    }
}

/// Runs a leader election loop in the background and exposes the current
/// leadership state via a `watch` channel.
///
/// # Background task
/// Two timers run concurrently:
/// - **Renew timer** (every `renew_period`): if this node is leader, extend the lease.
/// - **Acquire timer** (every `renew_period / 2`): if this node is follower, try to claim
///   an available or expired lease.
///
/// If renewal fails for any reason, the node immediately demotes itself to follower
/// (fail-safe: avoids zombie leaders during network partitions).
///
/// # Shutdown
/// Call [`Coordinator::shutdown`] to gracefully release the lock before stopping.
/// Dropping the coordinator without calling `shutdown` will abort the background task
/// without releasing the lock; the lease will expire naturally after `lease_duration`.
pub struct Coordinator {
    leader_rx: watch::Receiver<bool>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: JoinHandle<()>,
}

impl Coordinator {
    /// Start the coordinator.  Must be called from within a tokio runtime.
    ///
    /// # Panics
    /// Panics if `config.renew_period >= config.lease_duration`.
    pub fn start<L: Locker + 'static>(config: Config<L>) -> Self {
        assert!(
            config.renew_period < config.lease_duration,
            "renew_period ({:?}) must be less than lease_duration ({:?})",
            config.renew_period,
            config.lease_duration,
        );

        let (leader_tx, leader_rx) = watch::channel(false);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(run_loop(config, leader_tx, shutdown_rx));

        Self {
            leader_rx,
            shutdown_tx: Some(shutdown_tx),
            handle,
        }
    }

    /// Returns the current leadership state without blocking.
    pub fn is_leader(&self) -> bool {
        *self.leader_rx.borrow()
    }

    /// Subscribe to leadership state changes.
    ///
    /// The receiver holds the current value and is notified whenever it changes.
    /// Use [`tokio::sync::watch::Receiver::changed`] to wait for the next change.
    ///
    /// ```rust,ignore
    /// let mut rx = coordinator.subscribe();
    /// loop {
    ///     rx.changed().await.unwrap();
    ///     if *rx.borrow_and_update() {
    ///         println!("became leader");
    ///     } else {
    ///         println!("lost leadership");
    ///     }
    /// }
    /// ```
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.leader_rx.clone()
    }

    /// Gracefully release the lock and stop the background task.
    pub async fn shutdown(mut self) -> Result<(), Error> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.handle.await.map_err(|_| Error::Shutdown)
    }
}

// ---------------------------------------------------------------------------
// Background loop
// ---------------------------------------------------------------------------

async fn run_loop<L: Locker>(
    config: Config<L>,
    leader_tx: watch::Sender<bool>,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    let mut renew_ticker = interval(config.renew_period);
    renew_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    // Followers poll at half the renew period so they react faster to an expired lease.
    let acquire_period = config.renew_period / 2;
    let mut acquire_ticker = interval_at(
        tokio::time::Instant::now() + acquire_period,
        acquire_period,
    );
    acquire_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut is_leader = false;

    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                if is_leader {
                    info!(
                        shard_id = %config.shard_id,
                        owner_id = %config.owner_id,
                        "releasing lock on shutdown"
                    );
                    if let Err(e) = config.locker.release(&config.shard_id, &config.owner_id).await {
                        warn!(error = %e, "failed to release lock on shutdown");
                    }
                }
                break;
            }

            _ = renew_ticker.tick(), if is_leader => {
                let expires_at = SystemTime::now() + config.lease_duration;
                match config.locker.renew(&config.shard_id, &config.owner_id, expires_at).await {
                    Ok(true) => {
                        debug!(shard_id = %config.shard_id, "lease renewed");
                    }
                    Ok(false) => {
                        warn!(
                            shard_id = %config.shard_id,
                            owner_id = %config.owner_id,
                            "lease renewal returned false — demoting to follower"
                        );
                        is_leader = false;
                        let _ = leader_tx.send(false);
                    }
                    Err(e) => {
                        warn!(
                            shard_id = %config.shard_id,
                            error = %e,
                            "lease renewal failed — demoting to follower (fail-safe)"
                        );
                        is_leader = false;
                        let _ = leader_tx.send(false);
                    }
                }
            }

            _ = acquire_ticker.tick(), if !is_leader => {
                let expires_at = SystemTime::now() + config.lease_duration;
                match config.locker.try_acquire(&config.shard_id, &config.owner_id, expires_at).await {
                    Ok(true) => {
                        info!(
                            shard_id = %config.shard_id,
                            owner_id = %config.owner_id,
                            "acquired leadership"
                        );
                        is_leader = true;
                        let _ = leader_tx.send(true);
                    }
                    Ok(false) => {
                        debug!(shard_id = %config.shard_id, "lock still held by another node");
                    }
                    Err(e) => {
                        warn!(shard_id = %config.shard_id, error = %e, "acquire attempt failed");
                    }
                }
            }
        }
    }
}

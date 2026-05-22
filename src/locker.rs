use async_trait::async_trait;
use std::time::SystemTime;

use crate::Error;

/// Backend storage for a distributed lock.
///
/// Route53 is the primary implementation, but the trait exists so that test doubles
/// and alternative backends (e.g. DynamoDB, etcd) can be swapped in without changing
/// the coordinator logic.
///
/// ## Semantics
/// - `try_acquire` and `renew` return `Ok(true)` on success, `Ok(false)` on contention
///   (another node holds the lock or a CAS race was lost), and `Err` only for
///   infrastructure failures.
/// - All operations must be safe to call concurrently from multiple tokio tasks.
/// - Implementations should be idempotent wherever possible.
#[async_trait]
pub trait Locker: Send + Sync {
    /// Atomically acquire the lock for `shard_id` owned by `owner_id`, valid until
    /// `expires_at`. Returns `true` if the lock was acquired.
    ///
    /// Succeeds when:
    /// - No lock record exists, or
    /// - The existing lock has expired, or
    /// - The existing lock already belongs to `owner_id` (idempotent re-acquire).
    async fn try_acquire(
        &self,
        shard_id: &str,
        owner_id: &str,
        expires_at: SystemTime,
    ) -> Result<bool, Error>;

    /// Extend the lease for a lock that `owner_id` currently holds.
    /// Returns `false` if the record no longer belongs to `owner_id`.
    async fn renew(
        &self,
        shard_id: &str,
        owner_id: &str,
        expires_at: SystemTime,
    ) -> Result<bool, Error>;

    /// Release the lock if it is currently held by `owner_id`.
    /// Silently succeeds if the record is missing or owned by someone else.
    async fn release(&self, shard_id: &str, owner_id: &str) -> Result<(), Error>;
}

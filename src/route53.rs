use async_trait::async_trait;
use aws_sdk_route53::{
    Client,
    types::{Change, ChangeBatch, ChangeAction, ResourceRecord, ResourceRecordSet, RrType},
};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};

use crate::{
    error::{api_err, Error},
    locker::Locker,
};

/// A [`Locker`] backed by AWS Route53 TXT records.
///
/// Each shard gets a TXT record at `{shard_id}.{base_domain}` whose value is
/// `"{owner_id} {expiry_unix_secs}"`.  Leadership is transferred via an atomic
/// `ChangeResourceRecordSets` batch that DELETEs the old value and CREATEs the
/// new one in a single API call — if the DELETE no longer matches (another node
/// raced us), Route53 rejects the entire batch with `InvalidChangeBatch`.
///
/// ## Rate limits
/// Route53 allows ~5 `ChangeResourceRecordSets` calls/second per hosted zone.
/// With a 10-second renew period you can safely manage ~25 shards per zone.
pub struct Route53Locker {
    client: Client,
    hosted_zone_id: String,
    base_domain: String,
    /// DNS TTL on the TXT record (does not affect lease expiry, only resolver caching).
    dns_ttl: i64,
}

impl Route53Locker {
    /// Create a locker from an existing Route53 client.
    ///
    /// - `hosted_zone_id`: the Route53 hosted zone ID (`Z...`); the `/hostedzone/` prefix is
    ///   stripped automatically.
    /// - `base_domain`: domain suffix for lock records, e.g. `"locks.internal.example.com"`.
    ///   The final record name will be `{shard_id}.{base_domain}.`
    pub fn new(
        client: Client,
        hosted_zone_id: impl Into<String>,
        base_domain: impl Into<String>,
    ) -> Self {
        let zone_id = {
            let raw = hosted_zone_id.into();
            raw.strip_prefix("/hostedzone/").unwrap_or(&raw).to_string()
        };
        let domain = base_domain.into();
        let domain = domain.trim_end_matches('.').to_string();

        Self {
            client,
            hosted_zone_id: zone_id,
            base_domain: domain,
            dns_ttl: 60,
        }
    }

    /// Load AWS config from the environment and build a locker.
    ///
    /// Reads credentials from the standard chain: env vars → shared config file → IAM role.
    pub async fn from_env(
        hosted_zone_id: impl Into<String>,
        base_domain: impl Into<String>,
    ) -> Self {
        let cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = Client::new(&cfg);
        Self::new(client, hosted_zone_id, base_domain)
    }

    /// Override the DNS TTL on TXT records (default: 60 seconds).
    pub fn with_dns_ttl(mut self, ttl_secs: i64) -> Self {
        self.dns_ttl = ttl_secs;
        self
    }

    fn record_name(&self, shard_id: &str) -> String {
        format!("{}.{}.", shard_id, self.base_domain)
    }

    async fn get_current(&self, shard_id: &str) -> Result<Option<LockRecord>, Error> {
        let name = self.record_name(shard_id);

        let resp = self
            .client
            .list_resource_record_sets()
            .hosted_zone_id(&self.hosted_zone_id)
            .start_record_name(&name)
            .start_record_type(RrType::Txt)
            .max_items(1)
            .send()
            .await
            .map_err(api_err)?;

        for rrset in resp.resource_record_sets() {
            let rrset_name = rrset.name().to_lowercase();
            let rrset_name = rrset_name.trim_end_matches('.');
            let expected = name.to_lowercase();
            let expected = expected.trim_end_matches('.');

            if rrset_name == expected && rrset.r#type() == &RrType::Txt {
                for rr in rrset.resource_records() {
                    let raw = rr.value().trim_matches('"');
                    match LockRecord::parse(raw) {
                        Some(rec) => return Ok(Some(rec)),
                        None => {
                            warn!(value = raw, "ignoring unrecognized TXT record value");
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    /// Execute a DELETE-old + CREATE-new atomic batch.
    ///
    /// Returns `Ok(true)` if the swap succeeded, `Ok(false)` if `InvalidChangeBatch`
    /// was returned (another node raced us), and `Err` for all other failures.
    async fn atomic_swap(
        &self,
        shard_id: &str,
        old: Option<&LockRecord>,
        new: &LockRecord,
    ) -> Result<bool, Error> {
        let name = self.record_name(shard_id);
        let mut changes: Vec<Change> = Vec::new();

        if let Some(old_rec) = old {
            changes.push(
                Change::builder()
                    .action(ChangeAction::Delete)
                    .resource_record_set(txt_rrset(&name, old_rec.encode(), self.dns_ttl))
                    .build()
                    .expect("change has all required fields"),
            );
        }

        changes.push(
            Change::builder()
                .action(ChangeAction::Create)
                .resource_record_set(txt_rrset(&name, new.encode(), self.dns_ttl))
                .build()
                .expect("change has all required fields"),
        );

        let result = self
            .client
            .change_resource_record_sets()
            .hosted_zone_id(&self.hosted_zone_id)
            .change_batch(
                ChangeBatch::builder()
                    .set_changes(Some(changes))
                    .build()
                    .expect("change_batch has all required fields"),
            )
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(e) if is_condition_failed(&e) => {
                debug!(shard_id, "CAS failed: another node holds the lock");
                Ok(false)
            }
            Err(e) => Err(api_err(e)),
        }
    }
}

#[async_trait]
impl Locker for Route53Locker {
    async fn try_acquire(
        &self,
        shard_id: &str,
        owner_id: &str,
        expires_at: SystemTime,
    ) -> Result<bool, Error> {
        let new = LockRecord::new(owner_id, expires_at);
        let current = self.get_current(shard_id).await?;

        match &current {
            Some(existing) if !existing.is_expired() && existing.owner_id != owner_id => {
                // Lock is actively held by someone else.
                Ok(false)
            }
            existing => self.atomic_swap(shard_id, existing.as_ref(), &new).await,
        }
    }

    async fn renew(
        &self,
        shard_id: &str,
        owner_id: &str,
        expires_at: SystemTime,
    ) -> Result<bool, Error> {
        let current = self.get_current(shard_id).await?;

        match current {
            Some(existing) if existing.owner_id == owner_id => {
                let new = LockRecord::new(owner_id, expires_at);
                self.atomic_swap(shard_id, Some(&existing), &new).await
            }
            _ => Ok(false),
        }
    }

    async fn release(&self, shard_id: &str, owner_id: &str) -> Result<(), Error> {
        let current = self.get_current(shard_id).await?;

        let Some(existing) = current else {
            return Ok(());
        };
        if existing.owner_id != owner_id {
            return Ok(());
        }

        let name = self.record_name(shard_id);
        let result = self
            .client
            .change_resource_record_sets()
            .hosted_zone_id(&self.hosted_zone_id)
            .change_batch(
                ChangeBatch::builder()
                    .changes(
                        Change::builder()
                            .action(ChangeAction::Delete)
                            .resource_record_set(txt_rrset(&name, existing.encode(), self.dns_ttl))
                            .build()
                            .expect("change has all required fields"),
                    )
                    .build()
                    .expect("change_batch has all required fields"),
            )
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            // Record was already gone or taken — that's fine for a release.
            Err(e) if is_condition_failed(&e) => Ok(()),
            Err(e) => Err(api_err(e)),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn txt_rrset(name: &str, value: String, ttl: i64) -> ResourceRecordSet {
    ResourceRecordSet::builder()
        .name(name)
        .r#type(RrType::Txt)
        .ttl(ttl)
        .resource_records(
            ResourceRecord::builder()
                .value(format!("\"{value}\""))
                .build()
                .expect("resource record has all required fields"),
        )
        .build()
        .expect("resource record set has all required fields")
}

fn is_condition_failed(
    e: &aws_sdk_route53::error::SdkError<
        aws_sdk_route53::operation::change_resource_record_sets::ChangeResourceRecordSetsError,
    >,
) -> bool {
    e.as_service_error()
        .map(|se| se.is_invalid_change_batch())
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// LockRecord — the value stored in the TXT record
// ---------------------------------------------------------------------------

struct LockRecord {
    owner_id: String,
    expires_unix: u64,
}

impl LockRecord {
    fn new(owner_id: &str, expires_at: SystemTime) -> Self {
        let expires_unix = expires_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            owner_id: owner_id.to_string(),
            expires_unix,
        }
    }

    /// Parse `"owner_id expiry_unix"` (without surrounding quotes).
    fn parse(s: &str) -> Option<Self> {
        let (owner, expiry_str) = s.split_once(' ')?;
        let expires_unix = expiry_str.trim().parse().ok()?;
        Some(Self {
            owner_id: owner.to_string(),
            expires_unix,
        })
    }

    fn encode(&self) -> String {
        format!("{} {}", self.owner_id, self.expires_unix)
    }

    fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now >= self.expires_unix
    }
}

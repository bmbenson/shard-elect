# shard-elect

Distributed leader election for Rust applications using AWS Route53 as the coordination backend.

Leadership is stored as a DNS TXT record. Acquiring and renewing it uses Route53's `ChangeResourceRecordSets` in an atomic DELETE-old + CREATE-new batch, giving compare-and-swap semantics without a separate locking service.

## Credits

The core idea — using Route53 `ChangeResourceRecordSets` as a compare-and-swap primitive for leader election — comes from Craig Howard's talk **[Leader Election with Amazon DynamoDB](https://www.youtube.com/watch?v=YZUNNzLDWb8)**, which describes how Amazon uses this pattern internally.

The coordinator architecture, `Locker` trait design, and timing model are inspired by the excellent Go library **[gurre/shardcoordinator](https://github.com/gurre/shardcoordinator)**. This crate is a Rust port of the Route53 backend from that project, with a `tokio::sync::watch` channel added for reactive leadership notifications.

---

---

## Features

- **Single-leader guarantee** — atomic `ChangeResourceRecordSets` batches ensure only one node wins each election
- **Fail-safe demotion** — any renewal failure immediately demotes the node to follower, preventing zombie leaders
- **Reactive state** — leadership changes are broadcast over a `watch` channel; no polling required
- **Graceful shutdown** — releases the lock immediately so a follower can take over without waiting for the TTL to expire
- **Pluggable backend** — implement the `Locker` trait to use any storage layer (in-process mock, DynamoDB, etcd, …)

---

## Installation

```toml
[dependencies]
shard-elect = "0.1"
```

---

## Quick Start

```rust
use shard_elect::{Coordinator, Config, Route53Locker};
use std::time::Duration;

#[tokio::main]
async fn main() {
    // Reads credentials from the standard chain:
    //   env vars → ~/.aws/config → IAM instance role
    let locker = Route53Locker::from_env(
        "Z1234567890ABCDEF",        // hosted zone ID
        "locks.internal.example.com",
    ).await;

    let owner_id = format!(
        "{}-{}-{}",
        hostname(),
        std::process::id(),
        uuid::Uuid::new_v4(),
    );

    let config = Config::new("batch-processor", locker)
        .owner_id(owner_id)
        .lease_duration(Duration::from_secs(15))
        .renew_period(Duration::from_secs(5));

    let coordinator = Coordinator::start(config);

    // Option A — poll
    if coordinator.is_leader() {
        println!("I am the leader");
    }

    // Option B — react to changes via watch channel
    let mut rx = coordinator.subscribe();
    tokio::spawn(async move {
        loop {
            rx.changed().await.unwrap();
            match *rx.borrow_and_update() {
                true  => println!("acquired leadership"),
                false => println!("lost leadership"),
            }
        }
    });

    // Graceful shutdown: releases the lock immediately
    coordinator.shutdown().await.unwrap();
}

fn hostname() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into())
}
```

---

## How It Works

### The Lock Record

Each shard gets a TXT record in Route53:

```
Name:  {shard_id}.{base_domain}.
Type:  TXT
Value: "{owner_id} {expiry_unix_secs}"
TTL:   60  (resolver cache only — the lease expiry is in the value)
```

### Atomic Compare-and-Swap

Acquisition and renewal are a single `ChangeResourceRecordSets` call with two changes batched together:

```
DELETE  old TXT record (exact value match required)
CREATE  new TXT record (updated owner + expiry)
```

Route53 treats the batch as a transaction: if the DELETE no longer matches (another node updated the record first), the entire batch is rejected with `InvalidChangeBatch` and we back off.

**Race example — two followers competing for an expired lease:**

| Step | Coordinator A | Coordinator B |
|------|--------------|--------------|
| 1 | Read: `"worker-1 1000"` (expired) | Read: `"worker-1 1000"` (expired) |
| 2 | Submit batch: DELETE `"worker-1 1000"` + CREATE `"worker-2 2000"` | Submit batch: DELETE `"worker-1 1000"` + CREATE `"worker-3 2000"` |
| 3 | ✅ Batch accepted — A is the new leader | ❌ DELETE no longer matches `"worker-2 2000"` — batch rejected |
| 4 | — | Returns `Ok(false)`, stays follower, retries next tick |

### Coordinator Loop

Two timers run concurrently inside the background task:

| Timer | Period | Action |
|-------|--------|--------|
| **Renew** | `renew_period` | If leader: extend the lease. On any failure, demote immediately. |
| **Acquire** | `renew_period / 2` | If follower: try to claim an available or expired lease. |

Followers poll at half the renew period so they pick up expired leases before the next renew tick fires.

### Leadership State

```
                    ┌──────────────────────────────┐
                    │                              │
                    ▼                              │  renew ok
              ┌──────────┐  try_acquire ok   ┌──────────┐
  Start ────► │ Follower │──────────────────►│  Leader  │
              └──────────┘                   └──────────┘
                    ▲                              │
                    │    renew fails / returns     │
                    └──────────── false ───────────┘
```

---

## Configuration

```rust
Config::new("shard-id", locker)
    .owner_id("hostname-pid-uuid")   // default: random UUID
    .lease_duration(Duration::from_secs(15))  // default: 15s
    .renew_period(Duration::from_secs(5));    // default: 5s
```

**Rule:** `renew_period` must be strictly less than `lease_duration`. A 3× ratio (`lease = 3 × renew`) is the recommended starting point — it gives two missed renewals before the lease expires.

| Profile | `lease_duration` | `renew_period` | Use case |
|---------|-----------------|----------------|----------|
| Fast failover | 10 s | 3 s | Critical systems |
| **Balanced (default)** | **15 s** | **5 s** | **General purpose** |
| Stable | 30 s | 10 s | Low-churn workloads |

### Owner ID format

`owner_id` is stored verbatim in the TXT record value and split on the first space. **Do not include spaces, tabs, or quotes.** A safe pattern:

```rust
let owner_id = format!(
    "{}-{}-{}",
    hostname.replace(' ', "-"),
    std::process::id(),
    uuid::Uuid::new_v4(),
);
```

---

## AWS Setup

### Hosted Zone

Use an existing private hosted zone or create a dedicated one:

```bash
aws route53 create-hosted-zone \
    --name locks.internal.example.com \
    --caller-reference "$(date +%s)" \
    --hosted-zone-config PrivateZone=true \
    --vpc VPCRegion=us-east-1,VPCId=vpc-xxxxxxxx
```

### IAM Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "route53:ChangeResourceRecordSets",
        "route53:ListResourceRecordSets"
      ],
      "Resource": "arn:aws:route53:::hostedzone/Z1234567890ABCDEF"
    }
  ]
}
```

---

## Rate Limits and Scaling

Route53 enforces **5 `ChangeResourceRecordSets` calls per second per hosted zone**. At steady state each shard makes one call per `renew_period`; followers make one call per `renew_period / 2` while competing.

```
Steady-state calls/sec = num_shards / renew_period
```

Recommended safe shard counts:

| `renew_period` | Safe shards (50% headroom) |
|---------------|--------------------------|
| 5 s | 12 |
| 10 s | 25 |
| 15 s | 37 |
| 20 s | 50 |

**Cost estimate (Route53, 2024 pricing):**

```
Monthly cost ≈ (num_shards / renew_period_secs) × 86400 × 30 / 1_000_000 × $0.50
```

Example: 25 shards × 10 s renewal ≈ **$2.70/month**.

---

## Failure Modes

| Failure | Trigger | Consequence |
|---------|---------|-------------|
| **Renewal I/O error** | Transient network issue or throttle | Node demotes immediately (fail-safe). Leaderless until another node acquires. |
| **Process crash (no shutdown)** | SIGKILL, OOM, power loss | Lock held until TTL expires. Leaderless period = remaining TTL + next acquire tick. |
| **Clock skew between nodes** | NTP drift > 1 s | Node may consider a lease expired while the owner still thinks it's valid. Brief split-brain possible. |
| **Route53 rate limit exhaustion** | > 5 `ChangeResourceRecordSets`/sec | All operations fail. All nodes demote. Leaderless until rate recovers. |
| **Slow backend at shutdown** | Route53 slow during `shutdown()` | `shutdown()` blocks until the release call completes or errors. Add a context timeout if needed. |

---

## Custom Backends

Implement `Locker` to use any storage layer — useful for testing or alternative backends:

```rust
use async_trait::async_trait;
use shard_elect::{Locker, Error};
use std::time::SystemTime;

struct MyLocker { /* ... */ }

#[async_trait]
impl Locker for MyLocker {
    /// Return Ok(true) if acquired, Ok(false) if another node holds the lock.
    async fn try_acquire(
        &self,
        shard_id: &str,
        owner_id: &str,
        expires_at: SystemTime,
    ) -> Result<bool, Error> { todo!() }

    /// Return Ok(false) if ownership changed since last acquire.
    async fn renew(
        &self,
        shard_id: &str,
        owner_id: &str,
        expires_at: SystemTime,
    ) -> Result<bool, Error> { todo!() }

    /// Silently succeed if the lock is already gone or held by someone else.
    async fn release(&self, shard_id: &str, owner_id: &str) -> Result<(), Error> { todo!() }
}
```

**Contract:**
- `try_acquire` and `renew` return `Ok(false)` on contention, `Err` only for infrastructure failures.
- All three methods must be safe to call concurrently from multiple tokio tasks.
- Implementations must have atomic compare-and-swap semantics to guarantee single-leader.

---

## License

MIT

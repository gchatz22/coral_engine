//! In-memory [`AgentStorage`] backed by a `BTreeMap`.
//!
//! Gated behind `#[cfg(any(test, feature = "memory-storage"))]` (decision
//! 6 in `scratch/agent_storage.md` § 13). Tests in any workspace crate
//! get access for free via `#[cfg(test)]`; production binaries opt in
//! via the `memory-storage` feature.
//!
//! # Why `BTreeMap`?
//!
//! The trait's `list` method returns keys in lex-ascending order. A
//! `BTreeMap` iterator yields keys in that exact order natively, so
//! `list` is a cheap range walk rather than "scan + sort". `HashMap`
//! would force a sort on every list call for no benefit at the scale
//! `MemoryStorage` is intended for (tests + dev spike-checks).
//!
//! # Concurrency
//!
//! Wrapped in `tokio::sync::RwLock` so concurrent readers don't block
//! each other while the trait still presents an immutable `&self`
//! surface. Single-writer-per-agent — promised by the wider engine
//! architecture — means contention is bounded; we use the async-aware
//! `tokio::sync::RwLock` rather than `std::sync::RwLock` so a `.lock()`
//! call inside an `async fn` cannot deadlock the runtime when the lock
//! is contended.

use super::{AgentStorage, ListPage, PutOutcome, StorageResult};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeMap;
use tokio::sync::RwLock;

/// In-memory storage backed by a `BTreeMap<String, Bytes>`. Cheap to
/// clone via `Arc` (no need for `Clone` on the type itself).
#[derive(Debug, Default)]
pub struct MemoryStorage {
    inner: RwLock<BTreeMap<String, Bytes>>,
}

impl MemoryStorage {
    /// Build an empty `MemoryStorage`.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl AgentStorage for MemoryStorage {
    async fn put(&self, key: &str, value: Bytes) -> StorageResult<()> {
        let mut guard = self.inner.write().await;
        guard.insert(key.to_string(), value);
        Ok(())
    }

    async fn put_if_absent(&self, key: &str, value: Bytes) -> StorageResult<PutOutcome> {
        let mut guard = self.inner.write().await;
        if guard.contains_key(key) {
            return Ok(PutOutcome::Existed);
        }
        guard.insert(key.to_string(), value);
        Ok(PutOutcome::Created)
    }

    async fn get(&self, key: &str) -> StorageResult<Option<Bytes>> {
        let guard = self.inner.read().await;
        // `Bytes::clone` is Arc-cheap.
        Ok(guard.get(key).cloned())
    }

    async fn get_many(&self, keys: &[&str]) -> StorageResult<Vec<Option<Bytes>>> {
        // Single read-lock acquisition for the whole batch: the backing
        // store is in-memory so there's no per-key parallelism to gain
        // by spawning N tasks, and one lock keeps the snapshot
        // consistent across the batch.
        let guard = self.inner.read().await;
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            out.push(guard.get(*k).cloned());
        }
        Ok(out)
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        let mut guard = self.inner.write().await;
        guard.remove(key); // idempotent: None when absent
        Ok(())
    }

    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StorageResult<ListPage> {
        let guard = self.inner.read().await;
        let mut keys = Vec::with_capacity(limit.min(64));
        let mut overflow = false;

        // `BTreeMap::range` over the whole map then filter; using the
        // prefix as a lower bound would still need to stop at the upper
        // bound, and computing the upper bound from an arbitrary UTF-8
        // prefix (`prefix` + char::MAX) is more code than it's worth at
        // MemoryStorage's scale.
        for k in guard.keys() {
            if !k.starts_with(prefix) {
                continue;
            }
            if let Some(after) = after {
                if k.as_str() <= after {
                    continue;
                }
            }
            if keys.len() == limit {
                overflow = true;
                break;
            }
            keys.push(k.clone());
        }

        let next_cursor = if overflow { keys.last().cloned() } else { None };

        Ok(ListPage { keys, next_cursor })
    }
}

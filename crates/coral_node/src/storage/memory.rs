//! In-memory [`AgentStorage`] backed by a `BTreeMap` wrapped in a
//! `tokio::sync::RwLock`. `BTreeMap` yields keys in lex-ascending order
//! natively, matching the `list` contract without a sort step. Gated
//! behind `#[cfg(any(test, feature = "memory-storage"))]`.

use super::{AgentStorage, ListPage, PutOutcome, StorageResult};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeMap;
use tokio::sync::RwLock;

/// In-memory storage backed by a `BTreeMap<String, Bytes>`.
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
        Ok(guard.get(key).cloned())
    }

    async fn get_many(&self, keys: &[&str]) -> StorageResult<Vec<Option<Bytes>>> {
        // One read-lock keeps the batch snapshot consistent.
        let guard = self.inner.read().await;
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            out.push(guard.get(*k).cloned());
        }
        Ok(out)
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        let mut guard = self.inner.write().await;
        guard.remove(key);
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

        // Filtering the full key iterator beats computing a UTF-8 upper
        // bound at this scale.
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

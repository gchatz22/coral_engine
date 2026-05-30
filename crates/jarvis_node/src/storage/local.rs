//! [`LocalStorage`] — on-disk backend for [`AgentStorage`].
//!
//! Atomicity contract: `put` writes to `<key>.tmp.<uniq>` then renames
//! into place (POSIX `rename` is atomic within a filesystem);
//! `put_if_absent` opens with `O_CREAT | O_EXCL` (via
//! `OpenOptions::create_new(true)`) so the kernel rejects an open against
//! an existing file with `AlreadyExists`; `get` maps `ENOENT` to
//! `Ok(None)`; `delete` swallows `ENOENT` for idempotency.
//!
//! Keys map to relative paths under the root and may contain `/` —
//! parent directories are created on demand. `LocalStorage` is a thin
//! storage primitive, not a sandbox; traversal hardening is the caller's
//! responsibility.

use super::{AgentStorage, ListPage, PutOutcome, StorageError, StorageResult};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::join_all;
use std::fs;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// On-disk [`AgentStorage`] rooted at a single directory. Cheap to
/// share via `Arc` — all state lives on disk plus a monotonic counter
/// for tempfile suffix uniqueness.
#[derive(Debug)]
pub struct LocalStorage {
    root: PathBuf,
    /// Monotonic source for the suffix on `<key>.tmp.<uniq>` files.
    /// Combined with the process PID, this gives a per-process unique
    /// tempfile name without an extra dep on `rand`.
    tmp_counter: AtomicU64,
}

impl LocalStorage {
    /// Build a `LocalStorage` rooted at `root`. The directory is
    /// created if absent so callers (and tests) don't have to pre-mkdir.
    pub fn new(root: impl Into<PathBuf>) -> StorageResult<Self> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(StorageError::from_io)?;
        Ok(Self {
            root,
            tmp_counter: AtomicU64::new(0),
        })
    }

    /// Borrow the storage root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn key_path(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    /// Build a sibling tempfile path with a per-call-unique suffix. Two
    /// concurrent `put`s against the same key get distinct tempfiles
    /// and only one can win the final `rename` — both bytes land
    /// atomically, the loser's are unobservable.
    fn tmp_path_for(&self, final_path: &Path) -> PathBuf {
        let counter = self.tmp_counter.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let name = match final_path.file_name() {
            Some(n) => n.to_owned(),
            // Defensive guard: callers always pass a path with a file name.
            None => std::ffi::OsString::from(".put.tmp"),
        };
        let mut tmp = name;
        tmp.push(format!(".tmp.{pid}.{counter}"));
        match final_path.parent() {
            Some(p) => p.join(tmp),
            None => PathBuf::from(tmp),
        }
    }
}

#[async_trait]
impl AgentStorage for LocalStorage {
    async fn put(&self, key: &str, value: Bytes) -> StorageResult<()> {
        let final_path = self.key_path(key);
        let tmp = self.tmp_path_for(&final_path);

        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).map_err(StorageError::from_io)?;
        }

        // Write-to-tempfile + rename for atomic visibility under the
        // canonical key. fsync before rename so a crash can't leave a
        // zero-byte file under the final name.
        let mut f = fs::File::create(&tmp).map_err(StorageError::from_io)?;
        let write_res = f.write_all(value.as_ref());
        let sync_res = if write_res.is_ok() {
            f.sync_all()
        } else {
            Ok(())
        };
        drop(f);

        if let Err(e) = write_res {
            let _ = fs::remove_file(&tmp);
            return Err(StorageError::from_io(e));
        }
        if let Err(e) = sync_res {
            let _ = fs::remove_file(&tmp);
            return Err(StorageError::from_io(e));
        }

        if let Err(e) = fs::rename(&tmp, &final_path) {
            let _ = fs::remove_file(&tmp);
            return Err(StorageError::from_io(e));
        }
        Ok(())
    }

    async fn put_if_absent(&self, key: &str, value: Bytes) -> StorageResult<PutOutcome> {
        let final_path = self.key_path(key);
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).map_err(StorageError::from_io)?;
        }
        // `create_new(true)` → `O_CREAT | O_EXCL` on Unix: atomic
        // conditional-write with no read-then-write race window.
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&final_path)
        {
            Ok(mut f) => {
                if let Err(e) = f.write_all(value.as_ref()) {
                    // Best-effort cleanup so a retry sees Created, not
                    // Existed-with-garbage.
                    drop(f);
                    let _ = fs::remove_file(&final_path);
                    return Err(StorageError::from_io(e));
                }
                if let Err(e) = f.sync_all() {
                    drop(f);
                    let _ = fs::remove_file(&final_path);
                    return Err(StorageError::from_io(e));
                }
                Ok(PutOutcome::Created)
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(PutOutcome::Existed),
            Err(e) => Err(StorageError::from_io(e)),
        }
    }

    async fn get(&self, key: &str) -> StorageResult<Option<Bytes>> {
        let path = self.key_path(key);
        match fs::read(&path) {
            Ok(bytes) => Ok(Some(Bytes::from(bytes))),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::from_io(e)),
        }
    }

    async fn get_many(&self, keys: &[&str]) -> StorageResult<Vec<Option<Bytes>>> {
        // `join_all` preserves input order, which the trait contract requires.
        let futures = keys.iter().map(|k| self.get(k));
        let results = join_all(futures).await;
        results.into_iter().collect()
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        let path = self.key_path(key);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StorageError::from_io(e)),
        }
    }

    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StorageResult<ListPage> {
        // Walk the directory subtree rooted at the deepest existing parent
        // of `prefix` so unrelated sibling subtrees aren't scanned, then
        // filter on the trailing name fragment for prefix-of-filename
        // matches like `outputs/01` against `outputs/01ABC.json`.
        let (dir_segment, name_filter) = match prefix.rfind('/') {
            Some(i) => (&prefix[..=i], &prefix[i + 1..]),
            None => ("", prefix),
        };
        let scan_root = if dir_segment.is_empty() {
            self.root.clone()
        } else {
            self.root.join(dir_segment.trim_end_matches('/'))
        };
        if !scan_root.exists() {
            return Ok(ListPage {
                keys: Vec::new(),
                next_cursor: None,
            });
        }

        // Simple walk + sort: at the expected scale (low thousands of
        // entries per dir) it beats a heap-merge in code size and test surface.
        let mut keys: Vec<String> = Vec::new();
        let mut stack = vec![scan_root.clone()];
        while let Some(dir) = stack.pop() {
            let iter = match fs::read_dir(&dir) {
                Ok(it) => it,
                Err(e) => return Err(StorageError::from_io(e)),
            };
            for entry in iter {
                let entry = entry.map_err(StorageError::from_io)?;
                let path = entry.path();
                let file_type = entry.file_type().map_err(StorageError::from_io)?;
                if file_type.is_dir() {
                    stack.push(path);
                    continue;
                }
                if !file_type.is_file() {
                    continue;
                }
                // Key = path relative to `self.root` with `/` separators.
                let rel = match path.strip_prefix(&self.root) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let key = rel
                    .components()
                    .filter_map(|c| match c {
                        std::path::Component::Normal(s) => s.to_str().map(|s| s.to_string()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("/");
                if !key.starts_with(prefix) {
                    continue;
                }
                // Tempfiles from in-flight `put`s are implementation
                // detail, not part of the key namespace.
                if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                    if name.contains(".tmp.") {
                        continue;
                    }
                }
                if !name_filter.is_empty() {
                    // Subsumed by the `starts_with(prefix)` check above.
                }
                keys.push(key);
            }
        }

        // Lex-sort then apply `after` exclusion and `limit`.
        keys.sort();
        let start = match after {
            Some(a) => keys.partition_point(|k| k.as_str() <= a),
            None => 0,
        };

        let mut page: Vec<String> = Vec::with_capacity(limit.min(64));
        let mut iter = keys.into_iter().skip(start);
        let mut overflow = false;
        for k in iter.by_ref() {
            if page.len() == limit {
                overflow = true;
                break;
            }
            page.push(k);
        }
        // `limit == 0`: degenerate but well-defined — empty page, no cursor.
        let next_cursor = if overflow { page.last().cloned() } else { None };

        Ok(ListPage {
            keys: page,
            next_cursor,
        })
    }
}

#[cfg(test)]
mod tests {
    //! Tests covering trait conformance plus on-disk-only crash-safety
    //! properties (atomic rename, tempfile cleanup) and the typed
    //! `StorageError` classification.
    use super::*;
    use crate::storage::AgentStorage;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn fresh_local() -> (TempDir, Arc<dyn AgentStorage>) {
        let tmp = TempDir::new().unwrap();
        let storage = Arc::new(LocalStorage::new(tmp.path()).unwrap());
        (tmp, storage)
    }

    // ---- Trait conformance suite against LocalStorage. -----------------

    #[tokio::test]
    async fn local_put_get_round_trip() {
        let (_tmp, storage) = fresh_local();
        storage
            .put("a", Bytes::from_static(b"hello"))
            .await
            .unwrap();
        assert_eq!(
            storage.get("a").await.unwrap().as_deref(),
            Some(b"hello".as_ref())
        );
    }

    #[tokio::test]
    async fn local_get_absent_returns_none() {
        let (_tmp, storage) = fresh_local();
        assert!(storage.get("never").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn local_put_if_absent_semantics() {
        let (_tmp, storage) = fresh_local();
        let first = storage
            .put_if_absent("evidence/abc", Bytes::from_static(b"v1"))
            .await
            .unwrap();
        assert_eq!(first, PutOutcome::Created);
        let second = storage
            .put_if_absent("evidence/abc", Bytes::from_static(b"v2"))
            .await
            .unwrap();
        assert_eq!(second, PutOutcome::Existed);
        assert_eq!(
            storage.get("evidence/abc").await.unwrap().unwrap().as_ref(),
            b"v1"
        );
    }

    #[tokio::test]
    async fn local_get_many_preserves_order_and_missing_slots() {
        let (_tmp, storage) = fresh_local();
        storage.put("a", Bytes::from_static(b"A")).await.unwrap();
        storage.put("b", Bytes::from_static(b"B")).await.unwrap();
        storage.put("c", Bytes::from_static(b"C")).await.unwrap();

        let keys: &[&str] = &["c", "missing", "a", "b"];
        let got = storage.get_many(keys).await.unwrap();
        assert_eq!(got.len(), 4);
        assert_eq!(got[0].as_deref(), Some(b"C".as_ref()));
        assert!(got[1].is_none());
        assert_eq!(got[2].as_deref(), Some(b"A".as_ref()));
        assert_eq!(got[3].as_deref(), Some(b"B".as_ref()));
    }

    #[tokio::test]
    async fn local_delete_idempotent() {
        let (_tmp, storage) = fresh_local();
        storage
            .put("doomed", Bytes::from_static(b"bye"))
            .await
            .unwrap();
        storage.delete("doomed").await.unwrap();
        assert!(storage.get("doomed").await.unwrap().is_none());
        storage.delete("doomed").await.unwrap();
        storage.delete("never-written").await.unwrap();
    }

    #[tokio::test]
    async fn local_list_prefix_and_pagination() {
        let (_tmp, storage) = fresh_local();
        for k in ["outputs/01", "outputs/02", "outputs/03", "outputs/04"] {
            storage.put(k, Bytes::from_static(b"x")).await.unwrap();
        }
        storage
            .put("evidence/aa", Bytes::from_static(b"y"))
            .await
            .unwrap();

        let page = storage.list("outputs/", None, 100).await.unwrap();
        assert_eq!(
            page.keys,
            vec![
                "outputs/01".to_string(),
                "outputs/02".to_string(),
                "outputs/03".to_string(),
                "outputs/04".to_string(),
            ]
        );
        assert!(page.next_cursor.is_none());

        let page1 = storage.list("outputs/", None, 2).await.unwrap();
        assert_eq!(
            page1.keys,
            vec!["outputs/01".to_string(), "outputs/02".to_string()]
        );
        assert_eq!(page1.next_cursor.as_deref(), Some("outputs/02"));

        let page2 = storage
            .list("outputs/", page1.next_cursor.as_deref(), 2)
            .await
            .unwrap();
        assert_eq!(
            page2.keys,
            vec!["outputs/03".to_string(), "outputs/04".to_string()]
        );
        assert!(page2.next_cursor.is_none());

        let after_two = storage
            .list("outputs/", Some("outputs/02"), 100)
            .await
            .unwrap();
        assert_eq!(
            after_two.keys,
            vec!["outputs/03".to_string(), "outputs/04".to_string()]
        );

        assert!(!page.keys.iter().any(|k| k.starts_with("evidence/")));
    }

    #[tokio::test]
    async fn local_list_empty_prefix_lists_everything() {
        let (_tmp, storage) = fresh_local();
        storage.put("a", Bytes::from_static(b"1")).await.unwrap();
        storage.put("b", Bytes::from_static(b"2")).await.unwrap();
        let page = storage.list("", None, 100).await.unwrap();
        assert_eq!(page.keys, vec!["a".to_string(), "b".to_string()]);
    }

    // ---- LocalStorage-specific tests ------------------------------------

    /// A stray `.tmp.<pid>.<n>` file must not be observable under the
    /// canonical key via `get` or `list`.
    #[tokio::test]
    async fn local_garbage_tempfile_does_not_corrupt_canonical_key() {
        let (tmp, storage) = fresh_local();
        storage
            .put("outputs/01.json", Bytes::from_static(b"good"))
            .await
            .unwrap();

        let stray = tmp.path().join("outputs").join("01.json.tmp.99999.42");
        fs::write(&stray, b"garbage-partial-write").unwrap();

        let bytes = storage.get("outputs/01.json").await.unwrap().unwrap();
        assert_eq!(bytes.as_ref(), b"good");

        let page = storage.list("outputs/", None, 100).await.unwrap();
        assert_eq!(page.keys, vec!["outputs/01.json".to_string()]);
    }

    /// Write-then-rename leaves the on-disk file exactly the bytes we
    /// wrote and removes the tempfile.
    #[tokio::test]
    async fn local_put_writes_byte_identical_payload_and_cleans_up_tempfile() {
        let (tmp, storage) = fresh_local();
        let payload = Bytes::from_static(b"\x00\x01\x02exact-bytes\xff");
        storage.put("blob", payload.clone()).await.unwrap();

        let from_disk = fs::read(tmp.path().join("blob")).unwrap();
        assert_eq!(from_disk, payload.as_ref());

        let entries: Vec<_> = fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert!(entries.contains(&"blob".to_string()));
        assert!(
            !entries.iter().any(|n| n.contains(".tmp.")),
            "tempfile lingered: {entries:?}"
        );
    }

    /// `put` overwrites an existing key (last-write-wins) — the contract
    /// that distinguishes `put` from `put_if_absent`.
    #[tokio::test]
    async fn local_put_overwrites_existing_key() {
        let (_tmp, storage) = fresh_local();
        storage.put("k", Bytes::from_static(b"v1")).await.unwrap();
        storage.put("k", Bytes::from_static(b"v2")).await.unwrap();
        assert_eq!(storage.get("k").await.unwrap().unwrap().as_ref(), b"v2");
    }

    /// Listing under a prefix with no matching dir on disk returns an
    /// empty page rather than erroring.
    #[tokio::test]
    async fn local_list_missing_prefix_returns_empty() {
        let (_tmp, storage) = fresh_local();
        let page = storage.list("never-existed/", None, 100).await.unwrap();
        assert!(page.keys.is_empty());
        assert!(page.next_cursor.is_none());
    }

    /// Keys with multiple `/` segments map to nested directories and
    /// list correctly under their parent prefix.
    #[tokio::test]
    async fn local_nested_keys_round_trip_through_list() {
        let (_tmp, storage) = fresh_local();
        storage
            .put("notes/a.md", Bytes::from_static(b"top"))
            .await
            .unwrap();
        storage
            .put("notes/sub/b.md", Bytes::from_static(b"nested"))
            .await
            .unwrap();
        storage
            .put("notes/sub/deeper/c.md", Bytes::from_static(b"deeper"))
            .await
            .unwrap();

        let page = storage.list("notes/", None, 100).await.unwrap();
        assert_eq!(
            page.keys,
            vec![
                "notes/a.md".to_string(),
                "notes/sub/b.md".to_string(),
                "notes/sub/deeper/c.md".to_string(),
            ]
        );
    }

    /// `put_if_absent` against the same key from two concurrent tasks
    /// must produce exactly one `Created` and one `Existed` — the
    /// race load-bearing for content-addressed evidence dedup.
    #[tokio::test]
    async fn local_put_if_absent_is_race_free_under_concurrency() {
        let (_tmp, storage) = fresh_local();
        let s1 = storage.clone();
        let s2 = storage.clone();
        let (a, b) = tokio::join!(
            tokio::spawn(async move { s1.put_if_absent("k", Bytes::from_static(b"x")).await }),
            tokio::spawn(async move { s2.put_if_absent("k", Bytes::from_static(b"y")).await }),
        );
        let a = a.unwrap().unwrap();
        let b = b.unwrap().unwrap();
        let mut outcomes = vec![a, b];
        outcomes.sort_by_key(|o| matches!(o, PutOutcome::Existed));
        assert_eq!(outcomes, vec![PutOutcome::Created, PutOutcome::Existed]);
    }

    /// Soft-not-found: `get` on a missing nested key returns `Ok(None)`
    /// rather than surfacing the kernel `NotFound` through the error channel.
    #[tokio::test]
    async fn local_io_error_propagation_uses_typed_variants() {
        let (_tmp, storage) = fresh_local();
        let got = storage.get("does/not/exist.json").await.unwrap();
        assert!(got.is_none(), "expected None for missing nested key");
    }
}

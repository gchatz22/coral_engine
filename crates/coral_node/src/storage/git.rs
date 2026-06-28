//! [`GitStorage`] — local git-backed [`VersionedStorage`].
//!
//! Each agent root is a git repository. The working tree is the agent's
//! durable content — put/get/list pass straight through to an embedded
//! [`LocalStorage`] — and versioning is git's object database. The exposed
//! surface is exactly {commit-per-tick, read-blob-at-sha}: no branch,
//! checkout, merge, or rebase, so HEAD only ever advances and historical
//! reads resolve a blob sha without moving the working tree.
//!
//! `git2` is synchronous; every git operation runs on a blocking thread and
//! the `git2::Repository` handle never crosses an await point (it is opened
//! and dropped inside each blocking closure). That keeps `GitStorage`
//! `Send + Sync` despite `Repository` being neither.

use super::{
    AgentStorage, BlobSha, ListPage, LocalStorage, PutOutcome, StorageError, StorageResult,
    VersionedStorage,
};
use async_trait::async_trait;
use bytes::Bytes;
use git2::{
    ErrorCode, IndexAddOption, ObjectType, Oid, Repository, Signature, TreeWalkMode, TreeWalkResult,
};
use std::path::{Path, PathBuf};

/// Git-backed per-agent storage. The working tree is an embedded
/// [`LocalStorage`]; `.git/` holds the version history.
pub struct GitStorage {
    inner: LocalStorage,
    root: PathBuf,
}

impl std::fmt::Debug for GitStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitStorage")
            .field("root", &self.root)
            .finish()
    }
}

impl GitStorage {
    /// Open (or initialize) a git repo rooted at `root`, creating the
    /// directory if absent. Initialization is idempotent: an existing repo
    /// is reused.
    pub fn new(root: impl Into<PathBuf>) -> StorageResult<Self> {
        let root = root.into();
        let inner = LocalStorage::new(&root)?;
        open_or_init(&root).map_err(map_git_err)?;
        Ok(Self { inner, root })
    }

    /// Borrow the repository root.
    pub fn root(&self) -> &Path {
        &self.root
    }
}

fn open_or_init(root: &Path) -> Result<Repository, git2::Error> {
    match Repository::open(root) {
        Ok(repo) => Ok(repo),
        Err(_) => Repository::init(root),
    }
}

fn map_git_err(err: git2::Error) -> StorageError {
    match err.code() {
        // Concurrent index/ref contention is the one retryable git failure.
        ErrorCode::Locked => StorageError::Transient(err.to_string()),
        _ => StorageError::Other(anyhow::Error::from(err)),
    }
}

/// `<key>.tmp.<pid>.<n>` files are [`LocalStorage`]'s in-flight write
/// artifacts; a crash mid-`put` can leave one behind. They must never be
/// staged into a commit.
fn is_tempfile(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.contains(".tmp."))
        .unwrap_or(false)
}

fn manifest_from_tree(tree: &git2::Tree) -> Vec<(String, BlobSha)> {
    let mut out = Vec::new();
    // `dir` carries the path prefix with a trailing slash for nested
    // entries ("notes/") and is empty at the root, so `dir + name` is the
    // full repo-relative path.
    let _ = tree.walk(TreeWalkMode::PreOrder, |dir, entry| {
        if entry.kind() == Some(ObjectType::Blob) {
            if let Some(name) = entry.name() {
                out.push((format!("{dir}{name}"), BlobSha(entry.id().to_string())));
            }
        }
        TreeWalkResult::Ok
    });
    out
}

pub(crate) fn commit_blocking(root: &Path, message: &str) -> StorageResult<Vec<(String, BlobSha)>> {
    let repo = open_or_init(root).map_err(map_git_err)?;
    let mut index = repo.index().map_err(map_git_err)?;

    // Stage additions + modifications, skipping LocalStorage tempfiles.
    let mut skip_tmp = |path: &Path, _matched: &[u8]| -> i32 {
        if is_tempfile(path) {
            1
        } else {
            0
        }
    };
    index
        .add_all(["*"].iter(), IndexAddOption::DEFAULT, Some(&mut skip_tmp))
        .map_err(map_git_err)?;
    // Stage removals of tracked files that no longer exist on disk.
    index.update_all(["*"].iter(), None).map_err(map_git_err)?;
    index.write().map_err(map_git_err)?;

    let tree_oid = index.write_tree().map_err(map_git_err)?;
    let tree = repo.find_tree(tree_oid).map_err(map_git_err)?;

    let parent = match repo.head() {
        Ok(head) => head.peel_to_commit().ok(),
        Err(_) => None,
    };

    // Clean-tree no-op: a staged tree identical to HEAD's makes a retried
    // tick converge without a divergent commit.
    if let Some(ref parent) = parent {
        if parent.tree_id() == tree_oid {
            return Ok(manifest_from_tree(&tree));
        }
    }

    let sig = signature().map_err(map_git_err)?;
    let parents: Vec<&git2::Commit> = parent.iter().collect();
    repo.commit(Some("HEAD"), &sig, &sig, message, &tree, &parents)
        .map_err(map_git_err)?;

    Ok(manifest_from_tree(&tree))
}

fn signature() -> Result<Signature<'static>, git2::Error> {
    Signature::now("coral", "coral@localhost")
}

pub(crate) fn read_at_blocking(root: &Path, sha: &str) -> StorageResult<Option<Bytes>> {
    let repo = open_or_init(root).map_err(map_git_err)?;
    let oid = match Oid::from_str(sha) {
        Ok(oid) => oid,
        Err(_) => return Ok(None),
    };
    let result = match repo.find_blob(oid) {
        Ok(blob) => Ok(Some(Bytes::copy_from_slice(blob.content()))),
        Err(e) if e.code() == ErrorCode::NotFound => Ok(None),
        Err(e) => Err(map_git_err(e)),
    };
    result
}

pub(crate) fn join_err(err: tokio::task::JoinError) -> StorageError {
    StorageError::Other(anyhow::Error::from(err))
}

#[async_trait]
impl VersionedStorage for GitStorage {
    async fn commit(&self, message: &str) -> StorageResult<Vec<(String, BlobSha)>> {
        let root = self.root.clone();
        let message = message.to_string();
        tokio::task::spawn_blocking(move || commit_blocking(&root, &message))
            .await
            .map_err(join_err)?
    }

    async fn read_at(&self, sha: &BlobSha) -> StorageResult<Option<Bytes>> {
        let root = self.root.clone();
        let sha = sha.0.clone();
        tokio::task::spawn_blocking(move || read_at_blocking(&root, &sha))
            .await
            .map_err(join_err)?
    }
}

#[async_trait]
impl AgentStorage for GitStorage {
    async fn put(&self, key: &str, value: Bytes) -> StorageResult<()> {
        self.inner.put(key, value).await
    }

    async fn put_if_absent(&self, key: &str, value: Bytes) -> StorageResult<PutOutcome> {
        self.inner.put_if_absent(key, value).await
    }

    async fn get(&self, key: &str) -> StorageResult<Option<Bytes>> {
        self.inner.get(key).await
    }

    async fn get_many(&self, keys: &[&str]) -> StorageResult<Vec<Option<Bytes>>> {
        self.inner.get_many(keys).await
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        self.inner.delete(key).await
    }

    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StorageResult<ListPage> {
        self.inner.list(prefix, after, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{AgentStorage, VersionedStorage};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn fresh() -> (TempDir, GitStorage) {
        let tmp = TempDir::new().unwrap();
        let gs = GitStorage::new(tmp.path()).unwrap();
        (tmp, gs)
    }

    fn head_commit_id(root: &Path) -> Option<String> {
        let repo = Repository::open(root).unwrap();
        repo.head()
            .ok()
            .and_then(|h| h.peel_to_commit().ok())
            .map(|c| c.id().to_string())
    }

    fn sha_for<'a>(manifest: &'a [(String, BlobSha)], path: &str) -> &'a BlobSha {
        &manifest.iter().find(|(p, _)| p == path).unwrap().1
    }

    #[tokio::test]
    async fn commit_returns_manifest_resolvable_by_read_at() {
        let (_tmp, gs) = fresh();
        gs.put("mandate.md", Bytes::from_static(b"watch TSMC"))
            .await
            .unwrap();
        gs.put("notes/a.md", Bytes::from_static(b"note a"))
            .await
            .unwrap();

        let manifest = gs.commit("tick 1").await.unwrap();
        let paths: Vec<&str> = manifest.iter().map(|(p, _)| p.as_str()).collect();
        assert!(paths.contains(&"mandate.md"));
        assert!(paths.contains(&"notes/a.md"), "nested paths must appear");

        let mandate_sha = sha_for(&manifest, "mandate.md");
        assert_eq!(
            gs.read_at(mandate_sha).await.unwrap().unwrap().as_ref(),
            b"watch TSMC"
        );
        assert_eq!(mandate_sha.as_str().len(), 40, "blob sha is 40 hex chars");
    }

    #[tokio::test]
    async fn commit_is_clean_tree_noop_on_retry() {
        let (_tmp, gs) = fresh();
        gs.put("mandate.md", Bytes::from_static(b"m"))
            .await
            .unwrap();

        let m1 = gs.commit("tick").await.unwrap();
        let head1 = head_commit_id(gs.root()).unwrap();

        // Retry with nothing changed: HEAD must not advance, manifest stable.
        let m2 = gs.commit("tick").await.unwrap();
        let head2 = head_commit_id(gs.root()).unwrap();

        assert_eq!(head1, head2, "clean-tree retry must not advance HEAD");
        assert_eq!(m1, m2, "manifest stable across no-op retry");
    }

    #[tokio::test]
    async fn read_at_resolves_historical_version_after_overwrite() {
        let (_tmp, gs) = fresh();
        gs.put("outputs/o.md", Bytes::from_static(b"v1"))
            .await
            .unwrap();
        let m1 = gs.commit("tick 1").await.unwrap();
        let sha_v1 = sha_for(&m1, "outputs/o.md").clone();

        gs.put("outputs/o.md", Bytes::from_static(b"v2"))
            .await
            .unwrap();
        let m2 = gs.commit("tick 2").await.unwrap();
        let sha_v2 = sha_for(&m2, "outputs/o.md").clone();

        assert_ne!(sha_v1, sha_v2);
        // Old blob still resolves even though HEAD/worktree advanced to v2.
        assert_eq!(gs.read_at(&sha_v1).await.unwrap().unwrap().as_ref(), b"v1");
        assert_eq!(gs.read_at(&sha_v2).await.unwrap().unwrap().as_ref(), b"v2");
        assert_eq!(
            gs.get("outputs/o.md").await.unwrap().unwrap().as_ref(),
            b"v2",
            "working tree reflects HEAD"
        );
    }

    #[tokio::test]
    async fn of_bytes_matches_committed_blob_sha() {
        // The linchpin of version-pinning: a sha computed from bytes at
        // write time (DB) must equal the sha git records on commit, so a
        // pinned reference resolves back to the same blob later.
        let (_tmp, gs) = fresh();
        let content = b"watch TSMC fab utilization\nrising\n";
        gs.put("outputs/output.md", Bytes::copy_from_slice(content))
            .await
            .unwrap();
        let manifest = gs.commit("t").await.unwrap();
        let committed = sha_for(&manifest, "outputs/output.md");
        assert_eq!(
            BlobSha::of_bytes(content).as_str(),
            committed.as_str(),
            "of_bytes must equal git2's committed blob sha (no autocrlf/filter drift)"
        );
    }

    #[tokio::test]
    async fn identical_content_has_identical_blob_sha() {
        let (_tmp, gs) = fresh();
        gs.put("a.md", Bytes::from_static(b"same")).await.unwrap();
        gs.put("b.md", Bytes::from_static(b"same")).await.unwrap();
        let m = gs.commit("t").await.unwrap();
        assert_eq!(
            sha_for(&m, "a.md"),
            sha_for(&m, "b.md"),
            "content-addressed: same bytes -> same blob sha"
        );
    }

    #[tokio::test]
    async fn read_at_does_not_mutate_head_or_worktree() {
        let (_tmp, gs) = fresh();
        gs.put("f.md", Bytes::from_static(b"x")).await.unwrap();
        let m = gs.commit("t").await.unwrap();
        let head_before = head_commit_id(gs.root()).unwrap();

        let _ = gs.read_at(sha_for(&m, "f.md")).await.unwrap();

        let head_after = head_commit_id(gs.root()).unwrap();
        assert_eq!(head_before, head_after, "read_at must not move HEAD");
        assert_eq!(
            gs.get("f.md").await.unwrap().unwrap().as_ref(),
            b"x",
            "read_at must not touch the working tree"
        );
    }

    #[tokio::test]
    async fn read_at_absent_or_malformed_sha_returns_none() {
        let (_tmp, gs) = fresh();
        let absent = BlobSha("0000000000000000000000000000000000000000".to_string());
        assert!(gs.read_at(&absent).await.unwrap().is_none());
        let malformed = BlobSha("not-a-sha".to_string());
        assert!(gs.read_at(&malformed).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn commit_stages_deletions() {
        let (_tmp, gs) = fresh();
        gs.put("keep.md", Bytes::from_static(b"k")).await.unwrap();
        gs.put("gone.md", Bytes::from_static(b"g")).await.unwrap();
        gs.commit("t1").await.unwrap();

        gs.delete("gone.md").await.unwrap();
        let m = gs.commit("t2").await.unwrap();
        let paths: Vec<&str> = m.iter().map(|(p, _)| p.as_str()).collect();
        assert!(paths.contains(&"keep.md"));
        assert!(
            !paths.contains(&"gone.md"),
            "a deleted file must drop from the next manifest"
        );
    }

    #[tokio::test]
    async fn tempfiles_are_never_committed() {
        let (tmp, gs) = fresh();
        gs.put("real.md", Bytes::from_static(b"r")).await.unwrap();
        // A leaked LocalStorage tempfile sitting in the working tree.
        std::fs::write(tmp.path().join("real.md.tmp.99999.7"), b"garbage").unwrap();

        let m = gs.commit("t").await.unwrap();
        let paths: Vec<&str> = m.iter().map(|(p, _)| p.as_str()).collect();
        assert!(paths.contains(&"real.md"));
        assert!(
            !paths.iter().any(|p| p.contains(".tmp.")),
            "tempfiles must not be committed: {paths:?}"
        );
    }

    #[tokio::test]
    async fn agent_storage_plane_round_trips() {
        let (_tmp, gs) = fresh();
        gs.put("notes/x.md", Bytes::from_static(b"hi"))
            .await
            .unwrap();
        assert_eq!(gs.get("notes/x.md").await.unwrap().unwrap().as_ref(), b"hi");
        let page = gs.list("notes/", None, 10).await.unwrap();
        assert_eq!(page.keys, vec!["notes/x.md".to_string()]);
    }

    #[tokio::test]
    async fn usable_as_dyn_versioned_storage() {
        let (_tmp, gs) = fresh();
        // Both planes reachable through one object-safe handle.
        let storage: Arc<dyn VersionedStorage> = Arc::new(gs);
        storage.put("m.md", Bytes::from_static(b"z")).await.unwrap();
        let m = storage.commit("t").await.unwrap();
        assert_eq!(
            storage
                .read_at(sha_for(&m, "m.md"))
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"z"
        );
    }
}

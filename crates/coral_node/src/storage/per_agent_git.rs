//! [`PerAgentGitStorage`] — the production [`AgentStorage`] backend: a plain
//! on-disk data plane shared across all agents, with **per-agent git
//! versioning** layered on top.
//!
//! The data plane is a single [`LocalStorage`] rooted at `AGENT_FS_ROOT`,
//! keyed by the absolute `graphs/<g>/agents/<a>/…` paths every `AgentFs`
//! already uses — so put/get/list/delete behave byte-identically to a plain
//! `LocalStorage` (cross-agent reads, list pagination, cursors all unchanged).
//!
//! Versioning is **one git repository per agent**, rooted at the agent's own
//! `<root>/<prefix>/` directory (so `.git/` lives inside that prefix, never at
//! `<root>`). The [`VersionedStorage`] impl's
//! [`commit`](VersionedStorage::commit) snapshots a single agent's working tree
//! and [`read_at`](VersionedStorage::read_at) resolves a blob sha within that
//! agent's repo, both addressed by the agent's FS prefix. They reuse the git
//! plumbing in [`super::git`]; the working tree they operate on is exactly the
//! files the shared data plane already wrote.

use super::git::{commit_blocking, join_err, read_at_blocking};
use super::{
    AgentStorage, BlobSha, ListPage, LocalStorage, PutOutcome, StorageResult, VersionedStorage,
};
use async_trait::async_trait;
use bytes::Bytes;
use std::path::PathBuf;

/// Git-versioned, per-agent on-disk storage. Data plane → one shared
/// [`LocalStorage`]; versioning → one repo per agent prefix.
pub struct PerAgentGitStorage {
    data: LocalStorage,
    root: PathBuf,
}

impl std::fmt::Debug for PerAgentGitStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PerAgentGitStorage")
            .field("root", &self.root)
            .finish()
    }
}

impl PerAgentGitStorage {
    /// Open the data-plane root, creating it if absent. Per-agent repos are
    /// initialized lazily on the first [`commit`](VersionedStorage::commit).
    pub fn new(root: impl Into<PathBuf>) -> StorageResult<Self> {
        let root = root.into();
        let data = LocalStorage::new(&root)?;
        Ok(Self { data, root })
    }

    /// Filesystem root of an agent's repo: `<root>/<prefix>`.
    fn repo_root(&self, agent_prefix: &str) -> PathBuf {
        self.root.join(agent_prefix.trim_end_matches('/'))
    }
}

#[async_trait]
impl VersionedStorage for PerAgentGitStorage {
    async fn commit(
        &self,
        agent_prefix: &str,
        message: &str,
    ) -> StorageResult<Vec<(String, BlobSha)>> {
        let repo_root = self.repo_root(agent_prefix);
        let message = message.to_string();
        tokio::task::spawn_blocking(move || commit_blocking(&repo_root, &message))
            .await
            .map_err(join_err)?
    }

    async fn read_at(&self, agent_prefix: &str, sha: &BlobSha) -> StorageResult<Option<Bytes>> {
        let repo_root = self.repo_root(agent_prefix);
        let sha = sha.as_str().to_string();
        tokio::task::spawn_blocking(move || read_at_blocking(&repo_root, &sha))
            .await
            .map_err(join_err)?
    }
}

#[async_trait]
impl AgentStorage for PerAgentGitStorage {
    async fn put(&self, key: &str, value: Bytes) -> StorageResult<()> {
        self.data.put(key, value).await
    }

    async fn put_if_absent(&self, key: &str, value: Bytes) -> StorageResult<PutOutcome> {
        self.data.put_if_absent(key, value).await
    }

    async fn get(&self, key: &str) -> StorageResult<Option<Bytes>> {
        self.data.get(key).await
    }

    async fn get_many(&self, keys: &[&str]) -> StorageResult<Vec<Option<Bytes>>> {
        self.data.get_many(keys).await
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        self.data.delete(key).await
    }

    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StorageResult<ListPage> {
        self.data.list(prefix, after, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    const A: &str = "graphs/g/agents/a";
    const B: &str = "graphs/g/agents/b";

    fn fresh() -> (TempDir, PerAgentGitStorage) {
        let tmp = TempDir::new().unwrap();
        let gs = PerAgentGitStorage::new(tmp.path()).unwrap();
        (tmp, gs)
    }

    fn sha_for<'a>(manifest: &'a [(String, BlobSha)], path: &str) -> &'a BlobSha {
        &manifest.iter().find(|(p, _)| p == path).unwrap().1
    }

    /// HEAD commit id of an agent's repo, read directly via `git2`.
    fn head_commit_id(repo_root: &std::path::Path) -> Option<String> {
        let repo = git2::Repository::open(repo_root).unwrap();
        repo.head()
            .ok()
            .and_then(|h| h.peel_to_commit().ok())
            .map(|c| c.id().to_string())
    }

    #[tokio::test]
    async fn data_plane_round_trips_with_absolute_keys() {
        let (_tmp, gs) = fresh();
        let key = format!("{A}/notes/x.md");
        gs.put(&key, Bytes::from_static(b"hi")).await.unwrap();
        assert_eq!(gs.get(&key).await.unwrap().unwrap().as_ref(), b"hi");
        let page = gs.list(&format!("{A}/notes/"), None, 10).await.unwrap();
        assert_eq!(page.keys, vec![key]);
    }

    #[tokio::test]
    async fn commit_produces_resolvable_blob_sha() {
        let (_tmp, gs) = fresh();
        gs.put(&format!("{A}/outputs/output.md"), Bytes::from_static(b"v1"))
            .await
            .unwrap();
        let manifest = gs.commit(A, "tick 0").await.unwrap();

        // Paths are relative to the agent's prefix.
        let sha = sha_for(&manifest, "outputs/output.md");
        assert_eq!(sha.as_str().len(), 40, "blob sha is 40 hex chars");
        assert_eq!(gs.read_at(A, sha).await.unwrap().unwrap().as_ref(), b"v1");
        // The write-time content address equals the committed blob sha.
        assert_eq!(BlobSha::of_bytes(b"v1").as_str(), sha.as_str());
    }

    #[tokio::test]
    async fn commit_clean_tree_is_noop_on_retry() {
        let (_tmp, gs) = fresh();
        gs.put(&format!("{A}/mandate.md"), Bytes::from_static(b"m"))
            .await
            .unwrap();
        let m1 = gs.commit(A, "tick").await.unwrap();
        let m2 = gs.commit(A, "tick").await.unwrap();
        assert_eq!(m1, m2, "clean-tree retry yields a stable manifest");
    }

    #[tokio::test]
    async fn agents_version_independently_and_cross_agent_reads_resolve() {
        let (_tmp, gs) = fresh();
        gs.put(&format!("{A}/outputs/output.md"), Bytes::from_static(b"A"))
            .await
            .unwrap();
        gs.put(&format!("{B}/outputs/output.md"), Bytes::from_static(b"B"))
            .await
            .unwrap();
        let ma = gs.commit(A, "a").await.unwrap();
        let mb = gs.commit(B, "b").await.unwrap();

        // Each agent's repo only knows its own files.
        assert!(ma.iter().any(|(p, _)| p == "outputs/output.md"));
        assert!(mb.iter().any(|(p, _)| p == "outputs/output.md"));

        // Cross-agent read through the shared data plane (absolute key).
        assert_eq!(
            gs.get(&format!("{B}/outputs/output.md"))
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"B"
        );
        // A's sha resolves in A's repo but not in B's (separate object stores).
        let sha_a = sha_for(&ma, "outputs/output.md");
        assert_eq!(gs.read_at(A, sha_a).await.unwrap().unwrap().as_ref(), b"A");
        assert!(
            gs.read_at(B, sha_a).await.unwrap().is_none(),
            "A's blob must not resolve in B's repo"
        );
    }

    #[tokio::test]
    async fn read_at_resolves_historical_version_after_overwrite() {
        let (_tmp, gs) = fresh();
        let key = format!("{A}/outputs/output.md");
        gs.put(&key, Bytes::from_static(b"v1")).await.unwrap();
        let sha_v1 = sha_for(&gs.commit(A, "t1").await.unwrap(), "outputs/output.md").clone();
        gs.put(&key, Bytes::from_static(b"v2")).await.unwrap();
        let sha_v2 = sha_for(&gs.commit(A, "t2").await.unwrap(), "outputs/output.md").clone();

        assert_ne!(sha_v1, sha_v2);
        assert_eq!(
            gs.read_at(A, &sha_v1).await.unwrap().unwrap().as_ref(),
            b"v1",
            "old version still resolves after overwrite"
        );
        assert_eq!(gs.get(&key).await.unwrap().unwrap().as_ref(), b"v2");
    }

    #[tokio::test]
    async fn repo_lives_in_prefix_not_at_root_and_git_is_not_listed() {
        let (tmp, gs) = fresh();
        gs.put(&format!("{A}/mandate.md"), Bytes::from_static(b"m"))
            .await
            .unwrap();
        gs.commit(A, "t").await.unwrap();

        assert!(
            tmp.path().join(A).join(".git").exists(),
            "the agent's repo lives inside its own prefix"
        );
        assert!(
            !tmp.path().join(".git").exists(),
            "no repo at the shared root"
        );

        // Listing the agent root must not surface the repo's internals.
        let page = gs.list(&format!("{A}/"), None, 100).await.unwrap();
        assert!(
            page.keys.iter().all(|k| !k.contains(".git")),
            "git internals leaked into listing: {:?}",
            page.keys
        );
        assert!(page.keys.contains(&format!("{A}/mandate.md")));
    }

    #[tokio::test]
    async fn reopen_on_same_root_sees_prior_writes() {
        let tmp = TempDir::new().unwrap();
        {
            let gs = PerAgentGitStorage::new(tmp.path()).unwrap();
            gs.put(&format!("{A}/mandate.md"), Bytes::from_static(b"m"))
                .await
                .unwrap();
            gs.commit(A, "t").await.unwrap();
        }
        let gs2 = PerAgentGitStorage::new(tmp.path()).unwrap();
        assert_eq!(
            gs2.get(&format!("{A}/mandate.md"))
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"m"
        );
        // Re-opening and re-committing an unchanged tree is a no-op.
        let m = gs2.commit(A, "t-again").await.unwrap();
        assert!(m.iter().any(|(p, _)| p == "mandate.md"));
    }

    #[tokio::test]
    async fn usable_as_dyn_agent_storage() {
        let (_tmp, gs) = fresh();
        let storage: Arc<dyn AgentStorage> = Arc::new(gs);
        storage
            .put(&format!("{A}/notes/n.md"), Bytes::from_static(b"z"))
            .await
            .unwrap();
        assert_eq!(
            storage
                .get(&format!("{A}/notes/n.md"))
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"z"
        );
    }

    #[tokio::test]
    async fn read_at_does_not_mutate_head_or_worktree() {
        let (tmp, gs) = fresh();
        gs.put(&format!("{A}/f.md"), Bytes::from_static(b"x"))
            .await
            .unwrap();
        let m = gs.commit(A, "t").await.unwrap();
        let repo_root = tmp.path().join(A);
        let head_before = head_commit_id(&repo_root).unwrap();

        let _ = gs.read_at(A, sha_for(&m, "f.md")).await.unwrap();

        assert_eq!(
            head_before,
            head_commit_id(&repo_root).unwrap(),
            "read_at must not move HEAD"
        );
        assert_eq!(
            gs.get(&format!("{A}/f.md"))
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"x",
            "read_at must not touch the working tree"
        );
    }

    #[tokio::test]
    async fn read_at_absent_or_malformed_sha_returns_none() {
        let (_tmp, gs) = fresh();
        gs.put(&format!("{A}/f.md"), Bytes::from_static(b"x"))
            .await
            .unwrap();
        gs.commit(A, "t").await.unwrap();
        let absent = BlobSha::from_hex("0000000000000000000000000000000000000000");
        assert!(gs.read_at(A, &absent).await.unwrap().is_none());
        let malformed = BlobSha::from_hex("not-a-sha");
        assert!(gs.read_at(A, &malformed).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn commit_stages_deletions() {
        let (_tmp, gs) = fresh();
        gs.put(&format!("{A}/keep.md"), Bytes::from_static(b"k"))
            .await
            .unwrap();
        gs.put(&format!("{A}/gone.md"), Bytes::from_static(b"g"))
            .await
            .unwrap();
        gs.commit(A, "t1").await.unwrap();

        gs.delete(&format!("{A}/gone.md")).await.unwrap();
        let m = gs.commit(A, "t2").await.unwrap();
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
        gs.put(&format!("{A}/real.md"), Bytes::from_static(b"r"))
            .await
            .unwrap();
        // A leaked LocalStorage tempfile sitting in the agent's working tree.
        std::fs::write(tmp.path().join(A).join("real.md.tmp.99999.7"), b"garbage").unwrap();

        let m = gs.commit(A, "t").await.unwrap();
        let paths: Vec<&str> = m.iter().map(|(p, _)| p.as_str()).collect();
        assert!(paths.contains(&"real.md"));
        assert!(
            !paths.iter().any(|p| p.contains(".tmp.")),
            "tempfiles must not be committed: {paths:?}"
        );
    }
}

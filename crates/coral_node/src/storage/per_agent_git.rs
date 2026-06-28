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
//! `<root>`). [`commit_agent`](PerAgentGitStorage::commit_agent) snapshots a
//! single agent's working tree and [`read_agent_at`](PerAgentGitStorage::read_agent_at)
//! resolves a blob sha within that agent's repo. Both reuse the same git plumbing
//! as [`GitStorage`](super::GitStorage); the working tree they operate on is
//! exactly the files the shared data plane already wrote.

use super::git::{commit_blocking, join_err, read_at_blocking};
use super::{AgentStorage, BlobSha, ListPage, LocalStorage, PutOutcome, StorageResult};
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
    /// initialized lazily on the first [`commit_agent`](Self::commit_agent).
    pub fn new(root: impl Into<PathBuf>) -> StorageResult<Self> {
        let root = root.into();
        let data = LocalStorage::new(&root)?;
        Ok(Self { data, root })
    }

    /// Filesystem root of an agent's repo: `<root>/<prefix>`.
    fn repo_root(&self, agent_prefix: &str) -> PathBuf {
        self.root.join(agent_prefix.trim_end_matches('/'))
    }

    /// Commit one agent's working tree as a single tick, returning the
    /// `(path, blob_sha)` manifest of every file in the committed tree (paths
    /// are relative to the agent's prefix, e.g. `outputs/output.md`). Inits the
    /// agent's repo on first call. Idempotent: a clean tree is a no-op that
    /// still returns the current manifest, so a retried tick converges.
    pub async fn commit_agent(
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

    /// Resolve a blob sha to its bytes within one agent's repo. Read-only;
    /// never moves HEAD or the working tree. `Ok(None)` if no such blob exists.
    pub async fn read_agent_at(
        &self,
        agent_prefix: &str,
        sha: &BlobSha,
    ) -> StorageResult<Option<Bytes>> {
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
    async fn commit_agent_produces_resolvable_blob_sha() {
        let (_tmp, gs) = fresh();
        gs.put(&format!("{A}/outputs/output.md"), Bytes::from_static(b"v1"))
            .await
            .unwrap();
        let manifest = gs.commit_agent(A, "tick 0").await.unwrap();

        // Paths are relative to the agent's prefix.
        let sha = sha_for(&manifest, "outputs/output.md");
        assert_eq!(sha.as_str().len(), 40, "blob sha is 40 hex chars");
        assert_eq!(
            gs.read_agent_at(A, sha).await.unwrap().unwrap().as_ref(),
            b"v1"
        );
        // The write-time content address equals the committed blob sha.
        assert_eq!(BlobSha::of_bytes(b"v1").as_str(), sha.as_str());
    }

    #[tokio::test]
    async fn commit_agent_clean_tree_is_noop_on_retry() {
        let (_tmp, gs) = fresh();
        gs.put(&format!("{A}/mandate.md"), Bytes::from_static(b"m"))
            .await
            .unwrap();
        let m1 = gs.commit_agent(A, "tick").await.unwrap();
        let m2 = gs.commit_agent(A, "tick").await.unwrap();
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
        let ma = gs.commit_agent(A, "a").await.unwrap();
        let mb = gs.commit_agent(B, "b").await.unwrap();

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
        assert_eq!(
            gs.read_agent_at(A, sha_a).await.unwrap().unwrap().as_ref(),
            b"A"
        );
        assert!(
            gs.read_agent_at(B, sha_a).await.unwrap().is_none(),
            "A's blob must not resolve in B's repo"
        );
    }

    #[tokio::test]
    async fn read_at_resolves_historical_version_after_overwrite() {
        let (_tmp, gs) = fresh();
        let key = format!("{A}/outputs/output.md");
        gs.put(&key, Bytes::from_static(b"v1")).await.unwrap();
        let sha_v1 = sha_for(
            &gs.commit_agent(A, "t1").await.unwrap(),
            "outputs/output.md",
        )
        .clone();
        gs.put(&key, Bytes::from_static(b"v2")).await.unwrap();
        let sha_v2 = sha_for(
            &gs.commit_agent(A, "t2").await.unwrap(),
            "outputs/output.md",
        )
        .clone();

        assert_ne!(sha_v1, sha_v2);
        assert_eq!(
            gs.read_agent_at(A, &sha_v1)
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
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
        gs.commit_agent(A, "t").await.unwrap();

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
            gs.commit_agent(A, "t").await.unwrap();
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
        let m = gs2.commit_agent(A, "t-again").await.unwrap();
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
}

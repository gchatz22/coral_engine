//! Git plumbing free functions backing [`PerAgentGitStorage`](super::PerAgentGitStorage)'s
//! per-agent versioning.
//!
//! Each agent's FS prefix is a git repository rooted at its own directory; the
//! working tree is the durable content the shared data plane already wrote, and
//! versioning is git's object database. The exposed surface is exactly
//! {commit-per-tick, read-blob-at-sha}: no branch, checkout, merge, or rebase,
//! so HEAD only ever advances and historical reads resolve a blob sha without
//! moving the working tree.
//!
//! `git2` is synchronous; the caller runs each of these on a blocking thread,
//! and the `git2::Repository` handle never crosses an await point (it is opened
//! and dropped inside the call).

use super::{BlobSha, StorageError, StorageResult};
use bytes::Bytes;
use git2::{
    ErrorCode, IndexAddOption, ObjectType, Oid, Repository, Signature, TreeWalkMode, TreeWalkResult,
};
use std::path::Path;

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

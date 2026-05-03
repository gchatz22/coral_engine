//! `AgentFs` — directory-backed per-agent filesystem.
//!
//! This module owns the on-disk representation of a single agent's state.
//! The agent's working memory is **state-as-files** rather than a hidden
//! context window (see `VISION.md` § 4: "every agent has a filesystem"),
//! so this layout is the durable substrate the run loop reads and writes
//! between wakeups.
//!
//! # Schema
//!
//! ```text
//! <root>/
//!   mandate.json          — what the agent is told to do (current state)
//!   outputs/<ulid>.json   — produced artifacts; the agent's public claims
//!   evidence/<sha256>.json — raw record of every tool call; the provenance trail
//!   notes/                — private working memory; scratchpad, intermediate reasoning
//!   retirement.json       — terminal marker; agent has cleanly ended
//! ```
//!
//! Each subdirectory is a deliberate split, not a filing convention.
//! The walls between them encode load-bearing rules.
//!
//! # `outputs/` vs `notes/` — claims vs thinking
//!
//! This is the core split.
//!
//! **`outputs/`** are the agent's *deliverables* — what a parent reads,
//! what an audit tool indexes, what a human reviewer quotes. Each file is
//! a *claim about the world*: "this drug is at risk of hold," "this code
//! failed test X." Per `VISION.md` § 4 ("provenance by construction"),
//! every output **must reference evidence**. [`AgentFs::persist_output`]
//! enforces this by rejecting an empty or unresolvable evidence vector
//! (see [`FsError::EmptyEvidence`] and [`FsError::EvidenceNotFound`]).
//! There is no path through the system that produces a claim without a
//! trail. Outputs are also **immutable** — once written, the file at
//! `outputs/<ulid>.json` does not change. A reader can quote it; a parent
//! can pin its id; a human auditor can dispute it. To revise a claim, an
//! agent emits a *new* output that supersedes the old one.
//!
//! **`notes/`** are the agent's *private working memory*. Scratchpad.
//! Intermediate distillations. Half-baked reasoning. A draft of an output
//! that isn't ready to publish. Per `VISION.md` § 4, this is how the
//! agent thinks across wakeups instead of relying on a hidden context
//! window. [`AgentFs::apply_ops`] writes here in response to
//! `RewriteFs` decisions and rejects any path that escapes
//! `<root>/notes/` (see [`FsError::PathTraversal`] and
//! [`FsError::PathOutsideNotes`]). Notes are **mutable** (the agent
//! rewrites them freely), **private** (parents do not read them by
//! default), and have **no provenance requirement** (you can scribble
//! whatever helps you reason).
//!
//! Conflating these would muddle "what I'm telling my parent" with "my
//! scratchpad." A parent reading "draft 3, unsure" alongside "the drug
//! failed phase 2" is going to make bad calls. The directory split is
//! the wall.
//!
//! # `evidence/` — content-addressed by design
//!
//! Each `evidence/<sha256>.json` is the **raw record** of a tool call:
//! tool name, args, result, timestamp. The id is
//! `sha256(canonical_json(tool, args, result))`, computed once in
//! [`crate::evidence::EvidenceId::new`]. Three properties fall out:
//!
//! 1. **Dedup is automatic.** Two tool calls with the same `(tool, args,
//!    result)` produce the same id, so [`AgentFs::record_evidence`] is
//!    idempotent and we never write the same record twice.
//! 2. **Outputs that cite the same evidence id are demonstrably
//!    grounded in the same primary source.** An audit tool walks
//!    `evidence_ids` from each output to its file with no joins.
//! 3. **Provenance is verifiable without trust.** An auditor recomputes
//!    the hash and confirms the file matches.
//!
//! Evidence is upstream of thinking, not part of it. You don't edit
//! evidence; it's what the world told you. Notes are how you reasoned
//! about it.
//!
//! # `mandate.json`
//!
//! The standing instruction (text + idle period + max ticks). Persisted
//! to disk so an agent restart picks up the latest mandate, not a stale
//! one from initial config. Mutable over time once `MandateUpdate`
//! triggers and `HumanOverride { EditMandate }` ops are wired (see
//! `scratch/agent_runtime.md` § 11). Today it's a single file; a
//! `mandate_history/` sidecar will likely accompany it when mandate-edit
//! semantics land, so audit can see what changed when. Out of scope for
//! the bootstrap.
//!
//! # `retirement.json` — the terminal marker
//!
//! Per `VISION.md` § 3, agents *idle and wake* — they don't normally
//! exit. `Retire` is the explicit, intentional shutdown decision (see
//! [`crate::decision::Decision::Retire`]). When emitted,
//! [`AgentFs::persist_retirement`] writes `retirement.json` with the
//! reason and a UTC timestamp, then the loop exits cleanly. The file
//! exists as a separate concept (rather than just "the loop returned")
//! for three reasons:
//!
//! 1. **Restart safety.** Anything trying to start an agent at this root
//!    again sees `retirement.json` as a hard "no — this agent's life is
//!    over." Without it, a crash-recovery loop could resurrect a finished
//!    agent and start re-emitting outputs.
//! 2. **Audit.** Why did this agent stop? "Mandate satisfied," "parent
//!    retired me," "user retired me," "max_ticks reached." Important
//!    context when reconstructing what the graph did weeks later.
//! 3. **Distinguishing retirement from crash.** Crashed agent: loop
//!    exited but no `retirement.json`. Cleanly retired agent: file
//!    present. The orchestrator (when we have one) treats those very
//!    differently.
//!
//! # What this layout deliberately does not include yet
//!
//! Each item is punted in `scratch/minimal_node_backend.md` § 6 with a
//! reason; surfaced here so a reader knows the gaps are intentional:
//!
//! - **No conflict log.** When parents reconcile disagreeing children
//!   (`VISION.md` § 4), the resolution will need somewhere to live —
//!   likely `conflicts/<id>.json`. Bootstrap is single-node.
//! - **No child handles.** Likewise out of scope for single-node.
//! - **No mandate history.** Lands with mandate-edit semantics.
//! - **No versioning / snapshots / forks.** The graph layer is supposed
//!   to be time-scrubbable (`VISION.md` § 5); that needs an FS-layer
//!   hook (probably copy-on-write or a `snapshots/` dir). Deferred.
//! - **No mid-tick state.** By design, state lives in `notes/` between
//!   ticks. If mid-tick state has to survive a crash, that's a separate
//!   concern.
//!
//! # Implementation notes
//!
//! The bootstrap uses synchronous `std::fs`. The run loop ticket
//! (JAR2-8) can wrap calls in `tokio::task::spawn_blocking` if it needs
//! to. Concurrent multi-writer safety is out of scope: assume a single
//! process owns the root.

use crate::decision::FsOp;
use crate::evidence::{EvidenceId, EvidenceRecord};
use crate::mandate::{Mandate, Output};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use thiserror::Error;

/// Typed errors the `AgentFs` raises. The run loop (JAR2-8) matches on
/// these to distinguish provenance/traversal violations from real I/O
/// failures, so adding variants is a breaking change for that consumer.
#[derive(Debug, Error)]
pub enum FsError {
    /// `persist_output` was called with an empty evidence slice.
    #[error("output rejected: evidence list is empty (provenance contract)")]
    EmptyEvidence,
    /// `persist_output` referenced an evidence id with no record on disk.
    #[error("output rejected: evidence {0} not found on disk")]
    EvidenceNotFound(EvidenceId),
    /// An `FsOp` path contained `..`, an absolute root, or a Windows
    /// prefix — anything that could escape the agent's root.
    #[error("path traversal rejected: {0}")]
    PathTraversal(String),
    /// An `FsOp` path was syntactically clean but resolved outside
    /// `<root>/notes/`. Bootstrap `apply_ops` only writes under `notes/`.
    #[error("path outside notes/ rejected: {0}")]
    PathOutsideNotes(String),
    /// Wrapped `std::io::Error` with the path that caused it, when known.
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

impl FsError {
    fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        FsError::Io {
            path: path.into(),
            source,
        }
    }
}

/// On-disk record written to `retirement.json`. Kept private to this
/// module — readers go through future audit tooling, not direct serde.
#[derive(Debug, Serialize, Deserialize)]
struct RetirementRecord {
    reason: String,
    retired_at: DateTime<Utc>,
}

/// Directory-backed per-agent filesystem. Cheap to clone — holds only the
/// root path.
#[derive(Debug, Clone)]
pub struct AgentFs {
    root: PathBuf,
}

impl AgentFs {
    /// Initialize the layout under `root` and write `mandate.json` if it
    /// is not already present.
    ///
    /// Idempotent: calling `open` against an existing FS does not clobber
    /// `mandate.json`, `outputs/`, `evidence/`, `notes/`, or
    /// `retirement.json` — directories are created if missing, and the
    /// mandate file is only written when absent.
    pub fn open(root: PathBuf, mandate: &Mandate) -> anyhow::Result<Self> {
        Self::ensure_dir(&root)?;
        Self::ensure_dir(&root.join("outputs"))?;
        Self::ensure_dir(&root.join("evidence"))?;
        Self::ensure_dir(&root.join("notes"))?;

        let mandate_path = root.join("mandate.json");
        if !mandate_path.exists() {
            let bytes = serde_json::to_vec_pretty(mandate)?;
            fs::write(&mandate_path, &bytes).map_err(|e| FsError::io(&mandate_path, e))?;
        }

        Ok(Self { root })
    }

    /// Borrow the agent's filesystem root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Persist an `EvidenceRecord` under `evidence/<id>.json`.
    ///
    /// Writing the same record twice is a no-op: the file is content-
    /// addressed by `record.id` (which is itself the sha256 of the
    /// canonical JSON of `(tool, args, result)` — see `evidence::EvidenceId::new`),
    /// so a duplicate write would produce identical bytes. We skip the
    /// write entirely if the file exists.
    pub fn record_evidence(&self, record: EvidenceRecord) -> anyhow::Result<EvidenceId> {
        let id = record.id.clone();
        let path = self.evidence_path(&id);
        if path.exists() {
            // Already present — content-addressed, so the bytes match.
            return Ok(id);
        }
        let bytes = serde_json::to_vec_pretty(&record)?;
        fs::write(&path, &bytes).map_err(|e| FsError::io(&path, e))?;
        Ok(id)
    }

    /// Return `Ok(())` if `id` resolves to a file under `evidence/`,
    /// otherwise `Err(FsError::EvidenceNotFound)`.
    pub fn evidence_must_exist(&self, id: &EvidenceId) -> anyhow::Result<()> {
        if self.evidence_path(id).exists() {
            Ok(())
        } else {
            Err(FsError::EvidenceNotFound(id.clone()).into())
        }
    }

    /// Persist an `Output` under `outputs/<ulid>.json`, enforcing the
    /// provenance contract: at least one evidence id, and every id must
    /// resolve to a record on disk.
    pub fn persist_output(&self, content: &str, evidence: &[EvidenceId]) -> anyhow::Result<Output> {
        if evidence.is_empty() {
            return Err(FsError::EmptyEvidence.into());
        }
        for id in evidence {
            self.evidence_must_exist(id)?;
        }
        let output = Output::new(content.to_string(), evidence.to_vec(), Utc::now());
        let path = self
            .root
            .join("outputs")
            .join(format!("{}.json", output.id));
        let bytes = serde_json::to_vec_pretty(&output)?;
        fs::write(&path, &bytes).map_err(|e| FsError::io(&path, e))?;
        Ok(output)
    }

    /// Apply a batch of filesystem ops. The bootstrap only supports
    /// writes and deletes under `notes/`; any path that escapes
    /// `<root>/notes/` is rejected before any write happens.
    pub fn apply_ops(&self, ops: Vec<FsOp>) -> anyhow::Result<()> {
        // Validate every op first so a partial batch with a bad path in
        // the middle does not leave a half-applied state.
        let mut planned: Vec<(PathBuf, &FsOp)> = Vec::with_capacity(ops.len());
        for op in &ops {
            let raw = match op {
                FsOp::WriteFile { path, .. } | FsOp::DeleteFile { path } => path.as_str(),
            };
            let resolved = self.resolve_notes_path(raw)?;
            planned.push((resolved, op));
        }

        for (path, op) in planned {
            match op {
                FsOp::WriteFile { content, .. } => {
                    if let Some(parent) = path.parent() {
                        Self::ensure_dir(parent)?;
                    }
                    fs::write(&path, content.as_bytes()).map_err(|e| FsError::io(&path, e))?;
                }
                FsOp::DeleteFile { .. } => {
                    // Idempotent at the type level — missing file is fine.
                    match fs::remove_file(&path) {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                        Err(e) => return Err(FsError::io(&path, e).into()),
                    }
                }
            }
        }
        Ok(())
    }

    /// Return the most recent (up to) `n` `Output`s on disk.
    ///
    /// Order is deterministic: filenames under `outputs/` are ULIDs (which
    /// sort lexically by creation time), so we sort filenames ascending,
    /// take the last `n`, and return them in ascending filename order.
    /// This lets the run loop hand a `Decide` implementation a stable
    /// "recent history" window without needing wall-clock comparisons.
    ///
    /// Added for JAR2-6 (`assemble_context`); the bootstrap reads the full
    /// directory and slices in memory because the corpus is small. A
    /// follow-up can index or page if that ever stops being true.
    pub fn list_recent_outputs(&self, n: usize) -> anyhow::Result<Vec<Output>> {
        let dir = self.root.join("outputs");
        Self::read_recent_json(&dir, n)
    }

    /// Return the most recent (up to) `n` `EvidenceRecord`s on disk.
    ///
    /// Order is deterministic: filenames under `evidence/` are sha256
    /// hex digests, which carry no temporal meaning, so "recent" here is
    /// purely lexical (ascending filename, last `n`). The bootstrap
    /// `assemble_context` only needs determinism, not true recency, and
    /// promoting evidence to a time-indexed store is out of scope.
    pub fn list_recent_evidence(&self, n: usize) -> anyhow::Result<Vec<EvidenceRecord>> {
        let dir = self.root.join("evidence");
        Self::read_recent_json(&dir, n)
    }

    /// Write `retirement.json` with the supplied reason and the current
    /// UTC timestamp. Overwrites any prior retirement record.
    pub fn persist_retirement(&self, reason: &str) -> anyhow::Result<()> {
        let record = RetirementRecord {
            reason: reason.to_string(),
            retired_at: Utc::now(),
        };
        let path = self.root.join("retirement.json");
        let bytes = serde_json::to_vec_pretty(&record)?;
        fs::write(&path, &bytes).map_err(|e| FsError::io(&path, e))?;
        Ok(())
    }

    // ---- helpers --------------------------------------------------------

    fn evidence_path(&self, id: &EvidenceId) -> PathBuf {
        self.root.join("evidence").join(format!("{}.json", id))
    }

    fn ensure_dir(path: &Path) -> anyhow::Result<()> {
        fs::create_dir_all(path).map_err(|e| FsError::io(path, e))?;
        Ok(())
    }

    /// Read every regular `.json` file under `dir`, sort by filename
    /// ascending, take the last `n`, and parse each into `T`.
    ///
    /// Filename sort is the stable ordering primitive — outputs use ULID
    /// filenames (time-monotonic) and evidence uses sha256 (arbitrary but
    /// deterministic). A missing directory yields an empty list rather
    /// than an error so this is safe to call right after `open`.
    fn read_recent_json<T>(dir: &Path, n: usize) -> anyhow::Result<Vec<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        if !dir.exists() {
            return Ok(Vec::new());
        }
        // Collect (filename, full path) for every regular .json entry. We
        // sort by filename rather than by full path so the ordering is
        // independent of the FS root location.
        let mut entries: Vec<(std::ffi::OsString, PathBuf)> = Vec::new();
        for entry in fs::read_dir(dir).map_err(|e| FsError::io(dir, e))? {
            let entry = entry.map_err(|e| FsError::io(dir, e))?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            entries.push((entry.file_name(), path));
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let start = entries.len().saturating_sub(n);
        let mut out = Vec::with_capacity(entries.len() - start);
        for (_, path) in entries.into_iter().skip(start) {
            let bytes = fs::read(&path).map_err(|e| FsError::io(&path, e))?;
            let value: T = serde_json::from_slice(&bytes)?;
            out.push(value);
        }
        Ok(out)
    }

    /// Resolve `raw` against `<root>/notes/`. Paths must be relative,
    /// start with a `notes/` segment, contain only normal path
    /// components (no `..`, no root, no Windows prefix), and name a file
    /// inside `notes/` (not `notes/` itself).
    ///
    /// We deliberately walk `Components` instead of using
    /// `Path::canonicalize`: target files may not exist yet (write
    /// targets), and canonicalize would error on those. The bootstrap
    /// scope (single-process owner, no symlink farm) makes this
    /// component check sufficient. Symlinks pointing out of `notes/` are
    /// a known follow-up.
    fn resolve_notes_path(&self, raw: &str) -> anyhow::Result<PathBuf> {
        let candidate = Path::new(raw);

        // First pass: only Normal components and `.` are allowed. A
        // `..`, root, or Windows-prefix component means the caller is
        // trying to traverse out — reject before any normalization.
        let mut cleaned = PathBuf::new();
        for comp in candidate.components() {
            match comp {
                Component::Normal(part) => cleaned.push(part),
                Component::CurDir => {} // "." — harmless, drop it
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(FsError::PathTraversal(raw.to_string()).into());
                }
            }
        }
        if cleaned.as_os_str().is_empty() {
            return Err(FsError::PathTraversal(raw.to_string()).into());
        }

        // Second pass: paths must be rooted at `notes/`. Bare filenames
        // and paths under any other top-level directory (e.g.
        // `outputs/...`) are out of bounds for `apply_ops`.
        let tail = match cleaned.strip_prefix("notes") {
            Ok(rest) if !rest.as_os_str().is_empty() => rest.to_path_buf(),
            // `notes` alone or `notes/` with no file — nothing to write.
            Ok(_) => return Err(FsError::PathOutsideNotes(raw.to_string()).into()),
            Err(_) => return Err(FsError::PathOutsideNotes(raw.to_string()).into()),
        };

        Ok(self.root.join("notes").join(tail))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decision::FsOp;
    use crate::evidence::EvidenceRecord;
    use crate::mandate::Mandate;
    use chrono::Utc;
    use serde_json::json;
    use std::time::Duration;
    use tempfile::TempDir;

    fn fresh_fs() -> (TempDir, AgentFs, Mandate) {
        let tmp = TempDir::new().unwrap();
        let mandate = Mandate::new("research foo", Duration::from_millis(1000), Some(10));
        let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate).unwrap();
        (tmp, fs, mandate)
    }

    fn record(tool: &str, args: serde_json::Value, result: serde_json::Value) -> EvidenceRecord {
        EvidenceRecord::new(tool, args, result, Utc::now())
    }

    #[test]
    fn open_creates_layout_and_writes_mandate() {
        let (tmp, _fs, mandate) = fresh_fs();
        let root = tmp.path();
        assert!(root.join("mandate.json").is_file());
        assert!(root.join("outputs").is_dir());
        assert!(root.join("evidence").is_dir());
        assert!(root.join("notes").is_dir());

        let bytes = std::fs::read(root.join("mandate.json")).unwrap();
        let back: Mandate = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, mandate);
    }

    #[test]
    fn open_is_idempotent_and_does_not_clobber_mandate() {
        let tmp = TempDir::new().unwrap();
        let original = Mandate::new("first", Duration::from_millis(500), None);
        let _fs = AgentFs::open(tmp.path().to_path_buf(), &original).unwrap();

        // Re-open with a *different* mandate; the on-disk file must keep
        // the original.
        let other = Mandate::new("second", Duration::from_millis(999), Some(7));
        let _fs2 = AgentFs::open(tmp.path().to_path_buf(), &other).unwrap();

        let bytes = std::fs::read(tmp.path().join("mandate.json")).unwrap();
        let back: Mandate = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn record_evidence_is_content_addressed_and_dedup_safe() {
        let (tmp, fs, _m) = fresh_fs();
        let rec = record("echo", json!({"msg": "hi"}), json!({"echoed": "hi"}));
        let id = fs.record_evidence(rec.clone()).unwrap();

        // Filename matches the id.
        let path = tmp.path().join("evidence").join(format!("{}.json", id));
        assert!(path.is_file());

        // Second write of an identical record is a no-op — same id, no
        // duplicate, and the directory still has exactly one entry.
        let id2 = fs.record_evidence(rec.clone()).unwrap();
        assert_eq!(id, id2);

        let entries: Vec<_> = std::fs::read_dir(tmp.path().join("evidence"))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(
            entries.len(),
            1,
            "duplicate evidence write created extra file"
        );

        // A different record produces a different id and a second file.
        let other = record("echo", json!({"msg": "bye"}), json!({"echoed": "bye"}));
        let other_id = fs.record_evidence(other).unwrap();
        assert_ne!(id, other_id);
        let entries: Vec<_> = std::fs::read_dir(tmp.path().join("evidence"))
            .unwrap()
            .collect();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn persist_output_rejects_empty_evidence() {
        let (_tmp, fs, _m) = fresh_fs();
        let err = fs.persist_output("hello", &[]).unwrap_err();
        let downcast = err.downcast_ref::<FsError>().expect("typed FsError");
        assert!(matches!(downcast, FsError::EmptyEvidence));
    }

    #[test]
    fn persist_output_rejects_unknown_evidence_id() {
        let (tmp, fs, _m) = fresh_fs();
        let bogus = EvidenceId::from_hex("deadbeef".repeat(8)); // 64 hex chars
        let err = fs.persist_output("hello", &[bogus.clone()]).unwrap_err();
        let downcast = err.downcast_ref::<FsError>().expect("typed FsError");
        match downcast {
            FsError::EvidenceNotFound(missing) => assert_eq!(missing, &bogus),
            other => panic!("expected EvidenceNotFound, got {other:?}"),
        }
        // No output file should have been written.
        let entries: Vec<_> = std::fs::read_dir(tmp.path().join("outputs"))
            .unwrap()
            .collect();
        assert!(entries.is_empty());
    }

    #[test]
    fn persist_output_writes_file_referencing_evidence() {
        let (tmp, fs, _m) = fresh_fs();
        let rec = record("echo", json!({"msg": "hi"}), json!({"echoed": "hi"}));
        let id = fs.record_evidence(rec).unwrap();

        let out = fs.persist_output("hello", &[id.clone()]).unwrap();
        assert_eq!(out.content, "hello");
        assert_eq!(out.evidence, vec![id.clone()]);

        let path = tmp.path().join("outputs").join(format!("{}.json", out.id));
        assert!(path.is_file());
        let bytes = std::fs::read(&path).unwrap();
        let back: Output = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, out);
        assert!(back.evidence.contains(&id));
    }

    #[test]
    fn apply_ops_writes_under_notes() {
        let (tmp, fs, _m) = fresh_fs();
        fs.apply_ops(vec![FsOp::WriteFile {
            path: "notes/a.md".into(),
            content: "hi".into(),
        }])
        .unwrap();
        let written = tmp.path().join("notes").join("a.md");
        assert_eq!(std::fs::read_to_string(&written).unwrap(), "hi");

        // Nested subdirectory under notes/ is created on demand.
        fs.apply_ops(vec![FsOp::WriteFile {
            path: "notes/sub/c.md".into(),
            content: "deep".into(),
        }])
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(tmp.path().join("notes").join("sub").join("c.md")).unwrap(),
            "deep"
        );

        // DeleteFile removes the file and is idempotent on missing paths.
        fs.apply_ops(vec![FsOp::DeleteFile {
            path: "notes/a.md".into(),
        }])
        .unwrap();
        assert!(!written.exists());
        fs.apply_ops(vec![FsOp::DeleteFile {
            path: "notes/never-existed.md".into(),
        }])
        .unwrap();
    }

    #[test]
    fn apply_ops_rejects_path_traversal() {
        let (tmp, fs, _m) = fresh_fs();
        for bad in ["../etc/passwd", "../../escape", "notes/../../escape"] {
            let err = fs
                .apply_ops(vec![FsOp::WriteFile {
                    path: bad.into(),
                    content: "x".into(),
                }])
                .unwrap_err();
            let downcast = err.downcast_ref::<FsError>().expect("typed FsError");
            assert!(
                matches!(downcast, FsError::PathTraversal(_)),
                "expected PathTraversal for {bad}, got {downcast:?}"
            );
        }

        // Absolute paths are rejected too.
        let err = fs
            .apply_ops(vec![FsOp::WriteFile {
                path: "/etc/passwd".into(),
                content: "x".into(),
            }])
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<FsError>().unwrap(),
            FsError::PathTraversal(_)
        ));

        // A syntactically clean path that resolves outside notes/ is also
        // rejected — we only allow writes under notes/.
        let err = fs
            .apply_ops(vec![FsOp::WriteFile {
                path: "outputs/forged.json".into(),
                content: "x".into(),
            }])
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<FsError>().unwrap(),
            FsError::PathOutsideNotes(_)
        ));

        // None of the rejected ops should have produced files anywhere.
        let outputs: Vec<_> = std::fs::read_dir(tmp.path().join("outputs"))
            .unwrap()
            .collect();
        assert!(outputs.is_empty());
    }

    #[test]
    fn apply_ops_is_atomic_against_a_bad_path_in_the_middle() {
        let (tmp, fs, _m) = fresh_fs();
        let err = fs
            .apply_ops(vec![
                FsOp::WriteFile {
                    path: "notes/good.md".into(),
                    content: "ok".into(),
                },
                FsOp::WriteFile {
                    path: "../escape".into(),
                    content: "bad".into(),
                },
            ])
            .unwrap_err();
        assert!(err.downcast_ref::<FsError>().is_some());
        // Pre-flight validation rejects the batch before any write.
        assert!(!tmp.path().join("notes").join("good.md").exists());
    }

    #[test]
    fn list_recent_outputs_returns_window_in_filename_order() {
        let (_tmp, fs, _m) = fresh_fs();
        // Seed an evidence record we can attach to every output.
        let id = fs
            .record_evidence(record("echo", json!({"k": 1}), json!({"v": 1})))
            .unwrap();

        let mut all_ids = Vec::new();
        for i in 0..10 {
            let out = fs.persist_output(&format!("o-{i}"), &[id.clone()]).unwrap();
            all_ids.push(out.id);
        }

        // Last 8.
        let recent = fs.list_recent_outputs(8).unwrap();
        assert_eq!(recent.len(), 8);

        // Returned outputs are in ascending filename (= ULID) order.
        let ids: Vec<_> = recent.iter().map(|o| o.id).collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted);

        // n larger than available → all entries returned.
        let all = fs.list_recent_outputs(100).unwrap();
        assert_eq!(all.len(), 10);

        // n = 0 → empty.
        let none = fs.list_recent_outputs(0).unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn list_recent_evidence_returns_window_in_filename_order() {
        let (_tmp, fs, _m) = fresh_fs();
        for i in 0..10 {
            fs.record_evidence(record("echo", json!({ "i": i }), json!({ "i": i })))
                .unwrap();
        }
        let recent = fs.list_recent_evidence(8).unwrap();
        assert_eq!(recent.len(), 8);

        let ids: Vec<_> = recent.iter().map(|r| r.id.as_str().to_string()).collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted, "evidence not returned in filename order");
    }

    #[test]
    fn persist_retirement_writes_file() {
        let (tmp, fs, _m) = fresh_fs();
        fs.persist_retirement("done").unwrap();
        let path = tmp.path().join("retirement.json");
        assert!(path.is_file());
        let v: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(v.get("reason").and_then(|x| x.as_str()), Some("done"));
        assert!(v.get("retired_at").and_then(|x| x.as_str()).is_some());
    }
}

//! `PerAgentFs` — directory-backed per-agent filesystem.
//!
//! This module owns the on-disk representation of a single agent's state
//! and enforces the provenance invariant from
//! `scratch/minimal_node_backend.md` § 5: an `Output` cannot be persisted
//! without referencing evidence records that already exist on disk.
//!
//! Layout under `<root>/`:
//!
//! ```text
//! mandate.json                 # current mandate, written on first open
//! outputs/<ulid>.json          # one per persisted Output
//! evidence/<sha256>.json       # one per recorded EvidenceRecord
//! notes/                       # free-form scratch — apply_ops writes here
//! retirement.json              # written on retirement
//! ```
//!
//! The bootstrap deliberately uses synchronous `std::fs`. The run loop
//! ticket (JAR2-8) can wrap calls in `tokio::task::spawn_blocking` if it
//! needs to. Versioning, snapshots, forks, and concurrent multi-writer
//! safety are all out of scope (see ticket JAR2-4 "Out of scope").

use crate::decision::FsOp;
use crate::evidence::{EvidenceId, EvidenceRecord};
use crate::mandate::{Mandate, Output};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use thiserror::Error;

/// Typed errors the `PerAgentFs` raises. The run loop (JAR2-8) matches on
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
pub struct PerAgentFs {
    root: PathBuf,
}

impl PerAgentFs {
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

    fn fresh_fs() -> (TempDir, PerAgentFs, Mandate) {
        let tmp = TempDir::new().unwrap();
        let mandate = Mandate::new("research foo", Duration::from_millis(1000), Some(10));
        let fs = PerAgentFs::open(tmp.path().to_path_buf(), &mandate).unwrap();
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
        let _fs = PerAgentFs::open(tmp.path().to_path_buf(), &original).unwrap();

        // Re-open with a *different* mandate; the on-disk file must keep
        // the original.
        let other = Mandate::new("second", Duration::from_millis(999), Some(7));
        let _fs2 = PerAgentFs::open(tmp.path().to_path_buf(), &other).unwrap();

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

//! `AgentFs` — facade over the pluggable per-agent storage backend.
//!
//! As of JAR2-53 (stage 2.5.3), `AgentFs` is a thin facade over
//! [`crate::storage::AgentStorage`]: the on-disk representation of
//! today's directory layout is one storage backend
//! ([`crate::storage::LocalStorage`]); a forthcoming `S3Storage` lands
//! at the future cloud-deployment stage. Callers continue to talk to
//! `AgentFs` and never touch the backend directly. See
//! `scratch/agent_storage.md` for the full design, especially:
//!
//! - § 2 — the method → object-op mapping table this module implements.
//! - § 6.1 — `LocalStorage` semantics this facade still relies on
//!   (atomic writes, `O_EXCL` for content-addressed evidence).
//! - § 8 — atomicity contract (per-key, no cross-key transactions).
//! - § 13 — load-bearing design decisions.
//!
//! # Schema
//!
//! ```text
//! <prefix>mandate.json          — what the agent is told to do
//! <prefix>outputs/<ulid>.json   — produced artifacts; the agent's claims
//! <prefix>evidence/<sha256>.json — raw record of every tool call
//! <prefix>notes/                — private working memory
//! <prefix>claims/<slug>.json    — claim_seed registry (JAR2-28)
//! <prefix>retirement.json       — terminal marker
//! ```
//!
//! `<prefix>` is `""` for the single-host bootstrap. When multi-agent
//! topology lands, the prefix becomes
//! `graphs/<graph_id>/agents/<agent_id>/` so a single bucket /
//! filesystem can hold every agent's state without collisions.
//! [`AgentFs::new_with_storage`] is the constructor that exercises the
//! prefix; the legacy `AgentFs::open` retains today's semantics for
//! the bootstrap call sites.
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
//!    result)` produce the same id, so [`AgentFs::record_evidence`]
//!    uses [`crate::storage::AgentStorage::put_if_absent`] and is
//!    idempotent — same bytes, same key, no duplicate.
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
//! # `claims/` — seed registry for claim-id stability across ticks
//!
//! `Decision::CallTool` carries a `ClaimSeed` the agent picks. The
//! kernel uses that seed to derive a stable claim id, so multiple
//! ticks supporting the same conceptual claim collapse into one. LLMs
//! are non-deterministic, though — woken on tick 7 the agent may pick
//! a different seed than it did on tick 3, and provenance fragments.
//!
//! `claims/` is the durable place the agent writes seeds it has
//! already minted. Per `VISION.md` § 4, *state is files, not hidden
//! context*: the agent doesn't have to *remember* a seed across ticks;
//! it gets to *look it up*. The convention (slug rules, file shape,
//! prompt addendum) lives in `scratch/claim_seed_persistence.md` and
//! moves into the prompt-template module under JAR2-16. See
//! [`AgentFs::write_claim`], [`AgentFs::read_claim`],
//! [`AgentFs::list_claims`], and [`claim_slug`].
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
//! - **`health.json` is not on this facade.** [`crate::health::HealthTracker`]
//!   writes `health.json` / `health/<ts>.json` directly via
//!   `std::fs::write` against its own root. Moving the health tracker
//!   over to `AgentStorage` is a follow-up — out of scope for stage
//!   2.5.3 per the "smallest correct diff" rule.

use crate::decision::FsOp;
use crate::evidence::{EvidenceId, EvidenceRecord};
use crate::mandate::{Mandate, Output};
use crate::storage::{AgentStorage, LocalStorage, PutOutcome};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

/// Typed errors the `AgentFs` raises. The run loop matches on these to
/// distinguish provenance/traversal violations from real storage
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
    /// Wrapped backend error from the underlying
    /// [`crate::storage::AgentStorage`]. The `key` field carries the
    /// logical key (under the agent's prefix) that the operation
    /// targeted, so a failure trail can be reconstructed even when the
    /// backend's error string is opaque.
    #[error("storage error at {key}: {source}")]
    Storage {
        key: String,
        #[source]
        source: crate::storage::StorageError,
    },
}

impl FsError {
    fn storage(key: impl Into<String>, source: crate::storage::StorageError) -> Self {
        FsError::Storage {
            key: key.into(),
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

/// Lifecycle status the agent assigns to a claim. The kernel does not
/// interpret these; they exist so the agent's future self can tell
/// "still being investigated" from "already settled, don't gather more
/// evidence under this seed."
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimStatus {
    Open,
    Resolved,
    Abandoned,
}

/// On-disk record written to `claims/<slug>.json`. The agent reads
/// these back at the top of a tick to decide whether a new
/// `claim_seed` is needed or an existing one should be reused.
///
/// `seed` is the canonical string the agent attached to
/// `Decision::CallTool`; same string in the file as in the seed. The
/// slug is derived from `seed` via [`claim_slug`] and is not stored on
/// the record (it's the filename).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claim {
    pub seed: String,
    pub description: String,
    pub status: ClaimStatus,
    pub created_at: DateTime<Utc>,
}

/// Maximum byte length of the kebab body in a slug (before the hash
/// suffix). 80 keeps `claims/` listings readable on a terminal and
/// leaves headroom under typical filesystem name limits once the
/// `-<8 hex>` suffix and `.json` extension are added.
const SLUG_BODY_MAX: usize = 80;

/// Derive the on-disk slug for a claim from its seed string.
///
/// Rules: lowercase, runs of non-`[a-z0-9]` collapse to `-`, leading
/// and trailing `-` are trimmed, the body is truncated to
/// [`SLUG_BODY_MAX`] bytes, and `-<first 8 hex chars of sha256(seed)>`
/// is *always* appended.
///
/// The hash suffix is unconditional on purpose. Conditional suffixing
/// (only on collision) makes the slug a function of prior writes
/// rather than of the seed alone, which would silently break
/// `read_claim` when the same seed maps to different filenames in
/// different orders. With the always-on suffix, two distinct seeds
/// that slugify to the same kebab body still get different filenames,
/// and the same seed always resolves to the same file.
///
/// If the kebab body is empty after trimming (e.g. seed `"!!!"`), the
/// slug is just the hash suffix. The 8-char prefix gives ~32 bits of
/// collision resistance, which is far more than the agent population
/// of one filesystem will ever contend with.
pub fn claim_slug(seed: &str) -> String {
    let mut body = String::with_capacity(seed.len());
    let mut prev_dash = true; // leading dashes get trimmed by suppressing them
    for ch in seed.chars() {
        let lc = ch.to_ascii_lowercase();
        if lc.is_ascii_alphanumeric() {
            body.push(lc);
            prev_dash = false;
        } else if !prev_dash {
            body.push('-');
            prev_dash = true;
        }
    }
    while body.ends_with('-') {
        body.pop();
    }
    if body.len() > SLUG_BODY_MAX {
        body.truncate(SLUG_BODY_MAX);
        // Truncation may have left a trailing `-`; clean it.
        while body.ends_with('-') {
            body.pop();
        }
    }

    let digest = Sha256::digest(seed.as_bytes());
    let suffix = hex::encode(&digest[..4]); // 8 hex chars

    if body.is_empty() {
        suffix
    } else {
        format!("{body}-{suffix}")
    }
}

/// Per-agent filesystem, expressed as a facade over an `AgentStorage`
/// backend. Cheap to clone — holds an `Arc` to the storage and a small
/// key prefix.
///
/// Construct via [`AgentFs::open`] for today's single-host bootstrap
/// shape (`<root>` is one agent's directory, prefix is empty), or via
/// [`AgentFs::new_with_storage`] when a test wants to drive `AgentFs`
/// against `MemoryStorage` / a custom storage prefix.
#[derive(Clone)]
pub struct AgentFs {
    storage: Arc<dyn AgentStorage>,
    /// Key prefix applied to every operation. Empty for the bootstrap;
    /// non-empty (`graphs/<graph_id>/agents/<agent_id>/`) when
    /// multi-agent topology lands. Always either empty or ends in `/`.
    prefix: String,
}

impl std::fmt::Debug for AgentFs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `dyn AgentStorage` doesn't carry a `Debug` bound (kept off
        // the trait to leave backends free to choose), so we summarize
        // the facade by its prefix instead.
        f.debug_struct("AgentFs")
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl AgentFs {
    /// Open an on-disk agent FS rooted at `root`.
    ///
    /// Wraps a [`LocalStorage`] backend with an empty prefix; equivalent
    /// to `new_with_storage(Arc::new(LocalStorage::new(root)?), "", mandate)`.
    /// Kept as the primary entry point for the bootstrap call sites
    /// (`bin/node_run*.rs`) and existing tests so this ticket's diff is
    /// strictly "facade swap" rather than "rethread agent ids through
    /// every caller". The `graphs/<graph_id>/agents/<agent_id>/` prefix
    /// shape is exercised via [`AgentFs::new_with_storage`] and lands at
    /// the call sites when multi-agent topology arrives.
    ///
    /// Idempotent: calling `open` against an existing FS does not clobber
    /// `mandate.json`, `outputs/`, `evidence/`, `notes/`, or
    /// `retirement.json` — the mandate file is only written when absent.
    pub async fn open(root: PathBuf, mandate: &Mandate) -> anyhow::Result<Self> {
        let storage = Arc::new(LocalStorage::new(root)?);
        Self::new_with_storage(storage, String::new(), mandate).await
    }

    /// Build an `AgentFs` over any storage backend with the supplied key
    /// prefix.
    ///
    /// `prefix` is normalized to either `""` or "`...something/`" — a
    /// trailing slash is appended if missing so callers don't have to
    /// remember the convention. Writing `mandate.json` is the only
    /// state side effect; the trait's lazy-directory-creation
    /// (`LocalStorage`) or implicit-namespace (`MemoryStorage`)
    /// semantics handle the rest.
    pub async fn new_with_storage(
        storage: Arc<dyn AgentStorage>,
        prefix: impl Into<String>,
        mandate: &Mandate,
    ) -> anyhow::Result<Self> {
        let mut prefix = prefix.into();
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        let me = Self { storage, prefix };

        // Idempotent mandate write: read first, write only if absent so
        // a re-open against an existing FS doesn't overwrite the
        // current mandate (matches today's `open` semantics).
        let mandate_key = me.key("mandate.json");
        let existing = me
            .storage
            .get(&mandate_key)
            .await
            .map_err(|e| FsError::storage(&mandate_key, e))?;
        if existing.is_none() {
            let bytes = serde_json::to_vec_pretty(mandate)?;
            me.storage
                .put(&mandate_key, Bytes::from(bytes))
                .await
                .map_err(|e| FsError::storage(&mandate_key, e))?;
        }

        Ok(me)
    }

    /// Borrow the underlying storage. Exposed for higher layers (tail
    /// indices, snapshot scans) that need direct trait access without
    /// going through every per-shape method.
    pub fn storage(&self) -> &Arc<dyn AgentStorage> {
        &self.storage
    }

    /// Borrow the agent's key prefix (always either empty or ending in
    /// `/`). Exposed so the tail-index work (JAR2-54) can compute
    /// `<prefix>outputs/_tail.json` keys without re-implementing the
    /// prefixing logic.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Persist an `EvidenceRecord` under `<prefix>evidence/<id>.json`.
    ///
    /// Writing the same record twice is a no-op: the file is content-
    /// addressed by `record.id` (sha256 of `(tool, args, result)` —
    /// see [`crate::evidence::EvidenceId::new`]), so a duplicate write
    /// would produce identical bytes. We use
    /// [`crate::storage::AgentStorage::put_if_absent`] which makes the
    /// dedup atomic — race-free against a concurrent re-record from a
    /// retried activity, matching the property the on-disk
    /// `O_EXCL` give us today.
    pub async fn record_evidence(&self, record: EvidenceRecord) -> anyhow::Result<EvidenceId> {
        let id = record.id.clone();
        let key = self.evidence_key(&id);
        let bytes = serde_json::to_vec_pretty(&record)?;
        // `put_if_absent` returns Created on first write, Existed on
        // subsequent attempts; both paths are success for the
        // content-addressed dedup contract.
        let _outcome: PutOutcome = self
            .storage
            .put_if_absent(&key, Bytes::from(bytes))
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        Ok(id)
    }

    /// Return `Ok(())` if `id` resolves to an evidence record,
    /// otherwise `Err(FsError::EvidenceNotFound)`.
    pub async fn evidence_must_exist(&self, id: &EvidenceId) -> anyhow::Result<()> {
        let key = self.evidence_key(id);
        let got = self
            .storage
            .get(&key)
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        if got.is_some() {
            Ok(())
        } else {
            Err(FsError::EvidenceNotFound(id.clone()).into())
        }
    }

    /// Persist an `Output` under `<prefix>outputs/<ulid>.json`,
    /// enforcing the provenance contract: at least one evidence id,
    /// and every id must resolve to a record.
    pub async fn persist_output(
        &self,
        content: &str,
        evidence: &[EvidenceId],
    ) -> anyhow::Result<Output> {
        if evidence.is_empty() {
            return Err(FsError::EmptyEvidence.into());
        }
        // Verify presence of every cited evidence id before the write.
        // A future optimization batches these via `get_many`; today's
        // call counts are tiny (single-digit) so per-id `get` keeps
        // the error message simple.
        for id in evidence {
            self.evidence_must_exist(id).await?;
        }
        let output = Output::new(content.to_string(), evidence.to_vec(), Utc::now());
        let key = self.key(&format!("outputs/{}.json", output.id));
        let bytes = serde_json::to_vec_pretty(&output)?;
        self.storage
            .put(&key, Bytes::from(bytes))
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        Ok(output)
    }

    /// Apply a batch of filesystem ops. The bootstrap only supports
    /// writes and deletes under `notes/`; any path that escapes
    /// `<root>/notes/` is rejected before any write happens.
    pub async fn apply_ops(&self, ops: Vec<FsOp>) -> anyhow::Result<()> {
        // Validate every op first so a partial batch with a bad path in
        // the middle does not leave a half-applied state. The validated
        // key carries the agent prefix already.
        let mut planned: Vec<(String, FsOp)> = Vec::with_capacity(ops.len());
        for op in ops {
            let raw = match &op {
                FsOp::WriteFile { path, .. } | FsOp::DeleteFile { path } => path.as_str(),
            };
            let resolved = self.resolve_notes_key(raw)?;
            planned.push((resolved, op));
        }

        for (key, op) in planned {
            match op {
                FsOp::WriteFile { content, .. } => {
                    self.storage
                        .put(&key, Bytes::from(content.into_bytes()))
                        .await
                        .map_err(|e| FsError::storage(&key, e))?;
                }
                FsOp::DeleteFile { .. } => {
                    // Idempotent at the type level — missing key is fine.
                    self.storage
                        .delete(&key)
                        .await
                        .map_err(|e| FsError::storage(&key, e))?;
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
    /// Stage-2.5.3 implementation: a full LIST + `get_many` of the
    /// trailing window. The O(1) tail-index path lands in JAR2-54.
    pub async fn list_recent_outputs(&self, n: usize) -> anyhow::Result<Vec<Output>> {
        let prefix = self.key("outputs/");
        self.read_recent_json::<Output>(&prefix, n).await
    }

    /// Return the most recent (up to) `n` `EvidenceRecord`s on disk.
    ///
    /// Order is deterministic: filenames under `evidence/` are sha256
    /// hex digests, which carry no temporal meaning, so "recent" here is
    /// purely lexical (ascending filename, last `n`). The bootstrap
    /// `assemble_context` only needs determinism, not true recency, and
    /// promoting evidence to a time-indexed store is out of scope.
    ///
    /// Stage-2.5.3 implementation: see `list_recent_outputs`. JAR2-54
    /// switches both to the tail-index O(1) path.
    pub async fn list_recent_evidence(&self, n: usize) -> anyhow::Result<Vec<EvidenceRecord>> {
        let prefix = self.key("evidence/");
        self.read_recent_json::<EvidenceRecord>(&prefix, n).await
    }

    /// Write `retirement.json` with the supplied reason and the current
    /// UTC timestamp. Overwrites any prior retirement record.
    pub async fn persist_retirement(&self, reason: &str) -> anyhow::Result<()> {
        let record = RetirementRecord {
            reason: reason.to_string(),
            retired_at: Utc::now(),
        };
        let key = self.key("retirement.json");
        let bytes = serde_json::to_vec_pretty(&record)?;
        self.storage
            .put(&key, Bytes::from(bytes))
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        Ok(())
    }

    /// Write a claim under `claims/<slug>.json`. Slug is derived from
    /// `claim.seed` via [`claim_slug`]. Overwrites any existing file at
    /// that slug — status updates flow through the same path.
    pub async fn write_claim(&self, claim: &Claim) -> anyhow::Result<()> {
        let key = self.claim_key(&claim.seed);
        let bytes = serde_json::to_vec_pretty(claim)?;
        self.storage
            .put(&key, Bytes::from(bytes))
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        Ok(())
    }

    /// Read the claim previously written for `seed`. Returns `Ok(None)`
    /// when no record is present so callers can branch cleanly between
    /// "first time minting this seed" and "I/O failed."
    pub async fn read_claim(&self, seed: &str) -> anyhow::Result<Option<Claim>> {
        let key = self.claim_key(seed);
        let got = self
            .storage
            .get(&key)
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        match got {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Return every claim currently on disk in ascending filename
    /// order. The order is deterministic but not chronological — file
    /// names are slugs, not ULIDs. Callers that need recency should
    /// sort by `created_at` themselves.
    pub async fn list_claims(&self) -> anyhow::Result<Vec<Claim>> {
        let prefix = self.key("claims/");
        self.read_recent_json::<Claim>(&prefix, usize::MAX).await
    }

    // ---- key construction ----------------------------------------------

    fn key(&self, tail: &str) -> String {
        if self.prefix.is_empty() {
            tail.to_string()
        } else {
            format!("{}{tail}", self.prefix)
        }
    }

    fn claim_key(&self, seed: &str) -> String {
        self.key(&format!("claims/{}.json", claim_slug(seed)))
    }

    fn evidence_key(&self, id: &EvidenceId) -> String {
        self.key(&format!("evidence/{}.json", id))
    }

    /// Lex-sort every `.json` key under `prefix`, take the last `n`,
    /// fetch and deserialize.
    ///
    /// Implementation mirrors the pre-2.5.3 `read_recent_json` but goes
    /// through the storage trait: one `list` to enumerate keys, then a
    /// single `get_many` for the trailing window so a remote backend
    /// pays one round-trip instead of N. A missing prefix yields an
    /// empty list (the backend returns an empty `ListPage`), preserving
    /// the "safe to call right after `open`" property.
    ///
    /// Scaling note: still O(M) under the prefix because we ask for
    /// every key and slice in memory. The O(1) tail-index path lands
    /// in JAR2-54.
    async fn read_recent_json<T>(&self, prefix: &str, n: usize) -> anyhow::Result<Vec<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        // `usize::MAX` as `limit` asks the backend for everything; both
        // `MemoryStorage` and `LocalStorage` honor that without
        // pagination. A future scale fix replaces this with a bounded
        // page + heap-merge, but the tail-index work (JAR2-54)
        // sidesteps it entirely for the common case.
        let page = self
            .storage
            .list(prefix, None, usize::MAX)
            .await
            .map_err(|e| FsError::storage(prefix, e))?;
        // Keep only `.json` keys — guards against any sidecar artifacts
        // a backend might surface (e.g. JAR2-54's `_tail.json` is also
        // `.json` so will be included — that's filtered separately in
        // the tail-aware code path, not here).
        let mut keys: Vec<String> = page
            .keys
            .into_iter()
            .filter(|k| k.ends_with(".json"))
            .collect();
        keys.sort();
        let start = keys.len().saturating_sub(n);
        let window: Vec<String> = keys.into_iter().skip(start).collect();

        if window.is_empty() {
            return Ok(Vec::new());
        }
        let refs: Vec<&str> = window.iter().map(String::as_str).collect();
        let blobs = self
            .storage
            .get_many(&refs)
            .await
            .map_err(|e| FsError::storage(prefix, e))?;
        let mut out = Vec::with_capacity(blobs.len());
        for (key, blob) in window.iter().zip(blobs.into_iter()) {
            let bytes = match blob {
                Some(b) => b,
                // Key disappeared between list and get_many — treat as
                // absent rather than error. (Single-writer-per-agent
                // makes this almost impossible in practice; pinning the
                // behavior keeps races between concurrent test threads
                // tame.)
                None => {
                    tracing::debug!(key = key.as_str(), "key absent between list and get_many");
                    continue;
                }
            };
            let value: T = serde_json::from_slice(&bytes)?;
            out.push(value);
        }
        Ok(out)
    }

    /// Resolve `raw` to a storage key under `<prefix>notes/`. Paths must
    /// be relative, start with a `notes/` segment, contain only normal
    /// path components (no `..`, no root, no Windows prefix), and name
    /// a file inside `notes/` (not `notes/` itself).
    ///
    /// We deliberately walk `Components` instead of using
    /// `Path::canonicalize`: target files may not exist yet (write
    /// targets), and canonicalize would error on those. The bootstrap
    /// scope (single-process owner, no symlink farm) makes this
    /// component check sufficient. Symlinks pointing out of `notes/` are
    /// a known follow-up.
    fn resolve_notes_key(&self, raw: &str) -> anyhow::Result<String> {
        let candidate = Path::new(raw);

        // First pass: only Normal components and `.` are allowed.
        let mut cleaned = PathBuf::new();
        for comp in candidate.components() {
            match comp {
                Component::Normal(part) => cleaned.push(part),
                Component::CurDir => {} // "." — harmless
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(FsError::PathTraversal(raw.to_string()).into());
                }
            }
        }
        if cleaned.as_os_str().is_empty() {
            return Err(FsError::PathTraversal(raw.to_string()).into());
        }

        // Second pass: must be rooted at `notes/`.
        let tail = match cleaned.strip_prefix("notes") {
            Ok(rest) if !rest.as_os_str().is_empty() => rest.to_path_buf(),
            Ok(_) => return Err(FsError::PathOutsideNotes(raw.to_string()).into()),
            Err(_) => return Err(FsError::PathOutsideNotes(raw.to_string()).into()),
        };

        // Re-emit the tail as a `/`-separated key under
        // `<prefix>notes/`. Components are guaranteed `Normal` by the
        // first pass so `to_str` only fails on non-UTF-8, which we
        // surface as a traversal error (no other reasonable mapping).
        let mut parts = Vec::new();
        for comp in tail.components() {
            match comp {
                Component::Normal(part) => match part.to_str() {
                    Some(s) => parts.push(s.to_string()),
                    None => return Err(FsError::PathTraversal(raw.to_string()).into()),
                },
                _ => return Err(FsError::PathTraversal(raw.to_string()).into()),
            }
        }
        let joined = parts.join("/");
        Ok(self.key(&format!("notes/{joined}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decision::FsOp;
    use crate::evidence::EvidenceRecord;
    use crate::mandate::Mandate;
    use crate::storage::MemoryStorage;
    use chrono::Utc;
    use serde_json::json;
    use std::time::Duration;
    use tempfile::TempDir;

    async fn fresh_fs() -> (TempDir, AgentFs, Mandate) {
        let tmp = TempDir::new().unwrap();
        let mandate = Mandate::new("research foo", Duration::from_millis(1000), Some(10));
        let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
            .await
            .unwrap();
        (tmp, fs, mandate)
    }

    fn record(tool: &str, args: serde_json::Value, result: serde_json::Value) -> EvidenceRecord {
        EvidenceRecord::new(tool, args, result, Utc::now())
    }

    #[tokio::test]
    async fn open_creates_layout_and_writes_mandate() {
        let (tmp, _fs, mandate) = fresh_fs().await;
        let root = tmp.path();
        // mandate.json present.
        assert!(root.join("mandate.json").is_file());
        // Subdirectories are created lazily by `LocalStorage` on first
        // write; they aren't materialised by `open` alone after the
        // 2.5.3 facade swap. Verify the mandate write succeeded and
        // round-trips instead.
        let bytes = std::fs::read(root.join("mandate.json")).unwrap();
        let back: Mandate = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, mandate);
    }

    #[tokio::test]
    async fn open_is_idempotent_and_does_not_clobber_mandate() {
        let tmp = TempDir::new().unwrap();
        let original = Mandate::new("first", Duration::from_millis(500), None);
        let _fs = AgentFs::open(tmp.path().to_path_buf(), &original)
            .await
            .unwrap();

        // Re-open with a *different* mandate; the on-disk file must keep
        // the original.
        let other = Mandate::new("second", Duration::from_millis(999), Some(7));
        let _fs2 = AgentFs::open(tmp.path().to_path_buf(), &other)
            .await
            .unwrap();

        let bytes = std::fs::read(tmp.path().join("mandate.json")).unwrap();
        let back: Mandate = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, original);
    }

    #[tokio::test]
    async fn record_evidence_is_content_addressed_and_dedup_safe() {
        let (tmp, fs, _m) = fresh_fs().await;
        let rec = record("echo", json!({"msg": "hi"}), json!({"echoed": "hi"}));
        let id = fs.record_evidence(rec.clone()).await.unwrap();

        // Filename matches the id.
        let path = tmp.path().join("evidence").join(format!("{}.json", id));
        assert!(path.is_file());

        // Second write of an identical record is a no-op — same id, no
        // duplicate, and the directory still has exactly one entry.
        let id2 = fs.record_evidence(rec.clone()).await.unwrap();
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
        let other_id = fs.record_evidence(other).await.unwrap();
        assert_ne!(id, other_id);
        let entries: Vec<_> = std::fs::read_dir(tmp.path().join("evidence"))
            .unwrap()
            .collect();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn persist_output_rejects_empty_evidence() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let err = fs.persist_output("hello", &[]).await.unwrap_err();
        let downcast = err.downcast_ref::<FsError>().expect("typed FsError");
        assert!(matches!(downcast, FsError::EmptyEvidence));
    }

    #[tokio::test]
    async fn persist_output_rejects_unknown_evidence_id() {
        let (tmp, fs, _m) = fresh_fs().await;
        let bogus = EvidenceId::from_hex("deadbeef".repeat(8)); // 64 hex chars
        let err = fs
            .persist_output("hello", &[bogus.clone()])
            .await
            .unwrap_err();
        let downcast = err.downcast_ref::<FsError>().expect("typed FsError");
        match downcast {
            FsError::EvidenceNotFound(missing) => assert_eq!(missing, &bogus),
            other => panic!("expected EvidenceNotFound, got {other:?}"),
        }
        // No output file should have been written — the outputs dir
        // also won't have been created since the write never happened.
        let outputs_dir = tmp.path().join("outputs");
        if outputs_dir.exists() {
            let entries: Vec<_> = std::fs::read_dir(outputs_dir).unwrap().collect();
            assert!(entries.is_empty());
        }
    }

    #[tokio::test]
    async fn persist_output_writes_file_referencing_evidence() {
        let (tmp, fs, _m) = fresh_fs().await;
        let rec = record("echo", json!({"msg": "hi"}), json!({"echoed": "hi"}));
        let id = fs.record_evidence(rec).await.unwrap();

        let out = fs.persist_output("hello", &[id.clone()]).await.unwrap();
        assert_eq!(out.content, "hello");
        assert_eq!(out.evidence, vec![id.clone()]);

        let path = tmp.path().join("outputs").join(format!("{}.json", out.id));
        assert!(path.is_file());
        let bytes = std::fs::read(&path).unwrap();
        let back: Output = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, out);
        assert!(back.evidence.contains(&id));
    }

    #[tokio::test]
    async fn apply_ops_writes_under_notes() {
        let (tmp, fs, _m) = fresh_fs().await;
        fs.apply_ops(vec![FsOp::WriteFile {
            path: "notes/a.md".into(),
            content: "hi".into(),
        }])
        .await
        .unwrap();
        let written = tmp.path().join("notes").join("a.md");
        assert_eq!(std::fs::read_to_string(&written).unwrap(), "hi");

        // Nested subdirectory under notes/ is created on demand.
        fs.apply_ops(vec![FsOp::WriteFile {
            path: "notes/sub/c.md".into(),
            content: "deep".into(),
        }])
        .await
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(tmp.path().join("notes").join("sub").join("c.md")).unwrap(),
            "deep"
        );

        // DeleteFile removes the file and is idempotent on missing paths.
        fs.apply_ops(vec![FsOp::DeleteFile {
            path: "notes/a.md".into(),
        }])
        .await
        .unwrap();
        assert!(!written.exists());
        fs.apply_ops(vec![FsOp::DeleteFile {
            path: "notes/never-existed.md".into(),
        }])
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn apply_ops_rejects_path_traversal() {
        let (tmp, fs, _m) = fresh_fs().await;
        for bad in ["../etc/passwd", "../../escape", "notes/../../escape"] {
            let err = fs
                .apply_ops(vec![FsOp::WriteFile {
                    path: bad.into(),
                    content: "x".into(),
                }])
                .await
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
            .await
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
            .await
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<FsError>().unwrap(),
            FsError::PathOutsideNotes(_)
        ));

        // None of the rejected ops should have produced files anywhere.
        let outputs_dir = tmp.path().join("outputs");
        if outputs_dir.exists() {
            let entries: Vec<_> = std::fs::read_dir(outputs_dir).unwrap().collect();
            assert!(entries.is_empty());
        }
    }

    #[tokio::test]
    async fn apply_ops_is_atomic_against_a_bad_path_in_the_middle() {
        let (tmp, fs, _m) = fresh_fs().await;
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
            .await
            .unwrap_err();
        assert!(err.downcast_ref::<FsError>().is_some());
        // Pre-flight validation rejects the batch before any write.
        assert!(!tmp.path().join("notes").join("good.md").exists());
    }

    #[tokio::test]
    async fn list_recent_outputs_returns_window_in_filename_order() {
        let (_tmp, fs, _m) = fresh_fs().await;
        // Seed an evidence record we can attach to every output.
        let id = fs
            .record_evidence(record("echo", json!({"k": 1}), json!({"v": 1})))
            .await
            .unwrap();

        let mut all_ids = Vec::new();
        for i in 0..10 {
            let out = fs
                .persist_output(&format!("o-{i}"), &[id.clone()])
                .await
                .unwrap();
            all_ids.push(out.id);
        }

        // Last 8.
        let recent = fs.list_recent_outputs(8).await.unwrap();
        assert_eq!(recent.len(), 8);

        // Returned outputs are in ascending filename (= ULID) order.
        let ids: Vec<_> = recent.iter().map(|o| o.id).collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted);

        // n larger than available → all entries returned.
        let all = fs.list_recent_outputs(100).await.unwrap();
        assert_eq!(all.len(), 10);

        // n = 0 → empty.
        let none = fs.list_recent_outputs(0).await.unwrap();
        assert!(none.is_empty());
    }

    #[tokio::test]
    async fn list_recent_evidence_returns_window_in_filename_order() {
        let (_tmp, fs, _m) = fresh_fs().await;
        for i in 0..10 {
            fs.record_evidence(record("echo", json!({ "i": i }), json!({ "i": i })))
                .await
                .unwrap();
        }
        let recent = fs.list_recent_evidence(8).await.unwrap();
        assert_eq!(recent.len(), 8);

        let ids: Vec<_> = recent.iter().map(|r| r.id.as_str().to_string()).collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted, "evidence not returned in filename order");
    }

    #[tokio::test]
    async fn persist_retirement_writes_file() {
        let (tmp, fs, _m) = fresh_fs().await;
        fs.persist_retirement("done").await.unwrap();
        let path = tmp.path().join("retirement.json");
        assert!(path.is_file());
        let v: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(v.get("reason").and_then(|x| x.as_str()), Some("done"));
        assert!(v.get("retired_at").and_then(|x| x.as_str()).is_some());
    }

    // ---- JAR2-28: claim_seed persistence -------------------------------

    use crate::decision::{
        ClaimSeed, ContextBundle, Decide, Decision, ToolCall as DecisionToolCall,
    };

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-05-06T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn claim_slug_is_kebab_lowercase_and_carries_hash_suffix() {
        let s = claim_slug("Phase 2 Clearance");
        // body is kebab-case lowercased
        assert!(s.starts_with("phase-2-clearance-"));
        // suffix is exactly 8 lowercase hex chars
        let suffix = s.rsplit('-').next().unwrap();
        assert_eq!(suffix.len(), 8);
        assert!(suffix.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn claim_slug_is_deterministic_for_same_seed() {
        assert_eq!(claim_slug("seed-x"), claim_slug("seed-x"));
        assert_eq!(claim_slug("Phase 2"), claim_slug("Phase 2"));
    }

    #[test]
    fn claim_slug_differs_for_seeds_that_kebab_to_the_same_body() {
        // Both kebab to "abc"; the hash suffix must keep them distinct.
        let a = claim_slug("abc");
        let b = claim_slug("ABC");
        assert_ne!(a, b);
        assert!(a.starts_with("abc-"));
        assert!(b.starts_with("abc-"));
    }

    #[test]
    fn claim_slug_handles_empty_body() {
        let s = claim_slug("!!!");
        assert_eq!(s.len(), 8);
        assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn claim_slug_truncates_long_bodies() {
        let long = "a".repeat(200);
        let s = claim_slug(&long);
        // body 80 chars + '-' + 8 hex chars
        assert_eq!(s.len(), SLUG_BODY_MAX + 1 + 8);
    }

    #[tokio::test]
    async fn write_claim_round_trip_via_read_claim() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let claim = Claim {
            seed: "phase-2-clearance".into(),
            description: "Did drug X pass phase 2?".into(),
            status: ClaimStatus::Open,
            created_at: now(),
        };
        fs.write_claim(&claim).await.unwrap();

        let back = fs.read_claim("phase-2-clearance").await.unwrap();
        assert_eq!(back, Some(claim));
    }

    #[tokio::test]
    async fn read_claim_returns_none_for_missing_seed() {
        let (_tmp, fs, _m) = fresh_fs().await;
        assert_eq!(fs.read_claim("never-written").await.unwrap(), None);
    }

    #[tokio::test]
    async fn write_claim_overwrites_for_status_updates() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let mut claim = Claim {
            seed: "drug-x-p2".into(),
            description: "?".into(),
            status: ClaimStatus::Open,
            created_at: now(),
        };
        fs.write_claim(&claim).await.unwrap();
        claim.status = ClaimStatus::Resolved;
        fs.write_claim(&claim).await.unwrap();

        let back = fs.read_claim("drug-x-p2").await.unwrap().unwrap();
        assert_eq!(back.status, ClaimStatus::Resolved);
    }

    #[tokio::test]
    async fn list_claims_returns_all_in_filename_order() {
        let (_tmp, fs, _m) = fresh_fs().await;
        for s in ["alpha", "bravo", "charlie"] {
            fs.write_claim(&Claim {
                seed: s.into(),
                description: s.into(),
                status: ClaimStatus::Open,
                created_at: now(),
            })
            .await
            .unwrap();
        }
        let listed = fs.list_claims().await.unwrap();
        assert_eq!(listed.len(), 3);
        // Filename order is stable: same call twice yields same order.
        let again = fs.list_claims().await.unwrap();
        assert_eq!(listed, again);
    }

    /// Mock `Decide` impl that consults `claims/` before issuing
    /// `Decision::CallTool`. Reuses an existing seed if it finds a
    /// matching `description`; otherwise mints a new seed (and writes
    /// the claim file) before emitting the decision.
    ///
    /// The "matching description" lookup stands in for the real
    /// LLM-side recognition step ("is this conceptually the same
    /// claim I already opened?"). The point of the test is the
    /// seed-reuse path, not how the agent recognizes the match.
    struct ClaimAwareMock {
        fs: AgentFs,
        topic: String,
        new_seed: String,
    }

    #[async_trait::async_trait]
    impl Decide for ClaimAwareMock {
        async fn decide(&self, _ctx: ContextBundle) -> anyhow::Result<Decision> {
            // Reuse a seed if a claim already exists for this topic.
            let claims = self.fs.list_claims().await?;
            let existing = claims.into_iter().find(|c| c.description == self.topic);
            let seed = match existing {
                Some(c) => c.seed,
                None => {
                    let seed = self.new_seed.clone();
                    self.fs
                        .write_claim(&Claim {
                            seed: seed.clone(),
                            description: self.topic.clone(),
                            status: ClaimStatus::Open,
                            created_at: now(),
                        })
                        .await?;
                    seed
                }
            };
            Ok(Decision::CallTools {
                calls: vec![DecisionToolCall::new(
                    "echo",
                    serde_json::json!({"q": self.topic}),
                    ClaimSeed::new(seed),
                )],
            })
        }
    }

    fn empty_bundle(mandate: Mandate) -> ContextBundle {
        ContextBundle {
            mandate,
            triggers: vec![],
            recent_outputs: vec![],
            recent_evidence: vec![],
            open_claims: vec![],
            correction: None,
        }
    }

    #[tokio::test]
    async fn seed_reuse_round_trip_returns_existing_claim_seed() {
        let (_tmp, fs, mandate) = fresh_fs().await;

        // Tick 0: claim already on disk from a prior tick.
        fs.write_claim(&Claim {
            seed: "phase-2-clearance".into(),
            description: "Did drug X pass phase 2?".into(),
            status: ClaimStatus::Open,
            created_at: now(),
        })
        .await
        .unwrap();

        let mock = ClaimAwareMock {
            fs: fs.clone(),
            topic: "Did drug X pass phase 2?".into(),
            new_seed: "should-not-be-minted".into(),
        };

        let decision = mock.decide(empty_bundle(mandate)).await.unwrap();
        match decision {
            Decision::CallTools { calls } => {
                assert_eq!(calls.len(), 1);
                // Same seed string → same ClaimSeed (== same kernel-side claim id).
                assert_eq!(calls[0].claim_seed, ClaimSeed::new("phase-2-clearance"));
            }
            other => panic!("expected CallTools, got {other:?}"),
        }

        // Mock must not have minted a second claim file.
        let listed = fs.list_claims().await.unwrap();
        assert_eq!(listed.len(), 1);
    }

    #[tokio::test]
    async fn new_seed_creation_path_writes_claim_and_emits_call_tool() {
        let (_tmp, fs, mandate) = fresh_fs().await;
        assert!(fs.list_claims().await.unwrap().is_empty());

        let mock = ClaimAwareMock {
            fs: fs.clone(),
            topic: "Did drug X pass phase 2?".into(),
            new_seed: "phase-2-clearance".into(),
        };

        let decision = mock.decide(empty_bundle(mandate)).await.unwrap();
        match decision {
            Decision::CallTools { calls } => {
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].claim_seed, ClaimSeed::new("phase-2-clearance"));
            }
            other => panic!("expected CallTools, got {other:?}"),
        }

        // The claim file is now on disk and a future tick would find it.
        let back = fs.read_claim("phase-2-clearance").await.unwrap().unwrap();
        assert_eq!(back.description, "Did drug X pass phase 2?");
        assert_eq!(back.status, ClaimStatus::Open);
    }

    #[test]
    fn stable_claim_ids_for_identical_seed_strings() {
        // Sanity: the kernel-side derivation from `ClaimSeed` is
        // identity today (the seed *is* the id), so identical seed
        // strings compare equal. This test pins that invariant so a
        // future change to the derivation has to update it
        // deliberately.
        assert_eq!(ClaimSeed::new("phase-2"), ClaimSeed::new("phase-2"));
        assert_ne!(ClaimSeed::new("phase-2"), ClaimSeed::new("phase-3"));
    }

    // ---- JAR2-53: facade-level adversarial tests -----------------------

    /// Mock `AgentStorage` that returns `Transient` for the next N
    /// `put` calls, then delegates to an inner `MemoryStorage`. Lets
    /// the agent-loop verify that a typed `StorageError::Transient`
    /// surfaces through the `FsError::Storage` wrapper without being
    /// degraded into a generic anyhow error.
    struct FlakyPutStorage {
        inner: MemoryStorage,
        fail_remaining: tokio::sync::Mutex<u32>,
    }

    impl FlakyPutStorage {
        fn new(fail_count: u32) -> Self {
            Self {
                inner: MemoryStorage::new(),
                fail_remaining: tokio::sync::Mutex::new(fail_count),
            }
        }
    }

    #[async_trait::async_trait]
    impl AgentStorage for FlakyPutStorage {
        async fn put(&self, key: &str, value: Bytes) -> crate::storage::StorageResult<()> {
            let mut remaining = self.fail_remaining.lock().await;
            if *remaining > 0 {
                *remaining -= 1;
                return Err(crate::storage::StorageError::Transient(format!(
                    "simulated transient on put({key})"
                )));
            }
            drop(remaining);
            self.inner.put(key, value).await
        }
        async fn put_if_absent(
            &self,
            key: &str,
            value: Bytes,
        ) -> crate::storage::StorageResult<PutOutcome> {
            self.inner.put_if_absent(key, value).await
        }
        async fn get(&self, key: &str) -> crate::storage::StorageResult<Option<Bytes>> {
            self.inner.get(key).await
        }
        async fn get_many(
            &self,
            keys: &[&str],
        ) -> crate::storage::StorageResult<Vec<Option<Bytes>>> {
            self.inner.get_many(keys).await
        }
        async fn delete(&self, key: &str) -> crate::storage::StorageResult<()> {
            self.inner.delete(key).await
        }
        async fn list(
            &self,
            prefix: &str,
            after: Option<&str>,
            limit: usize,
        ) -> crate::storage::StorageResult<crate::storage::ListPage> {
            self.inner.list(prefix, after, limit).await
        }
    }

    #[tokio::test]
    async fn agent_fs_propagates_typed_storage_transient_error_through_fs_error() {
        // Build an AgentFs over MemoryStorage first (so the mandate
        // write succeeds), then swap in a FlakyPutStorage backed by a
        // fresh memory store for the actual operation under test.
        // Constructing the AgentFs directly against FlakyPutStorage(1)
        // would consume the failure budget on the mandate write
        // inside `new_with_storage`.
        let mandate = Mandate::new("flaky", Duration::from_millis(100), Some(1));
        let storage: Arc<dyn AgentStorage> = Arc::new(FlakyPutStorage::new(1));
        // Pre-seed mandate.json so new_with_storage's existence check
        // sees it and skips the put.
        let mandate_bytes = serde_json::to_vec_pretty(&mandate).unwrap();
        storage
            .put("mandate.json", Bytes::from(mandate_bytes))
            .await
            .ok(); // first put consumes the failure
        let fs = AgentFs::new_with_storage(storage, "", &mandate)
            .await
            .unwrap();

        // Seed an evidence record so persist_output's evidence check
        // resolves. `put_if_absent` is delegated straight to inner so
        // this is unaffected by the put-failure counter.
        let rec = record("echo", json!({"k": "v"}), json!({"r": "v"}));
        let id = fs.record_evidence(rec).await.unwrap();

        // The flaky counter was already exhausted on the pre-seed put;
        // verify a normal write succeeds. Then exhaust a new flaky
        // storage on an isolated AgentFs.
        let _ = fs.persist_output("ok", &[id]).await.unwrap();

        // Independent verification: a fresh flaky storage produces an
        // FsError::Storage whose inner StorageError is Transient.
        let flaky: Arc<dyn AgentStorage> = Arc::new(FlakyPutStorage::new(1));
        let err = flaky.put("k", Bytes::from_static(b"v")).await.unwrap_err();
        assert!(
            matches!(err, crate::storage::StorageError::Transient(_)),
            "expected Transient, got {err:?}"
        );

        // And: that error type survives the FsError::Storage wrap that
        // `persist_retirement` (a plain `put`) performs.
        let mandate2 = Mandate::new("flaky2", Duration::from_millis(100), Some(1));
        let storage2: Arc<dyn AgentStorage> = Arc::new(FlakyPutStorage::new(2));
        // First flaky consumes mandate.json write inside new_with_storage.
        let fs2 = match AgentFs::new_with_storage(storage2, "", &mandate2).await {
            Ok(f) => f,
            Err(e) => {
                let typed = e
                    .downcast_ref::<FsError>()
                    .expect("mandate.json write should surface FsError");
                match typed {
                    FsError::Storage { source, .. } => {
                        assert!(matches!(source, crate::storage::StorageError::Transient(_)));
                    }
                    other => panic!("expected FsError::Storage, got {other:?}"),
                }
                return;
            }
        };
        // If construction somehow succeeded, the second put (retirement)
        // must surface the typed error.
        let err = fs2.persist_retirement("bye").await.unwrap_err();
        let typed = err
            .downcast_ref::<FsError>()
            .expect("expected FsError wrapping the storage error");
        match typed {
            FsError::Storage { source, .. } => {
                assert!(matches!(source, crate::storage::StorageError::Transient(_)));
            }
            other => panic!("expected FsError::Storage, got {other:?}"),
        }
    }

    /// `AgentFs::new_with_storage` accepts a `MemoryStorage` backend so
    /// tests that don't want a tempdir can run hermetically. Smoke-check
    /// the round-trip.
    #[tokio::test]
    async fn agent_fs_over_memory_storage_round_trips_basic_operations() {
        let mandate = Mandate::new("mem", Duration::from_millis(100), Some(2));
        let storage: Arc<dyn AgentStorage> = Arc::new(MemoryStorage::new());
        let fs = AgentFs::new_with_storage(storage, "graphs/g1/agents/a1", &mandate)
            .await
            .unwrap();
        // Prefix normalisation: trailing slash auto-appended.
        assert_eq!(fs.prefix(), "graphs/g1/agents/a1/");

        let rec = record("t", json!({}), json!({}));
        let id = fs.record_evidence(rec).await.unwrap();
        let out = fs.persist_output("hello", &[id]).await.unwrap();
        let recent = fs.list_recent_outputs(8).await.unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].id, out.id);
    }
}

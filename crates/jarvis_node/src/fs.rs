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
//! <prefix>outputs/<sha256>.json — produced artifacts; the agent's claims
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
//! `outputs/<sha256>.json` does not change. A reader can quote it; a parent
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
//!    agent and start re-emitting outputs. (With content-addressed
//!    `OutputId`s as of JAR2-70, re-emitting an identical claim would
//!    collapse to the same file rather than duplicate — but a retired
//!    agent shouldn't be running at all, so the marker is still load-
//!    bearing.)
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

use crate::agent_ref::{AgentId, GraphId};
use crate::conflict::ConflictRecord;
use crate::decision::{ConflictId, FsOp};
use crate::evidence::{EvidenceId, EvidenceRecord};
use crate::mandate::{Mandate, Output, OutputId};
use crate::storage::{AgentStorage, LocalStorage, PutOutcome};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

/// Cap on tail-object size (`<prefix>outputs/_tail.json`,
/// `<prefix>evidence/_tail.json`). Per `scratch/agent_storage.md` § 13
/// decision 3: 64 entries gives 8× headroom over the default
/// `recent_outputs` / `recent_evidence` window of 8, keeps the tail
/// object < ~8 KB even with verbose entries, and is configurable
/// per-deployment if a workload needs more (out of scope today).
const TAIL_K: usize = 64;

/// One entry in a `_tail.json` object — the filename relative to the
/// indexed prefix (`outputs/` or `evidence/`) plus the wall-clock
/// timestamp the entry was added. Public so a downstream snapshot /
/// inspection tool can deserialize the tail object directly.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TailEntry {
    /// Bare filename (e.g. `01HX2...json`, not `outputs/01HX2...json`).
    /// Kept relative to the indexed prefix so the tail object survives
    /// a prefix change at relocation time.
    pub filename: String,
    /// When the entry was added to the tail. Distinct from the file's
    /// own `created_at` on disk (which is also serialised inside the
    /// object); on-disk timestamps may differ for replayed activities
    /// — the tail entry records *when this writer recorded the index
    /// update*.
    pub added_at: DateTime<Utc>,
}

/// The on-disk shape of `<prefix>outputs/_tail.json` and
/// `<prefix>evidence/_tail.json`. `entries[0]` is the most recently
/// written file; later entries are progressively older. The vector is
/// truncated to [`TAIL_K`] on every update so the object size stays
/// bounded.
///
/// **Completeness:** when `entries.len() < TAIL_K`, the tail is the
/// authoritative list of *every* file ever written under the indexed
/// prefix (modulo a torn-write that left a file without a tail
/// update). When `entries.len() == TAIL_K`, older files may exist on
/// disk that fell off the tail — readers that need the lex-greatest
/// N across the *whole* history must fall back to the LIST path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TailObject {
    pub entries: Vec<TailEntry>,
}

/// Tail-object key suffix for `outputs/`.
const OUTPUTS_TAIL_SUFFIX: &str = "outputs/_tail.json";
/// Tail-object key suffix for `evidence/`.
const EVIDENCE_TAIL_SUFFIX: &str = "evidence/_tail.json";

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
    /// [`AgentFs::read_output`] could not resolve the requested
    /// `OutputId` on disk. Surfaced as a typed error so the JAR2-82
    /// `reconcile_children` activity can wrap it as
    /// `ReconciliationError::ChildOutputNotFound` and the parent
    /// workflow body's correction-context path can stage a satisfiable
    /// failure description for the next tick (mirroring how
    /// `EvidenceNotFound` propagates through `persist_output`).
    #[error("output {0} not found on disk")]
    OutputNotFound(OutputId),
    /// JAR2-83 (stage 5.6): [`AgentFs::write_conflict`] was called with
    /// fewer than two alternatives. A single-alternative "conflict" is
    /// meaningless — there's nothing to disagree with — so the writer
    /// rejects it as a structural error. The activity boundary maps this
    /// to `ReconciliationError::ConflictAlternativesTooFew` and surfaces
    /// it to the workflow body as a non-retryable
    /// `CorrectionContext`-bearing failure (the LLM produced a bad
    /// `Decision::ReconcileChildren`; re-running the activity won't fix
    /// the shape).
    #[error("conflict rejected: only {count} alternatives (need >= 2)")]
    ConflictAlternativesTooFew { count: usize },
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

        // JAR2-54 tail-index reconciliation. A prior process may have
        // crashed between an `outputs/<sha256>.json` PUT and the
        // corresponding `_tail.json` PUT, leaving the tail lagging.
        // `read_recent_window_with_tail` trusts a tail with
        // `entries.len() < TAIL_K` as "complete" for O(1) reads — that
        // trust would silently miss lag-orphans without this
        // reconcile. Doing it once at open keeps the read path O(1)
        // while honoring the `agent_storage.md` § 7.1 promise that the
        // LIST fallback resolves lag. In-process single-writer-per-
        // agent maintains the invariant for the lifetime of this
        // `AgentFs`; cross-process inspectors (TUI, ad-hoc tools)
        // wanting a fresh view can call `AgentFs::open` again.
        //
        // Cost: one LIST + at most one PUT per indexed prefix at open;
        // subsequent `list_recent_*` calls stay O(1). When the on-disk
        // filename set is already a subset of the tail (the common
        // case) the PUT is skipped entirely.
        me.reconcile_tail(OUTPUTS_TAIL_SUFFIX, "outputs/").await?;
        me.reconcile_tail(EVIDENCE_TAIL_SUFFIX, "evidence/").await?;

        Ok(me)
    }

    /// Build an `AgentFs` over `storage` at `prefix` **without** the
    /// mandate.json read/write or the tail-index reconciliation that
    /// [`AgentFs::new_with_storage`] performs.
    ///
    /// Use when the caller needs the facade purely to write a known key
    /// (e.g. `retirement.json` from the Temporal `persist_retirement`
    /// activity) and either:
    ///
    /// - has no `Mandate` available in scope (the retirement-signal
    ///   short-circuit fires before `assemble_context` runs, so no
    ///   mandate is loaded into workflow state); **or**
    /// - wants to skip the per-attach LIST that drives tail-index
    ///   reconciliation when the operation doesn't touch
    ///   `outputs/` / `evidence/`.
    ///
    /// `attach` is **strictly weaker** than `new_with_storage`: it makes
    /// no I/O calls itself. Callers must not use it for paths that rely
    /// on the tail-index invariants (`list_recent_outputs`,
    /// `list_recent_evidence`) on a fresh per-call FS — those need
    /// `new_with_storage`'s reconcile step. The retirement path writes
    /// one key and exits, so the missing reconciliation is irrelevant.
    pub fn attach(storage: Arc<dyn AgentStorage>, prefix: impl Into<String>) -> Self {
        let mut prefix = prefix.into();
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        Self { storage, prefix }
    }

    /// JAR2-82 (stage 5.5): build an `AgentFs` scoped to an arbitrary
    /// agent's `graphs/<graph_id>/agents/<agent_id>/` prefix on the
    /// supplied storage backend. Cross-agent reads (the parent's
    /// `reconcile_children` activity reading a child's
    /// `outputs/<id>.json`) flow through this constructor.
    ///
    /// **Strictly an `attach` wrapper** — no `mandate.json` read, no
    /// tail-index reconcile. Both load-bearing for the cross-agent
    /// case: the caller doesn't have the *other* agent's `Mandate` in
    /// scope (it lives only on that agent's workflow input), and the
    /// reconcile-target reads are point lookups (`storage.get` against
    /// a known `OutputId`), not list/window reads that would care about
    /// the tail's freshness.
    ///
    /// Mirrors `crate::workflow::FsHandle::for_agent`'s prefix scheme
    /// (Stage 5 Project decision 6 — flat `graphs/<gid>/agents/<aid>`
    /// id form) so a future schema bump touches one call site rather
    /// than every spawn / reconcile / retire helper.
    pub fn open_for_agent(
        storage: Arc<dyn AgentStorage>,
        graph_id: GraphId,
        agent_id: AgentId,
    ) -> Self {
        let prefix = format!("graphs/{}/agents/{}/", graph_id, agent_id);
        Self::attach(storage, prefix)
    }

    /// Reconcile a tail object against the on-disk reality under its
    /// indexed prefix. Called once per `new_with_storage` to recover
    /// from any prior crash that PUT an object without updating the
    /// tail (`scratch/agent_storage.md` § 7.1).
    ///
    /// Algorithm:
    /// 1. LIST every key under the indexed prefix (skip the tail
    ///    object itself and any non-record sidecar files).
    /// 2. If every on-disk filename is in the tail (the common no-lag
    ///    case), do nothing — no PUT, no churn.
    /// 3. Otherwise, recompute the tail as the lex-greatest `TAIL_K`
    ///    on-disk filenames in newest-first order, preserving any
    ///    existing `added_at` timestamps from the prior tail (so a
    ///    re-reconciliation produces byte-identical bytes), and PUT
    ///    it back.
    ///
    /// Filenames-only comparison is correct for both `outputs/` and
    /// `evidence/`: lex-greatest is what `list_recent_*` returns and
    /// the tail's only role is to make that fetch O(1). Mis-ordered
    /// `added_at` between recovered entries is cosmetic — no caller
    /// uses the tail for chronology.
    async fn reconcile_tail(&self, tail_suffix: &str, indexed_prefix: &str) -> anyhow::Result<()> {
        let full_prefix = self.key(indexed_prefix);
        let page = self
            .storage
            .list(&full_prefix, None, usize::MAX)
            .await
            .map_err(|e| FsError::storage(&full_prefix, e))?;
        // Reduce keys to *record filenames* (strip the indexed prefix
        // off, drop sidecars and tempfile artifacts).
        let mut on_disk: Vec<String> = page
            .keys
            .into_iter()
            .filter_map(|k| k.strip_prefix(&full_prefix).map(|s| s.to_string()))
            .filter(|f| f.ends_with(".json"))
            .filter(|f| f != "_tail.json")
            .collect();
        if on_disk.is_empty() {
            return Ok(());
        }
        on_disk.sort();

        let tail_key = self.key(tail_suffix);
        let existing_tail: TailObject = match self
            .storage
            .get(&tail_key)
            .await
            .map_err(|e| FsError::storage(&tail_key, e))?
        {
            Some(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            None => TailObject::default(),
        };

        let tail_filenames: std::collections::HashSet<&str> = existing_tail
            .entries
            .iter()
            .map(|e| e.filename.as_str())
            .collect();

        // Fast path: every on-disk filename is in the tail. Skip the
        // PUT entirely. (The tail may carry stale entries pointing to
        // out-of-band-deleted files — we don't garbage-collect them
        // here; the read path silently drops absent keys.)
        if on_disk.iter().all(|f| tail_filenames.contains(f.as_str())) {
            return Ok(());
        }

        // Preserve original `added_at` for already-known filenames so
        // a no-op subsequent reconcile produces identical bytes.
        let existing_added_at: std::collections::HashMap<&str, DateTime<Utc>> = existing_tail
            .entries
            .iter()
            .map(|e| (e.filename.as_str(), e.added_at))
            .collect();

        // Lex-greatest TAIL_K, in newest-first order (reverse-lex so
        // entry 0 is the "most recent" in the tail's contract).
        let take_from = on_disk.len().saturating_sub(TAIL_K);
        let mut chosen: Vec<&str> = on_disk[take_from..].iter().map(String::as_str).collect();
        chosen.reverse();
        let now = Utc::now();
        let rebuilt = TailObject {
            entries: chosen
                .into_iter()
                .map(|f| TailEntry {
                    filename: f.to_string(),
                    added_at: existing_added_at.get(f).copied().unwrap_or(now),
                })
                .collect(),
        };

        let bytes = serde_json::to_vec(&rebuilt)?;
        self.storage
            .put(&tail_key, Bytes::from(bytes))
            .await
            .map_err(|e| FsError::storage(&tail_key, e))?;
        Ok(())
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
        let outcome: PutOutcome = self
            .storage
            .put_if_absent(&key, Bytes::from(bytes))
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        // Update the tail-index only on first write — a replayed
        // `record_evidence` for an already-seen sha256 would otherwise
        // shuffle the existing entry to the front, polluting recency
        // semantics with retry artefacts. `Existed` means we already
        // counted this evidence id in a prior call.
        if matches!(outcome, PutOutcome::Created) {
            let filename = format!("{}.json", id);
            self.append_to_tail(EVIDENCE_TAIL_SUFFIX, filename).await?;
        }
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

    /// Persist an `Output` under `<prefix>outputs/<sha256>.json`,
    /// enforcing the provenance contract: at least one evidence id,
    /// and every id must resolve to a record.
    ///
    /// **Idempotent under retries (JAR2-70).** `OutputId::new` is
    /// content-addressed over `(content, evidence)`, so two calls with
    /// the same arguments target the same key. We use
    /// [`crate::storage::AgentStorage::put_if_absent`] — a second call
    /// returns `Existed` and skips the tail-index update, so the
    /// `_tail.json` `added_at` for the entry stays at the first-write
    /// timestamp and the `entries` list does not double-count.
    ///
    /// `created_at` is **not** part of the hash, so the bytes written
    /// on the first call (`output.created_at = Utc::now()` at that
    /// moment) are the bytes that stay on disk; a retry's freshly
    /// minted `created_at` is silently discarded by `put_if_absent`.
    /// This matches `record_evidence`'s dedup contract.
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
        let filename = format!("{}.json", output.id);
        let key = self.key(&format!("outputs/{filename}"));
        let bytes = serde_json::to_vec_pretty(&output)?;
        let outcome: PutOutcome = self
            .storage
            .put_if_absent(&key, Bytes::from(bytes))
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        // Update the outputs tail-index only on first write — a
        // replayed `persist_output` for an already-seen id (a retry,
        // or a second tick emitting the same `(content, evidence)`)
        // would otherwise shuffle the entry to the front with a fresh
        // `added_at`, polluting recency semantics with retry
        // artefacts. Order matters: the file is PUT first, *then* the
        // tail. If a crash happens between the two PUTs, the file is
        // recoverable via the LIST-fallback in
        // `read_recent_window_with_tail` — see § 7.1 of
        // `scratch/agent_storage.md` for the recovery argument.
        if matches!(outcome, PutOutcome::Created) {
            self.append_to_tail(OUTPUTS_TAIL_SUFFIX, filename).await?;
        }
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
    /// Order is ascending filename. As of JAR2-70 output filenames are
    /// sha256 digests (content-addressed over `(content, evidence)`),
    /// which carry no temporal meaning — so the lex-greatest `n` across
    /// the whole history is *not* the same set as the `n` most recently
    /// written. The tail-fast-path therefore only kicks in when the
    /// tail object is provably complete (i.e. the tail holds fewer
    /// than `TAIL_K` entries, which means every output file ever
    /// written under this agent is in the tail). For agents that
    /// accumulate more than `TAIL_K` outputs the LIST fallback gives
    /// the existing lex-window semantics.
    ///
    /// This matches the evidence path's behavior exactly — see
    /// [`AgentFs::list_recent_evidence`] for the rationale. Pre-JAR2-70
    /// outputs were ULID-named and the fast path stayed safe at
    /// `TAIL_K` capacity; content-addressing trades that asymmetry for
    /// `persist_output` idempotency under retries.
    ///
    /// Crash-recovery: if a previous `persist_output` PUT the file but
    /// crashed before the tail update, the file isn't in the tail;
    /// `read_recent_window_with_tail` detects the lag and falls back
    /// to LIST automatically. See the `tail_lag_recovery_*` tests.
    pub async fn list_recent_outputs(&self, n: usize) -> anyhow::Result<Vec<Output>> {
        let prefix = self.key("outputs/");
        self.read_recent_window_with_tail::<Output>(&prefix, OUTPUTS_TAIL_SUFFIX, n, false)
            .await
    }

    /// JAR2-82 (stage 5.5): point lookup of one persisted [`Output`] by
    /// its content-addressed [`OutputId`].
    ///
    /// Returns `Err(FsError::OutputNotFound(id))` when the file is
    /// absent — the typed-error variant lets the
    /// `reconcile_children` activity wrap the miss as
    /// `ReconciliationError::ChildOutputNotFound` (which the parent
    /// workflow body folds into a `CorrectionContext` for the next
    /// tick) without losing the original id.
    ///
    /// Read-only by construction: a `storage.get` against
    /// `<prefix>outputs/<id>.json` plus a serde decode. No tail-index
    /// update, no mandate write. Composes safely with
    /// [`AgentFs::open_for_agent`] for the cross-agent reconcile path
    /// where the caller never opens the *other* agent's FS via
    /// `new_with_storage`.
    pub async fn read_output(&self, id: &OutputId) -> anyhow::Result<Output> {
        let key = self.key(&format!("outputs/{}.json", id));
        let got = self
            .storage
            .get(&key)
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        match got {
            Some(bytes) => Ok(serde_json::from_slice(&bytes)?),
            None => Err(FsError::OutputNotFound(id.clone()).into()),
        }
    }

    /// Return the most recent (up to) `n` `EvidenceRecord`s on disk.
    ///
    /// Order is ascending filename. Evidence filenames are sha256 hex
    /// digests, which carry no temporal meaning — so the lex-greatest
    /// `n` across the whole history is *not* the same set as the `n`
    /// most recently written. The tail-fast-path therefore only kicks
    /// in when the tail object is provably complete (i.e. the tail
    /// holds fewer than `TAIL_K` entries, which means every evidence
    /// file ever written under this agent is in the tail). For agents
    /// that accumulate more than `TAIL_K` evidence records the LIST
    /// fallback gives the existing lex-window semantics.
    ///
    /// This asymmetry vs. outputs is a deliberate design choice (see
    /// `scratch/agent_storage.md` § 7.1 — the tail object is a single-
    /// shape primitive across both; the *interpretation* differs by
    /// content-addressing scheme of the indexed prefix). The common
    /// case in deployed agents — well under `TAIL_K` evidence records
    /// alive at once — stays O(1).
    pub async fn list_recent_evidence(&self, n: usize) -> anyhow::Result<Vec<EvidenceRecord>> {
        let prefix = self.key("evidence/");
        self.read_recent_window_with_tail::<EvidenceRecord>(&prefix, EVIDENCE_TAIL_SUFFIX, n, false)
            .await
    }

    /// Write `retirement.json` with the supplied reason and `retired_at`
    /// timestamp. Overwrites any prior retirement record.
    ///
    /// **`retired_at` is supplied by the caller**, not stamped here. The
    /// in-process agent loop (`agent_core::dispatch`) passes `Utc::now()`;
    /// the Temporal workflow path (`jarvis_temporal::activities::persist_retirement`)
    /// passes a deterministic timestamp sourced from the activity's
    /// scheduled-time so replay produces byte-identical bytes — see
    /// JAR2-66 for the rationale (`scratch/temporal_rust_sdk_smoke.md`
    /// § 2 row 4: workflow-time must be deterministic from history,
    /// `Utc::now()` inside an activity body is fine but on retry would
    /// drift, so we anchor on the SDK's stored timestamp instead).
    pub async fn persist_retirement(
        &self,
        reason: &str,
        retired_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let record = RetirementRecord {
            reason: reason.to_string(),
            retired_at,
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

    /// JAR2-83 (stage 5.6): persist a [`ConflictRecord`] under
    /// `<prefix>conflicts/<id>.json` and return its content-addressed
    /// [`ConflictId`].
    ///
    /// Validates `record.alternatives.len() >= 2` and returns
    /// [`FsError::ConflictAlternativesTooFew`] otherwise — a single-
    /// alternative "conflict" carries no information and is treated as a
    /// malformed `Decision::ReconcileChildren` from the LLM.
    ///
    /// **Idempotent under retries.** `ConflictId` is content-addressed
    /// over `(alternatives, resolution)`, so a retried `reconcile_children`
    /// activity that re-PUTs the same record targets the same key. We
    /// use [`AgentStorage::put_if_absent`] which makes the dedup atomic
    /// — same shape as `record_evidence` / `persist_output`. `timestamp`
    /// is not in the hash; the bytes written on the first call (carrying
    /// the first attempt's `timestamp`) are the bytes that stay on disk,
    /// matching `Output::created_at`'s contract.
    ///
    /// **No tail-index update.** `conflicts/` is bounded — dozens per
    /// agent over its lifetime per Stage 5 Project decision 14 — so the
    /// 2.5.4 tail-index pattern is unjustified overhead here. The
    /// directory-scan path ([`AgentFs::list_conflicts`]) is the only
    /// reader and it's O(M) over a small M.
    pub async fn write_conflict(&self, record: &ConflictRecord) -> anyhow::Result<ConflictId> {
        if record.alternatives.len() < 2 {
            return Err(FsError::ConflictAlternativesTooFew {
                count: record.alternatives.len(),
            }
            .into());
        }
        let id = record.id.clone();
        let key = self.conflict_key(&id);
        let bytes = serde_json::to_vec_pretty(record)?;
        // `put_if_absent` returns Created on first write, Existed on
        // duplicate attempts — both paths are success for the
        // content-addressed dedup contract.
        self.storage
            .put_if_absent(&key, Bytes::from(bytes))
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        Ok(id)
    }

    /// JAR2-83 (stage 5.6): point lookup of one persisted
    /// [`ConflictRecord`] by its content-addressed [`ConflictId`].
    ///
    /// Returns `Ok(None)` when the file is absent — the typed-error
    /// shape (`OutputNotFound`/`EvidenceNotFound`) is reserved for read
    /// paths where the caller needs to distinguish "the id doesn't
    /// resolve" from "I/O failed". Conflict-record reads are audit /
    /// inspection only and a missing id is not a contract violation, so
    /// the simpler `Option`-returning shape mirrors `read_claim`.
    pub async fn read_conflict(&self, id: &ConflictId) -> anyhow::Result<Option<ConflictRecord>> {
        let key = self.conflict_key(id);
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

    /// JAR2-83 (stage 5.6): return every conflict record under
    /// `<prefix>conflicts/` in ascending filename (== ascending hex-id)
    /// order. Bounded by the projected count cited above; the LIST +
    /// `get_many` shape is the same as [`AgentFs::list_claims`].
    pub async fn list_conflicts(&self) -> anyhow::Result<Vec<ConflictRecord>> {
        let prefix = self.key("conflicts/");
        self.read_recent_json::<ConflictRecord>(&prefix, usize::MAX)
            .await
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

    fn conflict_key(&self, id: &ConflictId) -> String {
        self.key(&format!("conflicts/{}.json", id))
    }

    /// Prepend `filename` to the tail object at `<prefix><tail_suffix>`
    /// and truncate to [`TAIL_K`].
    ///
    /// Single-writer-per-agent is the wider engine contract, so we do
    /// a plain GET-modify-PUT here without CAS — the loser of a race
    /// would by definition be a violation of single-writer (the agent
    /// loop drives all writes for a given agent). A failing tail-PUT
    /// after a successful object-PUT leaves the tail lagging; the
    /// reader's `read_recent_window_with_tail` detects this and falls
    /// back to LIST. **No JSONL-append form ships** per decision 7 —
    /// the tail object is the canonical artefact across backends.
    async fn append_to_tail(&self, tail_suffix: &str, filename: String) -> anyhow::Result<()> {
        let key = self.key(tail_suffix);
        let existing = self
            .storage
            .get(&key)
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        let mut tail: TailObject = match existing {
            Some(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            None => TailObject::default(),
        };
        // Defensive dedup: if the same filename is already in the tail
        // (rare — would mean a non-content-addressed re-write that
        // somehow reached this path), drop the old entry first so the
        // newest position is the only one we keep. Avoids the tail
        // double-counting under odd retry patterns.
        tail.entries.retain(|e| e.filename != filename);
        tail.entries.insert(
            0,
            TailEntry {
                filename,
                added_at: Utc::now(),
            },
        );
        if tail.entries.len() > TAIL_K {
            tail.entries.truncate(TAIL_K);
        }
        let bytes = serde_json::to_vec(&tail)?;
        self.storage
            .put(&key, Bytes::from(bytes))
            .await
            .map_err(|e| FsError::storage(&key, e))?;
        Ok(())
    }

    /// Tail-fast-path read for `outputs/` or `evidence/` recent windows.
    ///
    /// `prefix` is the indexed prefix (e.g. `outputs/` or
    /// `<agent_prefix>outputs/`). `tail_suffix` is the relative tail-
    /// object key under that prefix's parent agent prefix (e.g.
    /// `outputs/_tail.json`). `n` is the requested window. `lex_monotonic`
    /// indicates whether "most recently written" is the same set as
    /// "lex-greatest" for this indexed prefix — `false` for both
    /// outputs and evidence post-JAR2-70 (both content-addressed),
    /// kept as a parameter for backends or future prefixes that may
    /// reintroduce a monotonic naming scheme.
    ///
    /// Decision matrix:
    ///
    /// - `n == 0` → empty result, no I/O.
    /// - Tail object missing → fall back to LIST.
    /// - Tail length `< TAIL_K` → tail is complete; use it (no LIST
    ///   needed regardless of `lex_monotonic`).
    /// - Tail length `== TAIL_K` AND `lex_monotonic` AND `n <= TAIL_K`
    ///   → use tail (outputs case; older entries that fell off the
    ///   tail are lex-smaller than every tail entry).
    /// - Otherwise → fall back to LIST.
    ///
    /// Falling back to LIST and the entire LIST + sort + get_many is
    /// the pre-2.5.4 behavior, preserved as the slow path so the
    /// answer stays correct even under the asymmetry above. The fast
    /// path keeps `list_recent_outputs` O(1) regardless of total
    /// output count (asserted by the `tail_index_outputs_is_o1_at_10k`
    /// `#[ignore]`d microbench).
    async fn read_recent_window_with_tail<T>(
        &self,
        prefix: &str,
        tail_suffix: &str,
        n: usize,
        lex_monotonic: bool,
    ) -> anyhow::Result<Vec<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        if n == 0 {
            return Ok(Vec::new());
        }
        let tail_key = self.key(tail_suffix);
        let tail_bytes = self
            .storage
            .get(&tail_key)
            .await
            .map_err(|e| FsError::storage(&tail_key, e))?;
        if let Some(bytes) = tail_bytes {
            // A serde error on the tail is treated as "fall back to
            // LIST" rather than fail loudly: a torn write or a
            // forward-incompatible schema shouldn't break recent-N
            // assembly, only slow it down.
            let parsed: Result<TailObject, _> = serde_json::from_slice(&bytes);
            if let Ok(tail) = parsed {
                let tail_complete = tail.entries.len() < TAIL_K;
                let fast_path_safe = tail_complete || (lex_monotonic && n <= TAIL_K);
                if fast_path_safe {
                    return self.read_keys_for_tail::<T>(prefix, &tail, n).await;
                }
            }
        }
        // LIST fallback — the pre-2.5.4 path. Covers:
        //   * Tail missing (fresh FS, or never-written prefix).
        //   * Tail at capacity with lex-non-monotonic filenames (the
        //     evidence > TAIL_K case).
        //   * `n > TAIL_K` with non-monotonic filenames.
        //   * Recovery: a write whose tail update failed mid-flight
        //     is still on disk; LIST picks it up. The single-writer
        //     contract means we won't be racing the agent-loop here.
        self.read_recent_json::<T>(prefix, n).await
    }

    /// Materialise the trailing-`n` slice of `tail` (which is reverse-
    /// chronological — newest entry at index 0) and return values in
    /// ascending filename order. The ordering matches the pre-2.5.4
    /// `read_recent_json` contract: callers (and tests) compare
    /// against the lex-sorted last-n. Missing files in the window
    /// (e.g. a tail entry whose object was deleted out-of-band) are
    /// silently dropped — same forgiveness as the LIST path.
    async fn read_keys_for_tail<T>(
        &self,
        prefix: &str,
        tail: &TailObject,
        n: usize,
    ) -> anyhow::Result<Vec<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        // Tail is reverse-chronological; the first `n` entries are
        // "the n most recent". Re-sort by filename ascending so the
        // returned vector matches the lex-sort semantics tests pin.
        let take_n = tail.entries.len().min(n);
        let mut filenames: Vec<String> = tail.entries[..take_n]
            .iter()
            .map(|e| e.filename.clone())
            .collect();
        filenames.sort();
        if filenames.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<String> = filenames.iter().map(|f| format!("{prefix}{f}")).collect();
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let blobs = self
            .storage
            .get_many(&refs)
            .await
            .map_err(|e| FsError::storage(prefix, e))?;
        let mut out = Vec::with_capacity(blobs.len());
        for (key, blob) in keys.iter().zip(blobs.into_iter()) {
            let bytes = match blob {
                Some(b) => b,
                None => {
                    tracing::debug!(
                        key = key.as_str(),
                        "tail entry resolves to absent key; skipping"
                    );
                    continue;
                }
            };
            out.push(serde_json::from_slice::<T>(&bytes)?);
        }
        Ok(out)
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
        // Keep only `.json` keys — guards against any sidecar
        // artifacts a backend might surface. Also exclude the
        // tail-index file (`_tail.json`, JAR2-54) so the LIST
        // fallback doesn't try to deserialise a `TailObject` as a
        // `Output`/`EvidenceRecord`. The fallback path runs from
        // `read_recent_window_with_tail`, which already routes around
        // the tail object when it can; this filter handles the case
        // where we ended up here because the tail was unparseable or
        // capacity-exceeded.
        let mut keys: Vec<String> = page
            .keys
            .into_iter()
            .filter(|k| k.ends_with(".json"))
            .filter(|k| !k.ends_with("/_tail.json"))
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

        // Count evidence record files only — `_tail.json` (JAR2-54)
        // is a separate index artefact under `evidence/` and not
        // itself an evidence record, so it doesn't violate dedup.
        let evidence_files: Vec<_> = std::fs::read_dir(tmp.path().join("evidence"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|n| n != "_tail.json")
            .collect();
        assert_eq!(
            evidence_files.len(),
            1,
            "duplicate evidence write created extra file: {evidence_files:?}"
        );

        // A different record produces a different id and a second file.
        let other = record("echo", json!({"msg": "bye"}), json!({"echoed": "bye"}));
        let other_id = fs.record_evidence(other).await.unwrap();
        assert_ne!(id, other_id);
        let evidence_files: Vec<_> = std::fs::read_dir(tmp.path().join("evidence"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|n| n != "_tail.json")
            .collect();
        assert_eq!(evidence_files.len(), 2);
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

    // ---- JAR2-82 (stage 5.5): read_output + open_for_agent ----

    #[tokio::test]
    async fn read_output_returns_persisted_output_by_id() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let rec = record("echo", json!({"q": "k"}), json!({"r": "v"}));
        let ev = fs.record_evidence(rec).await.unwrap();
        let out = fs.persist_output("the claim", &[ev.clone()]).await.unwrap();

        let back = fs.read_output(&out.id).await.unwrap();
        assert_eq!(back, out, "read_output must return the persisted Output");
        assert_eq!(back.evidence, vec![ev]);
    }

    #[tokio::test]
    async fn read_output_returns_typed_error_for_missing_id() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let ev = EvidenceId::from_hex("0".repeat(64));
        // Manufacture an OutputId for content we never persisted.
        let bogus = OutputId::new("never-written", &[ev]);
        let err = fs
            .read_output(&bogus)
            .await
            .expect_err("missing output must error");
        let typed = err.downcast_ref::<FsError>().expect("typed FsError");
        match typed {
            FsError::OutputNotFound(id) => assert_eq!(id, &bogus),
            other => panic!("expected OutputNotFound, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn open_for_agent_scopes_storage_to_workflow_id_prefix() {
        use crate::agent_ref::{AgentId, GraphId};
        use crate::storage::MemoryStorage;
        use uuid::Uuid;

        let storage: Arc<dyn AgentStorage> = Arc::new(MemoryStorage::new());
        let graph_id =
            GraphId::new(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap());
        let agent_id =
            AgentId::new(Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap());
        let fs = AgentFs::open_for_agent(storage.clone(), graph_id, agent_id);
        assert_eq!(
            fs.prefix(),
            "graphs/11111111-2222-3333-4444-555555555555/agents/66666666-7777-8888-9999-aaaaaaaaaaaa/",
            "prefix must match the flat workflow-id scheme",
        );

        // Sanity: writing evidence under this prefix and reading it
        // back works (cross-agent reads in 5.5 use exactly this shape).
        let rec = record("echo", json!({"x": 1}), json!({"y": 2}));
        let id = fs.record_evidence(rec.clone()).await.unwrap();
        let key = format!(
            "graphs/{}/agents/{}/evidence/{}.json",
            graph_id, agent_id, id,
        );
        assert!(
            storage.get(&key).await.unwrap().is_some(),
            "evidence must land at the prefixed key",
        );
    }

    #[tokio::test]
    async fn open_for_agent_supports_cross_agent_output_read() {
        // Models the JAR2-82 reconcile path: a parent's FS scoped to
        // its own prefix opens a child's FS over the *same* storage
        // backend (different prefix) and reads the child's output.
        use crate::agent_ref::{AgentId, GraphId};
        use crate::storage::MemoryStorage;
        use uuid::Uuid;

        let storage: Arc<dyn AgentStorage> = Arc::new(MemoryStorage::new());
        let graph_id = GraphId::new(Uuid::new_v4());
        let child_agent_id = AgentId::new(Uuid::new_v4());

        // Child writes an output via its own FS (mandate present).
        let child_mandate = Mandate::new("child", Duration::from_millis(100), None);
        let child_prefix = format!("graphs/{}/agents/{}/", graph_id, child_agent_id);
        let child_fs = AgentFs::new_with_storage(storage.clone(), &child_prefix, &child_mandate)
            .await
            .unwrap();
        let ev = child_fs
            .record_evidence(record("echo", json!({"q": "child"}), json!({"r": 1})))
            .await
            .unwrap();
        let child_out = child_fs
            .persist_output("child's claim", &[ev])
            .await
            .unwrap();

        // Parent uses `open_for_agent` (no mandate) to read the
        // child's output by id — exactly the surface the 5.5 activity
        // exercises.
        let parent_view = AgentFs::open_for_agent(storage, graph_id, child_agent_id);
        let read_back = parent_view.read_output(&child_out.id).await.unwrap();
        assert_eq!(read_back, child_out);
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

        // Returned outputs are in ascending filename (= sha256) order.
        // Post-JAR2-70 the filename has no temporal meaning, so this is
        // a lex-sort assertion only — recency semantics live on the
        // tail object's `added_at`, not the filename.
        let ids: Vec<_> = recent.iter().map(|o| o.id.clone()).collect();
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

    /// JAR2-70 — `persist_output` is idempotent over `(content, evidence)`:
    /// two calls with the same arguments produce one file and one
    /// tail-index entry, with the tail's `added_at` pinned at the first
    /// write (no shuffle on the retry).
    #[tokio::test]
    async fn persist_output_is_idempotent_for_identical_content_and_evidence() {
        let (tmp, fs, _m) = fresh_fs().await;
        let id = fs
            .record_evidence(record("echo", json!({"k": 1}), json!({"v": 1})))
            .await
            .unwrap();

        let first = fs
            .persist_output("the same claim", &[id.clone()])
            .await
            .unwrap();
        // Capture the first tail object's added_at for the new entry.
        let tail_path = tmp.path().join("outputs").join("_tail.json");
        let tail_first: TailObject =
            serde_json::from_slice(&std::fs::read(&tail_path).unwrap()).unwrap();
        assert_eq!(tail_first.entries.len(), 1);
        let added_at_first = tail_first.entries[0].added_at;

        // Ensure wall-clock advances enough for a fresh Utc::now() to
        // differ from the first one — otherwise the test would pass
        // trivially even if we did re-stamp added_at.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let second = fs
            .persist_output("the same claim", &[id.clone()])
            .await
            .unwrap();

        // Same content + evidence → same content-addressed OutputId.
        assert_eq!(first.id, second.id);

        // Exactly one file under outputs/ (sha256 dedup at the storage
        // layer — `put_if_absent` returned Existed).
        let output_files: Vec<_> = std::fs::read_dir(tmp.path().join("outputs"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|n| n != "_tail.json")
            .collect();
        assert_eq!(
            output_files.len(),
            1,
            "duplicate persist_output created extra file: {output_files:?}"
        );

        // Tail object has exactly one entry, with the original
        // `added_at` — the retry path skipped the tail update.
        let tail_after: TailObject =
            serde_json::from_slice(&std::fs::read(&tail_path).unwrap()).unwrap();
        assert_eq!(tail_after.entries.len(), 1);
        assert_eq!(
            tail_after.entries[0].added_at, added_at_first,
            "tail added_at must not move on a retry/dedup write"
        );

        // list_recent_outputs surfaces a single output, not two.
        let recent = fs.list_recent_outputs(8).await.unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].id, first.id);

        // Sanity: a *different* content with the same evidence still
        // mints a fresh id and lands a second file.
        let third = fs
            .persist_output("a different claim", &[id.clone()])
            .await
            .unwrap();
        assert_ne!(third.id, first.id);
        let output_files: Vec<_> = std::fs::read_dir(tmp.path().join("outputs"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|n| n != "_tail.json")
            .collect();
        assert_eq!(output_files.len(), 2);
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
        let pinned = DateTime::parse_from_rfc3339("2026-05-24T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        fs.persist_retirement("done", pinned).await.unwrap();
        let path = tmp.path().join("retirement.json");
        assert!(path.is_file());
        let v: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(v.get("reason").and_then(|x| x.as_str()), Some("done"));
        // `retired_at` is the caller-supplied timestamp, exactly — pin
        // it so a regression that re-stamps `Utc::now()` internally
        // (defeating workflow-replay determinism) fails loudly.
        // chrono's serde format for `DateTime<Utc>` emits `Z` for the
        // UTC offset (compact RFC 3339 form), not `+00:00`.
        assert_eq!(
            v.get("retired_at").and_then(|x| x.as_str()),
            Some("2026-05-24T12:00:00Z")
        );
    }

    #[tokio::test]
    async fn attach_skips_mandate_and_writes_retirement() {
        // `AgentFs::attach` is the no-mandate, no-reconcile constructor
        // used by the Temporal `persist_retirement` activity body where
        // no `Mandate` is in scope (the retirement-signal short-circuit
        // runs before `assemble_context`).
        let storage: Arc<dyn AgentStorage> = Arc::new(MemoryStorage::new());
        let fs = AgentFs::attach(Arc::clone(&storage), "graphs/g1/agents/a1");
        let pinned = DateTime::parse_from_rfc3339("2026-05-24T13:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        fs.persist_retirement("attached", pinned).await.unwrap();

        // mandate.json is *not* created — attach skipped it.
        let mandate = storage
            .get("graphs/g1/agents/a1/mandate.json")
            .await
            .unwrap();
        assert!(
            mandate.is_none(),
            "attach must not write mandate.json (no mandate in scope)"
        );

        // retirement.json lives under the prefix and carries the
        // caller-supplied retired_at byte-for-byte.
        let key = "graphs/g1/agents/a1/retirement.json";
        let bytes = storage.get(key).await.unwrap().expect("retirement.json");
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v.get("reason").and_then(|x| x.as_str()), Some("attached"));
        assert_eq!(
            v.get("retired_at").and_then(|x| x.as_str()),
            Some("2026-05-24T13:00:00Z")
        );
    }

    #[tokio::test]
    async fn attach_normalizes_prefix_with_trailing_slash() {
        // `new_with_storage` appends `/` to non-empty prefixes; `attach`
        // must follow the same rule so callers passing
        // `"graphs/g1/agents/a1"` and `"graphs/g1/agents/a1/"` land in
        // the same place.
        let storage: Arc<dyn AgentStorage> = Arc::new(MemoryStorage::new());
        let bare = AgentFs::attach(Arc::clone(&storage), "graphs/g1/agents/a1");
        let with_slash = AgentFs::attach(Arc::clone(&storage), "graphs/g1/agents/a1/");
        // The prefix() accessor exposes the normalized form.
        assert_eq!(bare.prefix(), with_slash.prefix());
        assert_eq!(bare.prefix(), "graphs/g1/agents/a1/");
        // Empty prefix stays empty (no spurious leading slash).
        let empty = AgentFs::attach(Arc::clone(&storage), "");
        assert_eq!(empty.prefix(), "");
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
        let err = fs2.persist_retirement("bye", Utc::now()).await.unwrap_err();
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

    // ---- JAR2-54: tail-index integration -------------------------------

    /// Round-trip a small workload through the tail-fast path. The
    /// `outputs/_tail.json` and `evidence/_tail.json` objects must be
    /// present after writes, and `list_recent_*` returns the same
    /// answer as the pre-2.5.4 LIST path would have.
    #[tokio::test]
    async fn tail_index_outputs_and_evidence_are_written_on_each_put() {
        let (tmp, fs, _m) = fresh_fs().await;
        let id = fs
            .record_evidence(record("echo", json!({"k": 1}), json!({"v": 1})))
            .await
            .unwrap();
        let _out = fs.persist_output("o", &[id.clone()]).await.unwrap();

        // Tail files on disk.
        let outputs_tail = tmp.path().join("outputs").join("_tail.json");
        let evidence_tail = tmp.path().join("evidence").join("_tail.json");
        assert!(outputs_tail.is_file(), "outputs/_tail.json missing");
        assert!(evidence_tail.is_file(), "evidence/_tail.json missing");

        let parsed: TailObject =
            serde_json::from_slice(&std::fs::read(&outputs_tail).unwrap()).unwrap();
        assert_eq!(parsed.entries.len(), 1);
        assert!(parsed.entries[0].filename.ends_with(".json"));
    }

    /// The tail trims to `TAIL_K`. Write `TAIL_K + 16` outputs and
    /// assert the on-disk tail has exactly `TAIL_K` entries with the
    /// newest at index 0 (by `added_at`, not filename — sha256-named
    /// post-JAR2-70 carries no temporal meaning).
    #[tokio::test]
    async fn tail_index_trims_to_tail_k() {
        let (tmp, fs, _m) = fresh_fs().await;
        let id = fs
            .record_evidence(record("echo", json!({"k": 1}), json!({"v": 1})))
            .await
            .unwrap();
        for i in 0..(TAIL_K + 16) {
            fs.persist_output(&format!("o-{i}"), &[id.clone()])
                .await
                .unwrap();
        }
        let bytes = std::fs::read(tmp.path().join("outputs").join("_tail.json")).unwrap();
        let parsed: TailObject = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.entries.len(), TAIL_K);
        // Newest first by `added_at`: index 0 is the most recently
        // appended entry. Filename ordering carries no temporal
        // meaning under content-addressing, so we assert on the
        // timestamp the tail records explicitly.
        assert!(
            parsed.entries[0].added_at >= parsed.entries[1].added_at,
            "tail must be reverse-chronological on added_at"
        );
    }

    /// `list_recent_outputs(N)` returns the same lex-ascending
    /// window as the pre-2.5.4 LIST path would have, even after the
    /// tail has trimmed older entries. Under JAR2-70's sha256
    /// filenames this asserts the LIST-fallback lex-window — the
    /// tail-fast-path is bypassed because `lex_monotonic=false` and
    /// the tail is at capacity (recency != lex ordering).
    #[tokio::test]
    async fn list_recent_outputs_after_trim_matches_lex_window() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let id = fs
            .record_evidence(record("echo", json!({"k": 1}), json!({"v": 1})))
            .await
            .unwrap();
        // Slightly over TAIL_K so we know the tail dropped some.
        let total = TAIL_K + 5;
        let mut all_ids = Vec::new();
        for i in 0..total {
            let o = fs
                .persist_output(&format!("o-{i}"), &[id.clone()])
                .await
                .unwrap();
            all_ids.push(o.id);
        }
        all_ids.sort();
        let want_last_8: Vec<_> = all_ids.iter().rev().take(8).rev().cloned().collect();

        let got = fs.list_recent_outputs(8).await.unwrap();
        let got_ids: Vec<_> = got.iter().map(|o| o.id.clone()).collect();
        assert_eq!(got_ids, want_last_8);
    }

    /// Crash recovery — missing tail object. The read path's LIST
    /// fallback kicks in when the tail object is absent (e.g. an
    /// operator deleted it for forensics, or the very first tail PUT
    /// in this agent's history never reached durable storage). On-
    /// disk records surface normally.
    #[tokio::test]
    async fn list_recent_outputs_recovers_via_list_when_tail_object_absent() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let id = fs
            .record_evidence(record("echo", json!({"k": 1}), json!({"v": 1})))
            .await
            .unwrap();
        let _ = fs.persist_output("o-1", &[id.clone()]).await.unwrap();
        let _ = fs.persist_output("o-2", &[id.clone()]).await.unwrap();
        let orphan = Output::new("o-orphan".to_string(), vec![id.clone()], Utc::now());
        let orphan_key = format!("outputs/{}.json", orphan.id);
        fs.storage()
            .put(
                &orphan_key,
                Bytes::from(serde_json::to_vec_pretty(&orphan).unwrap()),
            )
            .await
            .unwrap();
        // Simulate a tail-PUT that never reached durable storage.
        fs.storage().delete("outputs/_tail.json").await.unwrap();

        let got = fs.list_recent_outputs(8).await.unwrap();
        let ids: Vec<_> = got.iter().map(|o| o.id.clone()).collect();
        assert!(
            ids.contains(&orphan.id),
            "orphan output should be recovered via LIST fallback when tail is missing"
        );
        assert_eq!(got.len(), 3);
    }

    /// Same shape for `evidence/`: tail object absent, LIST fallback
    /// returns every on-disk record.
    #[tokio::test]
    async fn list_recent_evidence_recovers_via_list_when_tail_object_absent() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let _id_a = fs
            .record_evidence(record("echo", json!({"a": 1}), json!({"r": 1})))
            .await
            .unwrap();
        let _id_b = fs
            .record_evidence(record("echo", json!({"b": 2}), json!({"r": 2})))
            .await
            .unwrap();
        fs.storage().delete("evidence/_tail.json").await.unwrap();
        let got = fs.list_recent_evidence(8).await.unwrap();
        assert_eq!(got.len(), 2);
    }

    /// Crash recovery — tail lags on-disk reality (the real § 7.1
    /// scenario). A previous process PUT `outputs/<sha256>.json` but
    /// crashed before updating the tail. The tail has fewer entries
    /// than disk; the in-process read path's `entries.len() < TAIL_K`
    /// trust would silently drop the orphan. Open-time reconciliation
    /// in `new_with_storage` rebuilds the tail from the LIST so the
    /// in-process read path stays O(1) and correct.
    #[tokio::test]
    async fn open_time_reconcile_rebuilds_tail_when_lagging_behind_outputs() {
        let tmp = TempDir::new().unwrap();
        let mandate = Mandate::new("reconcile", Duration::from_millis(100), Some(1));
        // First session: write two outputs through the facade so the
        // tail is consistent.
        let id = {
            let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
                .await
                .unwrap();
            let id = fs
                .record_evidence(record("echo", json!({"k": 1}), json!({"v": 1})))
                .await
                .unwrap();
            let _ = fs.persist_output("o-1", &[id.clone()]).await.unwrap();
            let _ = fs.persist_output("o-2", &[id.clone()]).await.unwrap();
            id
        };

        // Simulate a crashed worker that PUT an orphan output but
        // never updated the tail. A fresh `LocalStorage` handle
        // mimics an out-of-band write — or a crashed-mid-update.
        let orphan = Output::new("orphan".to_string(), vec![id.clone()], Utc::now());
        let orphan_key = format!("outputs/{}.json", orphan.id);
        let storage_handle = Arc::new(LocalStorage::new(tmp.path().to_path_buf()).unwrap());
        storage_handle
            .put(
                &orphan_key,
                Bytes::from(serde_json::to_vec_pretty(&orphan).unwrap()),
            )
            .await
            .unwrap();

        // Sanity precondition: the pre-reconciliation tail does NOT
        // contain the orphan, so a naive O(1) read path would miss it.
        let pre_bytes = storage_handle
            .get("outputs/_tail.json")
            .await
            .unwrap()
            .unwrap();
        let pre_tail: TailObject = serde_json::from_slice(&pre_bytes).unwrap();
        let orphan_filename = format!("{}.json", orphan.id);
        assert!(
            !pre_tail
                .entries
                .iter()
                .any(|e| e.filename == orphan_filename),
            "precondition: tail should NOT yet contain the orphan"
        );

        // Re-open the FS — open-time reconcile rebuilds the tail.
        let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
            .await
            .unwrap();
        let got = fs.list_recent_outputs(8).await.unwrap();
        let ids: Vec<_> = got.iter().map(|o| o.id.clone()).collect();
        assert!(
            ids.contains(&orphan.id),
            "orphan output must surface after open-time reconcile"
        );
        assert_eq!(got.len(), 3);

        // The rebuilt tail object on disk now carries every entry.
        let post_bytes = storage_handle
            .get("outputs/_tail.json")
            .await
            .unwrap()
            .unwrap();
        let post_tail: TailObject = serde_json::from_slice(&post_bytes).unwrap();
        assert_eq!(post_tail.entries.len(), 3);
        assert!(
            post_tail
                .entries
                .iter()
                .any(|e| e.filename == orphan_filename),
            "rebuilt tail should contain the orphan"
        );
    }

    /// Open-time reconcile is a no-op (no tail PUT) when on-disk and
    /// tail agree — pins that we don't churn the tail file on every
    /// process restart.
    #[tokio::test]
    async fn open_time_reconcile_is_noop_when_tail_matches_disk() {
        let tmp = TempDir::new().unwrap();
        let mandate = Mandate::new("noop", Duration::from_millis(100), Some(1));
        let id;
        {
            let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
                .await
                .unwrap();
            id = fs
                .record_evidence(record("echo", json!({"k": 1}), json!({"v": 1})))
                .await
                .unwrap();
            let _ = fs.persist_output("o-1", &[id.clone()]).await.unwrap();
        }
        // Snapshot the tail file's bytes before re-open.
        let before = std::fs::read(tmp.path().join("outputs").join("_tail.json")).unwrap();

        // Re-open. Reconcile detects no lag and skips the PUT,
        // leaving the bytes byte-identical.
        let _fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
            .await
            .unwrap();
        let after = std::fs::read(tmp.path().join("outputs").join("_tail.json")).unwrap();
        assert_eq!(
            before, after,
            "tail file should be untouched when reconcile is a no-op"
        );
    }

    /// `list_recent_*` returns an empty Vec when the prefix has no
    /// writes yet (matches the pre-2.5.4 "safe to call right after
    /// open" property). Verifies the no-tail-object path.
    #[tokio::test]
    async fn list_recent_outputs_returns_empty_when_no_writes() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let out = fs.list_recent_outputs(8).await.unwrap();
        assert!(out.is_empty());
        let ev = fs.list_recent_evidence(8).await.unwrap();
        assert!(ev.is_empty());
    }

    /// `n == 0` short-circuits with no I/O. Pin the behaviour so a
    /// future caller passing `0` (e.g. a feature-flagged
    /// `recent_outputs = 0` policy) doesn't pay a round-trip.
    #[tokio::test]
    async fn list_recent_outputs_n_zero_returns_empty_without_io() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let id = fs
            .record_evidence(record("echo", json!({"k": 1}), json!({"v": 1})))
            .await
            .unwrap();
        let _ = fs.persist_output("o", &[id]).await.unwrap();
        let got = fs.list_recent_outputs(0).await.unwrap();
        assert!(got.is_empty());
    }

    /// Microbench: `list_recent_outputs(8)` over 10_000 outputs.
    ///
    /// **JAR2-70 caveat.** Pre-JAR2-70, output filenames were ULIDs
    /// (lex-monotonic with write time) and the tail-fast-path stayed
    /// O(1) regardless of total output count. Post-JAR2-70, output
    /// filenames are sha256 digests — non-monotonic — so once the
    /// tail is at `TAIL_K` capacity, `list_recent_outputs` falls back
    /// to the LIST path and the bench measures the LIST cost, not
    /// the tail-fast-path. The bench is retained as a regression
    /// guard against the LIST path silently degrading further; the
    /// O(1) property the original bench asserted no longer holds at
    /// the prefix level and would need a different naming scheme
    /// (or a sidecar recency index) to restore. Bound relaxed so the
    /// `#[ignore]`d bench still passes under the LIST path.
    #[tokio::test]
    #[ignore = "microbench: long-running"]
    async fn tail_index_outputs_is_o1_at_10k() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let id = fs
            .record_evidence(record("echo", json!({"k": 1}), json!({"v": 1})))
            .await
            .unwrap();
        for i in 0..10_000usize {
            fs.persist_output(&format!("o-{i}"), &[id.clone()])
                .await
                .unwrap();
        }
        let start = std::time::Instant::now();
        let got = fs.list_recent_outputs(8).await.unwrap();
        let elapsed = start.elapsed();
        assert_eq!(got.len(), 8);
        // Relaxed bound post-JAR2-70 — LIST fallback over 10 k
        // sha256 filenames + sort + 8-entry get_many. Tuned to be
        // unambiguous, not tight; primary purpose is a regression
        // guard against the LIST path slipping into something
        // outrageously slow.
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "list_recent_outputs(8) over 10k outputs took {elapsed:?}; bound exceeded"
        );
    }

    // ---- JAR2-83 (stage 5.6): conflict-log FS writer ------------------

    use crate::agent_ref::AgentRef;
    use crate::conflict::{ConflictKind, ConflictRecord};
    use crate::decision::{ConflictAlternative, ConflictResolution};
    use uuid::Uuid;

    fn alt(child_slug: &str, claim: &str, output_hex: &str) -> ConflictAlternative {
        ConflictAlternative {
            source_child: AgentRef::new(
                format!("graphs/g1/agents/{child_slug}"),
                AgentId::new(Uuid::new_v4()),
            ),
            source_output_id: OutputId::from_hex(output_hex.repeat(32)),
            claim: claim.to_string(),
        }
    }

    fn ts_fixed() -> chrono::DateTime<Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-05-25T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[tokio::test]
    async fn write_conflict_persists_held_open_record_under_content_addressed_path() {
        let (tmp, fs, _m) = fresh_fs().await;
        let record = ConflictRecord::new(
            ts_fixed(),
            vec![
                alt("child-a", "value is 42", "aa"),
                alt("child-b", "value is 43", "bb"),
            ],
            None,
        );
        let expected_id = record.id.clone();

        let id = fs.write_conflict(&record).await.unwrap();
        assert_eq!(id, expected_id, "write_conflict returns the record's id");

        // File exists at the expected path.
        let path = tmp.path().join("conflicts").join(format!("{}.json", id));
        assert!(
            path.is_file(),
            "conflict file missing at {}",
            path.display()
        );

        // Round-trips through read_conflict.
        let back = fs.read_conflict(&id).await.unwrap().expect("present");
        assert_eq!(back, record);
        assert_eq!(back.kind, ConflictKind::HeldOpen);
    }

    #[tokio::test]
    async fn write_conflict_persists_resolved_record_with_resolution_intact() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let resolution = ConflictResolution {
            chosen_alternative_idx: 1,
            reasoning: "newer evidence".into(),
        };
        let record = ConflictRecord::new(
            ts_fixed(),
            vec![
                alt("child-a", "claim a", "aa"),
                alt("child-b", "claim b", "bb"),
            ],
            Some(resolution.clone()),
        );

        let id = fs.write_conflict(&record).await.unwrap();
        let back = fs.read_conflict(&id).await.unwrap().expect("present");
        assert_eq!(back.kind, ConflictKind::Resolved);
        assert_eq!(back.resolution.as_ref().unwrap(), &resolution);
    }

    #[tokio::test]
    async fn write_conflict_rejects_fewer_than_two_alternatives() {
        let (_tmp, fs, _m) = fresh_fs().await;
        // Bypass `ConflictRecord::new`'s validation-free constructor —
        // we want to confirm the writer is the second line of defence.
        let bad = ConflictRecord {
            id: ConflictId::from_hex("00".repeat(32)),
            timestamp: ts_fixed(),
            kind: ConflictKind::HeldOpen,
            alternatives: vec![alt("only-child", "lonely claim", "cc")],
            resolution: None,
        };
        let err = fs.write_conflict(&bad).await.unwrap_err();
        match err.downcast_ref::<FsError>() {
            Some(FsError::ConflictAlternativesTooFew { count }) => assert_eq!(*count, 1),
            other => panic!("expected ConflictAlternativesTooFew, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn write_conflict_is_idempotent_under_retries() {
        let (tmp, fs, _m) = fresh_fs().await;
        let alts = vec![
            alt("child-a", "claim a", "aa"),
            alt("child-b", "claim b", "bb"),
        ];
        // First write at t0, second write at a later wall-clock t1 —
        // both should land on the same content-addressed file because
        // timestamp is NOT in the id.
        let r1 = ConflictRecord::new(ts_fixed(), alts.clone(), None);
        let id1 = fs.write_conflict(&r1).await.unwrap();

        let later = ts_fixed() + chrono::Duration::seconds(60);
        let r2 = ConflictRecord::new(later, alts, None);
        let id2 = fs.write_conflict(&r2).await.unwrap();
        assert_eq!(id1, id2, "retry must produce the same id");

        // Directory still holds exactly one file.
        let files: Vec<_> = std::fs::read_dir(tmp.path().join("conflicts"))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(files.len(), 1, "expected one file, got {files:?}");
    }

    #[tokio::test]
    async fn read_conflict_returns_none_for_missing_id() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let bogus = ConflictId::from_hex("ee".repeat(32));
        let got = fs.read_conflict(&bogus).await.unwrap();
        assert!(got.is_none(), "expected None for missing conflict id");
    }

    #[tokio::test]
    async fn list_conflicts_returns_all_written_records() {
        let (_tmp, fs, _m) = fresh_fs().await;
        let r1 = ConflictRecord::new(
            ts_fixed(),
            vec![alt("a", "claim a", "aa"), alt("b", "claim b", "bb")],
            None,
        );
        let r2 = ConflictRecord::new(
            ts_fixed(),
            vec![alt("c", "claim c", "cc"), alt("d", "claim d", "dd")],
            Some(ConflictResolution {
                chosen_alternative_idx: 0,
                reasoning: "first".into(),
            }),
        );
        fs.write_conflict(&r1).await.unwrap();
        fs.write_conflict(&r2).await.unwrap();

        let listed = fs.list_conflicts().await.unwrap();
        assert_eq!(listed.len(), 2);
        let ids: std::collections::HashSet<_> =
            listed.iter().map(|c| c.id.as_str().to_string()).collect();
        assert!(ids.contains(r1.id.as_str()));
        assert!(ids.contains(r2.id.as_str()));
    }

    #[tokio::test]
    async fn conflicts_land_under_agent_prefix_on_memory_storage() {
        // Same coverage as `open_for_agent_scopes_storage_to_workflow_id_prefix`
        // but for the conflicts/ prefix — confirms the path scheme survives
        // a non-empty agent prefix without colliding across agents.
        let storage = Arc::new(MemoryStorage::new());
        let dyn_storage: Arc<dyn AgentStorage> = storage.clone();
        let graph_id = GraphId::new(Uuid::new_v4());
        let agent_id = AgentId::new(Uuid::new_v4());
        let fs = AgentFs::open_for_agent(dyn_storage, graph_id, agent_id);

        let record = ConflictRecord::new(
            ts_fixed(),
            vec![
                alt("child-a", "claim a", "aa"),
                alt("child-b", "claim b", "bb"),
            ],
            None,
        );
        let id = fs.write_conflict(&record).await.unwrap();

        // The on-disk key carries the agent prefix.
        let expected_key = format!("graphs/{graph_id}/agents/{agent_id}/conflicts/{id}.json");
        let bytes = storage.get(&expected_key).await.unwrap();
        assert!(
            bytes.is_some(),
            "conflict not at expected key {expected_key}"
        );
    }
}

//! Stage 3.4 (JAR2-60) — activity surface for `AgentWorkflow`.
//!
//! Six activities, one for each branch the workflow loop body wants to
//! durably checkpoint:
//!
//! | activity              | invoked when                                             | real body lands in |
//! | --------------------- | -------------------------------------------------------- | ------------------ |
//! | `assemble_context`    | top of every tick (after drain)                          | JAR2-61            |
//! | `decide_next_action`  | after `assemble_context` returns a bundle                | JAR2-62            |
//! | `execute_tool`        | once per `ToolCall` in a `Decision::CallTools`           | JAR2-63            |
//! | `persist_output`      | `Decision::EmitOutput`                                   | JAR2-64            |
//! | `apply_fs_ops`        | `Decision::RewriteFs`                                    | JAR2-65            |
//! | `persist_retirement`  | `Decision::Retire` *or* the `retire` signal short-circuit | JAR2-66           |
//!
//! Every body here is a stub returning a canned `Ok(...)` so the workflow
//! loop runs end-to-end against `MockDecide`-style scripted decisions. The
//! input/output types are real — JAR2-61..66 subagents replace bodies
//! without touching the wire shape.
//!
//! As of JAR2-64, `persist_output` carries its real body: opens an
//! [`jarvis_node::fs::AgentFs`] over the process-wide [`AgentStorage`]
//! backend and delegates to `AgentFs::persist_output`. The body extracts
//! into the free helper [`persist_output_impl`] so hermetic tests can
//! exercise the FS-touching logic without an `ActivityContext` or the
//! `OnceLock` install path.
//!
//! ## Test injection
//!
//! `decide_next_action` consults a static `OnceLock<Mutex<VecDeque<Decision>>>`
//! before reaching for the installed [`Decide`] implementation. Tests
//! call [`set_decision_script`] before starting the workflow; the
//! activity pops from the script in order. This is the workflow-side
//! analogue of `agent_core`'s `MockDecide` — same scripted behaviour,
//! but reachable from inside an activity body (which must be a free
//! function over a value-typed registered instance per SDK constraint
//! § 3.4 of `temporal_rust_sdk_smoke.md`). When the script is empty
//! the activity falls through to `worker::decide_impl()` (JAR2-62) and
//! calls the installed `Decide::decide` once. Tests that don't install
//! a real `Decide` must script every decision the workflow body will
//! ask for — see `tests/workflow_loop.rs` for the canonical example.
//!
//! ## SDK constraints baked in
//!
//! - Each activity is a free `async fn` (not `&self`-receiver) per the
//!   `#[activities]` macro shape (see `bin/temporal_smoke.rs::SmokeActivities`
//!   line 76 and `examples/cancellation/workflows.rs::CancellationActivities`).
//! - First parameter is `ActivityContext`; second is the typed input.
//!   Return type is `Result<R, ActivityError>`. The `&self` form in the
//!   ticket sketch does not match the macro.
//! - `register_activities` takes the bare value, not `Arc<T>` (smoke § 3.4).
//!   `AgentActivities` is a unit struct; the macro impls
//!   `ActivityImplementer` for the bare type.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use jarvis_node::agent_core;
use jarvis_node::agent_ref::{AgentId, GraphId};
use jarvis_node::decision::{
    ConflictId, ConflictRecordIntent, ContextBundle, CorrectionContext, Decide, Decision, FsOp,
    ReconcileSource, ToolCall,
};
use jarvis_node::evidence::{EvidenceId, EvidenceRecord};
use jarvis_node::fs::{AgentFs, FsError};
use jarvis_node::mandate::{Mandate, OutputId};
use jarvis_node::model_client::ModelError;
use jarvis_node::storage::AgentStorage;
use jarvis_node::trigger::{HumanOp, MandatePatch, Trigger};
use serde::{Deserialize, Serialize};
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use temporalio_sdk::ApplicationFailure;

use crate::worker::{agent_storage, structural_db_store};
use crate::workflow::{AgentConfig, FsHandle};

// ---------------------------------------------------------------------------
// Input / output types
//
// Real fields chosen against the JAR2-61..66 target shapes
// (`scratch/temporal_staged_plan.md` § 5 stages 3.5–3.10). Stubs ignore the
// inputs and return canned outputs; the real bodies will plumb FS reads /
// LLM calls / tool dispatch / FS writes through these payloads.

/// Input to [`AgentActivities::assemble_context`]. Carries the per-tick
/// drained signal buckets (`triggers`, `human_ops`, `mandate_patches`) plus
/// the resolved [`Mandate`] + FS handle + prior-tick correction so the
/// activity can call into [`jarvis_node::agent_core::drain_triggers`].
///
/// JAR2-61 promoted the prior `cfg: AgentConfig` placeholder to a real
/// `mandate: Mandate` — `drain_triggers` requires a concrete `&Mandate`
/// to seed the `ContextBundle` and to write `mandate.json` on first FS
/// open. The other activity inputs (`ExecuteToolInput`, `PersistOutputInput`)
/// still carry the `AgentConfig` placeholder; siblings JAR2-62..66 will
/// promote each as their real bodies need it. No `Default` derive — the
/// real `Mandate` has no `Default` and the placeholder construction lives
/// at the workflow-body call site.
///
/// `mandate_patches` are surfaced here so JAR2-61 can apply them to the
/// per-agent FS before assembling the bundle (the workflow body itself
/// must not touch FS — see `scratch/temporal_staged_plan.md` § 2.5
/// "Drain triggers (typed, ordered)" and the JAR2-60 ticket's notes on
/// the drain/assemble merge in `agent_core`). Today the activity logs the
/// patch count and drops them on the floor; stage 6 wires the consumption.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssembleContextInput {
    pub mandate: Mandate,
    pub fs_handle: FsHandle,
    pub triggers: Vec<Trigger>,
    /// Human overrides drained alongside `triggers`. JAR2-61 folds these
    /// into the `Trigger::HumanOverride` taxonomy before calling
    /// `drain_triggers`, appending them after the regular triggers so the
    /// ordering matches the in-process loop (which sees the same signal
    /// stream serialized through one mpsc receiver).
    pub human_ops: Vec<HumanOp>,
    /// Mandate patches drained from the workflow's `pending_mandate_patches`
    /// bucket. Stage 6 owns the consumption (apply patch → write FS →
    /// re-resolve routing); the activity just records the count today.
    pub mandate_patches: Vec<MandatePatch>,
    /// Correction context staged by the previous tick — `Some` when the
    /// previous `DispatchOutcome` was `NeedsCorrection` or `ToolError`.
    /// `None` on the first tick of a run.
    pub prior_correction: Option<CorrectionContext>,
}

/// Output of [`AgentActivities::assemble_context`]. Real body returns the
/// fully-populated [`ContextBundle`] from `agent_core::drain_triggers`;
/// stub returns an empty bundle with a placeholder mandate so the
/// downstream `decide_next_action` activity has something to serialize.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssembleContextOutput {
    pub bundle: ContextBundle,
}

/// Input to [`AgentActivities::decide_next_action`]. Real body wraps
/// `LlmDecide::decide(bundle)`; stub consults the test script and falls
/// back to a canned `Idle`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DecideInput {
    pub bundle: ContextBundle,
}

/// Input to [`AgentActivities::execute_tool`]. One activity invocation per
/// `ToolCall` — the workflow body fans out via `workflows::join_all` so a
/// partial parallel batch survives a worker crash (only in-flight calls
/// re-execute on retry; completed ones already wrote their outcome to
/// workflow history). See `scratch/temporal_staged_plan.md` § 2.5 +
/// JAR2-60 ticket § "SDK constraints baked in" item 2.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecuteToolInput {
    pub cfg: AgentConfig,
    pub fs_handle: FsHandle,
    pub call: ToolCall,
}

/// Result of a single `execute_tool` activity invocation. Mirrors the
/// shape `agent_core::dispatch_call_tools` already produces — successful
/// calls carry an `EvidenceId`; failed calls carry a structured
/// [`ToolCallFailure`] the workflow can fold into next-tick correction
/// context.
///
/// **Why this mirrors `agent_core::ToolFailure` but isn't it.**
/// `agent_core::ToolFailure` doesn't derive `Serialize`/`Deserialize` — and
/// it must not, in this ticket: that crate is out of scope per JAR2-60
/// guardrail 1. We carry the same three fields here so JAR2-63 can
/// translate one to the other when wiring the real body.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ToolCallOutcome {
    Success { evidence_id: EvidenceId },
    Failure { failure: ToolCallFailure },
}

/// Mirror of `jarvis_node::agent_core::ToolFailure` with serde derives so
/// the value crosses the workflow ↔ activity boundary via Temporal's
/// payload codec. JAR2-63's real `execute_tool` body converts the
/// `agent_core::ToolFailure` from `dispatch_call_tools` into this shape.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolCallFailure {
    pub tool: String,
    pub args: serde_json::Value,
    pub error: String,
}

/// Input to [`AgentActivities::persist_output`]. Real body calls
/// `AgentFs::persist_output` and returns the minted `OutputId`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistOutputInput {
    pub cfg: AgentConfig,
    pub fs_handle: FsHandle,
    pub content: String,
    pub evidence: Vec<EvidenceId>,
}

/// Input to [`AgentActivities::apply_fs_ops`].
///
/// JAR2-65 carries a `Mandate` because [`jarvis_node::fs::AgentFs::new_with_storage`]
/// requires one to reify an `AgentFs` against the shared storage. The
/// mandate is decorative for this call path — `AgentFs::new_with_storage`
/// only writes `mandate.json` when absent, and `apply_fs_ops` runs only
/// against agents that have already gone through `assemble_context` at
/// least once (so `mandate.json` already exists on disk). Carrying the
/// real mandate, rather than fishing it out of disk inside the activity,
/// keeps the activity body single-storage-roundtrip.
///
/// **Today** the workflow body passes a placeholder
/// `Mandate::new("", Duration::ZERO, None)` because `AgentConfig` is the
/// JAR2-58 placeholder unit struct. When `AgentConfig` grows a real
/// mandate field (later stage), only the workflow call site changes —
/// this input shape stays the same.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplyFsOpsInput {
    pub fs_handle: FsHandle,
    pub mandate: Mandate,
    pub ops: Vec<FsOp>,
}

/// Input to [`AgentActivities::persist_retirement`]. Carries the reason so
/// retirement is auditable on disk.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistRetirementInput {
    pub fs_handle: FsHandle,
    pub reason: String,
}

/// Input to [`AgentActivities::append_decision_log`].
///
/// JAR2-68 / plan § 8 decision 6 — one entry per tick, written to
/// `<prefix>/decisions/<tick>.jsonl`. The workflow body calls the
/// activity after [`decide`](crate::workflow) returns a `Decision`, so
/// the entry is observable end-to-end (output decisions, retirements,
/// idle ticks all land in the same artifact stream).
///
/// `decision_summary` is the human-readable rendering of the
/// `Decision` enum variant. The full structured decision payload is
/// already captured by Temporal workflow history; the on-disk log is a
/// host-agnostic, FS-readable, replay-stable summary the TUI phase 1
/// (stage 7.6) consumes without talking to Temporal.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendDecisionLogInput {
    pub fs_handle: FsHandle,
    pub tick: u64,
    pub decision_summary: String,
}

/// JAR2-80 (stage 5.3) — input for the `register_child_in_structural_db`
/// activity body. Carries the parent's `(graph_id, agent_id)` so the
/// activity can write the child's `agents` row (scoped to the parent's
/// graph) and the parent → child `edges` row in one transaction's worth
/// of writes.
///
/// `child_mandate_ref` is the opaque text handle from the structural-DB
/// schema (`migrations/0001_initial.sql`). Runtime spawns
/// (`Decision::SpawnChild`) pass `None` — the child's mandate travels
/// via `AgentInput.mandate` per Stage 5 Project decision 9 and the
/// runtime spawn never produces a stable text handle. YAML-driven
/// spawns (5.8) may pass a real ref here once the resolver ships.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterChildInStructuralDbInput {
    pub parent_graph_id: GraphId,
    pub parent_agent_id: AgentId,
    pub child_agent_name: String,
    pub child_mandate_ref: Option<String>,
}

/// JAR2-80 (stage 5.3) — output of the `register_child_in_structural_db`
/// activity. Returns the child's freshly-allocated `AgentId` so the
/// workflow body can construct the child workflow id
/// (`graphs/<gid>/agents/<aid>`) and pass it to `ctx.child_workflow(..)`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterChildInStructuralDbOutput {
    pub child_agent_id: AgentId,
}

/// JAR2-82 (stage 5.5) — input to the `reconcile_children` activity.
///
/// Carries the parent's identity (so the activity can open the
/// parent's FS and write the synthetic evidence) plus the cited
/// child outputs and the optional conflict-record intent. Both
/// `parent_graph_id` and every `sources[i].child_ref` must live in
/// the same graph — cross-graph reads are explicitly out of scope per
/// the Stage 5 Project description.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconcileChildrenInput {
    pub parent_graph_id: GraphId,
    pub parent_agent_id: AgentId,
    pub sources: Vec<ReconcileSource>,
    /// `Some` iff the parent observed disagreement among the cited
    /// outputs. JAR2-82 leaves the conflict-log writer stubbed
    /// (the writer + canonical-form bytes land in JAR2-83 / 5.6);
    /// when `Some`, the activity emits a `tracing::warn!` and returns
    /// `conflict_id: None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict: Option<ConflictRecordIntent>,
}

/// JAR2-82 (stage 5.5) — output of the `reconcile_children` activity.
///
/// `synthetic_evidence[i]` is the freshly-minted `EvidenceId` for the
/// `sources[i]` cross-agent fold (written into the parent's
/// `evidence/<id>.json`). The parent's next-tick `assemble_context`
/// picks these up via the existing `list_recent_evidence` window with
/// no workflow-state slot involved (Stage 5 Project decision 3 +
/// JAR2-82 advisor item 4).
///
/// `conflict_id` is always `None` in JAR2-82 — the writer ships in
/// JAR2-83. The field exists on the wire today so 5.6's call-site
/// upgrade is a value change, not a struct rev.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconcileChildrenOutput {
    pub synthetic_evidence: Vec<EvidenceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_id: Option<ConflictId>,
}

/// JAR2-82 (stage 5.5) — typed reconciliation errors.
///
/// The reconcile_children activity wraps these as
/// `ApplicationFailure::non_retryable` so Temporal's outer retry loop
/// doesn't churn through them; the workflow body catches the
/// activity failure and stages a `CorrectionContext` for the next
/// tick (mirroring the existing `Decision::CallTools` tool-failure
/// flow).
#[derive(Debug, thiserror::Error)]
pub enum ReconciliationError {
    /// A `sources[i].output_id` did not resolve in the named child's
    /// `outputs/<id>.json`. Carries the child agent id + the output
    /// id so the workflow body's correction text is precise enough
    /// for the LLM to fix on the next tick.
    #[error("reconcile: child output {output_id} not found for agent {agent_id}")]
    ChildOutputNotFound {
        agent_id: AgentId,
        output_id: OutputId,
    },
}

/// One JSONL entry written by [`AgentActivities::append_decision_log`].
///
/// Wire format (one per line, no trailing newline on the last):
///
/// ```json
/// {"tick": 0, "decision_summary": "Idle { 50ms }", "ts": "2026-05-25T12:00:00Z"}
/// ```
///
/// Pinned as a typed struct (not free-form JSON) so the TUI phase 1
/// reader has a stable shape. `#[non_exhaustive]` reserves room for
/// per-tick health / cost meters (stage 7.6's "calibration" surface)
/// without a wire break.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub struct DecisionLogEntry {
    pub tick: u64,
    pub decision_summary: String,
    pub ts: DateTime<Utc>,
}

impl DecisionLogEntry {
    /// Convenience constructor for the workflow body call site.
    pub fn new(tick: u64, decision_summary: String, ts: DateTime<Utc>) -> Self {
        Self {
            tick,
            decision_summary,
            ts,
        }
    }
}

// ---------------------------------------------------------------------------
// Test-injectable decision script
//
// Lives outside the impl block because activity bodies are free functions
// over a value-typed registered instance (smoke § 3.4) — external
// observation/control of the registered `AgentActivities` value isn't
// available, so a process-wide static is the SDK-blessed workaround. The
// scripted decide_next_action activity consults this before returning the
// fallback `Decision::Idle`.

static DECISION_SCRIPT: OnceLock<Mutex<VecDeque<Decision>>> = OnceLock::new();

fn script_handle() -> &'static Mutex<VecDeque<Decision>> {
    DECISION_SCRIPT.get_or_init(|| Mutex::new(VecDeque::new()))
}

/// Install a script of decisions the [`AgentActivities::decide_next_action`]
/// stub returns, in order. Tests call this *before* starting the workflow.
///
/// When the script is empty, the activity falls back to its canned
/// `Decision::Idle { next_after: 1s }` so a misconfigured test doesn't
/// hang. To reset between tests, pass an empty `Vec`.
pub fn set_decision_script(script: Vec<Decision>) {
    let mut q = script_handle()
        .lock()
        .expect("DECISION_SCRIPT mutex poisoned");
    *q = script.into();
}

/// Pop the next scripted decision, or `None` if the script is empty.
fn pop_scripted_decision() -> Option<Decision> {
    script_handle()
        .lock()
        .expect("DECISION_SCRIPT mutex poisoned")
        .pop_front()
}

/// Substantive body of [`AgentActivities::apply_fs_ops`], factored out so
/// hermetic unit tests can drive it against a `MemoryStorage` backend
/// directly without the live-test-only `ActivityContext` indirection.
///
/// Builds an `AgentFs` over `storage` at the per-agent prefix and forwards
/// the op batch. Returns `anyhow::Result<()>` so the activity-level `?`
/// lifts the error into `ActivityError::Application(...)` via the SDK's
/// blanket impl.
async fn apply_fs_ops_impl(
    storage: std::sync::Arc<dyn jarvis_node::storage::AgentStorage>,
    input: ApplyFsOpsInput,
) -> anyhow::Result<()> {
    let fs = AgentFs::new_with_storage(storage, &input.fs_handle.prefix, &input.mandate).await?;
    fs.apply_ops(input.ops).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Activity body helpers
//
// Free functions extracted from the activity bodies so hermetic tests can
// exercise the FS-touching logic without constructing an `ActivityContext`
// (which has no `Default` impl and a non-trivial Core-tied constructor) or
// installing the process-wide `OnceLock<AgentStorage>` (which would race
// the `worker::install_then_access_*` test that already installs it in
// the lib test binary). The activity body is a 3-line wrapper around
// these helpers; the helpers carry the real shape.

/// Stage 3.8 helper — open an `AgentFs` over `storage` at `prefix` and
/// persist `content` as an output whose provenance trail is `evidence`.
/// Returns the `OutputId`, which post-JAR2-70 is `sha256(content, evidence)`
/// — see the `persist_output` doc comment for the idempotency contract.
///
/// `AgentFs::persist_output` rejects:
/// - Empty `evidence` (`FsError::EmptyEvidence`).
/// - Any cited id whose `evidence/<id>.json` is absent
///   (`FsError::EvidenceNotFound`).
///
/// Both errors propagate via `?` through `anyhow::Error` →
/// `ActivityError::Application`. The workflow body's next-tick
/// correction-context staging (JAR2-60 `dispatch_call_tools`) is the
/// agent-loop's mechanism for surfacing these failures to the LLM; the
/// activity itself just reports.
/// JAR2-68 helper — append a single [`DecisionLogEntry`] to the per-tick
/// JSONL file at `<prefix>/decisions/<tick>.jsonl`. Stage 3.12 / plan
/// § 8 decision 6.
///
/// Each tick gets its own file with exactly one line (one decision).
/// This keeps Temporal-retry idempotency trivial: a retry of this
/// activity with the same `(tick, decision_summary, ts)` triple PUTs
/// byte-identical bytes via [`AgentStorage::put`]. The `ts` arrives
/// from the workflow's `ctx.info().scheduled_time` for the same
/// deterministic-across-retries property `persist_retirement` enforces.
///
/// The single-line-per-file shape is deliberate: a per-prefix append-
/// log against a KV backend that has no native append would require a
/// read-modify-write loop with optimistic concurrency, which is more
/// machinery than this artifact justifies. The TUI phase 1 reader
/// concatenates the files in tick order.
pub(crate) async fn append_decision_log_impl(
    storage: Arc<dyn AgentStorage>,
    prefix: &str,
    entry: &DecisionLogEntry,
) -> anyhow::Result<()> {
    let fs = AgentFs::attach(storage, prefix);
    let prefix = fs.prefix(); // canonicalized with trailing '/'
    let key = format!("{prefix}decisions/{tick}.jsonl", tick = entry.tick);
    let line = serde_json::to_string(entry)?;
    // No trailing newline — one line per file, the TUI reader concatenates.
    fs.storage()
        .put(&key, bytes::Bytes::from(line.into_bytes()))
        .await?;
    Ok(())
}

pub(crate) async fn persist_output_impl(
    storage: Arc<dyn AgentStorage>,
    prefix: &str,
    content: &str,
    evidence: &[EvidenceId],
) -> anyhow::Result<OutputId> {
    // Placeholder mandate matches `assemble_context`'s stub — `AgentFs`
    // only writes `mandate.json` when absent, so the real mandate
    // persisted by JAR2-61's `assemble_context` (or a prior boot of
    // this same agent) is not clobbered when this activity opens the
    // FS to persist an output.
    let mandate = Mandate::new("", Duration::ZERO, None);
    let fs = AgentFs::new_with_storage(storage, prefix, &mandate).await?;
    let output = fs.persist_output(content, evidence).await?;
    Ok(output.id)
}

// ---------------------------------------------------------------------------
// Activity impl
//
// `AgentActivities` is the new value-typed activity bundle replacing
// JAR2-58's `NoopActivities`. The macro impls
// `ActivityImplementer for AgentActivities`; `register_activities` wraps
// in `Arc` internally (smoke § 3.4 — passing `Arc<AgentActivities>` is a
// type error).
//
// Every body is a stub returning canned `Ok(...)`. The real bodies land
// in JAR2-61..66; each one will:
//
// - `assemble_context` (JAR2-61): open the per-agent `AgentFs` from
//   `fs_handle`, apply any drained `mandate_patches`, fold `human_ops`
//   into the `Trigger` stream, call `agent_core::drain_triggers`.
// - `decide_next_action` (JAR2-62): construct an `LlmDecide` from `cfg`
//   (model routing, system prompt), call `.decide(bundle)`.
// - `execute_tool` (JAR2-63): resolve `cfg.tools` against the registry,
//   dispatch one `ToolCall`, record_evidence on success.
// - `persist_output` (JAR2-64): re-open `AgentFs`, call `persist_output`.
// - `apply_fs_ops` (JAR2-65): re-open `AgentFs`, call `apply_ops`.
// - `persist_retirement` (JAR2-66): re-open `AgentFs`, call `persist_retirement`.

/// Activity bundle registered on the worker. Replaces JAR2-58's
/// `NoopActivities` — the bare value passes through `register_activities`
/// unchanged (smoke § 3.4).
pub struct AgentActivities;

#[activities]
impl AgentActivities {
    /// Stage 3.5 (JAR2-61). Build a per-tick [`AgentFs`] over the
    /// worker-shared `AgentStorage` (JAR2-69) at the input's prefix, fold
    /// drained `human_ops` into the `Trigger::HumanOverride` taxonomy,
    /// then delegate to [`agent_core::drain_triggers`] for the
    /// FS-assemble that yields the warm `ContextBundle`.
    ///
    /// **Mandate patches.** Drained off the `mandate_update` signal
    /// queue and surfaced on the input for stage 6 — the activity logs
    /// the count and drops them today. Wiring the consumption (apply
    /// patch → re-resolve routing → re-open FS) is JAR2-67+ territory.
    ///
    /// **FS open is idempotent** — `AgentFs::new_with_storage` only
    /// writes `mandate.json` when absent, so passing the workflow's
    /// mandate through on every tick is correct. The cost is one storage
    /// `get` per tick + a one-time put on first open per agent.
    ///
    /// **`tokio` async is fine here** — activity bodies live outside
    /// workflow-replay determinism rules; the workflow itself is the
    /// piece that may only use `temporalio_sdk::workflows::*` primitives.
    #[activity]
    pub async fn assemble_context(
        _ctx: ActivityContext,
        input: AssembleContextInput,
    ) -> Result<AssembleContextOutput, ActivityError> {
        let storage = crate::worker::agent_storage();
        let fs = AgentFs::new_with_storage(storage, input.fs_handle.prefix.clone(), &input.mandate)
            .await?;

        // Fold drained `human_ops` into the trigger stream as
        // `Trigger::HumanOverride { op }`. Appended after the regular
        // triggers so ordering matches the in-process loop (which sees
        // every signal serialized through one mpsc receiver in arrival
        // order).
        let mut triggers = input.triggers;
        triggers.extend(
            input
                .human_ops
                .into_iter()
                .map(|op| Trigger::HumanOverride { op }),
        );

        if !input.mandate_patches.is_empty() {
            tracing::debug!(
                count = input.mandate_patches.len(),
                "assemble_context: dropping mandate_patches (stage 6 territory)"
            );
        }

        let bundle =
            agent_core::drain_triggers(triggers, &fs, &input.mandate, input.prior_correction)
                .await?;
        Ok(AssembleContextOutput { bundle })
    }

    /// Stage 3.6 (JAR2-62). Wraps the process-wide [`Decide`] impl
    /// installed via [`crate::worker::install_decide`] (typically an
    /// `LlmDecide` over a vendor `ModelClient`).
    ///
    /// **Script-first.** The activity consults the test-injected
    /// [`DECISION_SCRIPT`] *before* reaching for the installed
    /// implementation. This is load-bearing: the live `workflow_loop`
    /// test scripts every decision the workflow will ask for, and a
    /// real LLM call would defeat both the test's determinism and the
    /// CI envelope (no API keys, no network). The static-script
    /// injection path predates JAR2-62 and must keep working — see
    /// `tests/workflow_loop.rs` for the call site.
    ///
    /// **Error classification.** When the installed `Decide`
    /// implementation returns an `anyhow::Error`, the activity
    /// classifies it by downcasting to `&ModelError` (the typed error
    /// the `LlmDecide` adapter passes through from
    /// `ModelClient::complete`):
    ///
    /// - `ModelError::Transport` / `ModelError::RateLimit` →
    ///   **retryable**. The Temporal worker will reschedule the
    ///   activity per the workflow-side `ActivityOptions::retry_policy`
    ///   (default Temporal policy today; per-activity tuning is a
    ///   follow-up — see PR summary).
    /// - `ModelError::Auth` / `ModelError::Parse` /
    ///   `ModelError::Other` → **non-retryable**. Bad credentials,
    ///   malformed responses, and vendor-specific 4xxs don't get
    ///   better by retrying.
    /// - Downcast fails (e.g. `LlmDecide`'s "parse failed on all N
    ///   attempts" `anyhow!` after exhausting the inner correction
    ///   loop) → **non-retryable**. Validation failures bubble as
    ///   activity-layer failures so the workflow body can stage a
    ///   correction context on the next tick rather than retrying the
    ///   same broken decision in place (guardrail 3 of the ticket).
    ///
    /// **Heartbeats** are deliberately omitted in this revision. The
    /// activity timeout is 30s (`workflow::ACTIVITY_TIMEOUT`), which
    /// comfortably brackets a normal LLM call (sub-10s for short
    /// prompts); a long-running streaming variant would need
    /// heartbeats, but the batch-shape `ModelClient` doesn't.
    #[activity]
    pub async fn decide_next_action(
        _ctx: ActivityContext,
        input: DecideInput,
    ) -> Result<Decision, ActivityError> {
        // Script-first (guardrail 5). If a scripted decision is
        // queued, return it without touching the installed `Decide`.
        if let Some(d) = pop_scripted_decision() {
            return Ok(d);
        }

        let decide = crate::worker::decide_impl();
        decide_with(decide.as_ref(), input)
            .await
            .map_err(classify_decide_error)
    }

    /// Stage 3.7 (JAR2-63). Real body: dispatches one `ToolCall` through
    /// the process-wide [`ToolRegistry`] (installed at worker boot via
    /// [`crate::worker::install_tool_registry`]) and, on success,
    /// persists the resulting `EvidenceRecord` via the per-agent
    /// `AgentFs` facade backed by the installed
    /// [`crate::worker::agent_storage`].
    ///
    /// One activity invocation per `ToolCall`; the workflow body fans
    /// out N calls via `workflows::join_all` and stages a
    /// `CorrectionContext` for next tick when any of them surface as
    /// `Failure`. See [`crate::workflow::dispatch_call_tools`].
    ///
    /// **Retry layering.** Tool calls themselves are dispatched
    /// single-shot from this activity — `McpTool` (the production
    /// `ToolRegistry` entry built by `register_mcp_server_with_policy`)
    /// already runs its own `RetryPolicy` loop inside `Tool::call`
    /// (`crates/jarvis_node/src/mcp/tool.rs` `call_with_retry`). Adding
    /// a second retry loop here would compound those retries
    /// multiplicatively. The per-call surface this activity returns —
    /// `Success { evidence_id }` or `Failure { failure }` — already
    /// matches the in-process `agent_core::dispatch_call_tools`
    /// post-retry shape. The outer Temporal retry on activity errors
    /// (heartbeat timeout, worker crash) stays safe because evidence
    /// is content-addressed: a retried activity invocation with the
    /// same `(tool, args, result)` triple resolves to the same
    /// `EvidenceId` and `AgentFs::record_evidence` is idempotent via
    /// `put_if_absent` (`crates/jarvis_node/src/fs.rs`).
    ///
    /// **Tool error → Failure (not ActivityError).** A tool that
    /// errors after its own retry exhaustion does **not** surface as
    /// `ActivityError` (which would trip Temporal's outer retry —
    /// pointless work, given the inner retry already gave up). It
    /// returns `Ok(ToolCallOutcome::Failure { failure })` so the
    /// workflow body folds it into a `CorrectionContext` and the next
    /// tick's LLM sees the failure. This mirrors the in-process
    /// `DispatchOutcome::ToolError` semantics from
    /// `agent_core::dispatch_call_tools` (`scratch/temporal_staged_plan.md`
    /// § 2.5; JAR2-38). A tool name unknown to the registry takes the
    /// same path; the in-process loop returned `NeedsCorrection` for
    /// that case via a batch-wide pre-check, but at the per-call
    /// granularity the unknown-name failure is observationally
    /// identical to any other call-time error from the LLM's
    /// perspective.
    ///
    /// **Mandate placeholder.** `ExecuteToolInput.cfg` is the
    /// JAR2-60-era `AgentConfig {}` empty struct; promotion to
    /// `Mandate` lands later in the stack. `AgentFs::new_with_storage`
    /// still wants a `&Mandate` to seed an `mandate.json` write on
    /// first open — but that write is idempotent (read-then-PUT-only-
    /// if-absent), so a placeholder mandate here cannot corrupt
    /// whatever the agent's mandate-bearing path (e.g. JAR2-61's
    /// `assemble_context`) wrote first. Matches the same trick the
    /// `assemble_context` stub uses to construct its placeholder
    /// `ContextBundle.mandate`.
    ///
    /// **Heartbeats deferred.** `ActivityContext::record_heartbeat`
    /// exists on the pinned SDK (verified against
    /// `temporalio-sdk-0.4.0/src/activities.rs:170`), but with
    /// today's bootstrap tool surface — `EchoTool` (microseconds) and
    /// `McpTool` (sub-second under the default retry policy of
    /// 3×50ms) — neither approaches the workflow-set 30s
    /// start-to-close timeout. Add a heartbeat loop here when a
    /// tool's expected duration approaches or exceeds the timeout
    /// (e.g. JAR2-68's MCP-server wiring with a long-running fetch).
    #[activity]
    pub async fn execute_tool(
        _ctx: ActivityContext,
        input: ExecuteToolInput,
    ) -> Result<ToolCallOutcome, ActivityError> {
        let registry = crate::worker::tool_registry();
        // One-shot dispatch — the tool implementation owns its retry
        // policy (see McpTool::call). Wrapping in another retry here
        // would compound them multiplicatively.
        let call_result = registry
            .call(&input.call.name, input.call.args.clone())
            .await;
        match call_result {
            Ok(record) => {
                // Persist evidence via the per-agent AgentFs facade.
                // Construction is idempotent against the prefix's
                // mandate file (read-then-PUT-only-if-absent), so the
                // placeholder mandate below cannot overwrite a real
                // mandate already on disk.
                let storage = crate::worker::agent_storage();
                let placeholder_mandate = Mandate::new("", Duration::ZERO, None);
                let fs = AgentFs::new_with_storage(
                    storage,
                    input.fs_handle.prefix.clone(),
                    &placeholder_mandate,
                )
                .await
                .map_err(|e| ActivityError::from(anyhow::anyhow!("agent_fs open failed: {e:#}")))?;
                let evidence_id = fs.record_evidence(record).await.map_err(|e| {
                    ActivityError::from(anyhow::anyhow!("record_evidence failed: {e:#}"))
                })?;
                Ok(ToolCallOutcome::Success { evidence_id })
            }
            Err(e) => Ok(ToolCallOutcome::Failure {
                failure: ToolCallFailure {
                    tool: input.call.name.clone(),
                    args: input.call.args.clone(),
                    error: format!("{e:#}"),
                },
            }),
        }
    }

    /// Stage 3.8 (JAR2-64). Real body: opens an [`AgentFs`] over the
    /// process-wide [`AgentStorage`] backend (installed by the worker
    /// boot or a test harness) at the agent's prefix, then delegates to
    /// [`AgentFs::persist_output`] — which enforces the provenance
    /// contract from JAR2-4 (every cited `EvidenceId` must resolve to a
    /// file in `evidence/`) and updates the outputs tail-index from
    /// JAR2-54.
    ///
    /// **Mandate placeholder.** `AgentFs::new_with_storage` only writes
    /// `mandate.json` if the file is absent; the placeholder here is a
    /// no-op when JAR2-61's `assemble_context` has already persisted the
    /// real mandate. Sibling JAR2-61 will swap `PersistOutputInput.cfg`
    /// to the real `Mandate` shape; until that lands, this body matches
    /// the same placeholder `assemble_context` uses today.
    ///
    /// **Idempotency (JAR2-70).** `OutputId::new(content, evidence)` is
    /// content-addressed, and `AgentFs::persist_output` uses
    /// `put_if_absent`, so a Temporal retry of a successful FS write +
    /// failed activity ack returns the same `OutputId` and does not
    /// land a second file or shuffle the tail-index entry. Two ticks
    /// that emit byte-identical `(content, evidence)` also collapse to
    /// one file.
    #[activity]
    pub async fn persist_output(
        _ctx: ActivityContext,
        input: PersistOutputInput,
    ) -> Result<OutputId, ActivityError> {
        let storage = agent_storage();
        let id = persist_output_impl(
            storage,
            &input.fs_handle.prefix,
            &input.content,
            &input.evidence,
        )
        .await?;
        Ok(id)
    }

    /// Stage 3.9 (JAR2-65). Reify an [`AgentFs`] over the worker-shared
    /// storage at the agent's prefix and apply the batch of [`FsOp`]s.
    ///
    /// Path validation (no traversal, must live under `notes/`) is
    /// enforced inside [`AgentFs::apply_ops`]; this body does no
    /// re-validation. The activity has no idempotency primitive of its
    /// own — Temporal retries (heartbeat timeout, worker restart) re-run
    /// the activity body, but `FsOp` is deterministic state: replaying
    /// the same set of writes/deletes against the same prefix produces
    /// the same file state. Mutable, not content-addressed; effectively
    /// idempotent for the load-bearing case (Temporal-retry).
    ///
    /// Error mapping: [`apply_fs_ops_impl`] returns `anyhow::Result<()>`,
    /// which `?` lifts into `ActivityError::Application(...)` via the
    /// SDK's blanket `From<E> for ActivityError where E: Into<anyhow::Error>`.
    /// Typed `FsError::PathTraversal` / `FsError::PathOutsideNotes` /
    /// `FsError::Storage` all surface as application failures, which is
    /// what Temporal expects from an activity-level reject.
    ///
    /// Body delegates to the free-function [`apply_fs_ops_impl`] so the
    /// unit tests at the bottom of this module can exercise the storage
    /// roundtrip without needing to construct an `ActivityContext`
    /// (which requires an `Arc<CoreWorker>` and is therefore not
    /// hermetically buildable in a `#[test]`).
    #[activity]
    pub async fn apply_fs_ops(
        _ctx: ActivityContext,
        input: ApplyFsOpsInput,
    ) -> Result<(), ActivityError> {
        apply_fs_ops_impl(crate::worker::agent_storage(), input).await?;
        Ok(())
    }

    /// Stage 3.10 (JAR2-66). Real body — write `retirement.json` via
    /// [`AgentFs::persist_retirement`] using a deterministic timestamp
    /// drawn from `ctx.info().scheduled_time`.
    ///
    /// # Why `AgentFs::attach` (not `new_with_storage`)
    ///
    /// `new_with_storage` reads-or-writes `mandate.json` to confirm the
    /// per-agent FS is initialized. At the retirement-signal short-
    /// circuit (workflow.rs `Decision::Retire` arm or the `retire`
    /// signal short-circuit ahead of `assemble_context`) no `Mandate`
    /// is in scope — the workflow body never loaded one. `attach` is
    /// the strictly weaker constructor that skips the mandate write
    /// and the tail-index reconciliation. The retirement path writes
    /// exactly one key (`retirement.json`) and exits, so neither side
    /// effect is required.
    ///
    /// # Why `scheduled_time` (not `Utc::now()`)
    ///
    /// `Utc::now()` inside an activity body is wall-clock time at
    /// execution. If the activity fails and Temporal retries it, the
    /// retry attempt's `Utc::now()` differs from the first attempt's.
    /// Two attempts that both reach the `put` call would write
    /// different bytes to `retirement.json` — defeating the workflow-
    /// replay byte-identicality property the rest of the kernel
    /// promises. `ctx.info().scheduled_time` is stamped from workflow
    /// history (when the workflow *scheduled* the activity), so it is
    /// stable across retries.
    ///
    /// Fallback path: if `scheduled_time` is `None` (test harnesses
    /// that bypass the worker's activity-info plumbing, or an SDK that
    /// hasn't filled it in), we synthesize `Utc::now()` so the body
    /// still completes. This costs the replay-determinism property in
    /// that edge case; loud telemetry would make sense once
    /// observability lands (out of scope here per JAR2-66 guardrail 1).
    #[activity]
    pub async fn persist_retirement(
        ctx: ActivityContext,
        input: PersistRetirementInput,
    ) -> Result<(), ActivityError> {
        persist_retirement_inner(&input, ctx.info().scheduled_time).await?;
        Ok(())
    }

    /// Stage 3.12 (JAR2-68) — append a one-line JSONL entry describing
    /// the decision the workflow just took to
    /// `<prefix>/decisions/<tick>.jsonl`. Plan § 8 decision 6.
    ///
    /// One activity invocation per tick, called from the workflow body
    /// after `decide_next_action` returns (before the match-on-`Decision`
    /// arms run). Idempotency is trivial: `<tick>.jsonl` is a per-tick
    /// file containing exactly one line, and the timestamp is sourced
    /// from `ctx.info().scheduled_time` so retries PUT byte-identical
    /// bytes (same property `persist_retirement` enforces).
    ///
    /// **Fallback path** (mirror of `persist_retirement`): if
    /// `scheduled_time` is `None` — test harnesses that bypass the
    /// worker's activity-info plumbing — we synthesize `Utc::now()`.
    /// Costs the replay-determinism property in that edge case; live
    /// production workers always have `scheduled_time` filled in.
    #[activity]
    pub async fn append_decision_log(
        ctx: ActivityContext,
        input: AppendDecisionLogInput,
    ) -> Result<(), ActivityError> {
        let ts: DateTime<Utc> = ctx
            .info()
            .scheduled_time
            .map(DateTime::<Utc>::from)
            .unwrap_or_else(Utc::now);
        let entry = DecisionLogEntry::new(input.tick, input.decision_summary, ts);
        append_decision_log_impl(agent_storage(), &input.fs_handle.prefix, &entry).await?;
        Ok(())
    }

    /// JAR2-80 (stage 5.3) — write the child's `agents` row + the
    /// parent → child `edges` row into the structural DB, returning
    /// the freshly-allocated `AgentId`.
    ///
    /// Routed through the worker-shared [`crate::worker::StructuralDbStore`]
    /// trait object (installed at worker boot via
    /// [`crate::worker::install_structural_db_store`]). The trait
    /// lives in `jarvis_temporal` rather than `jarvis_node` because
    /// `jarvis_graph::GraphStore` (the production impl) already
    /// imports from `jarvis_temporal`'s public API — taking
    /// `jarvis_graph` as a dep here would cycle. See
    /// `crate::worker::StructuralDbStore`'s doc for the dependency-
    /// direction rationale.
    ///
    /// The activity does **not** write `mandate.json` to the child's
    /// FS — that's the child workflow's first-run `assemble_context`
    /// (JAR2-61) job per Stage 5 Project decision 9. The activity's
    /// scope is structural state only.
    ///
    /// Idempotency: not provided. Both writes are FK-bound — a
    /// retried activity invocation with a re-allocated child UUID
    /// would create a duplicate child row + duplicate edge. The
    /// alternative — content-addressed `(graph_id, child_agent_name,
    /// parent_agent_id)` keys — would require the DB schema to grow
    /// a non-null `name`-uniqueness constraint per graph, which v1's
    /// schema deliberately doesn't enforce (operator may want two
    /// children with the same name). For now, Temporal's at-most-once
    /// activity completion semantics keep the duplication
    /// vanishingly rare; if it surfaces, the right fix is the schema
    /// migration, not a workaround here.
    #[activity]
    pub async fn register_child_in_structural_db(
        _ctx: ActivityContext,
        input: RegisterChildInStructuralDbInput,
    ) -> Result<RegisterChildInStructuralDbOutput, ActivityError> {
        let store = structural_db_store();
        let out = register_child_in_structural_db_impl(store, input).await?;
        Ok(out)
    }

    /// JAR2-82 (stage 5.5) — fold N cited child outputs into the
    /// parent's `evidence/` directory as synthetic evidence records
    /// (Stage 5 Project decision 3). One activity invocation per
    /// `Decision::ReconcileChildren`; the workflow body does NOT
    /// push the resulting evidence into any workflow-state slot —
    /// the parent's next-tick `assemble_context` picks the synthetic
    /// records up via the existing `list_recent_evidence` window.
    ///
    /// Conflict-record writing stays in JAR2-83 (5.6). This activity
    /// leaves a `tracing::warn!` at the call site when
    /// `input.conflict.is_some()` and always returns `conflict_id:
    /// None` — see [`reconcile_children_impl`] for the rationale.
    ///
    /// Error mapping: only typed [`ReconciliationError`]s surface as
    /// `ActivityError::Application(non_retryable)` — the workflow
    /// body catches the failure and stages a `CorrectionContext` for
    /// the next tick (mirroring the existing tool-failure correction-
    /// context flow). `non_retryable` because `ChildOutputNotFound` is
    /// structural — re-running the activity with the same id won't
    /// make it resolve; the LLM must emit a satisfiable decision on
    /// the next tick.
    ///
    /// Every other error (storage backend errors, serde failures from
    /// the cross-agent read, `record_evidence` write failures) is
    /// surfaced as a *retryable* `ApplicationFailure` so Temporal's
    /// default retry policy gets a chance — a transient infra blip
    /// shouldn't be misreported to the LLM as a provenance miss.
    ///
    /// `now` is sourced from `ctx.info().scheduled_time` so a retry
    /// of this activity (transient worker failure between record-
    /// evidence completion and ack) PUTs byte-identical bytes under
    /// the same content-addressed `EvidenceId` — see the activity
    /// helper doc for the determinism argument.
    #[activity]
    pub async fn reconcile_children(
        ctx: ActivityContext,
        input: ReconcileChildrenInput,
    ) -> Result<ReconcileChildrenOutput, ActivityError> {
        let now: DateTime<Utc> = ctx
            .info()
            .scheduled_time
            .map(DateTime::<Utc>::from)
            .unwrap_or_else(Utc::now);
        let storage = agent_storage();
        match reconcile_children_impl(storage, input, now).await {
            Ok(out) => Ok(out),
            Err(e) => {
                let failure = if e.downcast_ref::<ReconciliationError>().is_some() {
                    ApplicationFailure::non_retryable(e)
                } else {
                    // Transient storage / serde / record_evidence
                    // error — retryable so Temporal can re-run the
                    // activity (idempotency: synthetic evidence is
                    // content-addressed via `record_evidence`'s
                    // `put_if_absent`, so a retry that completes
                    // after a previous partial isn't duplicating
                    // records).
                    ApplicationFailure::new(e)
                };
                Err(ActivityError::application(failure))
            }
        }
    }
}

/// JAR2-80 (stage 5.3) — substantive body of
/// [`AgentActivities::register_child_in_structural_db`], factored out so
/// hermetic / DB-backed integration tests can drive it against any
/// [`crate::worker::StructuralDbStore`] without an `ActivityContext`
/// (unconstructable outside a Worker per smoke § 3.4) or the
/// process-wide `OnceLock` install (which would race the worker-test
/// install). The activity-level wrapper above is a 3-line shim around
/// this helper.
///
/// Mirrors the helper-extraction shape already in use for
/// `apply_fs_ops_impl`, `persist_output_impl`, `append_decision_log_impl`,
/// `persist_retirement_inner`.
pub async fn register_child_in_structural_db_impl(
    store: std::sync::Arc<dyn crate::worker::StructuralDbStore>,
    input: RegisterChildInStructuralDbInput,
) -> anyhow::Result<RegisterChildInStructuralDbOutput> {
    let child_agent_id = store
        .add_agent(
            input.parent_graph_id,
            &input.child_agent_name,
            input.child_mandate_ref.as_deref(),
        )
        .await?;
    store
        .add_edge(input.parent_agent_id, child_agent_id)
        .await?;
    Ok(RegisterChildInStructuralDbOutput { child_agent_id })
}

/// JAR2-82 (stage 5.5) — substantive body of
/// [`AgentActivities::reconcile_children`], factored out so hermetic
/// unit tests can drive it against a `MemoryStorage` backend without
/// constructing an `ActivityContext` or racing the worker's
/// process-wide storage install.
///
/// Per-source loop:
///
/// 1. Open the child's FS read-only via
///    [`AgentFs::open_for_agent`] (Stage 5 Project decision 6's
///    flat workflow-id prefix — same shape `FsHandle::for_agent`
///    minted at spawn time). No `mandate.json` read, no tail-index
///    work — point lookup only.
/// 2. Read the cited [`Output`](jarvis_node::mandate::Output) via
///    [`AgentFs::read_output`]. On miss, return
///    [`ReconciliationError::ChildOutputNotFound`] so the workflow
///    body's correction path takes over.
/// 3. Build a synthetic [`EvidenceRecord`] with
///    `tool = "reconcile"`, the `(child_agent_id, child_workflow_id,
///    source_output_id)` triple as `args`, and the serialized child
///    `Output` as `result`. `EvidenceId` is content-addressed over
///    `(tool, args, result)` — same convention as every other
///    evidence record on disk, so the parent's existing provenance
///    contract (`persist_output` rejects unresolvable evidence ids)
///    keeps working with zero extensions.
/// 4. Write the synthetic record to the **parent's** `evidence/`
///    directory via [`AgentFs::record_evidence`]. Returns the
///    `EvidenceId`, which the activity collects into the output's
///    `synthetic_evidence` vector.
///
/// Conflict-record write: if `input.conflict.is_some()`, emit a
/// `tracing::warn!` and return `conflict_id: None`. JAR2-83 (5.6)
/// fills in the actual writer; this ticket leaves the call site as a
/// TODO so 5.6 has one obvious place to land.
///
/// **Error discipline.** Only a genuine `FsError::OutputNotFound`
/// (the cited child output doesn't resolve on the child's FS) is
/// wrapped as the typed [`ReconciliationError::ChildOutputNotFound`]
/// — that signals an LLM-level mistake the workflow body folds into
/// a `CorrectionContext` and Temporal does NOT retry. Every other
/// failure (storage backend errors from the cross-agent read,
/// `serde_json` failures, `record_evidence` write errors) propagates
/// as a plain `anyhow::Error`, which the activity wrapper surfaces
/// as a *retryable* `ApplicationFailure`. Conflating the two would
/// mean a transient storage blip lies to the next-tick LLM about
/// provenance AND skips Temporal's retry — both wrong.
///
/// **Pre-validation pass.** Every source is read before any
/// `record_evidence` write so a single bad source doesn't leave a
/// partial trail of synthetic evidence on the parent's FS. Cost: one
/// extra in-memory traversal per source over `sources.len()`;
/// negligible at the typical fan-in of a few children. The
/// alternative (write-as-you-go) would land good records before the
/// activity errored and the parent's next tick would see partial
/// provenance for a reconciliation it never completed — confusing
/// for both the LLM and human reviewers.
///
/// `now` is supplied by the caller so the activity body sources it
/// from `ctx.info().scheduled_time` (deterministic across Temporal
/// retries — load-bearing because the synthetic record's
/// `created_at` is part of the on-disk bytes and a retried activity
/// must PUT byte-identical content under the same content-addressed
/// id). The hermetic test passes a fixed timestamp.
pub async fn reconcile_children_impl(
    storage: std::sync::Arc<dyn jarvis_node::storage::AgentStorage>,
    input: ReconcileChildrenInput,
    now: DateTime<Utc>,
) -> anyhow::Result<ReconcileChildrenOutput> {
    // Parent FS — write target. `open_for_agent` uses `attach`
    // semantics (no mandate read, no tail reconcile) because the
    // parent's `assemble_context` activity has already written
    // `mandate.json` on its first tick. `record_evidence` itself
    // doesn't depend on the mandate file existing.
    let parent_fs = AgentFs::open_for_agent(
        storage.clone(),
        input.parent_graph_id,
        input.parent_agent_id,
    );

    // Phase 1: read every child output up-front. Only
    // `FsError::OutputNotFound` becomes the typed reconcile error;
    // storage / serde failures bubble as anyhow and stay retryable
    // at the activity layer.
    let mut child_outputs = Vec::with_capacity(input.sources.len());
    for source in &input.sources {
        // Cross-agent read: open the child's FS root over the shared
        // storage backend at the child's `(graph_id, agent_id)`
        // prefix. Both agents share `parent_graph_id` per Stage 5
        // Project decision 6's flat scheme (cross-graph reads are
        // out-of-scope project-wide).
        let child_fs = AgentFs::open_for_agent(
            storage.clone(),
            input.parent_graph_id,
            source.child_ref.agent_id,
        );
        let child_output = child_fs.read_output(&source.output_id).await.map_err(|e| {
            if matches!(
                e.downcast_ref::<FsError>(),
                Some(FsError::OutputNotFound(_))
            ) {
                anyhow::Error::new(ReconciliationError::ChildOutputNotFound {
                    agent_id: source.child_ref.agent_id,
                    output_id: source.output_id.clone(),
                })
            } else {
                // Storage-level failure or other infra error —
                // bubble verbatim so the activity wrapper marks it
                // retryable and Temporal can re-run the activity.
                e
            }
        })?;
        child_outputs.push(child_output);
    }

    // Phase 2: write one synthetic evidence record per source.
    // `serde_json::to_value` / `record_evidence` failures here
    // propagate as anyhow → retryable at the activity layer.
    let mut synthetic_evidence = Vec::with_capacity(input.sources.len());
    for (source, child_output) in input.sources.iter().zip(child_outputs.iter()) {
        // Synthetic evidence record. `tool = "reconcile"` is the
        // wire-locked discriminator (Stage 5 Project decision 3 +
        // JAR2-82 ticket "Decisions baked in"); do NOT introduce a
        // new EvidenceKind / sub-tool taxonomy.
        let args = serde_json::json!({
            "child_agent_id": source.child_ref.agent_id,
            "child_workflow_id": source.child_ref.workflow_id,
            "source_output_id": source.output_id,
        });
        let result = serde_json::to_value(child_output)?;
        let record = EvidenceRecord::new("reconcile", args, result, now);
        let ev_id = parent_fs.record_evidence(record).await?;
        synthetic_evidence.push(ev_id);
    }

    // Conflict-record stub. JAR2-83 (5.6) ships the writer; until
    // then we surface a tracing line so an operator can see the
    // intent landed at the boundary and was deliberately not
    // persisted. Never returns a synthetic `ConflictId` — that would
    // dangle a reference to a file that never gets written.
    if input.conflict.is_some() {
        tracing::warn!(
            "JAR2-82: conflict intent received but conflict-log writer not yet implemented (JAR2-83 / 5.6)"
        );
    }

    Ok(ReconcileChildrenOutput {
        synthetic_evidence,
        conflict_id: None,
    })
}

/// Body of [`AgentActivities::persist_retirement`], factored out so the
/// hermetic test in this file can call it without constructing an
/// `ActivityContext` (whose `pub fn new(...)` takes an `Arc<CoreWorker>`
/// — only reachable from inside a worker).
///
/// Sources the storage backend from [`agent_storage`] (the process-wide
/// `OnceLock` installed at worker boot per JAR2-69) and the timestamp
/// from `scheduled_time` — both load-bearing for the activity contract.
/// See the activity-method doc for why `scheduled_time` and not
/// `Utc::now()`.
pub async fn persist_retirement_inner(
    input: &PersistRetirementInput,
    scheduled_time: Option<std::time::SystemTime>,
) -> anyhow::Result<()> {
    let storage = agent_storage();
    let fs = AgentFs::attach(storage, &input.fs_handle.prefix);
    let retired_at: DateTime<Utc> = scheduled_time
        .map(DateTime::<Utc>::from)
        .unwrap_or_else(Utc::now);
    fs.persist_retirement(&input.reason, retired_at).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// JAR2-62 helpers — pulled out of the activity body so unit tests can call
// the inner shape directly without faking an `ActivityContext`. The split
// also keeps the `#[activity]`-decorated body short.

/// Call the supplied [`Decide`] with the activity's input. Separated
/// from [`AgentActivities::decide_next_action`] so the hermetic test in
/// this module can exercise the wiring against an arbitrary `Decide`
/// (typically a `MockDecide`) without going through the
/// `worker::decide_impl()` static — the unit test injects its own
/// dependency.
async fn decide_with(decide: &dyn Decide, input: DecideInput) -> anyhow::Result<Decision> {
    decide.decide(input.bundle).await
}

/// Map an `anyhow::Error` from `Decide::decide` to a Temporal
/// [`ActivityError`] with retryability flagged per the categorization
/// rules in [`AgentActivities::decide_next_action`].
///
/// The downcast to `&ModelError` is the contract `LlmDecide` exposes:
/// its `model_err_to_anyhow` helper wraps the typed `ModelError` via
/// `anyhow::Error::new` (see `decide_llm/llm_decide.rs::model_err_to_anyhow`),
/// so the source chain preserves the category. Non-`ModelError` causes
/// — `LlmDecide`'s "parse failed on all attempts" `anyhow!`, or any
/// other `Decide` impl's bespoke error — fall through to the
/// non-retryable default. That matches guardrail 3 of the ticket:
/// validation failures don't retry at the activity layer, they become
/// correction contexts in the next workflow tick.
fn classify_decide_error(err: anyhow::Error) -> ActivityError {
    let retryable = matches!(
        err.downcast_ref::<ModelError>(),
        Some(ModelError::Transport(_)) | Some(ModelError::RateLimit(_))
    );
    let failure = if retryable {
        ApplicationFailure::new(err)
    } else {
        ApplicationFailure::non_retryable(err)
    };
    ActivityError::application(failure)
}

// Compile-time witness that `crate::worker::decide_impl()` returns
// exactly `Arc<dyn Decide>`. Catches any future refactor that changes
// the worker-side signature out from under us — the activity body
// passes the result through `Arc::as_ref` to `decide_with`, which
// only works if the function returns an `Arc`-shaped trait object.
// The closure is never invoked; `let _ = ...` only references the
// function item, so the static analysis fires at compile time and
// nothing runs at startup. (Important: invoking `decide_impl()` here
// would panic when no `Decide` is installed.)
const _: fn() = || {
    fn assert_arc_dyn_decide() -> Arc<dyn Decide> {
        crate::worker::decide_impl()
    }
    let _ = assert_arc_dyn_decide;
};

#[cfg(test)]
mod tests {
    //! Hermetic unit coverage for the activity surface.
    //!
    //! The activity bodies are stubs; tests assert (a) the
    //! `set_decision_script` / pop pair round-trips a scripted decision,
    //! (b) the canned-fallback fires when the script is empty, and (c)
    //! every input/output type round-trips through serde (Temporal's
    //! payload codec uses serde under the hood). The live tests in
    //! `tests/workflow_loop.rs` exercise the activities through the real
    //! workflow against a Temporal Server.

    use super::*;
    use jarvis_node::decision::MockDecide;
    use serde_json::json;

    // Serializes the two tests below that mutate the process-wide
    // `DECISION_SCRIPT` static. Without this they race under cargo's
    // default parallel runner (CI hit it; locally they happened to
    // schedule far enough apart to pass).
    static SCRIPT_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Build an empty `ContextBundle` for tests that exercise the
    /// activity body. `Mandate::new("", Duration::ZERO, None)` is the
    /// cheapest valid construction (mirrors the stub fallback in
    /// `assemble_context`).
    fn empty_bundle() -> ContextBundle {
        ContextBundle {
            mandate: Mandate::new("", Duration::ZERO, None),
            triggers: Vec::new(),
            recent_outputs: Vec::new(),
            recent_evidence: Vec::new(),
            open_claims: Vec::new(),
            correction: None,
        }
    }

    #[test]
    fn decision_script_round_trips_in_order() {
        let _g = SCRIPT_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        // Reset (subsequent tests inherit process state).
        set_decision_script(vec![]);

        set_decision_script(vec![
            Decision::Idle {
                next_after: Duration::from_millis(100),
            },
            Decision::Retire {
                reason: "test".into(),
            },
        ]);
        let first = pop_scripted_decision();
        assert!(matches!(
            first,
            Some(Decision::Idle {
                next_after,
            }) if next_after == Duration::from_millis(100)
        ));
        let second = pop_scripted_decision();
        assert!(matches!(second, Some(Decision::Retire { reason }) if reason == "test"));
        // Drained — falls back to None.
        assert!(pop_scripted_decision().is_none());
    }

    #[test]
    fn decision_script_resets_between_tests() {
        let _g = SCRIPT_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        set_decision_script(vec![Decision::Retire {
            reason: "first".into(),
        }]);
        set_decision_script(vec![Decision::Idle {
            next_after: Duration::from_secs(5),
        }]);
        // Second `set_decision_script` replaces, not appends.
        let got = pop_scripted_decision();
        assert!(
            matches!(got, Some(Decision::Idle { next_after }) if next_after == Duration::from_secs(5))
        );
        assert!(pop_scripted_decision().is_none());
    }

    #[test]
    fn assemble_context_input_empty_buckets_pin_shape() {
        // JAR2-61 dropped the `Default` derive on `AssembleContextInput`
        // when promoting `cfg: AgentConfig` → `mandate: Mandate` (the
        // real `Mandate` has no `Default`). The empty-bucket invariant
        // is preserved via explicit construction so a future refactor
        // that adds a non-`Default` field has to think about the bucket
        // init the same way.
        let i = AssembleContextInput {
            mandate: Mandate::new("", Duration::ZERO, None),
            fs_handle: FsHandle::default(),
            triggers: Vec::new(),
            human_ops: Vec::new(),
            mandate_patches: Vec::new(),
            prior_correction: None,
        };
        assert!(i.triggers.is_empty());
        assert!(i.human_ops.is_empty());
        assert!(i.mandate_patches.is_empty());
        assert!(i.prior_correction.is_none());
    }

    #[test]
    fn assemble_context_input_round_trips_through_json() {
        let i = AssembleContextInput {
            mandate: Mandate::new("test", Duration::from_millis(100), Some(4)),
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            triggers: vec![Trigger::ScheduledWake],
            human_ops: vec![HumanOp::new(json!({"action": "pause"}))],
            mandate_patches: vec![MandatePatch::new(json!({"model": "x"}))],
            prior_correction: Some(CorrectionContext::new("prior failure")),
        };
        let s = serde_json::to_string(&i).unwrap();
        let back: AssembleContextInput = serde_json::from_str(&s).unwrap();
        assert_eq!(i, back);
    }

    #[test]
    fn tool_call_outcome_round_trips_through_json() {
        let id = EvidenceId::new("t", &json!({"a": 1}), &json!({"r": 1}));
        let oc = ToolCallOutcome::Success {
            evidence_id: id.clone(),
        };
        let s = serde_json::to_string(&oc).unwrap();
        assert!(s.contains("\"outcome\":\"success\""), "wire shape: {s}");
        let _back: ToolCallOutcome = serde_json::from_str(&s).unwrap();

        let f = ToolCallFailure {
            tool: "errbomb".into(),
            args: json!({"x": 1}),
            error: "boom".into(),
        };
        let oc2 = ToolCallOutcome::Failure { failure: f };
        let s2 = serde_json::to_string(&oc2).unwrap();
        assert!(s2.contains("\"outcome\":\"failure\""), "wire shape: {s2}");
        let _back2: ToolCallOutcome = serde_json::from_str(&s2).unwrap();
    }

    #[test]
    fn execute_tool_input_round_trips_through_json() {
        use jarvis_node::decision::ClaimSeed;
        let i = ExecuteToolInput {
            cfg: AgentConfig::default(),
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            call: ToolCall::new("echo", json!({"msg": "hi"}), ClaimSeed::new("s")),
        };
        let s = serde_json::to_string(&i).unwrap();
        let _back: ExecuteToolInput = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn persist_output_input_round_trips_through_json() {
        let id = EvidenceId::new("t", &json!({}), &json!({}));
        let i = PersistOutputInput {
            cfg: AgentConfig::default(),
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            content: "claim".into(),
            evidence: vec![id],
        };
        let s = serde_json::to_string(&i).unwrap();
        let _back: PersistOutputInput = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn apply_fs_ops_input_round_trips_through_json() {
        let i = ApplyFsOpsInput {
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            mandate: Mandate::new("test", Duration::from_millis(100), None),
            ops: vec![FsOp::WriteFile {
                path: "n/x.md".into(),
                content: "hi".into(),
            }],
        };
        let s = serde_json::to_string(&i).unwrap();
        let _back: ApplyFsOpsInput = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn persist_retirement_input_round_trips_through_json() {
        let i = PersistRetirementInput {
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            reason: "done".into(),
        };
        let s = serde_json::to_string(&i).unwrap();
        let _back: PersistRetirementInput = serde_json::from_str(&s).unwrap();
    }

    // ---------- JAR2-62: decide_with + classify_decide_error -------------

    /// Bespoke `Decide` impl that returns the supplied error verbatim on
    /// every `decide` call. Lets us drive the activity body's error
    /// classification path without standing up a full `LlmDecide` over
    /// a `MockModelClient` (cross-crate; lives in `decide_llm` tests).
    struct ErrDecide {
        make_err: fn() -> anyhow::Error,
    }

    #[async_trait::async_trait]
    impl Decide for ErrDecide {
        async fn decide(&self, _ctx: ContextBundle) -> anyhow::Result<Decision> {
            Err((self.make_err)())
        }
    }

    /// Happy path: `decide_with` forwards the bundle to the trait
    /// method and returns the trait's decision verbatim. Uses
    /// `MockDecide` (the in-tree scripted impl from
    /// `jarvis_node::decision`) so this test never touches a real
    /// vendor or its features.
    #[tokio::test]
    async fn decide_with_returns_trait_decision_on_success() {
        let want = Decision::Idle {
            next_after: Duration::from_millis(250),
        };
        let decide: Arc<dyn Decide> = Arc::new(MockDecide::new(vec![want.clone()]));
        let input = DecideInput {
            bundle: empty_bundle(),
        };
        let got = decide_with(decide.as_ref(), input).await.unwrap();
        assert_eq!(got, want);
    }

    /// Transport failures classify as retryable. The activity body
    /// surfaces this via `ActivityError::Application(_)` carrying an
    /// `ApplicationFailure` with `is_non_retryable() == false`, so
    /// Temporal's default retry policy will reschedule.
    #[test]
    fn classify_decide_error_transport_is_retryable() {
        let err = anyhow::Error::new(ModelError::Transport("DNS failure".into()));
        let activity_err = classify_decide_error(err);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(
            !failure.is_non_retryable(),
            "Transport errors must be retryable"
        );
    }

    /// Rate-limit failures classify as retryable. Same shape as
    /// Transport; vendor-side backoff handling lives outside the
    /// activity (and is out of scope for JAR2-62 — see PR summary
    /// about the missing per-activity retry policy).
    #[test]
    fn classify_decide_error_rate_limit_is_retryable() {
        let err = anyhow::Error::new(ModelError::RateLimit("slow down".into()));
        let activity_err = classify_decide_error(err);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(
            !failure.is_non_retryable(),
            "RateLimit errors must be retryable"
        );
    }

    /// Auth failures classify as non-retryable. Bad credentials don't
    /// fix themselves; surface to the workflow body as a terminal
    /// activity failure on the first attempt.
    #[test]
    fn classify_decide_error_auth_is_non_retryable() {
        let err = anyhow::Error::new(ModelError::Auth("ANTHROPIC_API_KEY missing".into()));
        let activity_err = classify_decide_error(err);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(
            failure.is_non_retryable(),
            "Auth errors must be non-retryable"
        );
    }

    /// `Parse` and `Other` failures classify as non-retryable. Bad
    /// response shapes and vendor-specific 4xxs don't get better by
    /// retrying; the workflow body's next-tick correction is the right
    /// place to surface them.
    #[test]
    fn classify_decide_error_parse_and_other_are_non_retryable() {
        for err in [
            anyhow::Error::new(ModelError::Parse("bad JSON".into())),
            anyhow::Error::new(ModelError::Other("4xx".into())),
        ] {
            let activity_err = classify_decide_error(err);
            let ActivityError::Application(failure) = activity_err else {
                panic!("expected ActivityError::Application");
            };
            assert!(
                failure.is_non_retryable(),
                "Parse/Other errors must be non-retryable"
            );
        }
    }

    // ---- JAR2-65: apply_fs_ops hermetic coverage -----------------------
    //
    // Exercise the substantive `apply_fs_ops_impl` body against a
    // `MemoryStorage` backend. Bypasses `worker::agent_storage()` (the
    // process-wide `OnceLock` is consumed by `worker::tests`) and the
    // `ActivityContext` (unconstructable without `Arc<CoreWorker>`).
    // Both happy-path and traversal-rejection are covered here; the live
    // path through Temporal is in `tests/workflow_loop.rs`.
    //
    // Imports `AgentStorage`/`Arc` already in scope via `use super::*`;
    // `FsError` and `MemoryStorage` are imported by JAR2-64's tests
    // block further down — no duplicate `use` here.

    fn fresh_storage_and_input(ops: Vec<FsOp>) -> (Arc<dyn AgentStorage>, ApplyFsOpsInput) {
        let storage: Arc<dyn AgentStorage> = Arc::new(MemoryStorage::new());
        let input = ApplyFsOpsInput {
            fs_handle: FsHandle {
                prefix: "graphs/g/agents/a".into(),
            },
            mandate: Mandate::new("hermetic", Duration::from_millis(100), None),
            ops,
        };
        (storage, input)
    }

    #[tokio::test]
    async fn apply_fs_ops_writes_both_notes_files_under_prefix() {
        let (storage, input) = fresh_storage_and_input(vec![
            FsOp::WriteFile {
                path: "notes/a.md".into(),
                content: "alpha".into(),
            },
            FsOp::WriteFile {
                path: "notes/sub/b.md".into(),
                content: "bravo".into(),
            },
        ]);

        apply_fs_ops_impl(storage.clone(), input)
            .await
            .expect("apply_fs_ops_impl");

        // Both files land at the agent-prefixed `notes/` key. Hit the
        // backend directly so we don't accidentally couple the assertion
        // to `AgentFs` read methods.
        let a = storage
            .get("graphs/g/agents/a/notes/a.md")
            .await
            .expect("get a")
            .expect("a present");
        assert_eq!(a.as_ref(), b"alpha");
        let b = storage
            .get("graphs/g/agents/a/notes/sub/b.md")
            .await
            .expect("get b")
            .expect("b present");
        assert_eq!(b.as_ref(), b"bravo");
    }

    #[tokio::test]
    async fn apply_fs_ops_rejects_traversal_and_leaves_fs_untouched() {
        // First write a known-good note so we can prove the second
        // batch's traversal op didn't clobber it.
        let (storage, seed_input) = fresh_storage_and_input(vec![FsOp::WriteFile {
            path: "notes/keep.md".into(),
            content: "preserved".into(),
        }]);
        apply_fs_ops_impl(storage.clone(), seed_input)
            .await
            .expect("seed apply_fs_ops_impl");

        let traversal_input = ApplyFsOpsInput {
            fs_handle: FsHandle {
                prefix: "graphs/g/agents/a".into(),
            },
            mandate: Mandate::new("hermetic", Duration::from_millis(100), None),
            ops: vec![FsOp::WriteFile {
                path: "../outside.md".into(),
                content: "escape".into(),
            }],
        };
        let err = apply_fs_ops_impl(storage.clone(), traversal_input)
            .await
            .expect_err("traversal op must reject");
        let downcast = err.downcast_ref::<FsError>().expect("typed FsError");
        assert!(
            matches!(downcast, FsError::PathTraversal(_)),
            "expected PathTraversal, got {downcast:?}"
        );

        // Original file unchanged.
        let still_there = storage
            .get("graphs/g/agents/a/notes/keep.md")
            .await
            .expect("get keep")
            .expect("keep present");
        assert_eq!(still_there.as_ref(), b"preserved");

        // No `../outside.md`-shaped key landed under the agent prefix
        // (or at the root). Scan via `list` because escape keys could
        // appear anywhere.
        let all = storage
            .list("", None, usize::MAX)
            .await
            .expect("list all keys");
        for key in &all.keys {
            assert!(
                !key.contains("outside"),
                "traversal write leaked to backend: {key}"
            );
        }
    }

    /// Non-`ModelError` causes (e.g. `LlmDecide`'s parse-exhaustion
    /// `anyhow!(...)`) classify as non-retryable. Guardrail 3:
    /// validation failures don't retry at the activity layer; they
    /// become correction contexts on the next workflow tick.
    #[test]
    fn classify_decide_error_non_model_error_is_non_retryable() {
        let err = anyhow::anyhow!("LlmDecide: parse failed on all 2 attempt(s)");
        let activity_err = classify_decide_error(err);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(
            failure.is_non_retryable(),
            "non-ModelError causes must be non-retryable"
        );
    }

    /// End-to-end of the `decide_with` + `classify_decide_error` pair:
    /// when the trait returns a transport-flavored error, the call
    /// site lands on a retryable `ApplicationFailure`. Closes the loop
    /// with the same shape the `#[activity]` body uses.
    #[tokio::test]
    async fn decide_with_then_classify_transport_yields_retryable_failure() {
        let decide: Arc<dyn Decide> = Arc::new(ErrDecide {
            make_err: || anyhow::Error::new(ModelError::Transport("downstream 503".into())),
        });
        let input = DecideInput {
            bundle: empty_bundle(),
        };
        let raw = decide_with(decide.as_ref(), input).await.unwrap_err();
        let activity_err = classify_decide_error(raw);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(!failure.is_non_retryable());
    }

    /// End-to-end with a parse-exhaustion `anyhow!` (the canonical
    /// `LlmDecide` validation-failure shape). The activity layer must
    /// surface this as non-retryable so the workflow body's correction
    /// path takes over on the next tick.
    #[tokio::test]
    async fn decide_with_then_classify_validation_yields_non_retryable_failure() {
        let decide: Arc<dyn Decide> = Arc::new(ErrDecide {
            make_err: || anyhow::anyhow!("LlmDecide: parse failed on all 2 attempts"),
        });
        let input = DecideInput {
            bundle: empty_bundle(),
        };
        let raw = decide_with(decide.as_ref(), input).await.unwrap_err();
        let activity_err = classify_decide_error(raw);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(failure.is_non_retryable());
    }

    // -----------------------------------------------------------------
    // JAR2-64 — `persist_output_impl` hermetic coverage.
    //
    // The tests below exercise the activity-body logic through the
    // extracted free helper so they don't need an `ActivityContext`
    // (no `Default` impl, non-trivial Core-tied construction) or the
    // process-wide `OnceLock<AgentStorage>` install path (which
    // `worker::install_then_access_*` already touches in the same
    // test binary). Each test creates its own `MemoryStorage` and
    // exercises the storage-prefix shape `<graph_id>/<agent_id>/`.

    use chrono::Utc;
    use jarvis_node::evidence::EvidenceRecord;
    use jarvis_node::fs::FsError;
    use jarvis_node::storage::MemoryStorage;

    /// Plant an evidence record under `prefix` so a subsequent
    /// `persist_output_impl` referencing the returned id passes the
    /// provenance check. Shared between the happy-path test and the
    /// failure tests so the planting shape doesn't drift between them.
    async fn plant_evidence(
        storage: Arc<dyn jarvis_node::storage::AgentStorage>,
        prefix: &str,
        tool: &str,
        args: serde_json::Value,
        result: serde_json::Value,
    ) -> EvidenceId {
        // Open an `AgentFs` over the *same* storage Arc + prefix the
        // activity body will open against. This is the load-bearing
        // shape: a separate `MemoryStorage` instance would never share
        // evidence with the activity's view because `MemoryStorage` is
        // in-process state, not a connected backend.
        let mandate = Mandate::new("plant", Duration::from_millis(0), None);
        let fs = AgentFs::new_with_storage(storage, prefix, &mandate)
            .await
            .expect("open planting AgentFs");
        let rec = EvidenceRecord::new(tool, args, result, Utc::now());
        fs.record_evidence(rec).await.expect("plant evidence")
    }

    #[tokio::test]
    async fn persist_output_impl_writes_output_with_resolved_evidence() {
        let storage: Arc<dyn jarvis_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let prefix = "graphs/g1/agents/a1/";

        // Plant two evidence records the output will cite.
        let id_a = plant_evidence(
            storage.clone(),
            prefix,
            "tool_a",
            json!({"q": "alpha"}),
            json!({"r": 1}),
        )
        .await;
        let id_b = plant_evidence(
            storage.clone(),
            prefix,
            "tool_b",
            json!({"q": "beta"}),
            json!({"r": 2}),
        )
        .await;
        assert_ne!(id_a, id_b);

        let out_id = persist_output_impl(
            storage.clone(),
            prefix,
            "claim X",
            &[id_a.clone(), id_b.clone()],
        )
        .await
        .expect("persist_output_impl ok");

        // Inspect what landed via a fresh `AgentFs` view over the same
        // storage. `list_recent_outputs` exercises the tail-index path
        // from JAR2-54 too — proving the activity body inherits that
        // wiring for free.
        let mandate = Mandate::new("inspect", Duration::from_millis(0), None);
        let fs = AgentFs::new_with_storage(storage, prefix, &mandate)
            .await
            .unwrap();
        let outs = fs.list_recent_outputs(8).await.expect("list outputs");
        assert_eq!(outs.len(), 1, "expected exactly one output on disk");
        let on_disk = &outs[0];
        assert_eq!(
            on_disk.id, out_id,
            "OutputId returned must match on-disk file"
        );
        assert_eq!(on_disk.content, "claim X");
        assert!(
            on_disk.evidence.contains(&id_a) && on_disk.evidence.contains(&id_b),
            "output must cite both planted evidence ids"
        );
    }

    #[tokio::test]
    async fn persist_output_impl_rejects_unresolved_evidence_id() {
        let storage: Arc<dyn jarvis_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let prefix = "graphs/g1/agents/a-missing/";

        // Reference an evidence id that was never planted — the
        // `AgentFs::persist_output` provenance check fires.
        let bogus = EvidenceId::new("tool_x", &json!({}), &json!({"never": "written"}));
        let err = persist_output_impl(storage.clone(), prefix, "claim Y", &[bogus.clone()])
            .await
            .expect_err("must fail on unresolved evidence id");
        let typed = err.downcast_ref::<FsError>().expect("typed FsError");
        match typed {
            FsError::EvidenceNotFound(missing) => assert_eq!(missing, &bogus),
            other => panic!("expected EvidenceNotFound, got {other:?}"),
        }

        // No output written.
        let mandate = Mandate::new("inspect", Duration::from_millis(0), None);
        let fs = AgentFs::new_with_storage(storage, prefix, &mandate)
            .await
            .unwrap();
        let outs = fs.list_recent_outputs(8).await.unwrap();
        assert!(outs.is_empty(), "no output should have been written");
    }

    #[tokio::test]
    async fn persist_output_impl_rejects_empty_evidence_list() {
        // Provenance contract from JAR2-4: an output with no evidence
        // is rejected before the file write. The activity body
        // inherits this — Temporal sees the error, the workflow's
        // next-tick correction-context staging gets the message.
        let storage: Arc<dyn jarvis_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let prefix = "graphs/g1/agents/a-empty/";

        let err = persist_output_impl(storage, prefix, "claim Z", &[])
            .await
            .expect_err("must fail on empty evidence");
        let typed = err.downcast_ref::<FsError>().expect("typed FsError");
        assert!(matches!(typed, FsError::EmptyEvidence));
    }

    // ---- JAR2-65: apply_fs_ops additional coverage (path validation + replay) ----

    #[tokio::test]
    async fn apply_fs_ops_rejects_path_outside_notes() {
        let (storage, input) = fresh_storage_and_input(vec![FsOp::WriteFile {
            path: "outputs/x.json".into(),
            content: "wrong dir".into(),
        }]);
        let err = apply_fs_ops_impl(storage.clone(), input)
            .await
            .expect_err("non-notes path must reject");
        let downcast = err.downcast_ref::<FsError>().expect("typed FsError");
        assert!(
            matches!(downcast, FsError::PathOutsideNotes(_)),
            "expected PathOutsideNotes, got {downcast:?}"
        );
    }

    #[tokio::test]
    async fn apply_fs_ops_replay_is_idempotent_for_writes() {
        // Models the Temporal retry path: same input, applied twice,
        // must leave file state identical.
        let (storage, _) = fresh_storage_and_input(vec![]);
        let ops_a = vec![
            FsOp::WriteFile {
                path: "notes/a.md".into(),
                content: "v1".into(),
            },
            FsOp::WriteFile {
                path: "notes/b.md".into(),
                content: "v1".into(),
            },
        ];
        let input_a = ApplyFsOpsInput {
            fs_handle: FsHandle {
                prefix: "graphs/g/agents/a".into(),
            },
            mandate: Mandate::new("hermetic", Duration::from_millis(100), None),
            ops: ops_a,
        };
        apply_fs_ops_impl(storage.clone(), input_a.clone())
            .await
            .unwrap();
        apply_fs_ops_impl(storage.clone(), input_a).await.unwrap();

        let a = storage
            .get("graphs/g/agents/a/notes/a.md")
            .await
            .unwrap()
            .unwrap();
        let b = storage
            .get("graphs/g/agents/a/notes/b.md")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a.as_ref(), b"v1");
        assert_eq!(b.as_ref(), b"v1");
    }

    // ---- JAR2-68: append_decision_log hermetic coverage ----------------

    /// `append_decision_log_impl` writes exactly `<prefix>decisions/<tick>.jsonl`
    /// containing one JSON line that deserializes back to the same entry.
    /// Mirror shape of `persist_retirement_inner` coverage — same
    /// `MemoryStorage`-backed roundtrip, no `ActivityContext` plumbing.
    #[tokio::test]
    async fn append_decision_log_impl_writes_per_tick_jsonl() {
        let storage: Arc<dyn jarvis_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let prefix = "graphs/g/agents/a";
        let ts = DateTime::parse_from_rfc3339("2026-05-25T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let entry = DecisionLogEntry::new(7, "Idle { 50ms }".into(), ts);
        append_decision_log_impl(storage.clone(), prefix, &entry)
            .await
            .expect("append_decision_log_impl ok");

        // File lands at `<prefix>/decisions/<tick>.jsonl` with the
        // single JSON line we wrote.
        let key = "graphs/g/agents/a/decisions/7.jsonl";
        let bytes = storage
            .get(key)
            .await
            .expect("storage.get ok")
            .unwrap_or_else(|| panic!("expected key {key}"));
        let line = std::str::from_utf8(bytes.as_ref()).unwrap();
        // No trailing newline; one JSONL line.
        assert!(
            !line.ends_with('\n'),
            "no trailing newline in single-line file, got: {line:?}"
        );
        let parsed: DecisionLogEntry = serde_json::from_str(line).unwrap();
        assert_eq!(parsed, entry);
    }

    /// Temporal-retry idempotency proxy: re-running the helper with the
    /// same `(tick, decision_summary, ts)` triple writes byte-identical
    /// bytes. This is the load-bearing property `append_decision_log`'s
    /// real activity body inherits by sourcing `ts` from
    /// `ctx.info().scheduled_time` (stable across retries).
    #[tokio::test]
    async fn append_decision_log_impl_is_idempotent_on_replay() {
        let storage: Arc<dyn jarvis_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let prefix = "graphs/g/agents/replay";
        let ts = DateTime::parse_from_rfc3339("2026-05-25T13:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let entry = DecisionLogEntry::new(0, "Retire { 'done' }".into(), ts);
        append_decision_log_impl(storage.clone(), prefix, &entry)
            .await
            .unwrap();
        let first = storage
            .get("graphs/g/agents/replay/decisions/0.jsonl")
            .await
            .unwrap()
            .unwrap();
        append_decision_log_impl(storage.clone(), prefix, &entry)
            .await
            .unwrap();
        let second = storage
            .get("graphs/g/agents/replay/decisions/0.jsonl")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.as_ref(), second.as_ref());
    }

    // ---- JAR2-80: register_child_in_structural_db_impl hermetic test ----

    /// In-memory `StructuralDbStore` fake. Records every `add_agent` /
    /// `add_edge` call so the hermetic tests can assert against them
    /// without spinning up Postgres. Mirrors the role
    /// `MemoryStorage` plays for the `AgentStorage` test surface.
    /// One recorded call to [`MemoryStructuralDbStore::add_agent`].
    /// Extracted to a struct (rather than a 4-tuple field on the fake)
    /// to keep clippy's `type_complexity` lint happy and to give the
    /// assertion below readable field names.
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RecordedAgent {
        graph_id: GraphId,
        name: String,
        mandate_ref: Option<String>,
        allocated_id: AgentId,
    }

    struct MemoryStructuralDbStore {
        agents: std::sync::Mutex<Vec<RecordedAgent>>,
        edges: std::sync::Mutex<Vec<(AgentId, AgentId)>>,
    }

    impl MemoryStructuralDbStore {
        fn new() -> Self {
            Self {
                agents: std::sync::Mutex::new(Vec::new()),
                edges: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::worker::StructuralDbStore for MemoryStructuralDbStore {
        async fn add_agent(
            &self,
            graph_id: GraphId,
            name: &str,
            mandate_ref: Option<&str>,
        ) -> anyhow::Result<AgentId> {
            let id = AgentId::new(uuid::Uuid::new_v4());
            self.agents.lock().unwrap().push(RecordedAgent {
                graph_id,
                name: name.to_string(),
                mandate_ref: mandate_ref.map(str::to_string),
                allocated_id: id,
            });
            Ok(id)
        }

        async fn add_edge(
            &self,
            parent_agent_id: AgentId,
            child_agent_id: AgentId,
        ) -> anyhow::Result<()> {
            self.edges
                .lock()
                .unwrap()
                .push((parent_agent_id, child_agent_id));
            Ok(())
        }
    }

    /// Activity-body hermetic coverage: the helper writes one agent
    /// row + one parent → child edge with the right endpoints, and
    /// the returned child id matches the recorded agent row's id.
    /// Mirrors the helper-extraction shape of
    /// `apply_fs_ops_impl_*` tests (drive the substantive logic against
    /// an in-memory backend, no `ActivityContext`).
    #[tokio::test]
    async fn register_child_in_structural_db_impl_records_agent_name_and_edge_endpoints() {
        let fake = std::sync::Arc::new(MemoryStructuralDbStore::new());
        let store: std::sync::Arc<dyn crate::worker::StructuralDbStore> = fake.clone();

        let parent_graph_id = GraphId::new(uuid::Uuid::new_v4());
        let parent_agent_id = AgentId::new(uuid::Uuid::new_v4());

        let out = register_child_in_structural_db_impl(
            store,
            RegisterChildInStructuralDbInput {
                parent_graph_id,
                parent_agent_id,
                child_agent_name: "fetcher".into(),
                child_mandate_ref: Some("v1".into()),
            },
        )
        .await
        .expect("activity body ok");

        let agents = fake.agents.lock().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].graph_id, parent_graph_id);
        assert_eq!(agents[0].name, "fetcher");
        assert_eq!(agents[0].mandate_ref.as_deref(), Some("v1"));
        assert_eq!(agents[0].allocated_id, out.child_agent_id);

        let edges = fake.edges.lock().unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].0, parent_agent_id);
        assert_eq!(edges[0].1, out.child_agent_id);
    }

    // ---- JAR2-80: register_child_in_structural_db wire-shape tests ----

    /// Pin the wire shape of the new activity's input/output types so
    /// a future field addition (e.g. inheritance metadata) shows up as
    /// a test miss. The activity body itself can't be hermetically
    /// driven without constructing an `ActivityContext` (unbuildable
    /// outside a Worker per smoke § 3.4) + installing a process-wide
    /// `StructuralDbStore` (would race the JAR2-80 worker tests that
    /// already install one); the live coverage lives in the live
    /// integration test gated on `TEMPORAL_LIVE_TEST=1` +
    /// `DATABASE_URL`.
    #[test]
    fn register_child_input_round_trips_through_json() {
        use uuid::Uuid;
        let i = RegisterChildInStructuralDbInput {
            parent_graph_id: GraphId::new(
                Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap(),
            ),
            parent_agent_id: AgentId::new(
                Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap(),
            ),
            child_agent_name: "fetcher".into(),
            child_mandate_ref: Some("v1".into()),
        };
        let s = serde_json::to_string(&i).unwrap();
        let back: RegisterChildInStructuralDbInput = serde_json::from_str(&s).unwrap();
        assert_eq!(i, back);
        // The mandate_ref Option is on the wire (no `skip_serializing_if`).
        assert!(
            s.contains("\"child_mandate_ref\":\"v1\""),
            "wire shape: {s}"
        );
    }

    #[test]
    fn register_child_input_round_trips_with_no_mandate_ref() {
        use uuid::Uuid;
        let i = RegisterChildInStructuralDbInput {
            parent_graph_id: GraphId::new(Uuid::nil()),
            parent_agent_id: AgentId::new(Uuid::nil()),
            child_agent_name: "x".into(),
            child_mandate_ref: None,
        };
        let s = serde_json::to_string(&i).unwrap();
        let back: RegisterChildInStructuralDbInput = serde_json::from_str(&s).unwrap();
        assert_eq!(i, back);
    }

    #[test]
    fn register_child_output_round_trips_through_json() {
        use uuid::Uuid;
        let o = RegisterChildInStructuralDbOutput {
            child_agent_id: AgentId::new(
                Uuid::parse_str("bbbbbbbb-cccc-dddd-eeee-ffffffffffff").unwrap(),
            ),
        };
        let s = serde_json::to_string(&o).unwrap();
        let back: RegisterChildInStructuralDbOutput = serde_json::from_str(&s).unwrap();
        assert_eq!(o, back);
    }

    /// Wire-shape check for `AppendDecisionLogInput` + `DecisionLogEntry`.
    /// Both cross the workflow ↔ activity boundary via Temporal's payload
    /// codec (serde-backed), so a round-trip through `serde_json` is a
    /// cheap proxy.
    #[test]
    fn decision_log_types_round_trip_through_json() {
        let i = AppendDecisionLogInput {
            fs_handle: FsHandle {
                prefix: "g/a".into(),
            },
            tick: 42,
            decision_summary: "CallTools { 3 calls }".into(),
        };
        let s = serde_json::to_string(&i).unwrap();
        let back: AppendDecisionLogInput = serde_json::from_str(&s).unwrap();
        assert_eq!(i, back);

        let ts = DateTime::parse_from_rfc3339("2026-05-25T14:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let e = DecisionLogEntry::new(42, "EmitOutput { evidence: 1 }".into(), ts);
        let s2 = serde_json::to_string(&e).unwrap();
        let back2: DecisionLogEntry = serde_json::from_str(&s2).unwrap();
        assert_eq!(e, back2);
    }

    // ---- JAR2-82 (stage 5.5): reconcile_children_impl hermetic coverage ----

    /// Deterministic timestamp for the synthetic-evidence records the
    /// reconcile activity writes — chosen so the resulting `EvidenceId`
    /// hashes (content-addressed over `(tool, args, result)`, NOT
    /// `created_at`) and the on-disk JSON bytes are byte-stable across
    /// test runs.
    fn fixed_now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-05-25T15:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    /// Plant a child agent's FS root with one persisted output (citing
    /// one planted evidence record) and return the (`child_agent_id`,
    /// child workflow id string, child `OutputId`, planted evidence
    /// id) tuple. Shared between the happy + failure-mode tests.
    async fn plant_child_output(
        storage: Arc<dyn jarvis_node::storage::AgentStorage>,
        graph_id: GraphId,
        child_agent_id: AgentId,
        content: &str,
    ) -> (String, OutputId, EvidenceId) {
        let child_prefix = format!("graphs/{graph_id}/agents/{child_agent_id}/");
        let mandate = Mandate::new("child", Duration::from_millis(0), None);
        let fs = AgentFs::new_with_storage(storage.clone(), &child_prefix, &mandate)
            .await
            .expect("open child FS");
        let ev = fs
            .record_evidence(EvidenceRecord::new(
                "echo",
                json!({"q": content}),
                json!({"r": "child result"}),
                fixed_now(),
            ))
            .await
            .expect("plant child evidence");
        let out = fs
            .persist_output(content, &[ev.clone()])
            .await
            .expect("plant child output");
        // Workflow id matches the canonical scheme that
        // `FsHandle::for_agent` mints.
        let child_workflow_id = format!("graphs/{graph_id}/agents/{child_agent_id}");
        (child_workflow_id, out.id, ev)
    }

    #[tokio::test]
    async fn reconcile_children_impl_writes_one_synthetic_evidence_per_source() {
        use jarvis_node::agent_ref::AgentRef;
        use uuid::Uuid;

        let storage: Arc<dyn jarvis_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let graph_id = GraphId::new(Uuid::new_v4());
        let parent_agent_id = AgentId::new(Uuid::new_v4());
        let child_a_id = AgentId::new(Uuid::new_v4());
        let child_b_id = AgentId::new(Uuid::new_v4());

        // Two distinct child outputs the parent will fold in.
        let (child_a_wf, child_a_out, _ev_a) =
            plant_child_output(storage.clone(), graph_id, child_a_id, "claim from A").await;
        let (child_b_wf, child_b_out, _ev_b) =
            plant_child_output(storage.clone(), graph_id, child_b_id, "claim from B").await;

        let input = ReconcileChildrenInput {
            parent_graph_id: graph_id,
            parent_agent_id,
            sources: vec![
                ReconcileSource {
                    child_ref: AgentRef::new(child_a_wf.clone(), child_a_id),
                    output_id: child_a_out.clone(),
                },
                ReconcileSource {
                    child_ref: AgentRef::new(child_b_wf.clone(), child_b_id),
                    output_id: child_b_out.clone(),
                },
            ],
            conflict: None,
        };

        let out = reconcile_children_impl(storage.clone(), input, fixed_now())
            .await
            .expect("reconcile_children_impl ok");

        assert_eq!(out.synthetic_evidence.len(), 2);
        assert!(
            out.conflict_id.is_none(),
            "JAR2-82 always returns conflict_id: None",
        );

        // Open the parent's FS and verify both synthetic evidence
        // records landed under the parent's prefix with the right
        // `tool` + `args` shape.
        let parent_view = AgentFs::open_for_agent(storage, graph_id, parent_agent_id);
        let evs = parent_view
            .list_recent_evidence(8)
            .await
            .expect("list parent evidence");
        assert_eq!(
            evs.len(),
            2,
            "expected exactly two synthetic evidence records under parent's prefix"
        );
        for ev in &evs {
            assert_eq!(
                ev.tool, "reconcile",
                "synthetic record's tool must lock to \"reconcile\""
            );
            // `args` carries the (child_agent_id, child_workflow_id,
            // source_output_id) triple. The serde wire form is a JSON
            // object; pull the fields out for spot-checks rather than
            // pinning a full string match (canonicalization sorts keys).
            let args = ev.args.as_object().expect("args is a JSON object");
            assert!(args.contains_key("child_agent_id"));
            assert!(args.contains_key("child_workflow_id"));
            assert!(args.contains_key("source_output_id"));
        }
        // Each returned EvidenceId resolves on disk under parent's prefix.
        for id in &out.synthetic_evidence {
            parent_view
                .evidence_must_exist(id)
                .await
                .unwrap_or_else(|e| panic!("synthetic evidence {id} not on disk: {e:#}"));
        }
    }

    #[tokio::test]
    async fn reconcile_children_impl_returns_typed_error_for_missing_child_output() {
        use jarvis_node::agent_ref::AgentRef;
        use uuid::Uuid;

        let storage: Arc<dyn jarvis_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let graph_id = GraphId::new(Uuid::new_v4());
        let parent_agent_id = AgentId::new(Uuid::new_v4());
        let child_agent_id = AgentId::new(Uuid::new_v4());

        // No child output planted. Synthesize an `OutputId` for content
        // we never persisted — the cross-agent read will miss.
        let bogus = OutputId::new(
            "never-written",
            &[EvidenceId::new("t", &json!({}), &json!({}))],
        );
        let child_workflow_id = format!("graphs/{graph_id}/agents/{child_agent_id}");
        let input = ReconcileChildrenInput {
            parent_graph_id: graph_id,
            parent_agent_id,
            sources: vec![ReconcileSource {
                child_ref: AgentRef::new(child_workflow_id, child_agent_id),
                output_id: bogus.clone(),
            }],
            conflict: None,
        };

        let err = reconcile_children_impl(storage.clone(), input, fixed_now())
            .await
            .expect_err("missing child output must surface typed error");
        // The helper returns `anyhow::Result`; downcast to the typed
        // `ReconciliationError` variant the activity wrapper uses for
        // the non-retryable classification.
        let typed = err
            .downcast_ref::<ReconciliationError>()
            .expect("typed ReconciliationError");
        match typed {
            ReconciliationError::ChildOutputNotFound {
                agent_id,
                output_id,
            } => {
                assert_eq!(*agent_id, child_agent_id);
                assert_eq!(*output_id, bogus);
            }
        }

        // No synthetic evidence landed on the parent's FS — the
        // helper's two-phase pre-validation pass reads every cited
        // child output before any record_evidence write fires, so a
        // single bad source short-circuits the activity without
        // leaving a partial provenance trail.
        let parent_view = AgentFs::open_for_agent(storage, graph_id, parent_agent_id);
        let evs = parent_view
            .list_recent_evidence(8)
            .await
            .expect("list parent evidence after failure");
        assert!(
            evs.is_empty(),
            "pre-validation pass: no synthetic evidence should have landed on parent after typed failure"
        );
    }

    #[tokio::test]
    async fn reconcile_children_impl_atomicity_no_partial_writes_with_good_and_bad_sources() {
        use jarvis_node::agent_ref::AgentRef;
        use uuid::Uuid;

        let storage: Arc<dyn jarvis_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let graph_id = GraphId::new(Uuid::new_v4());
        let parent_agent_id = AgentId::new(Uuid::new_v4());
        let good_child_id = AgentId::new(Uuid::new_v4());
        let bad_child_id = AgentId::new(Uuid::new_v4());
        let (good_wf, good_out, _ev) =
            plant_child_output(storage.clone(), graph_id, good_child_id, "good claim").await;
        let bad_output_id = OutputId::from_hex("de".repeat(32));
        let bad_wf = format!("graphs/{graph_id}/agents/{bad_child_id}");

        let input = ReconcileChildrenInput {
            parent_graph_id: graph_id,
            parent_agent_id,
            sources: vec![
                ReconcileSource {
                    child_ref: AgentRef::new(good_wf, good_child_id),
                    output_id: good_out,
                },
                ReconcileSource {
                    child_ref: AgentRef::new(bad_wf, bad_child_id),
                    output_id: bad_output_id.clone(),
                },
            ],
            conflict: None,
        };

        let err = reconcile_children_impl(storage.clone(), input, fixed_now())
            .await
            .expect_err("a single bad source must fail the whole activity");
        let typed = err
            .downcast_ref::<ReconciliationError>()
            .expect("typed ReconciliationError");
        assert!(
            matches!(
                typed,
                ReconciliationError::ChildOutputNotFound { agent_id, .. } if *agent_id == bad_child_id
            ),
            "typed error must point at the bad child"
        );

        // Atomicity property: the good source's synthetic evidence
        // record did NOT land on the parent (phase-1 pre-validation
        // saw the bad source's miss before any phase-2 write fired).
        let parent_view = AgentFs::open_for_agent(storage, graph_id, parent_agent_id);
        let evs = parent_view
            .list_recent_evidence(8)
            .await
            .expect("list parent evidence after partial-failure attempt");
        assert!(
            evs.is_empty(),
            "atomicity: good_source must not have left a record after bad_source missed"
        );
    }

    #[tokio::test]
    async fn reconcile_children_impl_with_conflict_intent_returns_no_conflict_id_yet() {
        // JAR2-82 stubs the conflict-record writer with a TODO + warn;
        // JAR2-83 (5.6) fills it. Confirm the activity returns
        // `conflict_id: None` even when `input.conflict.is_some()`.
        use jarvis_node::agent_ref::AgentRef;
        use jarvis_node::decision::{ConflictAlternative, ConflictRecordIntent};
        use uuid::Uuid;

        let storage: Arc<dyn jarvis_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let graph_id = GraphId::new(Uuid::new_v4());
        let parent_agent_id = AgentId::new(Uuid::new_v4());
        let child_agent_id = AgentId::new(Uuid::new_v4());
        let (child_wf, child_out, _ev) =
            plant_child_output(storage.clone(), graph_id, child_agent_id, "single claim").await;

        let conflict = ConflictRecordIntent {
            alternatives: vec![
                ConflictAlternative {
                    source_child: AgentRef::new(child_wf.clone(), child_agent_id),
                    source_output_id: child_out.clone(),
                    claim: "value is X".into(),
                },
                ConflictAlternative {
                    source_child: AgentRef::new(child_wf.clone(), child_agent_id),
                    source_output_id: child_out.clone(),
                    claim: "value is Y".into(),
                },
            ],
            resolution: None,
        };
        let input = ReconcileChildrenInput {
            parent_graph_id: graph_id,
            parent_agent_id,
            sources: vec![ReconcileSource {
                child_ref: AgentRef::new(child_wf, child_agent_id),
                output_id: child_out,
            }],
            conflict: Some(conflict),
        };
        let out = reconcile_children_impl(storage, input, fixed_now())
            .await
            .expect("reconcile with conflict intent ok");
        assert_eq!(out.synthetic_evidence.len(), 1);
        assert!(
            out.conflict_id.is_none(),
            "JAR2-82 returns conflict_id: None even when input.conflict is Some (JAR2-83 fills in writer)"
        );
    }

    // ---- JAR2-82: ReconcileChildrenInput / Output wire-shape ----

    #[test]
    fn reconcile_children_input_round_trips_through_json_with_no_conflict() {
        use jarvis_node::agent_ref::AgentRef;
        use uuid::Uuid;
        let i = ReconcileChildrenInput {
            parent_graph_id: GraphId::new(Uuid::nil()),
            parent_agent_id: AgentId::new(Uuid::nil()),
            sources: vec![ReconcileSource {
                child_ref: AgentRef::new("graphs/g/agents/c", AgentId::new(Uuid::nil())),
                output_id: OutputId::from_hex("ab".repeat(32)),
            }],
            conflict: None,
        };
        let s = serde_json::to_string(&i).unwrap();
        let back: ReconcileChildrenInput = serde_json::from_str(&s).unwrap();
        assert_eq!(i, back);
        // `skip_serializing_if` keeps the wire lean when conflict is None.
        assert!(!s.contains("conflict"), "wire shape: {s}");
    }

    #[test]
    fn reconcile_children_output_round_trips_through_json() {
        let o = ReconcileChildrenOutput {
            synthetic_evidence: vec![EvidenceId::new(
                "reconcile",
                &json!({"k": "v"}),
                &json!({"r": 1}),
            )],
            conflict_id: None,
        };
        let s = serde_json::to_string(&o).unwrap();
        let back: ReconcileChildrenOutput = serde_json::from_str(&s).unwrap();
        assert_eq!(o, back);
        assert!(!s.contains("conflict_id"), "wire shape: {s}");
    }
}

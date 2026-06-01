//! `AgentWorkflow` — workflow type, input/output shapes, signal/update surface,
//! and the per-tick loop that orchestrates the activities in [`crate::activities`].
//!
//! SDK constraints: concurrency primitives must come from
//! `temporalio_sdk::workflows` (`select!`, `join_all`) — `tokio::*` wake-ups
//! fail the workflow task. Workflow state mutation is via
//! `ctx.state_mut(|s| ...)`, not a bare `&mut self` receiver. The workflow
//! body does no I/O and consults no clocks — wall-clock arrives via
//! `ctx.timer(...)` and FS reads/writes live in activities, so the loop is
//! fully replayable against workflow history.
//!
//! Cross-workflow signaling uses a two-step SDK chain
//! `ctx.external_workflow(workflow_id, None).signal(SignalDef, payload).await`
//! — there is no single `signal_external_workflow(..)` method in this SDK
//! version. Signal failures are logged and swallowed (best-effort): the
//! sender's data is durable on its own FS regardless of whether the
//! recipient observed the signal. [`ParentRef::signal`] is informational
//! at this version — the dispatch site uses the compile-time
//! [`AgentWorkflow::external_signal`] marker regardless of the field's
//! value.
//!
//! Synthetic evidence: the parent's `reconcile_children` activity reads each
//! cited child output cross-agent and writes one synthetic
//! [`EvidenceRecord`](coral_node::evidence::EvidenceRecord) (`tool =
//! "reconcile"`) into the parent's `evidence/`. This preserves
//! [`AgentFs::persist_output`](coral_node::fs::AgentFs::persist_output)'s
//! provenance check unchanged — cross-agent provenance becomes a normal
//! evidence trail.

use std::time::Duration;

use coral_node::agent_ref::{AgentId, AgentRef, GraphId};
use coral_node::decision::{ContextBundle, CorrectionContext, Decision, ToolCall};
use coral_node::evidence::EvidenceId;
use coral_node::mandate::{Mandate, OutputId};
use coral_node::trigger::{HumanOp, MandatePatch, Trigger};
use serde::{Deserialize, Serialize};
use temporalio_common::protos::temporal::api::enums::v1::ParentClosePolicy;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ChildWorkflowOptions, ContinueAsNewOptions, SyncWorkflowContext,
    WorkflowContext, WorkflowResult,
};

use crate::activities::{
    AgentActivities, AppendDecisionLogInput, ApplyFsOpsInput, AssembleContextInput, DecideInput,
    ExecuteToolInput, PersistOutputInput, PersistRetirementInput, ReconcileChildrenInput,
    RegisterChildInStructuralDbInput, ToolCallFailure, ToolCallOutcome,
};
use coral_node::decision::{ConflictRecordIntent, ReconcileSource};

/// Resolved agent configuration handed to the workflow at start.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConfig {}

/// Storage handle scoping the agent to its `<graph_id>/<agent_id>` prefix.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsHandle {
    /// `<graph_id>/<agent_id>` — the prefix the storage trait scopes to.
    pub prefix: String,
}

impl FsHandle {
    /// Construct an [`FsHandle`] for a `(graph_id, agent_id)` pair using the
    /// canonical workflow-id prefix layout (`graphs/<graph_id>/agents/<agent_id>`).
    pub fn for_agent(graph_id: GraphId, agent_id: AgentId) -> Self {
        Self {
            prefix: agent_workflow_id(&graph_id.to_string(), &agent_id.to_string()),
        }
    }
}

/// Parent workflow reference for cross-workflow signal routing.
///
/// Populated by [`build_child_input`] so a child workflow can call
/// `WorkflowContext::external_workflow(parent_handle.workflow_id, None).signal(parent_handle.signal, ..)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentRef {
    /// Temporal workflow id of the parent — load-bearing for
    /// `external_workflow(workflow_id, None)` lookups. Flat
    /// `graphs/<gid>/agents/<aid>` form so reparenting doesn't rewrite ids.
    pub workflow_id: String,
    /// Signal name on the parent the child fires `Trigger`s through.
    /// Defaults to [`Self::DEFAULT_SIGNAL`].
    pub signal: String,
}

impl ParentRef {
    /// Default signal name routed to [`AgentWorkflow::external_signal`].
    pub const DEFAULT_SIGNAL: &'static str = "external_signal";
}

impl Default for ParentRef {
    /// Empty `workflow_id` is *not* a valid signal target — callers
    /// constructing a `ParentRef` for live use must populate `workflow_id`.
    /// The `Default` exists for serde compat and the test surface that
    /// constructs `AgentInput` with `parent_handle: None`.
    fn default() -> Self {
        Self {
            workflow_id: String::new(),
            signal: Self::DEFAULT_SIGNAL.to_string(),
        }
    }
}

/// Scheduler-state subset of the [`Carryover`].
///
/// Wraps `next_wake` in a struct (rather than a bare `Option<Duration>` on
/// `Carryover`) so future per-mandate cursor state can slot in without
/// renaming a field on the wire.
///
/// Deliberately no `last_tick_at` timestamp: a wall-clock timestamp on the
/// carryover would only be observed at encode time on the post-CAN run's
/// replay and adds zero scheduling value over `next_wake` alone — the new
/// run pins its own first-tick wake the same way the very first run does,
/// defaulting to [`INITIAL_NEXT_WAKE`] when `next_wake` is `None`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerCursor {
    /// `next_wake` cadence the previous run had pinned. `None` means the
    /// previous run never saw a `Decision::Idle` (so the new run defaults
    /// to the [`INITIAL_NEXT_WAKE`] floor on its first tick).
    pub next_wake: Option<Duration>,
}

/// Typed continue-as-new carryover.
///
/// A small, typed, deterministically-rebuildable subset of in-workflow state
/// that would otherwise be lost when `ctx.continue_as_new(...)` terminates
/// the current run. Not conversation history or tool results — those survive
/// via the per-agent FS, which is external to Temporal history.
///
/// Every field maps to a workflow-state field that the run loop observes or
/// mutates. The mapping is:
///
/// | Carryover field | Workflow-state field | Lifecycle |
/// |---|---|---|
/// | `pending_triggers` | [`AgentWorkflow::pending_triggers`] | Drained at top of each tick |
/// | `pending_human_ops` | [`AgentWorkflow::pending_human_ops`] | Drained at top of each tick |
/// | `pending_mandate_patches` | [`AgentWorkflow::pending_mandate_patches`] | Drained at top of each tick |
/// | `retirement_request` | [`AgentWorkflow::retirement_request`] | Drained at top of each tick (short-circuits) |
/// | `staged_correction` | [`AgentWorkflow::staged_correction`] | Threaded into next `assemble_context` |
/// | `scheduler_cursor` | [`AgentWorkflow::next_wake`] | Honored by the wake gate |
/// | `last_output_id` | [`AgentWorkflow::last_output_id`] | Latest persisted `EmitOutput` id |
/// | `mid_tick_evidence` | [`AgentWorkflow::mid_tick_evidence`] | EvidenceIds collected mid-tick |
/// | `cumulative_*_observed` | matching `AgentWorkflow::cumulative_*_observed` | Observability across CAN boundary |
/// | `child_handles` | [`AgentWorkflow::child_handles`] | Spawned-child handles across CAN |
///
/// `staged_correction` is preserved across CAN: dropping it would lose one
/// tick of correction context the previous run had already staged for the
/// next tick (visible behavior change).
///
/// `cumulative_*_observed` must survive CAN — without them, a snapshot taken
/// on the post-CAN run would report `cumulative_triggers_observed == 0` even
/// though the workflow lifetime had observed N signals on the pre-CAN run.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Carryover {
    pub pending_triggers: Vec<Trigger>,
    pub pending_human_ops: Vec<HumanOp>,
    pub pending_mandate_patches: Vec<MandatePatch>,
    pub retirement_request: Option<String>,
    pub staged_correction: Option<CorrectionContext>,
    pub scheduler_cursor: SchedulerCursor,
    /// Handles to spawned child agents the parent retains across
    /// continue-as-new. Each entry is an [`AgentRef`] populated by the
    /// `Decision::SpawnChild` arm of [`AgentWorkflow::run`].
    pub child_handles: Vec<AgentRef>,
    pub last_output_id: Option<OutputId>,
    /// EvidenceIds collected by activities partway through a tick that CAN
    /// fires during. Reserved for mid-tick checkpointing; today the CAN
    /// check fires at end-of-tick so this is structurally empty.
    pub mid_tick_evidence: Vec<EvidenceId>,
    /// Cumulative count of `Trigger`s observed via `external_signal` across
    /// the workflow's lifetime (including all prior CAN runs). Without
    /// this, a post-CAN snapshot would only reflect signals received on the
    /// current run, not the lifetime view.
    pub cumulative_triggers_observed: u64,
    pub cumulative_human_ops_observed: u64,
    pub cumulative_mandate_patches_observed: u64,
    /// Monotonically increasing tick counter the workflow body stamps onto
    /// every `<prefix>/decisions/<tick>.jsonl` artifact. Survives CAN so the
    /// post-CAN run continues numbering rather than clobbering pre-CAN
    /// files at `decisions/0.jsonl`.
    pub tick: u64,
}

/// Input handed to `AgentWorkflow::run` at start (and at every continue-as-new).
///
/// `carryover` is load-bearing: on hydrate the workflow body decodes it via
/// [`AgentWorkflow::hydrate_from_carryover`] back onto workflow state so
/// pending signal queues, retirement requests, `next_wake`, the
/// `staged_correction` from the previous tick, and the cumulative
/// observability counters all survive a CAN boundary. `None` means "first
/// run of this workflow" — the workflow starts from `Default` state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentInput {
    pub cfg: AgentConfig,
    pub fs_handle: FsHandle,
    pub parent_handle: Option<ParentRef>,
    pub carryover: Option<Carryover>,
    /// Resolved [`Mandate`] for this agent. The workflow body passes it into
    /// every `assemble_context` activity invocation so the LLM sees the real
    /// mandate text + idle period + max-ticks cap.
    pub mandate: Mandate,
    /// Graph this agent belongs to. Carried on `AgentInput` (rather than
    /// parsed from `ctx.workflow_id()` at activity-time) so the workflow
    /// body isn't tied to the id-scheme string format.
    pub graph_id: GraphId,
    /// This agent's structural-DB id. The `Decision::SpawnChild` arm needs
    /// the parent's `AgentId` to write the parent → child edge.
    pub agent_id: AgentId,
    /// Operator-authored agent name (the `agents[].id` from the YAML),
    /// distinct from the structural `agent_id` UUID. Used by the child →
    /// parent signal renderer for the `ChildOutput { child_name }` field.
    pub agent_name: String,
}

impl AgentInput {
    /// Test-only constructor with first-run defaults for every non-identity
    /// field (`cfg: Default`, `fs_handle: Default`, `parent_handle: None`,
    /// `carryover: None`, `mandate: Mandate::new("", ZERO, None)`), requiring
    /// the caller to supply the identity triple explicitly.
    ///
    /// Production constructors ([`build_child_input`] /
    /// [`build_root_input`]) carry a real mandate + fs_handle.
    pub fn new_for_test(
        graph_id: GraphId,
        agent_id: AgentId,
        agent_name: impl Into<String>,
    ) -> Self {
        Self {
            cfg: AgentConfig::default(),
            fs_handle: FsHandle::default(),
            parent_handle: None,
            carryover: None,
            mandate: Mandate::new("", Duration::ZERO, None),
            graph_id,
            agent_id,
            agent_name: agent_name.into(),
        }
    }
}

/// Result returned by `AgentWorkflow::run` when the workflow exits cleanly.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum AgentResult {
    /// The workflow's loop body completed because the agent retired (the
    /// `retire` signal fired or `Decision::Retire { reason }` was emitted).
    Retired { reason: String },
}

impl Default for AgentResult {
    fn default() -> Self {
        Self::Retired {
            reason: String::new(),
        }
    }
}

/// Snapshot of the workflow's signal-bucket counts + retirement flag,
/// returned by the [`AgentWorkflow::inspect_state`] update.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentSnapshot {
    /// Count of `Trigger`s currently queued in `pending_triggers`. The loop
    /// drains this at the top of every tick, so `0` here doesn't mean "the
    /// signal didn't land" — see `cumulative_triggers_observed` for the
    /// persistent "did-it-arrive?" view.
    pub pending_triggers_count: usize,
    pub pending_human_ops_count: usize,
    pub pending_mandate_patches_count: usize,
    pub retirement_request: Option<String>,
    pub recent_output_ids: Vec<String>,
    /// Cumulative count of `Trigger`s observed via `external_signal` since
    /// the workflow started (or its last continue-as-new). Bumped in the
    /// signal handler at receipt time so an inspect taken between signal
    /// arrival and the next loop drain still reflects the arrival.
    #[serde(default)]
    pub cumulative_triggers_observed: u64,
    #[serde(default)]
    pub cumulative_human_ops_observed: u64,
    #[serde(default)]
    pub cumulative_mandate_patches_observed: u64,
}

impl AgentSnapshot {
    fn from_state(workflow: &AgentWorkflow) -> Self {
        Self {
            pending_triggers_count: workflow.pending_triggers.len(),
            pending_human_ops_count: workflow.pending_human_ops.len(),
            pending_mandate_patches_count: workflow.pending_mandate_patches.len(),
            retirement_request: workflow.retirement_request.clone(),
            recent_output_ids: Vec::new(),
            cumulative_triggers_observed: workflow.cumulative_triggers_observed,
            cumulative_human_ops_observed: workflow.cumulative_human_ops_observed,
            cumulative_mandate_patches_observed: workflow.cumulative_mandate_patches_observed,
        }
    }
}

/// Build the workflow ID for an agent: `graphs/<graph_id>/agents/<agent_id>`.
pub fn agent_workflow_id(graph_id: &str, agent_id: &str) -> String {
    format!("graphs/{graph_id}/agents/{agent_id}")
}

/// `next_wake` value when the workflow state hasn't been told a specific
/// idle period yet (first tick of a run, or first tick after CAN).
/// Deliberately tiny — the first iteration's wake gate must fire
/// immediately so the workflow doesn't sit idle waiting for nothing.
const INITIAL_NEXT_WAKE: Duration = Duration::from_millis(1);

/// Per-activity start-to-close timeout. Generous so a stub activity and a
/// real activity (LLM calls, FS writes) both fit; the workflow loop's own
/// deadlines come from `next_wake` and the retirement signal.
const ACTIVITY_TIMEOUT: Duration = Duration::from_secs(30);

/// The agent workflow.
///
/// `#[derive(Default)]` is required by the SDK's `#[workflow]` macro.
#[workflow]
#[derive(Default)]
pub struct AgentWorkflow {
    /// `external_signal` queue. Pushed by the signal handler; drained at
    /// the top of every loop iteration.
    pending_triggers: Vec<Trigger>,
    /// `human_override` queue. Drained alongside `pending_triggers` and
    /// passed to `assemble_context` as a separate field.
    pending_human_ops: Vec<HumanOp>,
    /// `mandate_update` queue. Drained alongside `pending_triggers` and
    /// passed to `assemble_context` as a separate field.
    pending_mandate_patches: Vec<MandatePatch>,
    /// `retire` request. Checked at the top of every loop iteration; a set
    /// value short-circuits the tick to the retirement path.
    retirement_request: Option<String>,
    /// Wall-clock the next idle `ctx.timer(...)` waits for. Updated by
    /// `Decision::Idle { next_after }`. `None` on the very first tick of a
    /// run (the loop starts with [`INITIAL_NEXT_WAKE`] = 1ms so the first
    /// tick fires immediately). A continue-as-new preserves the prior run's
    /// `next_wake` via [`Carryover::scheduler_cursor`].
    next_wake: Option<Duration>,
    /// Correction context staged by the previous tick when its tool batch
    /// returned failures. Threaded into the next `assemble_context`
    /// activity input. Cleared on a non-failing tick.
    staged_correction: Option<CorrectionContext>,
    /// Cumulative count of `Trigger`s observed via `external_signal` since
    /// the workflow started (or last continue-as-new). Bumped inside the
    /// signal handler so a snapshot taken between signal arrival and the
    /// next loop drain still reflects the arrival.
    cumulative_triggers_observed: u64,
    cumulative_human_ops_observed: u64,
    cumulative_mandate_patches_observed: u64,
    /// Last `persist_output` `OutputId` observed by this run. The
    /// `persist_output` activity does not echo the id back into workflow
    /// state today (the field stays `None`); the slot exists so the
    /// [`Carryover`] round-trip is structurally complete.
    last_output_id: Option<OutputId>,
    /// Evidence ids collected by activities mid-tick. Empty today — the CAN
    /// check fires at end-of-tick after every activity has returned — but
    /// reserved for mid-tick checkpointing.
    mid_tick_evidence: Vec<EvidenceId>,
    /// Per-tick counter bumped at the bottom of each loop iteration.
    /// Stamped onto each `<prefix>/decisions/<tick>.jsonl` artifact via the
    /// `append_decision_log` activity. Hydrated from [`Carryover::tick`] on
    /// post-CAN runs so the artifact stream stays monotonic across the
    /// boundary.
    tick: u64,
    /// Handles to child agents this workflow has spawned via
    /// `Decision::SpawnChild`. Pushed by the spawn arm after
    /// `register_child_in_structural_db` returns the child's id and
    /// `ctx.child_workflow(..)` has dispatched the child run. Round-trips
    /// across continue-as-new via [`Carryover::child_handles`].
    child_handles: Vec<AgentRef>,
}

#[workflow_methods]
impl AgentWorkflow {
    /// `external_signal` — push a typed [`Trigger`] onto the per-tick queue.
    /// The loop drains the queue at the top of each iteration.
    ///
    /// Side-bookkeeps `cumulative_triggers_observed` at receipt time (not
    /// drain time) so the snapshot's cumulative view reflects every signal
    /// regardless of inspect timing relative to the loop.
    #[signal]
    pub fn external_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, trigger: Trigger) {
        self.pending_triggers.push(trigger);
        self.cumulative_triggers_observed = self.cumulative_triggers_observed.saturating_add(1);
    }

    /// `human_override` — push a typed [`HumanOp`] onto the override queue.
    #[signal]
    pub fn human_override(&mut self, _ctx: &mut SyncWorkflowContext<Self>, op: HumanOp) {
        self.pending_human_ops.push(op);
        self.cumulative_human_ops_observed = self.cumulative_human_ops_observed.saturating_add(1);
    }

    /// `mandate_update` — push a typed [`MandatePatch`] onto the patch queue.
    #[signal]
    pub fn mandate_update(&mut self, _ctx: &mut SyncWorkflowContext<Self>, patch: MandatePatch) {
        self.pending_mandate_patches.push(patch);
        self.cumulative_mandate_patches_observed =
            self.cumulative_mandate_patches_observed.saturating_add(1);
    }

    /// `retire` — record a retirement reason. The loop body observes
    /// `retirement_request.is_some()` at the top of every iteration and
    /// short-circuits to `persist_retirement` + return.
    #[signal]
    pub fn retire(&mut self, _ctx: &mut SyncWorkflowContext<Self>, reason: String) {
        self.retirement_request = Some(reason);
    }

    /// `inspect_state` — return a typed [`AgentSnapshot`] of the workflow's
    /// signal-bucket counts + retirement flag.
    #[update]
    pub fn inspect_state(
        &mut self,
        _ctx: &mut SyncWorkflowContext<Self>,
        _input: (),
    ) -> AgentSnapshot {
        AgentSnapshot::from_state(self)
    }

    /// Workflow entry point — the per-tick loop body.
    ///
    /// Reads top-to-bottom: hydrate carryover (if any) → loop {wake → drain
    /// → assemble → decide → dispatch → (maybe) continue-as-new}. Every
    /// external action (FS read/write, LLM call, tool dispatch) lives in an
    /// activity; the workflow body is pure orchestration.
    ///
    /// Continue-as-new shape:
    ///
    /// 1. On entry, if `input.carryover.is_some()`, the workflow state is
    ///    hydrated from it via [`hydrate_from_carryover`].
    /// 2. At end-of-tick (after the activity for the current decision
    ///    returned, *and only on non-retirement ticks*),
    ///    [`temporalio_sdk::WorkflowContext::continue_as_new_suggested`] is
    ///    consulted. If true, the workflow's current state is encoded into a
    ///    fresh [`Carryover`] via [`encode_carryover`] and passed to
    ///    `ctx.continue_as_new(&next_input, opts)`.
    ///
    /// Retirement structurally cannot trigger CAN: both retirement paths
    /// (`drained.retirement` short-circuit at the top, and
    /// `Decision::Retire { reason }` at the bottom of the `match`) return
    /// before the CAN check.
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: AgentInput,
    ) -> WorkflowResult<AgentResult> {
        if let Some(c) = input.carryover.clone() {
            ctx.state_mut(|s| s.hydrate_from_carryover(c));
        }
        loop {
            // `max_ticks` is the kernel-level guardrail stop (the only one
            // besides a retire signal). Checked before `wait_for_tick` so an
            // over-budget agent retires without waking or burning a decide
            // call, mirroring the in-process loop. `tick` is the hydrated
            // state value, so the cap spans a continue-as-new.
            let tick = ctx.state(|s| s.tick);
            if let Some(reason) = max_ticks_retire_reason(tick, input.mandate.max_ticks) {
                return retire(ctx, &input, reason).await;
            }

            wait_for_tick(ctx).await;

            // Retirement short-circuit fires before any activity invocation
            // and before any CAN check, so a `retire` signal can never
            // trigger a continue-as-new.
            let mut drained = ctx.state_mut(drain_buckets);
            if let Some(reason) = drained.retirement {
                return retire(ctx, &input, reason).await;
            }
            synthesize_scheduled_wake(&mut drained);

            let bundle = assemble(ctx, &input, drained).await?;
            let decision = decide(ctx, bundle).await?;
            // Append `<prefix>/decisions/<tick>.jsonl` entry BEFORE the
            // dispatch arm runs so the artifact lands even if a downstream
            // activity errors out and short-circuits the workflow. The
            // activity sources its timestamp from
            // `ctx.info().scheduled_time` so Temporal retries write
            // byte-identical bytes. The model's actual decision is logged
            // here even when the next line demotes a persistent agent's
            // `Retire`, so the artifact preserves what the model chose.
            log_decision(ctx, &input.fs_handle, tick, &decision).await?;
            let decision = demote_retire_if_persistent(decision, &input.mandate);
            match decision {
                Decision::CallTools { calls } => dispatch_call_tools(ctx, &input, calls).await?,
                Decision::EmitOutput { content, evidence } => {
                    emit_output(ctx, &input, content, evidence).await?;
                    ctx.state_mut(clear_correction);
                }
                Decision::RewriteFs { ops } => {
                    rewrite_fs(ctx, &input.fs_handle, ops).await?;
                    ctx.state_mut(clear_correction);
                }
                Decision::Idle { next_after } => ctx.state_mut(|s| {
                    s.next_wake = Some(next_after);
                    s.staged_correction = None;
                }),
                Decision::Retire { reason } => {
                    // The Retire arm short-circuits before the tick-bump
                    // below, so the decision-log entry just written above
                    // is the last artifact this run produces.
                    return retire(ctx, &input, reason).await;
                }
                // `staged_correction` is NOT cleared on SpawnChild: a
                // successful spawn does not satisfy a previously-staged
                // tool-failure correction (the correction is about the
                // parent's *own* prior failed tool call), and clearing it
                // would silently swallow next-tick LLM context. The
                // correction clears naturally on the next `EmitOutput` /
                // `RewriteFs` / `Idle` arm.
                Decision::SpawnChild {
                    agent_name,
                    mandate,
                } => {
                    spawn_child(ctx, &input, agent_name, mandate).await?;
                }
                Decision::ReconcileChildren { sources, conflict } => {
                    reconcile_children(ctx, &input, sources, conflict).await;
                }
                // `staged_correction` is NOT cleared here for the same
                // reason `SpawnChild` doesn't clear it: a RetireChild
                // doesn't satisfy a prior tool-failure correction about
                // the parent's own work.
                Decision::RetireChild { child_ref, reason } => {
                    retire_child(ctx, &child_ref, reason).await;
                }
                // Replacement is NOT in-place — the new child gets a fresh
                // `agent_id` + `workflow_id` + `edges` row. The old `edges`
                // row stays as an audit trail. Failure-mode: if
                // `spawn_child` errors after the old child has been
                // retire-signaled, there is no rollback — the error
                // propagates so Temporal's activity-failure surface makes
                // the partial state operator-visible.
                Decision::ReplaceChild {
                    child_ref,
                    new_mandate,
                } => {
                    let replacement_name = format!("replacement-of-{}", child_ref.agent_id);
                    retire_child(ctx, &child_ref, format!("replaced by {replacement_name}")).await;
                    spawn_child(ctx, &input, replacement_name, new_mandate).await?;
                }
            }
            // Bump the tick after non-retire arms so the next iteration's
            // decision lands at `decisions/<tick+1>.jsonl`. The retire arm
            // above intentionally bypasses this — the retirement-tick log
            // is the final entry for the workflow.
            ctx.state_mut(|s| s.tick = s.tick.saturating_add(1));

            // `continue_as_new_suggested` is server-driven, surfaced on
            // each `WorkflowActivation`. `ContinueAsNewOptions` exposes no
            // client-side knob to lower the suggested-CAN threshold; the
            // dev-server threshold is undocumented and substantially larger
            // than the 4096 figure some SDK docs cite (empirically, 175
            // idle ticks producing 3001 history events did not flip the
            // suggestion). Forcing a natural CAN under a unit-test
            // wall-clock budget is therefore not feasible — the hermetic
            // tests below cover the encode + JSON + hydrate wire path.
            if ctx.continue_as_new_suggested() {
                let carryover = ctx.state(|s| s.encode_carryover());
                let next = AgentInput {
                    carryover: Some(carryover),
                    ..input
                };
                ctx.continue_as_new(&next, ContinueAsNewOptions::default())?;
                unreachable!("continue_as_new should have terminated this run");
            }
        }
    }
}

impl AgentWorkflow {
    /// Encode the workflow's per-tick state into a [`Carryover`] for
    /// transmission across a `continue_as_new` boundary.
    ///
    /// `&self` (not `&mut self`) — the encode is observation-only; the live
    /// workflow run will terminate immediately after `ctx.continue_as_new(...)`
    /// returns, so there is no value in clearing local state.
    pub(crate) fn encode_carryover(&self) -> Carryover {
        Carryover {
            pending_triggers: self.pending_triggers.clone(),
            pending_human_ops: self.pending_human_ops.clone(),
            pending_mandate_patches: self.pending_mandate_patches.clone(),
            retirement_request: self.retirement_request.clone(),
            staged_correction: self.staged_correction.clone(),
            scheduler_cursor: SchedulerCursor {
                next_wake: self.next_wake,
            },
            child_handles: self.child_handles.clone(),
            last_output_id: self.last_output_id.clone(),
            mid_tick_evidence: self.mid_tick_evidence.clone(),
            cumulative_triggers_observed: self.cumulative_triggers_observed,
            cumulative_human_ops_observed: self.cumulative_human_ops_observed,
            cumulative_mandate_patches_observed: self.cumulative_mandate_patches_observed,
            tick: self.tick,
        }
    }

    /// Decode a [`Carryover`] back onto the workflow's mutable state.
    ///
    /// Symmetric inverse of [`Self::encode_carryover`]. Called exactly once
    /// at the top of [`Self::run`] when `input.carryover.is_some()`.
    pub(crate) fn hydrate_from_carryover(&mut self, c: Carryover) {
        self.pending_triggers = c.pending_triggers;
        self.pending_human_ops = c.pending_human_ops;
        self.pending_mandate_patches = c.pending_mandate_patches;
        self.retirement_request = c.retirement_request;
        self.staged_correction = c.staged_correction;
        self.next_wake = c.scheduler_cursor.next_wake;
        self.last_output_id = c.last_output_id;
        self.mid_tick_evidence = c.mid_tick_evidence;
        self.cumulative_triggers_observed = c.cumulative_triggers_observed;
        self.cumulative_human_ops_observed = c.cumulative_human_ops_observed;
        self.cumulative_mandate_patches_observed = c.cumulative_mandate_patches_observed;
        self.tick = c.tick;
        self.child_handles = c.child_handles;
    }
}

/// Retire reason when `tick` has reached the mandate's `max_ticks` safety
/// cap, else `None`. The wording matches the in-process loop in
/// `coral_node::agent` so `retirement.json` reads identically on both paths.
fn max_ticks_retire_reason(tick: u64, max_ticks: Option<u64>) -> Option<String> {
    max_ticks
        .filter(|&max| tick >= max)
        .map(|max| format!("max_ticks ({max}) reached"))
}

/// Enforce the persistent-agent stop contract on a fresh decision. A
/// `persistent` agent may not self-terminate, so a model-emitted
/// `Decision::Retire` is demoted to an `Idle` for one `idle_period`; only a
/// retire signal or `max_ticks` may stop it. Every other decision, and every
/// decision from a non-persistent agent, passes through unchanged.
fn demote_retire_if_persistent(decision: Decision, mandate: &Mandate) -> Decision {
    match decision {
        Decision::Retire { reason } if mandate.persistent => {
            tracing::info!(
                demoted_reason = %reason,
                idle_period_ms = mandate.idle_period.as_millis() as u64,
                "persistent agent: demoting model Retire to Idle (stop contract)"
            );
            Decision::Idle {
                next_after: mandate.idle_period,
            }
        }
        other => other,
    }
}

/// Give a pure idle-timer wake an explicit "you woke on schedule" signal.
///
/// When the drained tick carried no triggers, human ops, mandate patches,
/// pending correction, or retirement request, the agent woke because its
/// `idle_period` elapsed with nothing queued. Synthesize a `ScheduledWake`
/// so the model has a "why" to act on instead of an empty bundle — mirrors
/// the in-process loop, which pushes `ScheduledWake` only when the deadline
/// fires with an empty queue. No-op when any real work was drained.
fn synthesize_scheduled_wake(drained: &mut DrainedBuckets) {
    if drained.triggers.is_empty()
        && drained.human_ops.is_empty()
        && drained.mandate_patches.is_empty()
        && drained.prior_correction.is_none()
        && drained.retirement.is_none()
    {
        drained.triggers.push(Trigger::ScheduledWake);
    }
}

/// Wake gate for the loop body. Returns once any signal bucket is non-empty
/// (triggers, human ops, mandate patches, or retirement), or the per-tick
/// `next_wake` timer elapses. `workflows::select!` is the SDK's
/// deterministic race primitive — `tokio::select!` would break replay.
///
/// We wake on every non-retire signal bucket (not only `triggers_pending`)
/// so operator-sent overrides round-trip through the loop within one tick
/// rather than waiting up to `next_wake` for the next idle wake.
async fn wait_for_tick(ctx: &WorkflowContext<AgentWorkflow>) {
    let wake_after = ctx.state(|s| s.next_wake.unwrap_or(INITIAL_NEXT_WAKE));
    let mut wait_signal = ctx.wait_condition(|s| {
        !s.pending_triggers.is_empty()
            || !s.pending_human_ops.is_empty()
            || !s.pending_mandate_patches.is_empty()
            || s.retirement_request.is_some()
    });
    let mut wait_timer = ctx.timer(wake_after);
    temporalio_sdk::workflows::select! {
        _ = wait_signal => {},
        _ = wait_timer => {},
    };
}

/// Invoke the `assemble_context` activity with the per-tick drained buckets.
async fn assemble(
    ctx: &WorkflowContext<AgentWorkflow>,
    input: &AgentInput,
    drained: DrainedBuckets,
) -> WorkflowResult<ContextBundle> {
    let out = ctx
        .start_activity(
            AgentActivities::assemble_context,
            AssembleContextInput {
                mandate: input.mandate.clone(),
                fs_handle: input.fs_handle.clone(),
                triggers: drained.triggers,
                human_ops: drained.human_ops,
                mandate_patches: drained.mandate_patches,
                prior_correction: drained.prior_correction,
            },
            activity_opts(),
        )
        .await?;
    Ok(out.bundle)
}

/// Invoke the `decide_next_action` activity.
async fn decide(
    ctx: &WorkflowContext<AgentWorkflow>,
    bundle: ContextBundle,
) -> WorkflowResult<Decision> {
    Ok(ctx
        .start_activity(
            AgentActivities::decide_next_action,
            DecideInput { bundle },
            activity_opts(),
        )
        .await?)
}

/// Invoke the `persist_output` activity for a `Decision::EmitOutput`.
async fn emit_output(
    ctx: &WorkflowContext<AgentWorkflow>,
    input: &AgentInput,
    content: String,
    evidence: Vec<coral_node::evidence::EvidenceId>,
) -> WorkflowResult<()> {
    let output_id = ctx
        .start_activity(
            AgentActivities::persist_output,
            PersistOutputInput {
                cfg: input.cfg.clone(),
                fs_handle: input.fs_handle.clone(),
                content,
                evidence,
            },
            activity_opts(),
        )
        .await?;
    if let Some(parent) = &input.parent_handle {
        let trigger = Trigger::ChildOutput {
            child_ref: AgentRef::new(ctx.workflow_id().to_string(), input.agent_id),
            agent_name: input.agent_name.clone(),
            output_id,
        };
        signal_parent_with_trigger(ctx, parent, trigger).await;
    }
    Ok(())
}

/// Fire a [`Trigger`] payload at the parent workflow via the SDK's
/// `ExternalWorkflowHandle::signal`. Errors are logged + swallowed:
/// cross-workflow signaling is best-effort — the child's data is durable on
/// its own FS regardless of whether the parent observed the signal.
///
/// Building the typed [`Trigger`] is the caller's job — `ChildOutput` and
/// `ChildRetired` each carry distinct fields that depend on workflow-local
/// state (output id vs. retirement reason) the helper shouldn't abstract over.
async fn signal_parent_with_trigger(
    ctx: &WorkflowContext<AgentWorkflow>,
    parent: &ParentRef,
    trigger: Trigger,
) {
    // SDK two-step: handle = external_workflow(workflow_id, run_id), then
    // handle.signal(SignalDef, payload). `run_id = None` targets the latest
    // run (the parent's currently-active execution).
    let result = ctx
        .external_workflow(parent.workflow_id.clone(), None)
        .signal(AgentWorkflow::external_signal, trigger)
        .await;
    if let Err(failure) = result {
        tracing::warn!(
            parent_workflow_id = %parent.workflow_id,
            error = ?failure,
            "signal_external_workflow to parent failed; child continuing best-effort"
        );
    }
}

/// Invoke the `apply_fs_ops` activity for a `Decision::RewriteFs`.
///
/// `Mandate::new("", Duration::ZERO, None)` is decorative because
/// `AgentFs::new_with_storage` only writes `mandate.json` when absent, and
/// `apply_fs_ops` runs only against agents whose `mandate.json` already
/// exists on disk (assemble_context wrote it on tick 1). The activity body
/// never reads the mandate — it only forwards it to `new_with_storage`,
/// which short-circuits the write.
async fn rewrite_fs(
    ctx: &WorkflowContext<AgentWorkflow>,
    fs_handle: &FsHandle,
    ops: Vec<coral_node::decision::FsOp>,
) -> WorkflowResult<()> {
    ctx.start_activity(
        AgentActivities::apply_fs_ops,
        ApplyFsOpsInput {
            fs_handle: fs_handle.clone(),
            mandate: Mandate::new("", Duration::ZERO, None),
            ops,
        },
        activity_opts(),
    )
    .await?;
    Ok(())
}

/// Invoke the `append_decision_log` activity for the current tick's
/// decision. Called by the loop body right after `decide(...)` returns and
/// before the dispatch arm — see the call site for the "artifact even on
/// dispatch error" rationale.
async fn log_decision(
    ctx: &WorkflowContext<AgentWorkflow>,
    fs_handle: &FsHandle,
    tick: u64,
    decision: &Decision,
) -> WorkflowResult<()> {
    ctx.start_activity(
        AgentActivities::append_decision_log,
        AppendDecisionLogInput {
            fs_handle: fs_handle.clone(),
            tick,
            decision_summary: decision_summary(decision),
        },
        activity_opts(),
    )
    .await?;
    Ok(())
}

/// Render a one-line, human-readable summary of a [`Decision`] for the
/// decision log artifact. Format is not part of any wire contract — the TUI
/// parses the JSONL line's `decision_summary` string verbatim.
fn decision_summary(decision: &Decision) -> String {
    match decision {
        Decision::CallTools { calls } => format!("CallTools {{ count: {} }}", calls.len()),
        Decision::EmitOutput { evidence, .. } => {
            format!("EmitOutput {{ evidence: {} }}", evidence.len())
        }
        Decision::RewriteFs { ops } => format!("RewriteFs {{ ops: {} }}", ops.len()),
        Decision::Idle { next_after } => {
            format!("Idle {{ next_after_ms: {} }}", next_after.as_millis())
        }
        Decision::Retire { reason } => format!("Retire {{ reason: {reason:?} }}"),
        Decision::SpawnChild { agent_name, .. } => {
            format!("SpawnChild {{ agent_name: {agent_name:?} }}")
        }
        Decision::ReconcileChildren { sources, conflict } => format!(
            "ReconcileChildren {{ sources: {}, conflict: {} }}",
            sources.len(),
            conflict.is_some(),
        ),
        Decision::RetireChild { child_ref, reason } => format!(
            "RetireChild {{ agent_id: {}, reason: {reason:?} }}",
            child_ref.agent_id,
        ),
        Decision::ReplaceChild { child_ref, .. } => {
            format!("ReplaceChild {{ agent_id: {} }}", child_ref.agent_id)
        }
    }
}

/// Invoke the `persist_retirement` activity and return the workflow result.
/// Shared between the retire-signal short-circuit and the `Decision::Retire`
/// arm.
///
/// After `persist_retirement` returns and before the workflow exits, if
/// `input.parent_handle.is_some()` the workflow body fires one final
/// `Trigger::ChildRetired` at the parent via [`signal_parent_with_trigger`].
/// The signal is best-effort: failure is logged but does NOT prevent the
/// child from exiting cleanly — `retirement.json` is durable on the child's
/// own FS regardless of whether the parent observed the signal. Orphan
/// children (`parent_handle.is_none()`) skip the signal step entirely.
async fn retire(
    ctx: &WorkflowContext<AgentWorkflow>,
    input: &AgentInput,
    reason: String,
) -> WorkflowResult<AgentResult> {
    ctx.start_activity(
        AgentActivities::persist_retirement,
        PersistRetirementInput {
            fs_handle: input.fs_handle.clone(),
            reason: reason.clone(),
        },
        activity_opts(),
    )
    .await?;
    if let Some(parent) = &input.parent_handle {
        let trigger = Trigger::ChildRetired {
            child_ref: AgentRef::new(ctx.workflow_id().to_string(), input.agent_id),
            agent_name: input.agent_name.clone(),
            reason: reason.clone(),
        };
        signal_parent_with_trigger(ctx, parent, trigger).await;
    }
    Ok(AgentResult::Retired { reason })
}

/// Clear the staged correction in workflow state. Used by the non-failing
/// `Decision` arms so a previously-staged correction doesn't carry into the
/// next tick once the LLM has produced a satisfiable decision.
fn clear_correction(s: &mut AgentWorkflow) {
    s.staged_correction = None;
}

/// Owned payload produced by [`drain_buckets`] — the per-tick view of every
/// signal-staged bucket plus the previously-staged correction. Kept distinct
/// from `AssembleContextInput` because the workflow body short-circuits on
/// `retirement` before assembling a context.
struct DrainedBuckets {
    triggers: Vec<Trigger>,
    human_ops: Vec<HumanOp>,
    mandate_patches: Vec<MandatePatch>,
    retirement: Option<String>,
    prior_correction: Option<CorrectionContext>,
}

/// Drain the five signal-tracked fields out of workflow state into owned
/// values.
///
/// `cumulative_*_observed` counters are bumped by the signal handlers at
/// receipt time (not here at drain time) so a snapshot taken between a
/// signal landing and the next loop tick still reflects the arrival.
fn drain_buckets(s: &mut AgentWorkflow) -> DrainedBuckets {
    DrainedBuckets {
        triggers: std::mem::take(&mut s.pending_triggers),
        human_ops: std::mem::take(&mut s.pending_human_ops),
        mandate_patches: std::mem::take(&mut s.pending_mandate_patches),
        retirement: s.retirement_request.take(),
        prior_correction: s.staged_correction.take(),
    }
}

/// Build the standard activity options.
fn activity_opts() -> ActivityOptions {
    ActivityOptions::start_to_close_timeout(ACTIVITY_TIMEOUT)
}

/// Fan out N `execute_tool` activity invocations via the SDK's deterministic
/// `workflows::join_all`. On any failure, stage a correction context for
/// next tick's `assemble_context` input — the workflow does NOT ape
/// `agent_core`'s budget state machine, it just delivers a description of
/// the failure so the LLM can see it on the next tick.
async fn dispatch_call_tools(
    ctx: &WorkflowContext<AgentWorkflow>,
    input: &AgentInput,
    calls: Vec<ToolCall>,
) -> WorkflowResult<()> {
    let futures = calls.into_iter().map(|call| {
        ctx.start_activity(
            AgentActivities::execute_tool,
            ExecuteToolInput {
                cfg: input.cfg.clone(),
                fs_handle: input.fs_handle.clone(),
                graph_id: input.graph_id,
                call,
            },
            activity_opts(),
        )
    });
    let results = temporalio_sdk::workflows::join_all(futures).await;

    let mut failures: Vec<ToolCallFailure> = Vec::new();
    for r in results {
        match r? {
            ToolCallOutcome::Success { .. } => {}
            ToolCallOutcome::Failure { failure } => failures.push(failure),
        }
    }
    let correction = if failures.is_empty() {
        None
    } else {
        Some(CorrectionContext::new(format_correction(&failures)))
    };
    ctx.state_mut(|s| s.staged_correction = correction);
    Ok(())
}

/// Construct the [`AgentInput`] for a freshly-spawned child workflow.
/// Shared between the `Decision::SpawnChild` arm of [`AgentWorkflow::run`]
/// and the `coral apply` walker so the two surfaces cannot drift on
/// `parent_handle` shape, FS prefix layout, or inherited cfg.
///
/// The child shares the parent's `graph_id` rather than getting a fresh one
/// — only `agent_id` is fresh per spawn. Returns an `AgentInput` with
/// `carryover: None` (fresh first run) and `parent_handle: Some(..)`
/// populated to route child → parent signals back to the parent.
pub fn build_child_input(
    parent_workflow_id: &str,
    parent_agent_id: AgentId,
    parent_graph_id: GraphId,
    child_agent_id: AgentId,
    child_agent_name: String,
    child_mandate: Mandate,
    inherited_cfg: AgentConfig,
) -> AgentInput {
    // `parent_agent_id` is on the signature for symmetry + future use (e.g.
    // a `parent_handle.agent_id` field for routing), but today's
    // `ParentRef` shape only carries the workflow id. Acknowledge the
    // binding so clippy's unused-variable lint doesn't fire and a future
    // field addition doesn't need a new positional argument.
    let _ = parent_agent_id;
    AgentInput {
        cfg: inherited_cfg,
        fs_handle: FsHandle::for_agent(parent_graph_id, child_agent_id),
        parent_handle: Some(ParentRef {
            workflow_id: parent_workflow_id.to_string(),
            signal: ParentRef::DEFAULT_SIGNAL.to_string(),
        }),
        carryover: None,
        mandate: child_mandate,
        graph_id: parent_graph_id,
        agent_id: child_agent_id,
        agent_name: child_agent_name,
    }
}

/// Construct the [`AgentInput`] for a freshly-applied **root** agent (no
/// parent). Counterpart to [`build_child_input`]; the only difference is
/// `parent_handle: None`.
///
/// For roots there's nothing to inherit from, so the caller passes
/// `AgentConfig::default()` as `cfg`.
pub fn build_root_input(
    graph_id: GraphId,
    agent_id: AgentId,
    agent_name: String,
    mandate: Mandate,
    cfg: AgentConfig,
) -> AgentInput {
    AgentInput {
        cfg,
        fs_handle: FsHandle::for_agent(graph_id, agent_id),
        parent_handle: None,
        carryover: None,
        mandate,
        graph_id,
        agent_id,
        agent_name,
    }
}

/// The `Decision::SpawnChild` workflow arm body.
///
/// Sequence:
///
/// 1. Invoke `register_child_in_structural_db` activity — writes the
///    child's `agents` row + parent→child `edges` row, returns the
///    freshly-minted `AgentId`.
/// 2. Construct child workflow id (`graphs/<gid>/agents/<child_aid>`).
/// 3. Build the child's `AgentInput` via [`build_child_input`].
/// 4. `ctx.child_workflow(AgentWorkflow::run, ..)` with
///    `ParentClosePolicy::Abandon`. The `.await` here resolves once the
///    child workflow has *started*, not when it completes.
/// 5. Drop the started child handle without awaiting its result. The parent
///    does NOT block on the child; the child runs independently and reports
///    back via the `signal_external_workflow` path.
/// 6. Push the child's `AgentRef` onto `self.child_handles` for later
///    snapshot / reconcile / retire reads + carryover round-trip.
async fn spawn_child(
    ctx: &WorkflowContext<AgentWorkflow>,
    input: &AgentInput,
    child_agent_name: String,
    child_mandate: Mandate,
) -> WorkflowResult<()> {
    let reg = ctx
        .start_activity(
            AgentActivities::register_child_in_structural_db,
            RegisterChildInStructuralDbInput {
                parent_graph_id: input.graph_id,
                parent_agent_id: input.agent_id,
                child_agent_name: child_agent_name.clone(),
                child_mandate_ref: None,
            },
            activity_opts(),
        )
        .await?;
    let child_agent_id = reg.child_agent_id;

    let child_workflow_id =
        agent_workflow_id(&input.graph_id.to_string(), &child_agent_id.to_string());
    let parent_workflow_id = ctx.workflow_id().to_string();
    let child_input = build_child_input(
        &parent_workflow_id,
        input.agent_id,
        input.graph_id,
        child_agent_id,
        child_agent_name,
        child_mandate,
        input.cfg.clone(),
    );

    // Every child is spawned with `ParentClosePolicy::Abandon` so it
    // survives parent CAN, parent restart, and even parent retirement. The
    // only kill path is `Decision::RetireChild`.
    //
    // The SDK's `child_workflow(..)` returns a future that resolves once
    // the child has *started*; we await that (to surface a start failure as
    // a workflow error) and then drop the started handle without awaiting
    // its `.result()` — detached. Awaiting `started.result()` would block
    // the parent for the child's full lifetime, defeating the whole
    // `Abandon` design.
    let opts = ChildWorkflowOptions {
        workflow_id: child_workflow_id.clone(),
        parent_close_policy: ParentClosePolicy::Abandon,
        ..Default::default()
    };
    let started = ctx
        .child_workflow(AgentWorkflow::run, child_input, opts)
        .await
        .map_err(|e| anyhow::anyhow!("child_workflow start failed: {e:?}"))?;
    drop(started);

    ctx.state_mut(|s| {
        s.child_handles
            .push(AgentRef::new(child_workflow_id, child_agent_id));
    });
    Ok(())
}

/// The `Decision::RetireChild` workflow arm body (also reused by
/// `Decision::ReplaceChild`'s retire half).
///
/// Sequence:
///
/// 1. Fire `AgentWorkflow::retire` at the child's workflow via the SDK
///    two-step `external_workflow().signal()` chain.
/// 2. Log + continue on signal failure: if the child already exited (or its
///    workflow id is wrong) the signal fails and the parent proceeds. The
///    child's exit is durable on its own FS regardless of whether this
///    signal lands.
/// 3. Remove the child's `AgentRef` from `self.child_handles` so the
///    parent's snapshot / future reconcile / future retire paths see only
///    the live child set. Round-trips through CAN.
async fn retire_child(ctx: &WorkflowContext<AgentWorkflow>, child_ref: &AgentRef, reason: String) {
    let result = ctx
        .external_workflow(child_ref.workflow_id.clone(), None)
        .signal(AgentWorkflow::retire, reason)
        .await;
    if let Err(failure) = result {
        // A child that already exited (e.g. retired naturally on a previous
        // tick) is the common case here, not a hard error.
        tracing::warn!(
            child_workflow_id = %child_ref.workflow_id,
            child_agent_id = %child_ref.agent_id,
            error = ?failure,
            "signal_external_workflow(retire) to child failed; parent continuing best-effort"
        );
    }
    // Drop the child from the parent's live-handle set regardless of signal
    // outcome — the intent ("this child is gone from the parent's model")
    // is the load-bearing state mutation; the signal is best-effort delivery.
    let target_agent_id = child_ref.agent_id;
    ctx.state_mut(|s| {
        s.child_handles.retain(|h| h.agent_id != target_agent_id);
    });
}

/// The `Decision::ReconcileChildren` workflow arm body.
///
/// Calls the `reconcile_children` activity (which opens the parent's FS +
/// each child's FS read-only, writes one synthetic evidence record per
/// source into the parent's `evidence/`, and returns the freshly-minted
/// `EvidenceId`s). The synthetic records flow into the parent's next-tick
/// `assemble_context` bundle via the existing `list_recent_evidence`
/// window — no workflow-state slot is needed.
///
/// Errors do NOT propagate via `?` — that would fail the whole workflow on
/// a single bad source. Instead the typed activity failure is folded into a
/// `CorrectionContext` staged for the next tick, mirroring the existing
/// `Decision::CallTools` tool-failure flow.
async fn reconcile_children(
    ctx: &WorkflowContext<AgentWorkflow>,
    input: &AgentInput,
    sources: Vec<ReconcileSource>,
    conflict: Option<ConflictRecordIntent>,
) {
    let activity_input = ReconcileChildrenInput {
        parent_graph_id: input.graph_id,
        parent_agent_id: input.agent_id,
        sources,
        conflict,
    };
    match ctx
        .start_activity(
            AgentActivities::reconcile_children,
            activity_input,
            activity_opts(),
        )
        .await
    {
        Ok(_out) => {
            ctx.state_mut(clear_correction);
        }
        Err(failure) => {
            // The activity returned an `ApplicationFailure` carrying either
            // a typed `ReconciliationError` (non-retryable, structural) or
            // a wrapped transient error (retryable). Either way we stage a
            // `CorrectionContext` so the next tick's LLM sees the failure
            // and emits a satisfiable replacement decision.
            let correction =
                CorrectionContext::new(format!("reconcile: activity failed: {failure:?}"));
            ctx.state_mut(|s| s.staged_correction = Some(correction));
        }
    }
}

/// Render the staged correction text for a tool-batch failure.
fn format_correction(failures: &[ToolCallFailure]) -> String {
    if failures.len() == 1 {
        format!(
            "call_tool {:?} failed: {} (args={})",
            failures[0].tool, failures[0].error, failures[0].args
        )
    } else {
        let mut s = format!(
            "{} parallel call_tool(s) failed after exhausting retries:",
            failures.len()
        );
        for f in failures {
            s.push_str(&format!("\n- {:?}: {} (args={})", f.tool, f.error, f.args));
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_workflow_id_is_url_shaped() {
        assert_eq!(agent_workflow_id("g1", "a1"), "graphs/g1/agents/a1");
    }

    #[test]
    fn agent_workflow_id_passes_components_through_unchanged() {
        assert_eq!(
            agent_workflow_id("graph-with-dashes", "agent_with_underscores"),
            "graphs/graph-with-dashes/agents/agent_with_underscores"
        );
        assert_eq!(agent_workflow_id("", ""), "graphs//agents/");
    }

    #[test]
    fn agent_input_new_for_test_has_no_carryover_and_no_parent() {
        let input = AgentInput::new_for_test(
            GraphId::new(uuid::Uuid::nil()),
            AgentId::new(uuid::Uuid::nil()),
            "root",
        );
        assert!(input.carryover.is_none());
        assert!(input.parent_handle.is_none());
        assert_eq!(input.agent_name, "root");
    }

    #[test]
    fn agent_input_roundtrips_through_json() {
        let input = AgentInput {
            cfg: AgentConfig::default(),
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            parent_handle: None,
            carryover: Some(Carryover::default()),
            mandate: Mandate::new("hello", Duration::from_millis(123), Some(7)),
            graph_id: GraphId::new(uuid::Uuid::from_u128(0xAB)),
            agent_id: AgentId::new(uuid::Uuid::from_u128(0xCD)),
            agent_name: "root".into(),
        };
        let json = serde_json::to_string(&input).expect("serialize AgentInput");
        let back: AgentInput = serde_json::from_str(&json).expect("deserialize AgentInput");
        assert_eq!(input, back);
    }

    #[test]
    fn agent_result_default_is_retired_with_empty_reason() {
        let r = AgentResult::default();
        assert!(matches!(r, AgentResult::Retired { reason } if reason.is_empty()));
    }

    #[test]
    fn agent_result_roundtrips_through_json() {
        let r = AgentResult::Retired {
            reason: "op asked".into(),
        };
        let s = serde_json::to_string(&r).unwrap();
        assert!(s.contains("\"result\":\"retired\""), "wire shape: {s}");
        assert!(s.contains("\"reason\":\"op asked\""), "wire shape: {s}");
        let back: AgentResult = serde_json::from_str(&s).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn agent_result_default_roundtrips_through_json() {
        let r = AgentResult::default();
        let s = serde_json::to_string(&r).unwrap();
        let back: AgentResult = serde_json::from_str(&s).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn agent_snapshot_default_roundtrips_through_json() {
        let s = AgentSnapshot::default();
        let json = serde_json::to_string(&s).unwrap();
        let back: AgentSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
        assert_eq!(s.pending_triggers_count, 0);
        assert_eq!(s.pending_human_ops_count, 0);
        assert_eq!(s.pending_mandate_patches_count, 0);
        assert!(s.retirement_request.is_none());
        assert!(s.recent_output_ids.is_empty());
        assert_eq!(s.cumulative_triggers_observed, 0);
        assert_eq!(s.cumulative_human_ops_observed, 0);
        assert_eq!(s.cumulative_mandate_patches_observed, 0);
    }

    #[test]
    fn agent_snapshot_accepts_wire_shape_without_cumulative_counters() {
        // `cumulative_*_observed` fields are `#[serde(default)]` so a wire
        // form missing them still deserializes — the counters default to 0.
        let without_counters = r#"{
            "pending_triggers_count": 2,
            "pending_human_ops_count": 1,
            "pending_mandate_patches_count": 3,
            "retirement_request": "shutdown",
            "recent_output_ids": []
        }"#;
        let s: AgentSnapshot = serde_json::from_str(without_counters).unwrap();
        assert_eq!(s.pending_triggers_count, 2);
        assert_eq!(s.cumulative_triggers_observed, 0);
        assert_eq!(s.cumulative_human_ops_observed, 0);
        assert_eq!(s.cumulative_mandate_patches_observed, 0);
    }

    #[test]
    fn agent_snapshot_with_signals_roundtrips_through_json() {
        let s = AgentSnapshot {
            pending_triggers_count: 2,
            pending_human_ops_count: 1,
            pending_mandate_patches_count: 3,
            retirement_request: Some("shutdown for upgrade".into()),
            recent_output_ids: Vec::new(),
            cumulative_triggers_observed: 5,
            cumulative_human_ops_observed: 7,
            cumulative_mandate_patches_observed: 11,
        };
        let json = serde_json::to_string(&s).unwrap();
        let back: AgentSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn agent_snapshot_from_state_projects_bucket_lengths_and_retirement() {
        let mut wf = AgentWorkflow::default();
        wf.pending_triggers.push(Trigger::ScheduledWake);
        wf.pending_triggers.push(Trigger::External {
            kind: "k".into(),
            payload: serde_json::json!({}),
        });
        wf.pending_human_ops
            .push(HumanOp::new(serde_json::json!({"action": "pause"})));
        wf.pending_mandate_patches
            .push(MandatePatch::new(serde_json::json!({"model": "gpt-x"})));
        wf.pending_mandate_patches
            .push(MandatePatch::new(serde_json::json!({"budget_ms": 5000})));
        wf.retirement_request = Some("op asked".into());

        let snap = AgentSnapshot::from_state(&wf);
        assert_eq!(snap.pending_triggers_count, 2);
        assert_eq!(snap.pending_human_ops_count, 1);
        assert_eq!(snap.pending_mandate_patches_count, 2);
        assert_eq!(snap.retirement_request.as_deref(), Some("op asked"));
        assert!(snap.recent_output_ids.is_empty());
    }

    #[test]
    fn agent_workflow_default_has_empty_buckets_and_no_retirement() {
        let wf = AgentWorkflow::default();
        assert!(wf.pending_triggers.is_empty());
        assert!(wf.pending_human_ops.is_empty());
        assert!(wf.pending_mandate_patches.is_empty());
        assert!(wf.retirement_request.is_none());
        assert!(wf.next_wake.is_none());
        assert!(wf.staged_correction.is_none());
        assert_eq!(wf.cumulative_triggers_observed, 0);
        assert_eq!(wf.cumulative_human_ops_observed, 0);
        assert_eq!(wf.cumulative_mandate_patches_observed, 0);
    }

    #[test]
    fn drain_buckets_takes_all_state_and_clears_workflow() {
        // Critical that all five buckets get cleared so a redundant retire
        // signal arriving mid-tick doesn't trip the next iteration's
        // short-circuit.
        let mut wf = AgentWorkflow::default();
        wf.pending_triggers.push(Trigger::ScheduledWake);
        wf.pending_human_ops
            .push(HumanOp::new(serde_json::json!({"a": 1})));
        wf.pending_mandate_patches
            .push(MandatePatch::new(serde_json::json!({"m": 1})));
        wf.retirement_request = Some("done".into());
        wf.staged_correction = Some(CorrectionContext::new("prior failure"));

        let drained = drain_buckets(&mut wf);
        assert_eq!(drained.triggers.len(), 1);
        assert_eq!(drained.human_ops.len(), 1);
        assert_eq!(drained.mandate_patches.len(), 1);
        assert_eq!(drained.retirement.as_deref(), Some("done"));
        assert!(drained.prior_correction.is_some());

        assert!(wf.pending_triggers.is_empty());
        assert!(wf.pending_human_ops.is_empty());
        assert!(wf.pending_mandate_patches.is_empty());
        assert!(wf.retirement_request.is_none());
        assert!(wf.staged_correction.is_none());

        // drain_buckets itself does NOT bump cumulative counters; the
        // signal handlers do that at receipt time. The buckets were
        // populated directly here, bypassing the signal path, so counters
        // stay at 0.
        assert_eq!(wf.cumulative_triggers_observed, 0);
        assert_eq!(wf.cumulative_human_ops_observed, 0);
        assert_eq!(wf.cumulative_mandate_patches_observed, 0);
    }

    #[test]
    fn signal_handlers_bump_cumulative_counters_at_receipt() {
        // Mutating the bare fields directly because the SDK's
        // SyncWorkflowContext can't be constructed in a unit test — the
        // handler body invariant we care about is bucket push + counter
        // bump.
        let mut wf = AgentWorkflow::default();

        wf.pending_triggers.push(Trigger::ScheduledWake);
        wf.cumulative_triggers_observed = wf.cumulative_triggers_observed.saturating_add(1);
        wf.pending_human_ops
            .push(HumanOp::new(serde_json::json!({"a": 1})));
        wf.cumulative_human_ops_observed = wf.cumulative_human_ops_observed.saturating_add(1);
        wf.pending_mandate_patches
            .push(MandatePatch::new(serde_json::json!({"m": 1})));
        wf.cumulative_mandate_patches_observed =
            wf.cumulative_mandate_patches_observed.saturating_add(1);

        assert_eq!(wf.cumulative_triggers_observed, 1);
        assert_eq!(wf.cumulative_human_ops_observed, 1);
        assert_eq!(wf.cumulative_mandate_patches_observed, 1);

        let _ = drain_buckets(&mut wf);
        assert_eq!(wf.cumulative_triggers_observed, 1);
        assert_eq!(wf.cumulative_human_ops_observed, 1);
        assert_eq!(wf.cumulative_mandate_patches_observed, 1);
    }

    #[test]
    fn format_correction_single_failure_quotes_tool_and_includes_error_and_args() {
        let f = ToolCallFailure {
            tool: "search_web".into(),
            args: serde_json::json!({"q": "rust"}),
            error: "503".into(),
        };
        let s = format_correction(&[f]);
        assert!(s.contains("\"search_web\""), "got: {s}");
        assert!(s.contains("503"), "got: {s}");
        assert!(s.contains("\"q\""), "got: {s}");
    }

    /// Fully-populated [`Carryover`] fixture — the JSON round-trip and
    /// hydrate/encode tests below all build against this so a future field
    /// addition shows up as a test miss if not represented.
    fn fully_populated_carryover() -> Carryover {
        use uuid::Uuid;
        Carryover {
            pending_triggers: vec![
                Trigger::ScheduledWake,
                Trigger::External {
                    kind: "webhook".into(),
                    payload: serde_json::json!({"k": "v"}),
                },
            ],
            pending_human_ops: vec![HumanOp::new(serde_json::json!({"action": "pause"}))],
            pending_mandate_patches: vec![MandatePatch::new(serde_json::json!({"model": "gpt-x"}))],
            retirement_request: Some("op asked".into()),
            staged_correction: Some(CorrectionContext::new("prior tool failure")),
            scheduler_cursor: SchedulerCursor {
                next_wake: Some(Duration::from_millis(250)),
            },
            child_handles: vec![AgentRef::new(
                "graphs/g1/agents/c1",
                AgentId::new(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap()),
            )],
            last_output_id: Some(OutputId::from_hex("ab".repeat(32))),
            mid_tick_evidence: vec![EvidenceId::from_hex("0123456789abcdef")],
            cumulative_triggers_observed: 5,
            cumulative_human_ops_observed: 7,
            cumulative_mandate_patches_observed: 11,
            tick: 13,
        }
    }

    #[test]
    fn carryover_default_roundtrips_through_json() {
        let c = Carryover::default();
        let json = serde_json::to_string(&c).expect("serialize default Carryover");
        let back: Carryover = serde_json::from_str(&json).expect("deserialize default Carryover");
        assert_eq!(c, back);
    }

    #[test]
    fn carryover_fully_populated_roundtrips_through_json() {
        let c = fully_populated_carryover();
        let json = serde_json::to_string(&c).expect("serialize populated Carryover");
        let back: Carryover = serde_json::from_str(&json).expect("deserialize populated Carryover");
        assert_eq!(c, back);
    }

    #[test]
    fn agent_input_with_populated_carryover_roundtrips_through_json() {
        let input = AgentInput {
            cfg: AgentConfig::default(),
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            parent_handle: None,
            carryover: Some(fully_populated_carryover()),
            mandate: Mandate::new("populated-carryover", Duration::from_millis(50), None),
            graph_id: GraphId::new(uuid::Uuid::from_u128(0xAB)),
            agent_id: AgentId::new(uuid::Uuid::from_u128(0xCD)),
            agent_name: "root".into(),
        };
        let json = serde_json::to_string(&input).unwrap();
        let back: AgentInput = serde_json::from_str(&json).unwrap();
        assert_eq!(input, back);
    }

    #[test]
    fn encode_then_hydrate_is_identity_on_workflow_state() {
        let mut original = AgentWorkflow::default();
        original.pending_triggers.push(Trigger::ScheduledWake);
        original
            .pending_human_ops
            .push(HumanOp::new(serde_json::json!({"a": 1})));
        original
            .pending_mandate_patches
            .push(MandatePatch::new(serde_json::json!({"m": 1})));
        original.retirement_request = Some("op asked".into());
        original.staged_correction = Some(CorrectionContext::new("prior failure"));
        original.next_wake = Some(Duration::from_millis(123));
        original.cumulative_triggers_observed = 9;
        original.cumulative_human_ops_observed = 13;
        original.cumulative_mandate_patches_observed = 17;
        original.last_output_id = Some(OutputId::from_hex("cd".repeat(32)));
        original.tick = 23;

        let c = original.encode_carryover();
        let mut hydrated = AgentWorkflow::default();
        hydrated.hydrate_from_carryover(c);

        assert_eq!(hydrated.pending_triggers, original.pending_triggers);
        assert_eq!(hydrated.pending_human_ops, original.pending_human_ops);
        assert_eq!(
            hydrated.pending_mandate_patches,
            original.pending_mandate_patches
        );
        assert_eq!(hydrated.retirement_request, original.retirement_request);
        assert_eq!(hydrated.staged_correction, original.staged_correction);
        assert_eq!(hydrated.next_wake, original.next_wake);
        assert_eq!(
            hydrated.cumulative_triggers_observed,
            original.cumulative_triggers_observed
        );
        assert_eq!(
            hydrated.cumulative_human_ops_observed,
            original.cumulative_human_ops_observed
        );
        assert_eq!(
            hydrated.cumulative_mandate_patches_observed,
            original.cumulative_mandate_patches_observed
        );
        assert_eq!(hydrated.last_output_id, original.last_output_id);
        assert_eq!(hydrated.tick, original.tick);
    }

    #[test]
    fn encode_then_serialize_then_deserialize_then_hydrate_round_trips_state() {
        // The full wire path that a real `continue_as_new` boundary
        // exercises: workflow state → encode_carryover → JSON (Temporal's
        // default payload codec) → JSON parse → hydrate_from_carryover →
        // workflow state on the new run.
        let mut pre_can = AgentWorkflow::default();
        pre_can.pending_triggers.push(Trigger::External {
            kind: "wire-roundtrip".into(),
            payload: serde_json::json!({"i": 42}),
        });
        pre_can
            .pending_human_ops
            .push(HumanOp::new(serde_json::json!({"a": "b"})));
        pre_can
            .pending_mandate_patches
            .push(MandatePatch::new(serde_json::json!({"m": "n"})));
        pre_can.retirement_request = Some("not yet".into());
        pre_can.staged_correction = Some(CorrectionContext::new("prior batch failed"));
        pre_can.next_wake = Some(Duration::from_millis(500));
        pre_can.cumulative_triggers_observed = 3;
        pre_can.cumulative_human_ops_observed = 5;
        pre_can.cumulative_mandate_patches_observed = 7;
        pre_can.last_output_id = Some(OutputId::from_hex("ef".repeat(32)));
        pre_can.tick = 19;

        let carryover_pre = pre_can.encode_carryover();
        let wire = serde_json::to_string(&carryover_pre).expect("wire-encode Carryover");
        let carryover_post: Carryover = serde_json::from_str(&wire).expect("wire-decode Carryover");
        let mut post_can = AgentWorkflow::default();
        post_can.hydrate_from_carryover(carryover_post);

        assert_eq!(post_can.pending_triggers, pre_can.pending_triggers);
        assert_eq!(post_can.pending_human_ops, pre_can.pending_human_ops);
        assert_eq!(
            post_can.pending_mandate_patches,
            pre_can.pending_mandate_patches
        );
        assert_eq!(post_can.retirement_request, pre_can.retirement_request);
        assert_eq!(post_can.staged_correction, pre_can.staged_correction);
        assert_eq!(post_can.next_wake, pre_can.next_wake);
        assert_eq!(
            post_can.cumulative_triggers_observed,
            pre_can.cumulative_triggers_observed
        );
        assert_eq!(
            post_can.cumulative_human_ops_observed,
            pre_can.cumulative_human_ops_observed
        );
        assert_eq!(
            post_can.cumulative_mandate_patches_observed,
            pre_can.cumulative_mandate_patches_observed
        );
        assert_eq!(post_can.last_output_id, pre_can.last_output_id);
        assert_eq!(post_can.tick, pre_can.tick);
    }

    #[test]
    fn hydrate_then_signal_handler_bumps_counter_past_carryover_value() {
        // The cumulative_*_observed counters must bridge a CAN boundary.
        // We can't construct a `SyncWorkflowContext` in a unit test (it's
        // SDK-private), so simulate the signal handler's effect by
        // replicating its `push + saturating_add` bookkeeping. The
        // load-bearing invariant is that the value the counter starts from
        // is the carryover's value, not zero.
        let pre_can = Carryover {
            cumulative_triggers_observed: 5,
            cumulative_human_ops_observed: 6,
            cumulative_mandate_patches_observed: 7,
            ..Carryover::default()
        };
        let mut wf = AgentWorkflow::default();
        wf.hydrate_from_carryover(pre_can);

        wf.pending_triggers.push(Trigger::ScheduledWake);
        wf.cumulative_triggers_observed = wf.cumulative_triggers_observed.saturating_add(1);

        // Cumulative view: 5 (pre-CAN) + 1 (post-CAN signal) = 6, NOT 1.
        assert_eq!(
            wf.cumulative_triggers_observed, 6,
            "post-CAN signal must increment past the carried value"
        );
        assert_eq!(wf.cumulative_human_ops_observed, 6);
        assert_eq!(wf.cumulative_mandate_patches_observed, 7);

        let snap = AgentSnapshot::from_state(&wf);
        assert_eq!(snap.cumulative_triggers_observed, 6);
        assert_eq!(snap.cumulative_human_ops_observed, 6);
        assert_eq!(snap.cumulative_mandate_patches_observed, 7);
    }

    #[test]
    fn carryover_from_default_workflow_is_default() {
        let wf = AgentWorkflow::default();
        let c = wf.encode_carryover();
        assert_eq!(c, Carryover::default());
    }

    #[test]
    fn scheduler_cursor_default_has_no_next_wake() {
        // The first-tick floor [`INITIAL_NEXT_WAKE`] is applied by the wake
        // gate when `next_wake.is_none()`, NOT by the SchedulerCursor
        // itself. Default cursor must surface a None.
        let c = SchedulerCursor::default();
        assert!(c.next_wake.is_none());
    }

    #[test]
    fn parent_ref_default_uses_external_signal_constant() {
        let p = ParentRef::default();
        assert!(p.workflow_id.is_empty());
        assert_eq!(p.signal, ParentRef::DEFAULT_SIGNAL);
        assert_eq!(p.signal, "external_signal");
    }

    #[test]
    fn parent_ref_round_trips_through_json() {
        let p = ParentRef {
            workflow_id: "graphs/g1/agents/parent".into(),
            signal: "custom_signal".into(),
        };
        let s = serde_json::to_string(&p).unwrap();
        let back: ParentRef = serde_json::from_str(&s).unwrap();
        assert_eq!(p, back);
        assert!(s.contains("\"workflow_id\":\"graphs/g1/agents/parent\""));
    }

    #[test]
    fn fs_handle_for_agent_uses_workflow_id_layout() {
        use uuid::Uuid;
        let g = GraphId::new(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap());
        let a = AgentId::new(Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap());
        let h = FsHandle::for_agent(g, a);
        assert_eq!(
            h.prefix,
            "graphs/11111111-2222-3333-4444-555555555555/agents/66666666-7777-8888-9999-aaaaaaaaaaaa",
        );
    }

    #[test]
    fn build_child_input_populates_parent_handle_and_identity() {
        use uuid::Uuid;
        let parent_graph_id =
            GraphId::new(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap());
        let parent_agent_id =
            AgentId::new(Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap());
        let child_agent_id =
            AgentId::new(Uuid::parse_str("bbbbbbbb-cccc-dddd-eeee-ffffffffffff").unwrap());
        let mandate = Mandate::new("child mandate", Duration::from_millis(500), Some(8));

        let input = build_child_input(
            "graphs/g1/agents/parent",
            parent_agent_id,
            parent_graph_id,
            child_agent_id,
            "fetcher".into(),
            mandate.clone(),
            AgentConfig::default(),
        );

        assert_eq!(input.graph_id, parent_graph_id);
        assert_eq!(input.agent_id, child_agent_id);
        assert_eq!(input.agent_name, "fetcher");
        assert_eq!(input.mandate, mandate);

        assert!(input.carryover.is_none());

        // FS prefix scopes under the parent's graph (NOT a fresh graph_id).
        assert_eq!(
            input.fs_handle.prefix,
            format!("graphs/{parent_graph_id}/agents/{child_agent_id}"),
        );

        let parent_handle = input
            .parent_handle
            .as_ref()
            .expect("build_child_input must populate parent_handle");
        assert_eq!(parent_handle.workflow_id, "graphs/g1/agents/parent");
        assert_eq!(parent_handle.signal, ParentRef::DEFAULT_SIGNAL);
    }

    #[test]
    fn child_handles_round_trip_via_carryover() {
        use uuid::Uuid;
        let h1 = AgentRef::new(
            "graphs/g1/agents/c1",
            AgentId::new(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap()),
        );
        let h2 = AgentRef::new(
            "graphs/g1/agents/c2",
            AgentId::new(Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap()),
        );
        let mut wf = AgentWorkflow::default();
        wf.child_handles.push(h1.clone());
        wf.child_handles.push(h2.clone());

        let c = wf.encode_carryover();
        assert_eq!(c.child_handles, vec![h1.clone(), h2.clone()]);

        let json = serde_json::to_string(&c).expect("serialize carryover w/ child_handles");
        let c2: Carryover =
            serde_json::from_str(&json).expect("deserialize carryover w/ child_handles");
        assert_eq!(c2.child_handles, vec![h1.clone(), h2.clone()]);

        let mut wf2 = AgentWorkflow::default();
        wf2.hydrate_from_carryover(c2);
        assert_eq!(wf2.child_handles, vec![h1, h2]);
    }

    /// Simulate the workflow-state mutation `Decision::RetireChild`
    /// performs (drop the named child's [`AgentRef`] from `child_handles`)
    /// and assert the surviving set round-trips through [`Carryover`].
    ///
    /// We cannot construct a `WorkflowContext` in a unit test (it's SDK-
    /// private), so we exercise the load-bearing invariants at the
    /// workflow-state level.
    #[test]
    fn retire_child_removes_handle_and_survives_carryover() {
        use uuid::Uuid;
        let h1 = AgentRef::new(
            "graphs/g1/agents/c1",
            AgentId::new(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap()),
        );
        let h2 = AgentRef::new(
            "graphs/g1/agents/c2",
            AgentId::new(Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap()),
        );
        let mut wf = AgentWorkflow::default();
        wf.child_handles.push(h1.clone());
        wf.child_handles.push(h2.clone());

        let target_agent_id = h1.agent_id;
        wf.child_handles.retain(|h| h.agent_id != target_agent_id);
        assert_eq!(wf.child_handles, vec![h2.clone()]);

        let c = wf.encode_carryover();
        let json = serde_json::to_string(&c).expect("serialize carryover");
        let c2: Carryover = serde_json::from_str(&json).expect("deserialize carryover");
        let mut wf2 = AgentWorkflow::default();
        wf2.hydrate_from_carryover(c2);
        assert_eq!(
            wf2.child_handles,
            vec![h2],
            "retire_child's removal must survive the CAN boundary",
        );
    }

    /// Simulate the workflow-state mutation `Decision::ReplaceChild`
    /// performs (drop the old child's [`AgentRef`], add the replacement's)
    /// and assert the swap round-trips through [`Carryover`].
    #[test]
    fn replace_child_swaps_handle_and_survives_carryover() {
        use uuid::Uuid;
        let old = AgentRef::new(
            "graphs/g1/agents/c1",
            AgentId::new(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap()),
        );
        let other = AgentRef::new(
            "graphs/g1/agents/c2",
            AgentId::new(Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap()),
        );
        let replacement = AgentRef::new(
            "graphs/g1/agents/c3",
            AgentId::new(Uuid::parse_str("bbbbbbbb-cccc-dddd-eeee-ffffffffffff").unwrap()),
        );
        let mut wf = AgentWorkflow::default();
        wf.child_handles.push(old.clone());
        wf.child_handles.push(other.clone());

        // `retire_child` drops `old`, then `spawn_child`'s state_mut
        // pushes the replacement ref.
        let target_agent_id = old.agent_id;
        wf.child_handles.retain(|h| h.agent_id != target_agent_id);
        wf.child_handles.push(replacement.clone());
        assert_eq!(wf.child_handles, vec![other.clone(), replacement.clone()]);

        let c = wf.encode_carryover();
        let json = serde_json::to_string(&c).expect("serialize carryover");
        let c2: Carryover = serde_json::from_str(&json).expect("deserialize carryover");
        let mut wf2 = AgentWorkflow::default();
        wf2.hydrate_from_carryover(c2);
        assert_eq!(
            wf2.child_handles,
            vec![other, replacement],
            "replace_child's swap (old removed, replacement added) must survive the CAN boundary",
        );
    }

    /// `retire_child`'s retain step must not drop unrelated children when
    /// the target id doesn't match anything in `child_handles` (e.g. the
    /// LLM emitted a `RetireChild` for a child the parent never spawned, or
    /// the same `RetireChild` arm ran twice on a tick boundary). The set is
    /// unchanged in that case — the signal still goes out (best-effort),
    /// but the workflow-state mutation is a no-op.
    #[test]
    fn retire_child_with_unknown_id_leaves_handles_unchanged() {
        use uuid::Uuid;
        let h1 = AgentRef::new(
            "graphs/g1/agents/c1",
            AgentId::new(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap()),
        );
        let h2 = AgentRef::new(
            "graphs/g1/agents/c2",
            AgentId::new(Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap()),
        );
        let mut wf = AgentWorkflow::default();
        wf.child_handles.push(h1.clone());
        wf.child_handles.push(h2.clone());

        let unknown_agent_id =
            AgentId::new(Uuid::parse_str("dddddddd-eeee-ffff-0000-111111111111").unwrap());
        wf.child_handles.retain(|h| h.agent_id != unknown_agent_id);
        assert_eq!(wf.child_handles, vec![h1, h2]);
    }

    /// The decision-log summary string is what the TUI reader displays per
    /// tick. Pin the shape of each `Decision` arm so a future refactor of
    /// the formatter can't silently drop one.
    #[test]
    fn decision_summary_covers_every_decision_arm() {
        use coral_node::decision::{ClaimSeed, FsOp};

        let s = decision_summary(&Decision::Idle {
            next_after: Duration::from_millis(250),
        });
        assert!(s.starts_with("Idle"), "got: {s}");
        assert!(s.contains("250"), "got: {s}");

        let s = decision_summary(&Decision::Retire {
            reason: "max_ticks".into(),
        });
        assert!(s.starts_with("Retire"), "got: {s}");
        assert!(s.contains("max_ticks"), "got: {s}");

        let s = decision_summary(&Decision::CallTools {
            calls: vec![
                coral_node::decision::ToolCall::new(
                    "echo",
                    serde_json::json!({}),
                    ClaimSeed::new("a"),
                ),
                coral_node::decision::ToolCall::new(
                    "echo",
                    serde_json::json!({}),
                    ClaimSeed::new("b"),
                ),
            ],
        });
        assert!(s.contains("CallTools"), "got: {s}");
        assert!(s.contains("count: 2"), "got: {s}");

        let s = decision_summary(&Decision::EmitOutput {
            content: "claim".into(),
            evidence: vec![EvidenceId::from_hex("0123456789abcdef")],
        });
        assert!(s.contains("EmitOutput"), "got: {s}");
        assert!(s.contains("evidence: 1"), "got: {s}");

        let s = decision_summary(&Decision::RewriteFs {
            ops: vec![FsOp::WriteFile {
                path: "notes/x.md".into(),
                content: "hi".into(),
            }],
        });
        assert!(s.contains("RewriteFs"), "got: {s}");
        assert!(s.contains("ops: 1"), "got: {s}");
    }

    #[test]
    fn format_correction_batch_failure_enumerates_each_call() {
        let s = format_correction(&[
            ToolCallFailure {
                tool: "a".into(),
                args: serde_json::json!({}),
                error: "x".into(),
            },
            ToolCallFailure {
                tool: "b".into(),
                args: serde_json::json!({}),
                error: "y".into(),
            },
        ]);
        assert!(s.contains("2 parallel"), "got: {s}");
        assert!(s.contains("\"a\""), "got: {s}");
        assert!(s.contains("\"b\""), "got: {s}");
    }

    #[test]
    fn max_ticks_retire_reason_fires_at_or_past_the_cap() {
        // Boundary: tick == max retires (the in-process loop performs `max`
        // ticks then stops on the would-be tick `max`).
        assert_eq!(
            max_ticks_retire_reason(3, Some(3)).as_deref(),
            Some("max_ticks (3) reached"),
        );
        assert_eq!(
            max_ticks_retire_reason(4, Some(3)).as_deref(),
            Some("max_ticks (3) reached"),
        );
        // Under budget: keep running.
        assert_eq!(max_ticks_retire_reason(2, Some(3)), None);
        assert_eq!(max_ticks_retire_reason(0, Some(1)), None);
        // No cap: never retires on this axis.
        assert_eq!(max_ticks_retire_reason(u64::MAX, None), None);
    }

    #[test]
    fn demote_retire_if_persistent_only_demotes_a_persistent_retire() {
        let idle = Duration::from_millis(500);
        let mut persistent = Mandate::new("monitor", idle, None);
        persistent.persistent = true;
        let one_shot = Mandate::new("one-shot", idle, None);

        // Persistent + Retire → Idle for one idle_period.
        let demoted = demote_retire_if_persistent(
            Decision::Retire {
                reason: "model wants out".into(),
            },
            &persistent,
        );
        assert_eq!(demoted, Decision::Idle { next_after: idle });

        // Non-persistent + Retire → unchanged (today's stop-on-decision).
        let kept = demote_retire_if_persistent(
            Decision::Retire {
                reason: "done".into(),
            },
            &one_shot,
        );
        assert_eq!(
            kept,
            Decision::Retire {
                reason: "done".into()
            }
        );

        // Persistent + non-Retire → passes through untouched.
        let passthrough = demote_retire_if_persistent(
            Decision::EmitOutput {
                content: "refreshed report".into(),
                evidence: vec![],
            },
            &persistent,
        );
        assert_eq!(
            passthrough,
            Decision::EmitOutput {
                content: "refreshed report".into(),
                evidence: vec![],
            }
        );
    }

    fn empty_drained() -> DrainedBuckets {
        DrainedBuckets {
            triggers: vec![],
            human_ops: vec![],
            mandate_patches: vec![],
            retirement: None,
            prior_correction: None,
        }
    }

    #[test]
    fn synthesize_scheduled_wake_injects_only_on_an_empty_idle_tick() {
        // Pure idle wake: nothing was queued → one synthesized ScheduledWake
        // lands in the trigger list that `assemble` forwards verbatim.
        let mut idle = empty_drained();
        synthesize_scheduled_wake(&mut idle);
        assert_eq!(idle.triggers, vec![Trigger::ScheduledWake]);
    }

    #[test]
    fn synthesize_scheduled_wake_is_a_noop_when_any_work_was_drained() {
        // A real trigger already present: don't add a spurious wake.
        let mut with_trigger = DrainedBuckets {
            triggers: vec![Trigger::External {
                kind: "webhook".into(),
                payload: serde_json::json!({}),
            }],
            ..empty_drained()
        };
        synthesize_scheduled_wake(&mut with_trigger);
        assert_eq!(with_trigger.triggers.len(), 1);
        assert!(!with_trigger
            .triggers
            .iter()
            .any(|t| matches!(t, Trigger::ScheduledWake)));

        // Human op pending → the tick has work; no wake.
        let mut with_human_op = DrainedBuckets {
            human_ops: vec![HumanOp::new(serde_json::json!({"action": "pause"}))],
            ..empty_drained()
        };
        synthesize_scheduled_wake(&mut with_human_op);
        assert!(with_human_op.triggers.is_empty());

        // Mandate patch pending → work; no wake.
        let mut with_patch = DrainedBuckets {
            mandate_patches: vec![MandatePatch::new(serde_json::json!({"model": "x"}))],
            ..empty_drained()
        };
        synthesize_scheduled_wake(&mut with_patch);
        assert!(with_patch.triggers.is_empty());

        // Re-deciding after a tool failure is "other work" — match the
        // in-process semantic and skip the wake.
        let mut with_correction = DrainedBuckets {
            prior_correction: Some(CorrectionContext::new("prior failure")),
            ..empty_drained()
        };
        synthesize_scheduled_wake(&mut with_correction);
        assert!(with_correction.triggers.is_empty());
    }
}

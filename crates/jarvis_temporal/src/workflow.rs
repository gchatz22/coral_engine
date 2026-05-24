//! Stage 3.2 (JAR2-58) — `AgentWorkflow` skeleton.
//! Stage 3.3 (JAR2-59) — signal handlers + `inspect_state` update.
//! Stage 3.4 (JAR2-60) — per-tick orchestration loop body.
//! Stage 3.11 (JAR2-67) — typed [`Carryover`] + real continue-as-new.
//!
//! This module owns the workflow type, the input/output shapes, the
//! signal/update surface, and the per-tick loop that orchestrates the
//! six activities defined in [`crate::activities`].
//!
//! ## What stage 3.4 changes (JAR2-60)
//!
//! - **Replaces** the placeholder body that lived here in stages 3.2 and
//!   3.3 (a one-shot `continue_as_new` followed by a `wait_condition(retire)
//!   || timer(POST_CAN_RETIREMENT_WAIT)` race). The real loop runs the
//!   per-tick shape from `scratch/temporal_staged_plan.md` § 2:
//!
//!   ```text
//!   loop {
//!       wait until pending_triggers non-empty OR retirement_request set OR next_wake elapsed
//!       drain (triggers, human_ops, mandate_patches, retirement_request)
//!       if retirement_request: persist_retirement; return Retired
//!       bundle  = assemble_context(...)
//!       decision = decide_next_action(bundle)
//!       match decision { CallTools, EmitOutput, RewriteFs, Retire, Idle }
//!       if continue_as_new_suggested(): continue_as_new(Carryover::default())
//!   }
//!   ```
//!
//! - **Replaces** the JAR2-58 `Option<Carryover>` sentinel with the
//!   SDK-blessed `ctx.continue_as_new_suggested()` signal. The first run
//!   no longer continues-as-new immediately; instead the loop runs until
//!   the SDK suggests continuation (history pressure), at which point the
//!   workflow continues-as-new with a real typed [`Carryover`] (filled
//!   in by JAR2-67, which lands the carryover schema + encode/hydrate
//!   helpers).
//!
//! - **Adds** six activity invocations via [`crate::activities::AgentActivities`].
//!   Every body is a stub returning canned `Ok(...)` so the loop runs
//!   end-to-end against [`crate::activities::set_decision_script`] in
//!   tests. Real activity bodies land in JAR2-61..66.
//!
//! - **Adds** a `next_wake: Option<Duration>` workflow-state field
//!   updated by `Decision::Idle { next_after }`. On the very first
//!   tick of a run (and after every continue-as-new) it defaults to
//!   [`INITIAL_NEXT_WAKE`] (1ms) so the loop fires immediately rather
//!   than sitting on an arbitrary timeout. JAR2-67 may pin a real
//!   per-mandate initial cadence.
//!
//! - **Adds** a `staged_correction: Option<CorrectionContext>` workflow-
//!   state field for next-tick `assemble_context` input. When a tick's
//!   tool batch returns a `Failure` outcome, the loop stages a
//!   correction context (mirroring `agent_core::tool_failure_correction_text`'s
//!   intent) so the next tick's LLM sees the failure. Real correction
//!   semantics — budget accounting, retry trail — stay in
//!   `agent_core::Agent::run`; the workflow does not ape that state
//!   machine (see JAR2-60 ticket guardrail 6).
//!
//! ## SDK constraints (see `scratch/temporal_rust_sdk_smoke.md`)
//!
//! - Concurrency primitives are
//!   `temporalio_sdk::workflows::{select!, join_all}` — **never**
//!   `tokio::select!` / `tokio::join!`. Non-SDK wake-ups fail the
//!   workflow task (smoke § 2 row "wait_condition racing signal vs timer").
//! - `start_activity` returns a `CancellableFuture<Result<...>>`; the
//!   awaited form `.start_activity(...).await?` works, the `?` before
//!   `await` is a compile error (smoke § 3.3).
//! - Workflow state mutation is via `ctx.state_mut(|s| ...)`, not bare
//!   `self`-receiver (the `run` body has `ctx: &mut WorkflowContext<Self>`
//!   and no `&self`).
//!
//! ## Determinism
//!
//! No clocks, no random, no I/O in the workflow body. Wall-clock time
//! arrives only through `ctx.timer(Duration)`; FS reads/writes live
//! inside activities. The loop is fully replayable against workflow
//! history.

use std::time::Duration;

use jarvis_node::decision::{ContextBundle, CorrectionContext, Decision, ToolCall};
use jarvis_node::evidence::EvidenceId;
use jarvis_node::mandate::{Mandate, OutputId};
use jarvis_node::trigger::{HumanOp, MandatePatch, Trigger};
use serde::{Deserialize, Serialize};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ContinueAsNewOptions, SyncWorkflowContext, WorkflowContext, WorkflowResult,
};

use crate::activities::{
    AgentActivities, ApplyFsOpsInput, AssembleContextInput, DecideInput, ExecuteToolInput,
    PersistOutputInput, PersistRetirementInput, ToolCallFailure, ToolCallOutcome,
};

/// Resolved agent configuration handed to the workflow at start.
///
/// **Placeholder.** Stage 3.2 only needed the type to exist; stage 3.4
/// passes it through to every activity input so JAR2-61..66 can fill in
/// the real shape (mandate, tool refs, model routing) without changing
/// the workflow body. The field will be driven by what `AgentCore` needs
/// to make a `Decision`, sourced from the structural DB + per-agent FS
/// overrides per stage 1's three-layer-resolution decision
/// (`scratch/temporal_staged_plan.md` § 8 decision 4).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConfig {}

/// Storage handle scoping the agent to its `<graph_id>/<agent_id>` prefix.
///
/// **Placeholder.** Stage 2.5 (`scratch/agent_storage.md`) ships the
/// `AgentStorage` trait + `AgentFs` facade with the prefix baked in;
/// stage 3 plumbs the `Arc<dyn AgentStorage>` + prefix through the
/// workflow input. The workflow body today passes `fs_handle` into every
/// activity input but does not dereference it — FS reads/writes belong
/// to activities, not the workflow body.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsHandle {
    /// `<graph_id>/<agent_id>` — the prefix the storage trait scopes to.
    /// JAR2-61..66 read this to instantiate `AgentFs` inside the activity
    /// body.
    pub prefix: String,
}

/// Parent workflow reference for cross-workflow signal routing.
///
/// **Placeholder.** Stage 5 territory (parent → child topology + child →
/// parent signal path per `scratch/temporal_staged_plan.md` § 5 stage 5).
/// Field exists on `AgentInput` so the input schema doesn't churn when
/// stage 5 fills it in.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentRef {}

/// Scheduler-state subset of the [`Carryover`].
///
/// Today this is just `next_wake` — the per-mandate idle cadence the
/// previous run pinned via `Decision::Idle { next_after }`. Wrapping it
/// in a struct (rather than carrying a bare `Option<Duration>` on
/// `Carryover`) reserves the slot for the per-mandate cursor work in
/// later stages (scheduler v2, parent-side fan-out cadence) without
/// renaming a field on the wire.
///
/// **Deliberately no `last_tick_at` timestamp.** `ctx.workflow_time()`
/// is deterministic per-replay but a wall-clock timestamp on the
/// carryover would only be observed at encode time on the post-CAN
/// run's replay — and adds zero scheduling value over `next_wake`
/// alone, since the new run pins its own first-tick wake the same way
/// the very first run does (defaulting to [`INITIAL_NEXT_WAKE`] when
/// `next_wake` is `None`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerCursor {
    /// The `next_wake` cadence the previous run had pinned. Restored
    /// onto [`AgentWorkflow::next_wake`] on hydrate; `None` means the
    /// previous run never saw a `Decision::Idle` (so the new run
    /// defaults to the [`INITIAL_NEXT_WAKE`] floor on its first tick,
    /// same as a brand-new workflow).
    pub next_wake: Option<Duration>,
}

/// Stage-5 child-workflow handle placeholder.
///
/// Empty struct because stage 3 does not have a parent-child topology
/// (see `scratch/temporal_staged_plan.md` § 5 stage 5). The
/// `Carryover.child_handles` vector is structurally always empty
/// today; carrying it across CAN is a no-op until stage 5 fills in
/// the field. `#[non_exhaustive]` reserves room for future fields
/// (`workflow_id`, `run_id`, `parent_signal_path`, …) without a wire
/// break.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ChildRef {}

/// Typed continue-as-new carryover.
///
/// Per `scratch/agent_runtime.md` § 9, the carryover is *not*
/// conversation history or tool results (those survive trivially via
/// the per-agent FS, which is external to Temporal history). It is a
/// small, typed, deterministically-rebuildable subset of in-workflow
/// state that would otherwise be lost when `ctx.continue_as_new(...)`
/// terminates the current run and starts a fresh one.
///
/// Every field maps to a workflow-state field that the run loop
/// observes or mutates. The mapping is:
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
/// | `child_handles` | (stage 5) | Always empty today |
///
/// **`staged_correction` is preserved across CAN** (the ticket's spec
/// list omitted it; we include it because dropping it would lose one
/// tick of correction context the previous run had already staged for
/// the next tick — visible behavior change). It's a `CorrectionContext`
/// itself, which is `Serialize`/`Deserialize` via `jarvis_node::decision`.
///
/// **`mid_tick_evidence` is structurally empty today** because the
/// CAN check happens at end-of-tick, after every activity has returned.
/// The field exists for stage 4+'s mid-tick checkpointing; today it
/// round-trips as `Vec::new()`.
///
/// **`cumulative_*_observed` survive CAN.** Without this, a snapshot
/// taken on the post-CAN run would report `cumulative_triggers_observed
/// == 0` even though the workflow lifetime had observed N signals on
/// the pre-CAN run — breaking the JAR2-59 semantics of "did we
/// observe a signal across the workflow's lifetime?".
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Carryover {
    /// `external_signal` payloads that arrived after the previous run's
    /// last drain. Restored to [`AgentWorkflow::pending_triggers`] on
    /// hydrate so the new run's first wake fires on them.
    pub pending_triggers: Vec<Trigger>,
    /// `human_override` payloads pending consumption.
    pub pending_human_ops: Vec<HumanOp>,
    /// `mandate_update` payloads pending consumption.
    pub pending_mandate_patches: Vec<MandatePatch>,
    /// Set by the `retire` signal handler; if `Some(_)` on hydrate, the
    /// new run's first tick short-circuits to `persist_retirement`.
    pub retirement_request: Option<String>,
    /// One-tick correction context staged by the previous tick's
    /// `Decision::CallTools` failure handling. Threaded into the next
    /// `assemble_context` activity input on the new run.
    pub staged_correction: Option<CorrectionContext>,
    /// Wraps [`AgentWorkflow::next_wake`]. The field is a struct rather
    /// than a bare `Option<Duration>` so future scheduler state can
    /// slot in without a wire break (see [`SchedulerCursor`] doc).
    pub scheduler_cursor: SchedulerCursor,
    /// Stage-5 placeholder — always `Vec::new()` today (see
    /// [`ChildRef`] doc).
    pub child_handles: Vec<ChildRef>,
    /// `EmitOutput`-side last-persisted output id. Today the workflow
    /// body does not consume this (`persist_output` activity writes the
    /// output without echoing the id back into workflow state), but
    /// carrying it across CAN unlocks stage 6.5's TUI snapshot field
    /// `recent_output_ids` and stage 4's parent → child output
    /// chaining. Today round-trips as `None`.
    pub last_output_id: Option<OutputId>,
    /// EvidenceIds collected by activities partway through a tick that
    /// CAN fires *during*. Empty in stage 3 — CAN is checked at
    /// end-of-tick — but reserved on the wire for stage 4+'s mid-tick
    /// checkpointing.
    pub mid_tick_evidence: Vec<EvidenceId>,
    /// Cumulative count of `Trigger`s observed via `external_signal`
    /// across the **workflow's lifetime** (including all prior CAN
    /// runs). Critical: without this, the [`AgentSnapshot`]
    /// `cumulative_triggers_observed` field on a post-CAN snapshot
    /// would only reflect signals received on the current run, not the
    /// lifetime view JAR2-59 promised.
    pub cumulative_triggers_observed: u64,
    pub cumulative_human_ops_observed: u64,
    pub cumulative_mandate_patches_observed: u64,
}

/// Input handed to `AgentWorkflow::run` at start (and at every
/// continue-as-new).
///
/// Stage 3.11 (JAR2-67): `carryover` is now a *load-bearing* field, not
/// informational. On hydrate the workflow body decodes it via
/// [`AgentWorkflow::hydrate_from_carryover`] back onto workflow state so
/// pending signal queues, retirement requests, `next_wake`, the
/// `staged_correction` from the previous tick, and the cumulative
/// observability counters all survive a CAN boundary. `None` means
/// "first run of this workflow" — the workflow starts from `Default`
/// state, identical to JAR2-58's first-run shape.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentInput {
    pub cfg: AgentConfig,
    pub fs_handle: FsHandle,
    pub parent_handle: Option<ParentRef>,
    pub carryover: Option<Carryover>,
}

/// Result returned by `AgentWorkflow::run` when the workflow exits cleanly.
///
/// Stage 3.4 (JAR2-60) adds the `Retired` variant — the loop returns it
/// from the retirement path (either via `Decision::Retire` or the `retire`
/// signal short-circuit). `Default` is retained for wire compatibility
/// with JAR2-58's tests and is equivalent to `AgentResult::Retired { reason: String::new() }`.
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
///
/// **Placeholder shape.** Stage 6.5 (`scratch/temporal_staged_plan.md`
/// § 5 stage 6.5) ships the real fields the TUI live-feed needs
/// (`mandate`, `last_decision`, `health`, `recent_output_ids`,
/// `child_handles`). Today the only consumer is JAR2-59's live test,
/// which asserts each signal arm landed on workflow state — so the
/// snapshot exposes per-bucket counts, the last-observed retirement
/// reason, and (for parity with the future shape) a placeholder
/// `recent_output_ids: Vec<String>` that stays empty until JAR2-64 wires
/// `persist_output`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentSnapshot {
    /// Count of `Trigger`s currently queued in `pending_triggers`.
    /// JAR2-60: the loop body drains this at the top of every tick, so
    /// `0` here doesn't mean "the signal didn't land" — see
    /// `cumulative_triggers_observed` for the persistent
    /// "did-it-arrive?" view.
    pub pending_triggers_count: usize,
    pub pending_human_ops_count: usize,
    pub pending_mandate_patches_count: usize,
    pub retirement_request: Option<String>,
    pub recent_output_ids: Vec<String>,
    /// JAR2-60: cumulative count of `Trigger`s observed via
    /// `external_signal` since the workflow started (or its last
    /// continue-as-new). Bumped in the signal handler at receipt time
    /// so an inspect taken between signal arrival and the next loop
    /// drain still reflects the arrival. `u64` for platform-stable
    /// wire shape.
    ///
    /// `#[serde(default)]` keeps the wire form backward-compatible with
    /// pre-JAR2-60 snapshots (which had no cumulative counters).
    #[serde(default)]
    pub cumulative_triggers_observed: u64,
    /// JAR2-60: cumulative `HumanOp`s observed via `human_override`.
    #[serde(default)]
    pub cumulative_human_ops_observed: u64,
    /// JAR2-60: cumulative `MandatePatch`es observed via
    /// `mandate_update`.
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

/// Build the workflow ID for an agent.
///
/// URL-shaped scheme per `scratch/temporal_staged_plan.md` § 8 decision 2:
/// **`graphs/<graph_id>/agents/<agent_id>`**.
pub fn agent_workflow_id(graph_id: &str, agent_id: &str) -> String {
    format!("graphs/{graph_id}/agents/{agent_id}")
}

/// `next_wake` value when the workflow state hasn't been told a specific
/// idle period yet (the first tick of a run, or the first tick after a
/// continue-as-new). Deliberately tiny — the first iteration's wake
/// gate must fire immediately so the workflow doesn't sit idle waiting
/// for nothing. Subsequent ticks honor whatever the LLM (or test
/// script) emits via `Decision::Idle`. JAR2-67's real `Carryover` may
/// pin a per-mandate initial cadence; today the placeholder is enough.
const INITIAL_NEXT_WAKE: Duration = Duration::from_millis(1);

/// Per-activity start-to-close timeout. Generous so a stub activity
/// (microseconds of work) and a real activity (LLM calls, FS writes)
/// both fit; the workflow loop's own deadlines come from `next_wake`
/// and the retirement signal, not the activity timeout.
const ACTIVITY_TIMEOUT: Duration = Duration::from_secs(30);

/// The agent workflow.
///
/// `#[derive(Default)]` is required by the SDK's `#[workflow]` macro.
/// Every field defaults to its empty value; the loop body reads/writes
/// state via `ctx.state_mut(|s| ...)` per the SDK shape.
#[workflow]
#[derive(Default)]
pub struct AgentWorkflow {
    /// `external_signal` queue. Pushed by the signal handler; drained at
    /// the top of every loop iteration.
    pending_triggers: Vec<Trigger>,
    /// `human_override` queue. Drained alongside `pending_triggers` and
    /// passed to `assemble_context` as a separate field. JAR2-61 may
    /// fold these into the `Trigger::HumanOverride` taxonomy or thread
    /// them through `CorrectionContext`; the workflow body doesn't
    /// decide.
    pending_human_ops: Vec<HumanOp>,
    /// `mandate_update` queue. Drained alongside `pending_triggers` and
    /// passed to `assemble_context` as a separate field. Stage 6 owns
    /// the consumption semantics.
    pending_mandate_patches: Vec<MandatePatch>,
    /// `retire` request. Checked at the top of every loop iteration; a
    /// set value short-circuits the tick to the retirement path.
    retirement_request: Option<String>,
    /// Wall-clock the next idle `ctx.timer(...)` waits for. Updated by
    /// `Decision::Idle { next_after }`. `None` on the very first tick of
    /// a run (the loop starts with [`INITIAL_NEXT_WAKE`] = 1ms so the
    /// first tick fires immediately). JAR2-67: a continue-as-new
    /// preserves the prior run's `next_wake` via
    /// [`Carryover::scheduler_cursor`], so a post-CAN run resumes with
    /// the cadence the pre-CAN run had pinned (`None` only if the
    /// pre-CAN run never observed a `Decision::Idle`).
    next_wake: Option<Duration>,
    /// Correction context staged by the previous tick when its tool
    /// batch returned failures. Threaded into the next
    /// `assemble_context` activity input. Cleared on a non-failing tick.
    staged_correction: Option<CorrectionContext>,
    /// Cumulative count of `Trigger`s observed via `external_signal`
    /// since the workflow started (or last continue-as-new). Bumped
    /// inside the [`AgentWorkflow::external_signal`] handler so a
    /// snapshot taken between signal arrival and the next loop drain
    /// still reflects the arrival (the JAR2-59 live test's
    /// "did the signal land on workflow state?" assertion).
    cumulative_triggers_observed: u64,
    /// Cumulative count of [`HumanOp`]s observed via `human_override`.
    /// Same rationale as `cumulative_triggers_observed`.
    cumulative_human_ops_observed: u64,
    /// Cumulative count of [`MandatePatch`]es observed via
    /// `mandate_update`. Same rationale.
    cumulative_mandate_patches_observed: u64,
    /// JAR2-67: last `persist_output` `OutputId` observed by this
    /// workflow run. Today the `persist_output` activity does not echo
    /// the id back into workflow state (the field stays `None`); the
    /// slot exists so the [`Carryover`] round-trip is structurally
    /// complete and stage 4+'s parent → child output chaining doesn't
    /// require a wire change.
    last_output_id: Option<OutputId>,
    /// JAR2-67: evidence ids collected by activities mid-tick. Empty
    /// in stage 3 — the CAN check fires at end-of-tick after every
    /// activity has returned — but reserved for stage 4+'s mid-tick
    /// checkpointing.
    mid_tick_evidence: Vec<EvidenceId>,
}

#[workflow_methods]
impl AgentWorkflow {
    /// `external_signal` — push a typed [`Trigger`] onto the per-tick
    /// queue. The loop drains the queue at the top of each iteration.
    ///
    /// Side-bookkeeps `cumulative_triggers_observed` at receipt time
    /// (not drain time) so the snapshot's cumulative view reflects
    /// every signal regardless of inspect timing relative to the loop.
    #[signal]
    pub fn external_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, trigger: Trigger) {
        self.pending_triggers.push(trigger);
        self.cumulative_triggers_observed = self.cumulative_triggers_observed.saturating_add(1);
    }

    /// `human_override` — push a typed [`HumanOp`] onto the override
    /// queue. Bookkeeps `cumulative_human_ops_observed` at receipt time.
    #[signal]
    pub fn human_override(&mut self, _ctx: &mut SyncWorkflowContext<Self>, op: HumanOp) {
        self.pending_human_ops.push(op);
        self.cumulative_human_ops_observed = self.cumulative_human_ops_observed.saturating_add(1);
    }

    /// `mandate_update` — push a typed [`MandatePatch`] onto the patch
    /// queue. Bookkeeps `cumulative_mandate_patches_observed` at
    /// receipt time.
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

    /// `inspect_state` — return a typed [`AgentSnapshot`] of the
    /// workflow's signal-bucket counts + retirement flag. Sync update;
    /// `&mut self` receiver per the macro's required shape.
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
    /// Reads top-to-bottom: hydrate carryover (if any) → loop {wake →
    /// drain → assemble → decide → dispatch → (maybe) continue-as-new}.
    /// Every external action (FS read/write, LLM call, tool dispatch)
    /// lives in an activity; the workflow body is pure orchestration.
    ///
    /// JAR2-67 wires the real continue-as-new shape:
    ///
    /// 1. On entry, if `input.carryover.is_some()`, the workflow state
    ///    is hydrated from it via [`hydrate_from_carryover`]. This is
    ///    the only place [`Carryover`] is decoded.
    /// 2. At end-of-tick (after the activity for the current decision
    ///    returned, *and only on non-retirement ticks*),
    ///    [`temporalio_sdk::WorkflowContext::continue_as_new_suggested`]
    ///    is consulted. If true, the workflow's current state is
    ///    encoded into a fresh [`Carryover`] via [`encode_carryover`]
    ///    and passed to `ctx.continue_as_new(&next_input, opts)`, which
    ///    returns `Err(WorkflowTermination::continue_as_new(...))` —
    ///    `?` propagates the termination out of the workflow body.
    ///
    /// **Retirement structurally cannot trigger CAN.** Both retirement
    /// paths (`drained.retirement` short-circuit at the top of the
    /// loop, and `Decision::Retire { reason }` at the bottom of the
    /// `match`) `return retire(...).await` before the CAN check, which
    /// lives only after the non-retire arms of the `match`.
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: AgentInput,
    ) -> WorkflowResult<AgentResult> {
        // JAR2-67: hydrate workflow state from carryover before the loop
        // begins, so the very first wake/drain sees every pre-CAN
        // pending signal, the prior `next_wake`, and the prior
        // `staged_correction`. `None` means "first run of this
        // workflow" — the workflow stays on its `Default` state, which
        // is the JAR2-58 first-run shape.
        if let Some(c) = input.carryover.clone() {
            ctx.state_mut(|s| s.hydrate_from_carryover(c));
        }
        loop {
            // Wake gate: triggers arrived, retirement requested, or the
            // idle deadline elapsed. Block-scoped so the `&self` borrows
            // on `wait_*` drop before subsequent activity calls.
            wait_for_tick(ctx).await;

            // Drain in-workflow state. The retirement short-circuit fires
            // before any activity invocation, AND before any CAN check,
            // so a `retire` signal can never trigger a continue-as-new.
            let drained = ctx.state_mut(drain_buckets);
            if let Some(reason) = drained.retirement {
                return retire(ctx, &input.fs_handle, reason).await;
            }

            // assemble → decide → dispatch.
            let bundle = assemble(ctx, &input, drained).await?;
            let decision = decide(ctx, bundle).await?;
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
                    return retire(ctx, &input.fs_handle, reason).await;
                }
            }

            // JAR2-67: continue-as-new when the SDK suggests it
            // (history pressure). This is the *only* trigger — there
            // is no manual history-length counter and no once-marker
            // sentinel. Note the early-`return` retirement arms above:
            // CAN is structurally never reached on a retirement tick.
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
    /// `&self` (not `&mut self`) so the encode is observation-only;
    /// the live workflow run will terminate immediately after `ctx.continue_as_new(...)`
    /// returns, so there is no value in clearing local state.
    ///
    /// JAR2-67 invariant: every workflow-state field that affects
    /// observable behavior — pending signal queues, retirement
    /// request, staged correction, next-wake cadence, cumulative
    /// observability counters — round-trips via this function.
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
            child_handles: Vec::new(),
            last_output_id: self.last_output_id,
            mid_tick_evidence: self.mid_tick_evidence.clone(),
            cumulative_triggers_observed: self.cumulative_triggers_observed,
            cumulative_human_ops_observed: self.cumulative_human_ops_observed,
            cumulative_mandate_patches_observed: self.cumulative_mandate_patches_observed,
        }
    }

    /// Decode a [`Carryover`] back onto the workflow's mutable state.
    ///
    /// Symmetric inverse of [`Self::encode_carryover`]. Called exactly
    /// once at the top of [`Self::run`] when `input.carryover.is_some()`.
    /// The workflow's [`Default`] starting state is the JAR2-58
    /// first-run shape; hydrate overwrites those fields with the
    /// carryover's values.
    ///
    /// `child_handles` is consumed but ignored — stage 3 has no
    /// parent-child topology (see [`ChildRef`]).
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
        // `child_handles` is structurally empty in stage 3 — ignored.
        let _ = c.child_handles;
    }
}

/// Wake gate for the loop body. Returns once any signal bucket is
/// non-empty (triggers, human ops, mandate patches, or retirement),
/// or the per-tick `next_wake` timer elapses.
/// `workflows::select!` is the SDK's deterministic race primitive
/// (`tokio::select!` would break replay).
///
/// Note: the JAR2-60 ticket's pseudocode wakes only on
/// `triggers_pending`. We wake on every non-retire signal bucket too so
/// operator-sent overrides (`human_override`, `mandate_update`) round-
/// trip through the loop within one tick rather than waiting up to
/// `next_wake` for the next idle wake. Matches the in-process
/// `Agent::run` behavior (every signal wakes the select).
///
/// The wait/timer futures borrow `ctx` immutably; the function-scope
/// boundary drops them before the caller resumes activity invocations.
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

/// Invoke the `assemble_context` activity with the per-tick drained
/// buckets. JAR2-61 wired the real activity body; this builder constructs
/// the typed input the activity consumes.
///
/// JAR2-61 note on the `mandate` argument: `AgentInput` still carries the
/// placeholder `AgentConfig` rather than a real [`Mandate`] (JAR2-67's
/// carryover work owns the upgrade), so the workflow body synthesizes a
/// minimal placeholder mandate here and ships it through to the activity.
/// `AgentFs::new_with_storage` is idempotent on `mandate.json` — passing a
/// placeholder is harmless on agents that already have a written mandate,
/// and is the bootstrap shape for fresh agents until JAR2-67 lands.
async fn assemble(
    ctx: &WorkflowContext<AgentWorkflow>,
    input: &AgentInput,
    drained: DrainedBuckets,
) -> WorkflowResult<ContextBundle> {
    let out = ctx
        .start_activity(
            AgentActivities::assemble_context,
            AssembleContextInput {
                mandate: placeholder_mandate(&input.cfg),
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

/// Build a placeholder [`Mandate`] from the workflow's [`AgentConfig`].
///
/// Today `AgentConfig` is an empty placeholder struct (stage 3 hasn't
/// resolved the three-layer mandate routing yet — see
/// `scratch/temporal_staged_plan.md` § 8 decision 4). When JAR2-67 ships
/// real continue-as-new carryover or stage 6 wires the structural DB →
/// mandate resolver, this helper goes away in favor of an
/// `input.mandate: Mandate` field on `AgentInput`. Until then the
/// `assemble_context` activity needs *some* `Mandate` to seed
/// `ContextBundle.mandate` + `AgentFs::new_with_storage`, so we
/// synthesize a minimal one here.
fn placeholder_mandate(_cfg: &AgentConfig) -> jarvis_node::mandate::Mandate {
    jarvis_node::mandate::Mandate::new("", Duration::ZERO, None)
}

/// Invoke the `decide_next_action` activity. Stub consults the
/// test-injected script in `activities::DECISION_SCRIPT`; JAR2-62 wraps
/// `LlmDecide`.
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
    evidence: Vec<jarvis_node::evidence::EvidenceId>,
) -> WorkflowResult<()> {
    ctx.start_activity(
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
    Ok(())
}

/// Invoke the `apply_fs_ops` activity for a `Decision::RewriteFs`.
///
/// JAR2-65: the activity body needs a [`Mandate`] to reify an
/// `AgentFs` against the worker-shared storage backend. `AgentConfig` is
/// still the JAR2-58 placeholder unit struct (`AgentConfig {}`); when a
/// later stage threads the resolved mandate into `AgentConfig`, this
/// call site swaps the placeholder for `input.cfg.mandate.clone()` and
/// no other code changes.
///
/// `Mandate::new("", Duration::ZERO, None)` is decorative because
/// `AgentFs::new_with_storage` only writes `mandate.json` when absent,
/// and `apply_fs_ops` runs only against agents whose `mandate.json`
/// already exists on disk (assemble_context wrote it on tick 1). The
/// activity body never reads the mandate — it only forwards it to
/// `new_with_storage`, which short-circuits the write.
async fn rewrite_fs(
    ctx: &WorkflowContext<AgentWorkflow>,
    fs_handle: &FsHandle,
    ops: Vec<jarvis_node::decision::FsOp>,
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

/// Invoke the `persist_retirement` activity and return the workflow
/// result. Shared between the retire-signal short-circuit and the
/// `Decision::Retire` arm.
async fn retire(
    ctx: &WorkflowContext<AgentWorkflow>,
    fs_handle: &FsHandle,
    reason: String,
) -> WorkflowResult<AgentResult> {
    ctx.start_activity(
        AgentActivities::persist_retirement,
        PersistRetirementInput {
            fs_handle: fs_handle.clone(),
            reason: reason.clone(),
        },
        activity_opts(),
    )
    .await?;
    Ok(AgentResult::Retired { reason })
}

/// Clear the staged correction in workflow state. Used by the
/// non-failing `Decision` arms (`EmitOutput`, `RewriteFs`, `Idle`) so a
/// previously-staged correction doesn't carry into the next tick once
/// the LLM has produced a satisfiable decision.
fn clear_correction(s: &mut AgentWorkflow) {
    s.staged_correction = None;
}

/// Owned payload produced by [`drain_buckets`] — the per-tick view of
/// every signal-staged bucket plus the previously-staged correction.
/// Kept distinct from `AssembleContextInput` because the workflow body
/// short-circuits on `retirement` before assembling a context.
struct DrainedBuckets {
    triggers: Vec<Trigger>,
    human_ops: Vec<HumanOp>,
    mandate_patches: Vec<MandatePatch>,
    retirement: Option<String>,
    prior_correction: Option<CorrectionContext>,
}

/// Drain the five signal-tracked fields out of workflow state into owned
/// values. Pure state mutation — pulled out of `run` so the loop body
/// stays inside the <100-line target the ticket calls out.
///
/// `cumulative_*_observed` counters are bumped by the signal handlers at
/// receipt time (not here at drain time) so a snapshot taken between a
/// signal landing and the next loop tick still reflects the arrival.
/// JAR2-67: counters now survive a continue-as-new via
/// [`Carryover::cumulative_triggers_observed`] et al.
fn drain_buckets(s: &mut AgentWorkflow) -> DrainedBuckets {
    DrainedBuckets {
        triggers: std::mem::take(&mut s.pending_triggers),
        human_ops: std::mem::take(&mut s.pending_human_ops),
        mandate_patches: std::mem::take(&mut s.pending_mandate_patches),
        retirement: s.retirement_request.take(),
        prior_correction: s.staged_correction.take(),
    }
}

/// Build the standard activity options. Pulled into a helper so every
/// activity invocation in the loop uses the same timeout shape.
fn activity_opts() -> ActivityOptions {
    ActivityOptions::start_to_close_timeout(ACTIVITY_TIMEOUT)
}

/// Fan out N `execute_tool` activity invocations via the SDK's
/// deterministic `workflows::join_all`. On any failure, stage a
/// correction context for next tick's `assemble_context` input
/// (mirroring `agent_core::tool_failure_correction_text`'s intent — see
/// JAR2-60 ticket guardrail 6: the workflow does NOT ape `agent_core`'s
/// budget state machine; it just delivers a description of the failure
/// so the LLM can see it on the next tick).
///
/// Free function so the loop body stays inside the line budget.
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

/// Render the staged correction text for a tool-batch failure. Mirrors
/// the *shape* of `agent_core::tool_failure_correction_text` (the
/// in-process loop's equivalent) but lives here so the workflow can
/// stage the message without a dependency cycle. JAR2-63 may unify the
/// two formatters once the real `execute_tool` body lands.
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
    fn agent_input_default_has_no_carryover() {
        // `Default` pinned by JAR2-58 + JAR2-59 tests. JAR2-60 keeps the
        // shape: `carryover.is_none()` is "first run", but the loop body
        // no longer branches on it — `ctx.continue_as_new_suggested()`
        // is the trigger.
        let input = AgentInput::default();
        assert!(input.carryover.is_none());
        assert!(input.parent_handle.is_none());
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
        };
        let json = serde_json::to_string(&input).expect("serialize AgentInput");
        let back: AgentInput = serde_json::from_str(&json).expect("deserialize AgentInput");
        assert_eq!(input, back);
    }

    #[test]
    fn agent_result_default_is_retired_with_empty_reason() {
        // JAR2-60 turns AgentResult into a tagged enum (added `Retired`
        // variant). The Default impl preserves wire compatibility with
        // JAR2-58 (which round-tripped `AgentResult::default()`); a new
        // round-trip test below proves both Default and a populated
        // value serialize cleanly.
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
    fn agent_snapshot_round_trips_pre_jar2_60_wire_shape() {
        // JAR2-60 added `cumulative_*_observed` to `AgentSnapshot`.
        // Each new field is `#[serde(default)]` so a pre-JAR2-60
        // snapshot (missing those fields) still deserializes — the
        // counters default to 0. Pin the contract.
        let pre_jar2_60 = r#"{
            "pending_triggers_count": 2,
            "pending_human_ops_count": 1,
            "pending_mandate_patches_count": 3,
            "retirement_request": "shutdown",
            "recent_output_ids": []
        }"#;
        let s: AgentSnapshot = serde_json::from_str(pre_jar2_60).unwrap();
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
        // Pin the in-workflow drain semantics — what `run` calls every
        // tick before invoking any activity. Critical that all five
        // buckets get cleared so a redundant retire signal arriving
        // mid-tick doesn't trip the next iteration's short-circuit.
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

        // Every bucket is empty after the drain.
        assert!(wf.pending_triggers.is_empty());
        assert!(wf.pending_human_ops.is_empty());
        assert!(wf.pending_mandate_patches.is_empty());
        assert!(wf.retirement_request.is_none());
        assert!(wf.staged_correction.is_none());

        // drain_buckets itself does NOT bump cumulative counters; the
        // signal handlers do that at receipt time. The bucket was
        // populated directly here in the test, bypassing the signal
        // path, so counters stay at 0.
        assert_eq!(wf.cumulative_triggers_observed, 0);
        assert_eq!(wf.cumulative_human_ops_observed, 0);
        assert_eq!(wf.cumulative_mandate_patches_observed, 0);
    }

    #[test]
    fn signal_handlers_bump_cumulative_counters_at_receipt() {
        // The cumulative view of "did the signal land?" is asserted
        // here against the handler-side bookkeeping. JAR2-59's live
        // test asserts the same property end-to-end via Temporal.
        // Mutating the bare field directly because the SDK's
        // SyncWorkflowContext can't be constructed in a unit test —
        // the handler body invariant we care about is bucket push +
        // counter bump.
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

        // Counters survive a drain.
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
        // serde_json::Value's Display renders the JSON form.
        assert!(s.contains("\"q\""), "got: {s}");
    }

    // ------------------------------------------------------------------
    // JAR2-67: Carryover tests — round-trip + cumulative counter bridging.
    // ------------------------------------------------------------------

    /// Build a [`Carryover`] with non-default values for every field —
    /// the JSON round-trip and hydrate/encode tests below all build
    /// against this fixture so a future field addition automatically
    /// shows up as a test miss if not represented.
    fn fully_populated_carryover() -> Carryover {
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
            child_handles: Vec::new(),
            last_output_id: Some(OutputId::new()),
            mid_tick_evidence: vec![EvidenceId::from_hex("0123456789abcdef")],
            cumulative_triggers_observed: 5,
            cumulative_human_ops_observed: 7,
            cumulative_mandate_patches_observed: 11,
        }
    }

    #[test]
    fn carryover_default_roundtrips_through_json() {
        // The first-CAN-from-cleanly-default-state case — the carryover
        // is `Carryover::default()`. Pin that the empty wire form is
        // deserialisable back into the same `Default` value.
        let c = Carryover::default();
        let json = serde_json::to_string(&c).expect("serialize default Carryover");
        let back: Carryover = serde_json::from_str(&json).expect("deserialize default Carryover");
        assert_eq!(c, back);
    }

    #[test]
    fn carryover_fully_populated_roundtrips_through_json() {
        // JAR2-67 § "Hard guardrails" 3: Carryover is serde
        // round-trippable end-to-end. Every field exercised, no
        // `#[serde(default)]` on individual fields (the wire shape
        // changes atomically with the type, per the no-back-compat
        // memory).
        let c = fully_populated_carryover();
        let json = serde_json::to_string(&c).expect("serialize populated Carryover");
        let back: Carryover = serde_json::from_str(&json).expect("deserialize populated Carryover");
        assert_eq!(c, back);
    }

    #[test]
    fn agent_input_with_populated_carryover_roundtrips_through_json() {
        // Workflow start receives the carryover wrapped in
        // `AgentInput`; this ensures the outer envelope's serde shape
        // round-trips with a non-empty carryover (the JAR2-58 test
        // only covered an empty default).
        let input = AgentInput {
            cfg: AgentConfig::default(),
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            parent_handle: None,
            carryover: Some(fully_populated_carryover()),
        };
        let json = serde_json::to_string(&input).unwrap();
        let back: AgentInput = serde_json::from_str(&json).unwrap();
        assert_eq!(input, back);
    }

    #[test]
    fn encode_then_hydrate_is_identity_on_workflow_state() {
        // Seed every workflow-state field that the carryover claims to
        // round-trip; encode; hydrate into a fresh `AgentWorkflow`;
        // assert the per-field projection matches. This is the local
        // analogue of the live CAN test — same invariant, no Temporal
        // round-trip.
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
        original.last_output_id = Some(OutputId::new());

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
    }

    #[test]
    fn hydrate_then_signal_handler_bumps_counter_past_carryover_value() {
        // JAR2-67 § "Hard guardrails" 4 / ticket "Cumulative counter
        // bridging" — the cumulative_*_observed counters must bridge
        // a CAN boundary. We can't construct a `SyncWorkflowContext`
        // in a unit test (it's SDK-private), so simulate the signal
        // handler's effect by replicating its `push + saturating_add`
        // bookkeeping. The handler's body is one line; the load-
        // bearing invariant is that the *value the counter starts
        // from* is the carryover's value, not zero.
        let pre_can = Carryover {
            cumulative_triggers_observed: 5,
            cumulative_human_ops_observed: 6,
            cumulative_mandate_patches_observed: 7,
            ..Carryover::default()
        };
        let mut wf = AgentWorkflow::default();
        wf.hydrate_from_carryover(pre_can);

        // Simulate the `external_signal` handler body.
        wf.pending_triggers.push(Trigger::ScheduledWake);
        wf.cumulative_triggers_observed = wf.cumulative_triggers_observed.saturating_add(1);

        // Cumulative view: 5 (pre-CAN) + 1 (post-CAN signal) = 6,
        // NOT 1. This is the load-bearing assertion: counter survived
        // the boundary AND the new signal lands on top of it.
        assert_eq!(
            wf.cumulative_triggers_observed, 6,
            "post-CAN signal must increment past the carried value"
        );
        // The other counters are unchanged but still reflect their
        // pre-CAN values, not 0.
        assert_eq!(wf.cumulative_human_ops_observed, 6);
        assert_eq!(wf.cumulative_mandate_patches_observed, 7);

        // And the snapshot the live update returns sees the bridged
        // value too — this is the JAR2-59 "did the signal land
        // across the workflow's lifetime?" contract.
        let snap = AgentSnapshot::from_state(&wf);
        assert_eq!(snap.cumulative_triggers_observed, 6);
        assert_eq!(snap.cumulative_human_ops_observed, 6);
        assert_eq!(snap.cumulative_mandate_patches_observed, 7);
    }

    #[test]
    fn carryover_from_default_workflow_is_default() {
        // A workflow that has never observed a signal, never ticked,
        // never staged a correction encodes to `Carryover::default()`.
        // Pin that no field accidentally picks up a non-default value
        // from `AgentWorkflow::default()`'s shape.
        let wf = AgentWorkflow::default();
        let c = wf.encode_carryover();
        assert_eq!(c, Carryover::default());
    }

    #[test]
    fn scheduler_cursor_default_has_no_next_wake() {
        // The first-tick floor [`INITIAL_NEXT_WAKE`] is applied by the
        // wake gate when `next_wake.is_none()`, NOT by the
        // SchedulerCursor itself. Default cursor must surface a None.
        let c = SchedulerCursor::default();
        assert!(c.next_wake.is_none());
    }

    #[test]
    fn child_handles_empty_on_stage_3_carryover() {
        // Stage 3 has no parent-child topology — every carryover
        // produced by the workflow body must have an empty
        // `child_handles`. Stage 5 fills this in.
        let mut wf = AgentWorkflow::default();
        wf.pending_triggers.push(Trigger::ScheduledWake);
        let c = wf.encode_carryover();
        assert!(c.child_handles.is_empty());
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
}

//! Stage 3.2 (JAR2-58) — `AgentWorkflow` skeleton.
//! Stage 3.3 (JAR2-59) — signal handlers + `inspect_state` update.
//! Stage 3.4 (JAR2-60) — per-tick orchestration loop body.
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
//!   workflow continues-as-new with a placeholder `Carryover` (JAR2-67
//!   fills in the real shape). The `Option<Carryover>` field stays on
//!   `AgentInput` for wire compatibility but is now informational only
//!   ("`Some` ⇒ this run was reached via continue-as-new").
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
use jarvis_node::mandate::Mandate;
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

/// Typed continue-as-new carryover.
///
/// **Placeholder.** JAR2-67 ships the real shape (trigger queue,
/// scheduler cursor, child handles, last output id, mid-tick evidence).
/// Stage 3.4 (JAR2-60) uses `Carryover::default()` as the placeholder
/// argument to `ctx.continue_as_new(...)` when
/// `ctx.continue_as_new_suggested()` fires — the workflow gets a fresh
/// run with empty state, which is correct for the stub-activities loop
/// because no in-flight semantic state crosses the boundary yet.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Carryover {}

/// Input handed to `AgentWorkflow::run` at start (and at every
/// continue-as-new).
///
/// The four-field shape matches the stage 3.2 ticket exactly so JAR2-67
/// (real carryover) can fill `carryover` without renaming.
///
/// Stage 3.4 (JAR2-60) note on `carryover`: the field is now
/// informational only — `Some(_)` means "this run was reached via
/// `continue_as_new`", `None` means "first run". The loop body no longer
/// branches on it; the JAR2-58 once-marker sentinel has been replaced by
/// `ctx.continue_as_new_suggested()`.
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
    /// first tick fires immediately) and after every continue-as-new
    /// (the workflow gets a fresh `Default` state on the new run).
    /// `Some(_)` once a `Decision::Idle` has pinned a cadence.
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
    /// Reads top-to-bottom: drain → assemble → decide → dispatch →
    /// (maybe) continue-as-new. Every external action (FS read/write,
    /// LLM call, tool dispatch) lives in an activity; the workflow body
    /// is pure orchestration.
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: AgentInput,
    ) -> WorkflowResult<AgentResult> {
        loop {
            // Wake gate: triggers arrived, retirement requested, or the
            // idle deadline elapsed. Block-scoped so the `&self` borrows
            // on `wait_*` drop before subsequent activity calls.
            wait_for_tick(ctx).await;

            // Drain in-workflow state. The retirement short-circuit fires
            // before any activity invocation. JAR2-67 may move this drain
            // into the assemble_context activity if the carryover wants
            // the buckets visible across continue-as-new.
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

            // continue_as_new when the SDK suggests it (history
            // pressure). JAR2-67 fills in the real `Carryover`.
            if ctx.continue_as_new_suggested() {
                let next = AgentInput {
                    carryover: Some(Carryover::default()),
                    ..input
                };
                ctx.continue_as_new(&next, ContinueAsNewOptions::default())?;
                unreachable!("continue_as_new should have terminated this run");
            }
        }
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
/// Counters do not survive a continue-as-new — `Carryover` is empty
/// today (JAR2-67 may carry them).
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

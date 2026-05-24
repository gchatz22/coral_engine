//! Stage 3.2 (JAR2-58) — `AgentWorkflow` skeleton.
//! Stage 3.3 (JAR2-59) — signal handlers + `inspect_state` update.
//!
//! This module defines the workflow type and input/output shapes that the
//! rest of stage 3 builds on. The body remains deliberately empty: the
//! `run` method continues-as-new once and then, on the post-CAN run,
//! waits on a `retirement_request` signal (or a short timer ceiling) and
//! exits. The real loop body (drain triggers → assemble context →
//! decide → dispatch) lands in JAR2-60; activities in JAR2-61..66; real
//! `Carryover` semantics in JAR2-67.
//!
//! ## What stage 3.3 adds (JAR2-59)
//!
//! - Four `#[signal]` handlers (`external_signal`, `human_override`,
//!   `mandate_update`, `retire`) that push their typed payloads onto
//!   per-bucket `Vec<_>` / `Option<_>` state. Nothing here consumes them
//!   yet — JAR2-60 drains them. The retirement signal is the lone
//!   exception: the post-CAN body returns early when
//!   `retirement_request.is_some()`, per the ticket's explicit allowance
//!   ("extend trivially to read `retirement_request` and return early").
//! - One `#[update]` (`inspect_state`) returning an [`AgentSnapshot`] that
//!   surfaces the current queue counts + retirement flag. Used by stage
//!   6.5's TUI live-feed; here it's the live test's assertion path
//!   ("did the signal land?").
//!
//! Signal handler bodies are sync `fn(&mut self, &mut SyncWorkflowContext<Self>, T)`
//! per the SDK's `temporalio-macros` doc (`#[signal]` line) and the
//! smoke binary's `external_signal` (`bin/temporal_smoke.rs` line 161).
//! The ticket's "target shape" elided the `&mut` and `<Self>`; we copy
//! the working smoke precedent, not the ticket text.
//!
//! `inspect_state` is a **sync** `#[update]` taking `&mut self` per the
//! macro's published shape (`temporalio-macros-0.4.0/src/lib.rs` line 61
//! and the SDK's `examples/message_passing/workflows.rs`). The ticket's
//! "target shape" listed it as `async fn(&self, &WorkflowContext)`,
//! which the macro rejects at compile time (async updates must drop
//! `self`, sync updates require `&mut self`). The sync shape matches the
//! semantics we want (read-only snapshot of state) and the SDK example.
//!
//! ## What this ticket proves
//!
//! - The workflow registers and is reachable via the URL-shaped ID scheme
//!   `graphs/<graph_id>/agents/<agent_id>` (see [`agent_workflow_id`]).
//! - The `Worker::new` → register-workflow → register-activities pipeline
//!   wires up cleanly against the live SDK pinned in
//!   `crates/jarvis_temporal/Cargo.toml`.
//! - `ctx.continue_as_new(...)` fires from inside `AgentWorkflow::run` and
//!   the SDK reschedules a fresh run that reaches a clean `Ok` return.
//! - Four signals + one update round-trip through the Temporal client and
//!   land on workflow state; `inspect_state` reads them back.
//!
//! ## Placeholder types
//!
//! `AgentConfig`, `FsHandle`, `ParentRef`, and `Carryover` exist only so
//! `AgentInput` compiles with the field shape stage 3 needs. They are
//! intentionally opaque (`{}` structs) right now. Real shapes:
//!
//! - `AgentConfig` — JAR2-60+, sources from `jarvis_node` (`Mandate`, tool
//!   refs, model routing resolved at start).
//! - `FsHandle` — built from `<graph_id>/<agent_id>` per stage 2.5's
//!   storage trait (`scratch/agent_storage.md`).
//! - `ParentRef` — JAR2 stage 5 territory (parent-child topology). Present
//!   here only so `AgentInput.parent_handle: Option<ParentRef>` doesn't
//!   need a schema migration when stage 5 fills it in.
//! - `Carryover` — JAR2-67. Today it's an empty marker: presence on
//!   `AgentInput.carryover` is the only signal the workflow body reads
//!   (`None` = first run, `Some(_)` = post-continue-as-new). Real fields
//!   (trigger queue snapshot, scheduler cursor, child handles, last
//!   output id, mid-tick evidence) land in JAR2-67.

use std::time::Duration;

use jarvis_node::trigger::{HumanOp, MandatePatch, Trigger};
use serde::{Deserialize, Serialize};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ContinueAsNewOptions, SyncWorkflowContext, WorkflowContext, WorkflowResult};

/// Resolved agent configuration handed to the workflow at start.
///
/// **Placeholder.** Stage 3.2 only needs the type to exist; real fields
/// land in JAR2-60 (mandate, tool refs, model routing). The shape will be
/// driven by what `AgentCore` needs to make a `Decision`, sourced from the
/// structural DB + per-agent FS overrides per stage 1's
/// three-layer-resolution decision (`scratch/temporal_staged_plan.md`
/// § 8 decision 4).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConfig {}

/// Storage handle scoping the agent to its `<graph_id>/<agent_id>` prefix.
///
/// **Placeholder.** Stage 2.5 (`scratch/agent_storage.md`) ships the
/// `AgentStorage` trait + `AgentFs` facade with the prefix baked in;
/// stage 3 will plumb the `Arc<dyn AgentStorage>` + prefix through the
/// workflow input. Today the workflow body never reads it, so the shape
/// is empty.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsHandle {
    /// `<graph_id>/<agent_id>` — the prefix the storage trait scopes to.
    /// Populated for documentation and the live test only; the workflow
    /// body does not consume it yet.
    pub prefix: String,
}

/// Parent workflow reference for cross-workflow signal routing.
///
/// **Placeholder.** Stage 5 territory (parent → child topology + child →
/// parent signal path per `scratch/temporal_staged_plan.md` § 5 stage 5).
/// Field exists on `AgentInput` now so the input schema doesn't churn
/// when stage 5 fills it in.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentRef {}

/// Typed continue-as-new carryover.
///
/// **Placeholder.** JAR2-67 ships the real shape (trigger queue,
/// scheduler cursor, child handles, last output id, mid-tick evidence).
/// For stage 3.2 the only thing the workflow body cares about is
/// presence: `AgentInput.carryover.is_some()` means "this is a
/// continue-as-new run, exit cleanly"; `None` means "first run,
/// continue-as-new once to prove the wiring". That's structural, not
/// semantic — no real carryover state crosses the boundary.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Carryover {}

/// Input handed to `AgentWorkflow::run` at start (and at every
/// continue-as-new).
///
/// The four-field shape matches the stage 3.2 ticket exactly so JAR2-59
/// (signals), JAR2-60 (loop body), and JAR2-67 (real carryover) can fill
/// each field in without renaming.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentInput {
    /// Resolved configuration (mandate, tool refs, model routing).
    /// Resolved once at workflow start; in-flight changes arrive via
    /// `mandate_update` signals (JAR2-59).
    pub cfg: AgentConfig,
    /// `<graph_id>/<agent_id>` storage prefix for the per-agent FS.
    pub fs_handle: FsHandle,
    /// Parent reference for child-of-parent agents. `None` for root
    /// agents (stage 3 default).
    pub parent_handle: Option<ParentRef>,
    /// Continue-as-new carryover. `None` on the first run, `Some(_)`
    /// after a continue-as-new. Real shape lands in JAR2-67.
    pub carryover: Option<Carryover>,
}

/// Result returned by `AgentWorkflow::run` when the workflow exits cleanly
/// (retirement path — JAR2-60+). Today the empty body returns
/// `AgentResult::default()` after one continue-as-new round-trip.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentResult {}

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
/// `recent_output_ids: Vec<String>` that stays empty until JAR2-65
/// wires `persist_output`.
///
/// The struct is `non_exhaustive` so stage 6.5 can extend it without
/// churning every caller — once a real TUI consumer lands, this comment
/// can be dropped.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentSnapshot {
    /// Count of `external_signal` payloads currently queued.
    pub pending_triggers_count: usize,
    /// Count of `human_override` payloads currently queued.
    pub pending_human_ops_count: usize,
    /// Count of `mandate_update` payloads currently queued.
    pub pending_mandate_patches_count: usize,
    /// The most recent `retire` signal reason, if any. `Some(_)` means
    /// the post-CAN body has been (or will be) asked to exit.
    pub retirement_request: Option<String>,
    /// Placeholder for JAR2-65 + stage 6.5. Always empty until then.
    pub recent_output_ids: Vec<String>,
}

impl AgentSnapshot {
    /// Construct a snapshot from the live workflow state. Pulled out to
    /// keep the `#[update]` body legible and to enable a hermetic unit
    /// test against the projection without standing up a workflow.
    fn from_state(workflow: &AgentWorkflow) -> Self {
        Self {
            pending_triggers_count: workflow.pending_triggers.len(),
            pending_human_ops_count: workflow.pending_human_ops.len(),
            pending_mandate_patches_count: workflow.pending_mandate_patches.len(),
            retirement_request: workflow.retirement_request.clone(),
            recent_output_ids: Vec::new(),
        }
    }
}

/// Build the workflow ID for an agent.
///
/// URL-shaped scheme per `scratch/temporal_staged_plan.md` § 8 decision 2:
/// **`graphs/<graph_id>/agents/<agent_id>`** — mirrors the eventual HTTP
/// API (`GET /api/v1/graphs/<id>/agents/<id>/...`), stays flat within a
/// graph (parent-child topology lives in the structural DB, not in the
/// ID), and doesn't carry a leading slash (Temporal IDs don't).
pub fn agent_workflow_id(graph_id: &str, agent_id: &str) -> String {
    format!("graphs/{graph_id}/agents/{agent_id}")
}

/// Max wall-clock the post-CAN body waits for a `retire` signal before
/// returning. The bound is a structural safety net only — JAR2-60 will
/// replace it with the real loop. Sized so the existing JAR2-58 live
/// test (`workflow_skeleton_continues_as_new_and_exits`) stays inside
/// its 60s timeout while still giving operator-driven signal traffic
/// generous time to land.
const POST_CAN_RETIREMENT_WAIT: Duration = Duration::from_secs(10);

/// The agent workflow.
///
/// Stage 3.2 body: if `input.carryover.is_none()`, continue-as-new with
/// `carryover = Some(Carryover::default())`. Stage 3.3 extends the
/// post-CAN run to wait on the `retirement_request` signal bucket (or a
/// short timer ceiling) before exiting cleanly — the ticket explicitly
/// allows this trivial extension so the live test has a window to send
/// signals and call `inspect_state`. Real loop body lands in JAR2-60.
///
/// `#[derive(Default)]` is required by the SDK's `#[workflow]` macro —
/// see the smoke binary's `AgentLoopWorkflow` line 142 for the precedent.
/// The four signal buckets below default to empty so the macro's
/// generated `Default` impl Just Works.
#[workflow]
#[derive(Default)]
pub struct AgentWorkflow {
    /// `external_signal` queue. Pushed by the signal handler; drained by
    /// JAR2-60's loop. Today nothing consumes it.
    pending_triggers: Vec<Trigger>,
    /// `human_override` queue. Same shape as `pending_triggers`; the
    /// loop will fold these into the per-tick trigger drain in JAR2-60.
    /// Kept as a separate bucket today to keep the typed payload visible
    /// in `inspect_state` — once JAR2-60 lands they may collapse into a
    /// single `Vec<Trigger>` by wrapping `HumanOp` in
    /// `Trigger::HumanOverride { op }` at handler time.
    pending_human_ops: Vec<HumanOp>,
    /// `mandate_update` queue. Stage 6 owns the consumption semantics
    /// (apply patch → write back to per-agent FS → re-resolve routing on
    /// next tick); here we only buffer.
    pending_mandate_patches: Vec<MandatePatch>,
    /// `retire` request. `Option<_>` not `Vec<_>` because retirement is
    /// monotonic — once asked to retire, the agent retires; subsequent
    /// retire signals overwrite the reason but the agent still exits at
    /// the next observation point. Read by the post-CAN body to drive
    /// the clean-exit path.
    retirement_request: Option<String>,
}

#[workflow_methods]
impl AgentWorkflow {
    /// `external_signal` — push a typed [`Trigger`] onto the per-tick
    /// queue. JAR2-60's loop drains the queue at the top of each tick.
    ///
    /// **Sync handler shape.** Per `temporalio-macros-0.4.0/src/lib.rs`
    /// line 59 and the smoke binary's `external_signal` (line 161 of
    /// `bin/temporal_smoke.rs`): `fn(&mut self, &mut SyncWorkflowContext<Self>, T)`.
    /// The `_ctx` parameter is required by the macro but unused here —
    /// signal handlers mutate `self` directly.
    #[signal]
    pub fn external_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, trigger: Trigger) {
        self.pending_triggers.push(trigger);
    }

    /// `human_override` — push a typed [`HumanOp`] onto the override
    /// queue. The kernel does not enforce override semantics yet
    /// (`scratch/agent_runtime.md` § 8); stage 6 wires the meanings.
    /// Today we only deliver the payload.
    #[signal]
    pub fn human_override(&mut self, _ctx: &mut SyncWorkflowContext<Self>, op: HumanOp) {
        self.pending_human_ops.push(op);
    }

    /// `mandate_update` — push a typed [`MandatePatch`] onto the patch
    /// queue. Stage 6 / `mandate_update` semantics: apply patch to the
    /// per-agent FS, re-resolve routing on the next tick. Today we
    /// only buffer.
    #[signal]
    pub fn mandate_update(&mut self, _ctx: &mut SyncWorkflowContext<Self>, patch: MandatePatch) {
        self.pending_mandate_patches.push(patch);
    }

    /// `retire` — record a retirement reason. The post-CAN body
    /// observes `retirement_request.is_some()` and exits the workflow
    /// cleanly. Last write wins — the kernel treats retirement as a
    /// one-way door, so re-sending `retire` with a new reason just
    /// updates the recorded reason without changing the outcome.
    #[signal]
    pub fn retire(&mut self, _ctx: &mut SyncWorkflowContext<Self>, reason: String) {
        self.retirement_request = Some(reason);
    }

    /// `inspect_state` — return a typed [`AgentSnapshot`] of the
    /// workflow's signal-bucket counts + retirement flag.
    ///
    /// **Sync update shape.** `#[update]` per
    /// `temporalio-macros-0.4.0/src/lib.rs` line 61 and the SDK's
    /// `examples/message_passing/workflows.rs::set_counter`:
    /// `fn(&mut self, &mut SyncWorkflowContext<Self>, T) -> R`. The
    /// macro requires `&mut self` even for read-only sync updates
    /// (`workflow_definitions.rs` line 414 validates the receiver
    /// shape); we don't mutate here, the projection is read-only. The
    /// ticket's "target shape" listed this as `async fn(&self,
    /// &WorkflowContext)`, which the macro rejects — async updates must
    /// drop `self`. The sync shape preserves the intended semantics
    /// (read-only snapshot) and compiles.
    ///
    /// `_input` is `()`; updates require an input type, but inspect
    /// doesn't need one. The Temporal client sends `()` from
    /// `handle.execute_update(AgentWorkflow::inspect_state, (), ...)`.
    #[update]
    pub fn inspect_state(
        &mut self,
        _ctx: &mut SyncWorkflowContext<Self>,
        _input: (),
    ) -> AgentSnapshot {
        AgentSnapshot::from_state(self)
    }

    /// Workflow entry point.
    ///
    /// Behaviour:
    ///
    /// 1. **First run** (`input.carryover.is_none()`): tiny warm-up timer,
    ///    then `ctx.continue_as_new(...)` with `carryover = Some(_)`.
    ///    This proves the CAN wiring; the body below the CAN call is
    ///    unreachable on this path.
    /// 2. **Post-CAN run** (`input.carryover.is_some()`): wait on either
    ///    a `retirement_request` arriving via the `retire` signal OR a
    ///    short timer ceiling ([`POST_CAN_RETIREMENT_WAIT`]). Whichever
    ///    fires first, return `Ok(AgentResult::default())`. The timer
    ///    is a structural safety net so the JAR2-58 live test (which
    ///    never sends `retire`) still terminates inside its 60s
    ///    timeout; JAR2-60 replaces this entire branch with the real
    ///    loop.
    ///
    /// The ticket explicitly permits the post-CAN retirement-wait
    /// extension ("extend trivially to read `retirement_request` and
    /// return early without consuming any other signals"). All other
    /// signal buckets are left untouched — JAR2-60 drains them.
    ///
    /// **Deterministic.** No clocks, no random, no I/O — the race below
    /// uses the SDK's `temporalio_sdk::workflows::select!`, never
    /// `tokio::select!` (see `scratch/temporal_rust_sdk_smoke.md` § 2.5
    /// and § 3.4).
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: AgentInput,
    ) -> WorkflowResult<AgentResult> {
        // Single deterministic suspension point on the first run. See
        // the smoke binary's `ContinueAsNewSmokeWorkflow::run` for the
        // precedent.
        ctx.timer(Duration::from_millis(1)).await;

        if input.carryover.is_none() {
            let next = AgentInput {
                carryover: Some(Carryover::default()),
                ..input
            };
            // `continue_as_new` returns `Err(WorkflowTermination::...)`;
            // the SDK terminates this run and schedules a fresh one with
            // the new input. The line below the call is unreachable on
            // the continue-as-new path. Smoke precedent:
            // `ContinueAsNewSmokeWorkflow::run` line 306 of
            // `temporal_smoke.rs`.
            ctx.continue_as_new(&next, ContinueAsNewOptions::default())?;
            unreachable!("continue_as_new should have terminated this run");
        }

        // Post-CAN: race the retirement signal against a short timer
        // ceiling. `workflows::select!` is the SDK-blessed deterministic
        // race primitive (see `temporal_smoke.rs::AgentLoopWorkflow::run`
        // lines 192-195 for the canonical pattern). Either arm fires →
        // we drop both futures → return cleanly.
        // `workflows::select!` requires `&mut` futures; mirror the smoke
        // binary's `let mut wait_fut = …; let mut timer_fut = …;` shape
        // at line 189-190 of `temporal_smoke.rs`.
        let mut wait_retire = ctx.wait_condition(|s| s.retirement_request.is_some());
        let mut wait_timeout = ctx.timer(POST_CAN_RETIREMENT_WAIT);
        temporalio_sdk::workflows::select! {
            _ = wait_retire => {},
            _ = wait_timeout => {},
        };

        Ok(AgentResult::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_workflow_id_is_url_shaped() {
        // URL-shaped scheme per `temporal_staged_plan.md` § 8 decision 2.
        assert_eq!(
            agent_workflow_id("g1", "a1"),
            "graphs/g1/agents/a1",
            "workflow ID must be `graphs/<graph_id>/agents/<agent_id>` (no leading slash, no trailing slash)"
        );
    }

    #[test]
    fn agent_workflow_id_passes_components_through_unchanged() {
        // Today we don't escape or normalize. Capture today's behaviour
        // so a future move to validation is a deliberate, reviewable
        // change.
        assert_eq!(
            agent_workflow_id("graph-with-dashes", "agent_with_underscores"),
            "graphs/graph-with-dashes/agents/agent_with_underscores"
        );
        // Empty components survive — the structural DB layer will reject
        // bad IDs before we get here, so this layer stays mechanical.
        assert_eq!(agent_workflow_id("", ""), "graphs//agents/");
    }

    #[test]
    fn agent_input_default_has_no_carryover() {
        // The "first run, no continue-as-new yet" sentinel is
        // `carryover.is_none()`. Lock the default so JAR2-67 doesn't
        // accidentally flip it.
        let input = AgentInput::default();
        assert!(input.carryover.is_none());
        assert!(input.parent_handle.is_none());
    }

    #[test]
    fn agent_input_roundtrips_through_json() {
        // Temporal's payload codec round-trips workflow inputs through
        // serde. A JSON round-trip is a cheap, hermetic proxy that
        // catches `Serialize`/`Deserialize` derives missing on any of
        // the placeholder types.
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
    fn agent_result_default_roundtrips_through_json() {
        let r = AgentResult::default();
        let json = serde_json::to_string(&r).expect("serialize AgentResult");
        let back: AgentResult = serde_json::from_str(&json).expect("deserialize AgentResult");
        assert_eq!(r, back);
    }

    #[test]
    fn agent_snapshot_default_roundtrips_through_json() {
        // The snapshot is wire-transferred from worker → client over
        // Temporal's payload codec; a JSON round-trip is a cheap
        // hermetic proxy that catches missing serde derives.
        let s = AgentSnapshot::default();
        let json = serde_json::to_string(&s).expect("serialize AgentSnapshot");
        let back: AgentSnapshot = serde_json::from_str(&json).expect("deserialize AgentSnapshot");
        assert_eq!(s, back);
        // All counters start at zero and there is no retirement reason
        // until `retire` lands. Lock today's behaviour so a future
        // refactor doesn't silently flip a default.
        assert_eq!(s.pending_triggers_count, 0);
        assert_eq!(s.pending_human_ops_count, 0);
        assert_eq!(s.pending_mandate_patches_count, 0);
        assert!(s.retirement_request.is_none());
        assert!(s.recent_output_ids.is_empty());
    }

    #[test]
    fn agent_snapshot_with_signals_roundtrips_through_json() {
        // A populated snapshot — proves every field round-trips
        // independently. Mirrors the payload the live test asserts on.
        let s = AgentSnapshot {
            pending_triggers_count: 2,
            pending_human_ops_count: 1,
            pending_mandate_patches_count: 3,
            retirement_request: Some("shutdown for upgrade".into()),
            recent_output_ids: Vec::new(),
        };
        let json = serde_json::to_string(&s).expect("serialize AgentSnapshot");
        let back: AgentSnapshot = serde_json::from_str(&json).expect("deserialize AgentSnapshot");
        assert_eq!(s, back);
    }

    #[test]
    fn agent_snapshot_from_state_projects_bucket_lengths_and_retirement() {
        // `AgentSnapshot::from_state` is the projection the `#[update]`
        // handler uses. Test it directly so we don't need to stand up a
        // workflow to verify the field mapping.
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
        // The macro generates `Default` for `AgentWorkflow` because of
        // `#[derive(Default)]`; this test pins the new fielded shape so
        // a future refactor that, say, adds a non-`Default` field has to
        // think about the bucket initial state explicitly.
        let wf = AgentWorkflow::default();
        assert!(wf.pending_triggers.is_empty());
        assert!(wf.pending_human_ops.is_empty());
        assert!(wf.pending_mandate_patches.is_empty());
        assert!(wf.retirement_request.is_none());
    }
}

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
//! ## Test injection
//!
//! `decide_next_action` consults a static `OnceLock<Mutex<VecDeque<Decision>>>`
//! before falling back to the canned `Decision::Idle { next_after: 1s }`.
//! Tests call [`set_decision_script`] before starting the workflow; the
//! activity pops from the script in order. This is the workflow-side
//! analogue of `agent_core`'s `MockDecide` — same scripted behaviour,
//! but reachable from inside an activity body (which must be a free
//! function over a value-typed registered instance per SDK constraint
//! § 3.4 of `temporal_rust_sdk_smoke.md`).
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
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use jarvis_node::decision::{ContextBundle, CorrectionContext, Decision, FsOp, ToolCall};
use jarvis_node::evidence::EvidenceId;
use jarvis_node::fs::AgentFs;
use jarvis_node::mandate::{Mandate, OutputId};
use jarvis_node::trigger::{HumanOp, MandatePatch, Trigger};
use serde::{Deserialize, Serialize};
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use crate::worker::agent_storage;
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
/// the resolved cfg + FS handle + prior-tick correction so the activity
/// can call into `agent_core::drain_triggers` once the real body lands
/// in JAR2-61.
///
/// `mandate_patches` are surfaced here so JAR2-61 can apply them to the
/// per-agent FS before assembling the bundle (the workflow body itself
/// must not touch FS — see `scratch/temporal_staged_plan.md` § 2.5
/// "Drain triggers (typed, ordered)" and the JAR2-60 ticket's notes on
/// the drain/assemble merge in `agent_core`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssembleContextInput {
    pub cfg: AgentConfig,
    pub fs_handle: FsHandle,
    pub triggers: Vec<Trigger>,
    /// Human overrides drained alongside `triggers`. JAR2-61 will either
    /// fold them into the `Trigger::HumanOverride` taxonomy at assemble
    /// time or thread them through `CorrectionContext` — the workflow
    /// doesn't decide; it just delivers.
    pub human_ops: Vec<HumanOp>,
    /// Mandate patches drained from the workflow's `pending_mandate_patches`
    /// bucket. Stage 6 owns the consumption (apply patch → write FS →
    /// re-resolve routing); the workflow body just hands them off.
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplyFsOpsInput {
    pub fs_handle: FsHandle,
    pub ops: Vec<FsOp>,
}

/// Input to [`AgentActivities::persist_retirement`]. Carries the reason so
/// retirement is auditable on disk.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistRetirementInput {
    pub fs_handle: FsHandle,
    pub reason: String,
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
    /// Stage 3.5 (JAR2-61). Stub returns an empty `ContextBundle` with a
    /// placeholder `Mandate` so the downstream `decide_next_action`
    /// activity has a payload to serialize.
    ///
    /// `Mandate::new("", Duration::ZERO, None)` is the cheapest valid
    /// construction — `ContextBundle.mandate: Mandate` is non-`Default`
    /// so we cannot fall back to `..Default::default()` here. JAR2-61
    /// replaces the body with a call to `agent_core::drain_triggers`.
    #[activity]
    pub async fn assemble_context(
        _ctx: ActivityContext,
        _input: AssembleContextInput,
    ) -> Result<AssembleContextOutput, ActivityError> {
        Ok(AssembleContextOutput {
            bundle: ContextBundle {
                mandate: Mandate::new("", Duration::ZERO, None),
                triggers: Vec::new(),
                recent_outputs: Vec::new(),
                recent_evidence: Vec::new(),
                open_claims: Vec::new(),
                correction: None,
            },
        })
    }

    /// Stage 3.6 (JAR2-62). Stub pops from the test-injected
    /// [`DECISION_SCRIPT`]; falls back to `Decision::Idle { next_after: 1s }`
    /// when the script is empty. JAR2-62 replaces the body with
    /// `LlmDecide::decide`.
    #[activity]
    pub async fn decide_next_action(
        _ctx: ActivityContext,
        _input: DecideInput,
    ) -> Result<Decision, ActivityError> {
        Ok(pop_scripted_decision().unwrap_or(Decision::Idle {
            next_after: Duration::from_secs(1),
        }))
    }

    /// Stage 3.7 (JAR2-63). Stub returns a deterministic placeholder
    /// `EvidenceId` so the workflow's per-call accounting has a token to
    /// stash in history. JAR2-63 replaces the body with
    /// `ToolRegistry::call` + `AgentFs::record_evidence`.
    #[activity]
    pub async fn execute_tool(
        _ctx: ActivityContext,
        input: ExecuteToolInput,
    ) -> Result<ToolCallOutcome, ActivityError> {
        // Deterministic placeholder id seeded from the call's name + args
        // so two stub invocations with identical inputs collide on the
        // same id — matches the content-addressed semantics of the real
        // EvidenceId without doing any I/O.
        let evidence_id = EvidenceId::new(
            &input.call.name,
            &input.call.args,
            &serde_json::json!({"stub": "execute_tool"}),
        );
        Ok(ToolCallOutcome::Success { evidence_id })
    }

    /// Stage 3.8 (JAR2-64). Stub returns a fresh placeholder `OutputId`
    /// (a new ULID — `OutputId::new()`). JAR2-64 replaces the body with
    /// `AgentFs::persist_output`.
    #[activity]
    pub async fn persist_output(
        _ctx: ActivityContext,
        _input: PersistOutputInput,
    ) -> Result<OutputId, ActivityError> {
        Ok(OutputId::new())
    }

    /// Stage 3.9 (JAR2-65). Stub is a no-op. JAR2-65 replaces the body
    /// with `AgentFs::apply_ops`.
    #[activity]
    pub async fn apply_fs_ops(
        _ctx: ActivityContext,
        _input: ApplyFsOpsInput,
    ) -> Result<(), ActivityError> {
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
    use serde_json::json;

    // Serializes the two tests below that mutate the process-wide
    // `DECISION_SCRIPT` static. Without this they race under cargo's
    // default parallel runner (CI hit it; locally they happened to
    // schedule far enough apart to pass).
    static SCRIPT_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
    fn assemble_context_input_default_is_empty() {
        // `Default` is required by the AgentInput round-trip test in
        // `workflow.rs`; lock the empty-bucket shape so a future
        // refactor that, say, adds a non-`Default` field has to think
        // about the bucket init explicitly.
        let i = AssembleContextInput::default();
        assert!(i.triggers.is_empty());
        assert!(i.human_ops.is_empty());
        assert!(i.mandate_patches.is_empty());
        assert!(i.prior_correction.is_none());
    }

    #[test]
    fn assemble_context_input_round_trips_through_json() {
        let i = AssembleContextInput {
            cfg: AgentConfig::default(),
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
}

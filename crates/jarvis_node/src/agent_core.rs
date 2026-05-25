//! `AgentCore` — pure per-tick logic shared between the in-process
//! `Agent::run` loop and the future `AgentWorkflow` Temporal host
//! (stage 3.2+).
//!
//! # Why this module exists
//!
//! Today's `Agent::run` (in [`crate::agent`]) interleaves three concerns:
//!
//! 1. The **signal source** (`mpsc` + `tokio::select!` against the
//!    scheduler deadline).
//! 2. The **durability host** (in-memory state carried across ticks: the
//!    correction continuation, the retry trail, the per-tick budget
//!    accounting).
//! 3. The **logic** (drain triggers → assemble context → decide → dispatch
//!    → persist / retire / correction).
//!
//! Per the stage-3 plan in `scratch/temporal_staged_plan.md` § 2, the
//! Temporal workflow that lands at stage 3.2 hosts (1) and (2) very
//! differently — workflow signals replace the mpsc queue, workflow state
//! replaces the in-memory continuation, and history replay replaces the
//! `tokio::select!` race. What stays identical between the two hosts is
//! exactly (3). Pulling it into a dedicated module is the seam every
//! later sub-ticket in the stage-3 project depends on.
//!
//! # The seam
//!
//! Three free functions, no internal `async` runtime concerns (no
//! `tokio::select!`, no channels, no `tokio::time::sleep`):
//!
//! * [`drain_triggers`] — sync packaging of the already-drained
//!   `Vec<Trigger>` plus FS reads into a `ContextBundle`. Today this is a
//!   thin wrapper around [`crate::decision::assemble_context`]; the rename
//!   matches the seam vocabulary in `scratch/temporal_staged_plan.md` § 2.
//! * [`decide`] — calls into a `&dyn Decide` and returns a `Decision`. A
//!   one-line wrapper today, kept here so the workflow host has a single
//!   shared call site for "ask the model what to do" and so the
//!   in-process loop and workflow code remain symmetric.
//! * [`dispatch`] — applies a `Decision` against the FS, the tool
//!   registry, and the scheduler, returning a typed [`DispatchOutcome`].
//!
//! # `DispatchOutcome` and the workflow seam
//!
//! The ticket text describes `dispatch` as "returning a description of
//! what to do" rather than calling side effects. That phrasing is
//! aspirational for the workflow host: in `AgentWorkflow` (stage 3.4+),
//! the workflow code matches on `Decision` directly and orchestrates
//! activities (`execute_tool`, `persist_output`, `apply_fs_ops`, etc.) —
//! it does **not** call `AgentCore::dispatch`. The workflow's per-tick
//! match arms produce a value isomorphic to `DispatchOutcome` so the
//! shared post-dispatch state machine in the host (correction staging,
//! budget accounting, health transition) can branch on the same enum
//! regardless of who produced it.
//!
//! For the in-process loop, `dispatch` is the *in-process* executor: it
//! calls into [`crate::fs::AgentFs`] and [`crate::tools::ToolRegistry`]
//! the same way today's `Agent::run` does. The seam value is the **typed
//! outcome enum**, not literal side-effect-freeness — that interpretation
//! is documented here and in the PR body for JAR2-57 so a future reader
//! can validate the choice.
//!
//! # `Continue` / `Retired` / `NeedsCorrection` / `ToolError`
//!
//! The four variants of `DispatchOutcome` mirror today's
//! `Agent::ApplyOutcome` shape one-for-one. They are kept distinct
//! (rather than collapsing, say, `ToolError` into `NeedsCorrection`)
//! because the in-process host's budget accounting in `Agent::run`
//! depends on the distinction: `NeedsCorrection` consumes one
//! `FailureKind::Inference` slot; `ToolError { failures }` consumes K
//! `FailureKind::ToolCall` slots, one per failed call, per JAR2-38. The
//! workflow host preserves the same accounting on top of its own
//! per-activity outcomes.
//!
//! `Continue` carries no payload today; the ticket sketch's
//! `next_idle: Idle, recorded_evidence: Vec<EvidenceId>` are deliberately
//! omitted as they are not load-bearing for the no-behavior-change
//! refactor and would broaden the surface unnecessarily. The follow-up
//! sub-tickets in the stage-3 project may extend `Continue` as the
//! workflow host's needs become concrete.

use anyhow::Result;
use chrono::Utc;
use futures::future::join_all;
use tracing::debug;

use crate::agent::RetireReason;
use crate::decision::{
    assemble_context, ContextBundle, CorrectionContext, Decide, Decision, ToolCall,
};
use crate::evidence::EvidenceId;
use crate::fs::{AgentFs, FsError};
use crate::mandate::Mandate;
use crate::scheduler::Scheduler;
use crate::tools::ToolRegistry;
use crate::trigger::Trigger;

/// Outcome of [`dispatch`] — a typed description of "what should the host
/// do next?" the in-process loop or the future workflow can branch on.
///
/// One-for-one with today's private `Agent::ApplyOutcome`: every variant
/// here is what the host sees after a tick's `Decision` has been applied.
///
/// * `Continue` — the tick produced no terminal or recoverable-failure
///   outcome; the host clears any correction state, marks the tick a
///   success, and proceeds to the next iteration.
/// * `Retired` — the agent emitted `Decision::Retire { reason }`; the
///   in-process executor has already written `retirement.json` to the FS
///   (the side-effect lives in `dispatch` because the host needs to exit
///   *after* the marker lands, not before). The variant carries the
///   `RetireReason` so the host can return it from `Agent::run`.
/// * `NeedsCorrection` — JAR2-19's "decision parsed but the runtime
///   cannot satisfy it" case. The string is a human-readable failure
///   description the host threads into the next tick's
///   `CorrectionContext` (and into the `HealthIncident` retry trail on
///   budget exhaustion).
/// * `ToolError { failures }` — JAR2-25 / JAR2-38's "tool's internal
///   retry policy exhausted on one or more of K parallel calls" case.
///   Successful sibling calls in the same batch already had their
///   evidence persisted to disk before this variant is constructed;
///   the host does **not** unwind on partial failure (per JAR2-38's
///   "successful evidence is kept" contract). Per-call accounting (K
///   against budget) lives in the host, not here.
#[derive(Debug)]
pub enum DispatchOutcome {
    Continue,
    Retired(RetireReason),
    NeedsCorrection(String),
    ToolError {
        /// One entry per failed call in the parallel batch. Order matches
        /// the original `Decision::CallTools` vec — i.e. the deterministic
        /// per-call-index order the dispatch site relies on for both
        /// evidence persistence and per-failure budget accounting.
        failures: Vec<ToolFailure>,
    },
}

/// One failed call in a `Decision::CallTools` batch. `tool` and `args`
/// echo what the model asked for so the corrective message can tell the
/// model exactly what failed; `error` is the underlying error string from
/// `ToolRegistry::call`.
///
/// `pub` because it leaks through [`DispatchOutcome::ToolError`], which
/// must be `pub` for the workflow host (stage 3.4+) to branch on the
/// shared seam. The in-process host (`Agent::run`) and the future
/// workflow host both read these fields when staging the correction
/// context and constructing the `HealthIncident` retry trail.
#[derive(Debug, Clone)]
pub struct ToolFailure {
    pub tool: String,
    pub args: serde_json::Value,
    pub error: String,
}

/// Drain the already-collected triggers into a `ContextBundle` for the
/// current tick.
///
/// The "drain" name matches the seam vocabulary in
/// `scratch/temporal_staged_plan.md` § 2 — in the workflow host, the
/// `Vec<Trigger>` comes from a workflow-state buffer rather than an
/// `mpsc::Receiver`, but the per-tick semantics are identical. The
/// in-process host calls `triggers.drain_ordered()` first and then hands
/// the resulting `Vec<Trigger>` to this function.
///
/// Today the body is a one-line delegate to
/// [`crate::decision::assemble_context`]; the indirection costs us
/// nothing and pins the seam shape for stage 3.4+.
pub async fn drain_triggers(
    triggers: Vec<Trigger>,
    fs: &AgentFs,
    cfg: &Mandate,
    prior_correction: Option<CorrectionContext>,
) -> Result<ContextBundle> {
    assemble_context(fs, &triggers, cfg, prior_correction).await
}

/// Ask the [`Decide`] impl what to do this tick.
///
/// One-line wrapper today; lives here so the in-process loop and the
/// workflow host share a single named call site for "ask the model" and
/// so the workflow's `decide_next_action` activity (stage 3.6) can wrap
/// the same function without duplicating semantics.
pub async fn decide<D: Decide + ?Sized>(bundle: ContextBundle, d: &D) -> Result<Decision> {
    d.decide(bundle).await
}

/// Apply a single `Decision` against the FS, tools, and scheduler;
/// return a typed [`DispatchOutcome`].
///
/// Recoverable apply-time failures (empty/unresolvable evidence, unknown
/// tool name, exhausted tool retries) surface as
/// [`DispatchOutcome::NeedsCorrection`] or
/// [`DispatchOutcome::ToolError`]; everything else either continues,
/// retires, or bubbles via `?`.
///
/// **Side-effect semantics.** In the in-process host this is where
/// today's `Agent::run` calls `fs.persist_output`, `tools.call`,
/// `fs.record_evidence`, etc. In the workflow host (stage 3.4+) the
/// workflow code matches on `Decision` directly and orchestrates
/// activities — it does not call this function. The shared piece is the
/// returned `DispatchOutcome` shape, not the body. See the module-level
/// doc for the rationale.
pub async fn dispatch(
    fs: &AgentFs,
    tools: &ToolRegistry,
    scheduler: &mut Scheduler,
    decision: Decision,
) -> Result<DispatchOutcome> {
    match decision {
        Decision::CallTools { calls } => dispatch_call_tools(fs, tools, calls).await,
        Decision::EmitOutput { content, evidence } => {
            debug!(evidence_count = evidence.len(), "decision: emit_output");
            match fs.persist_output(&content, &evidence).await {
                Ok(_) => Ok(DispatchOutcome::Continue),
                Err(e) => match e.downcast_ref::<FsError>() {
                    Some(FsError::EmptyEvidence) => Ok(DispatchOutcome::NeedsCorrection(
                        "emit_output: evidence list is empty (provenance contract)".into(),
                    )),
                    Some(FsError::EvidenceNotFound(id)) => {
                        let id: EvidenceId = id.clone();
                        Ok(DispatchOutcome::NeedsCorrection(format!(
                            "emit_output: evidence {id} not found on disk"
                        )))
                    }
                    _ => Err(e),
                },
            }
        }
        Decision::RewriteFs { ops } => {
            debug!(op_count = ops.len(), "decision: rewrite_fs");
            fs.apply_ops(ops).await?;
            Ok(DispatchOutcome::Continue)
        }
        Decision::Idle { next_after } => {
            debug!(
                next_after_ms = next_after.as_millis() as u64,
                "decision: idle"
            );
            scheduler.set_next_after(next_after);
            Ok(DispatchOutcome::Continue)
        }
        Decision::Retire { reason } => {
            debug!(%reason, "decision: retire");
            fs.persist_retirement(&reason, Utc::now()).await?;
            Ok(DispatchOutcome::Retired(RetireReason(reason)))
        }
        // JAR2-78 (stage 5.1): the four parent-child topology variants
        // are not dispatchable from `Agent::run`. Per Stage 5 Project
        // decision 11, the in-process loop stays single-agent forever;
        // the workflow host (5.3 / 5.5 / 5.7) is the only place these
        // variants execute. Reaching this arm in the in-process loop is
        // a wiring bug — `unimplemented!` is the boundary signal the
        // ticket explicitly calls for.
        Decision::SpawnChild { .. } => unimplemented!(
            "stage 5: in-process dispatch for parent-child decisions is \
             intentionally not wired — see Stage 5 Project decision 11"
        ),
        Decision::ReconcileChildren { .. } => unimplemented!(
            "stage 5: in-process dispatch for parent-child decisions is \
             intentionally not wired — see Stage 5 Project decision 11"
        ),
        Decision::RetireChild { .. } => unimplemented!(
            "stage 5: in-process dispatch for parent-child decisions is \
             intentionally not wired — see Stage 5 Project decision 11"
        ),
        Decision::ReplaceChild { .. } => unimplemented!(
            "stage 5: in-process dispatch for parent-child decisions is \
             intentionally not wired — see Stage 5 Project decision 11"
        ),
    }
}

/// JAR2-38: dispatch the K calls in a `Decision::CallTools` together.
///
/// Order of operations matters for both correctness and determinism:
///
/// 1. **Pre-check every tool name.** If any call names a tool the registry
///    does not know, surface a single `NeedsCorrection` that lists every
///    missing name and dispatch *none* of the batch. This matches the
///    JAR2-19 invariant that "no tool registered" is an inference-level
///    correctable error rather than a tool-call failure — and refusing to
///    dispatch the sibling calls keeps evidence persistence consistent
///    with the rejection.
/// 2. **Dispatch all K concurrently** via `futures::future::join_all`,
///    capturing every result rather than short-circuiting on the first
///    error. The agent loop's K-against-budget accounting needs every
///    failure, not just the first one.
/// 3. **Persist successful evidence in input order.** `join_all` resolves
///    futures concurrently but `into_iter().enumerate()` over the result
///    vec preserves the original index, so the **write order** of the
///    `evidence/<sha256>.json` files matches the `Decision::CallTools`
///    vec position. The on-disk *listing* order is still lex-sorted by
///    sha256 (evidence is content-addressed; the FS layer doesn't carry
///    write-order metadata), so `AgentFs::list_recent_evidence` returns
///    records in hash order rather than dispatch order — that's a
///    separate ordering domain from the one this step pins.
/// 4. **Successful evidence is kept even when some sibling calls fail.**
///    The model can cite a partial-success evidence id on a later tick;
///    rolling back would discard load-bearing observations of the world.
///    The corrective context describes only the failures so the model
///    knows what to retry.
async fn dispatch_call_tools(
    fs: &AgentFs,
    tools: &ToolRegistry,
    calls: Vec<ToolCall>,
) -> Result<DispatchOutcome> {
    debug!(count = calls.len(), "decision: call_tools");

    // Step 1: pre-check tool-name registration for every call. A single
    // unknown name takes the whole batch through the inference
    // correction loop (JAR2-19 semantics).
    let unknown: Vec<&str> = calls
        .iter()
        .filter(|c| !tools.contains(&c.name))
        .map(|c| c.name.as_str())
        .collect();
    if !unknown.is_empty() {
        return Ok(DispatchOutcome::NeedsCorrection(format!(
            "call_tools: no tool registered under name(s) {unknown:?}"
        )));
    }

    // Step 2: dispatch all K calls concurrently. We borrow `tools` for
    // the duration of every future (no `Arc::clone` needed because
    // `ToolRegistry` isn't `Clone`; the futures live for the duration of
    // this `await` and `tools` outlives them).
    let futures = calls.iter().map(|c| {
        let name = &c.name;
        let args = c.args.clone();
        async move { tools.call(name, args).await }
    });
    let results = join_all(futures).await;

    // Step 3+4: classify each result, persist successful evidence in
    // input order, collect failures into a batch outcome.
    let mut failures: Vec<ToolFailure> = Vec::new();
    for (i, result) in results.into_iter().enumerate() {
        let call = &calls[i];
        match result {
            Ok(ev) => {
                fs.record_evidence(ev).await?;
            }
            Err(e) => {
                failures.push(ToolFailure {
                    tool: call.name.clone(),
                    args: call.args.clone(),
                    error: format!("{e:#}"),
                });
            }
        }
    }

    if failures.is_empty() {
        Ok(DispatchOutcome::Continue)
    } else {
        Ok(DispatchOutcome::ToolError { failures })
    }
}

/// Build the human-readable failure description for the
/// `CorrectionContext` staged after a tool-call exhaustion (JAR2-30).
///
/// JAR2-38: accepts a batch of failed calls. For K=1 the wording
/// matches the original single-tool phrasing; for K>1 the message
/// lists every failed call so the model sees what each sibling did and
/// failed with.
///
/// `pub(crate)` because the in-process host (`Agent::run`) calls this
/// to stage the `CorrectionContext` after a `DispatchOutcome::ToolError`.
pub(crate) fn tool_failure_correction_text(failures: &[ToolFailure]) -> String {
    if failures.len() == 1 {
        let f = &failures[0];
        let args_text =
            serde_json::to_string(&f.args).unwrap_or_else(|_| "<unserializable args>".to_string());
        return format!(
            "call_tool {tool:?} failed after exhausting retries: {error}. \
             Args were {args_text}. Reply with a different decision \
             (different args, a different tool, an idle, or a retire).",
            tool = f.tool,
            error = f.error,
        );
    }
    let mut s = format!(
        "{} parallel call_tool(s) failed after exhausting retries:",
        failures.len()
    );
    for f in failures {
        let args_text =
            serde_json::to_string(&f.args).unwrap_or_else(|_| "<unserializable args>".to_string());
        s.push_str(&format!(
            "\n- {tool:?}: {error}. Args were {args_text}.",
            tool = f.tool,
            error = f.error,
        ));
    }
    s.push_str(
        "\nReply with a different decision (different args, different tools, \
         an idle, or a retire).",
    );
    s
}

#[cfg(test)]
mod tests {
    //! Unit tests for the seam itself.
    //!
    //! The four `DispatchOutcome` arms each get a focused, hermetic test
    //! against [`crate::storage::MemoryStorage`] (wired through
    //! [`AgentFs::new_with_storage`] so the seam exercises the same
    //! facade the in-process loop uses — only the backend is swapped).
    //!
    //! The adversarial test (`dispatch_idle_does_not_touch_outputs_or_evidence_or_retirement`)
    //! exercises the "no spurious side effects" property: an Idle decision
    //! must not write anything to disk, and the scheduler must take the
    //! requested cadence. It uses a scripted `MockDecide` reached via the
    //! exported [`decide`] free function rather than the in-process loop,
    //! confirming the seam is callable without the signal source / async
    //! runtime concerns the loop currently owns.
    //!
    //! End-to-end coverage of the budget-accounting state machine that
    //! sits *on top of* these outcomes lives in
    //! `crates/jarvis_node/tests/loop_smoke.rs` — those tests exercise the
    //! refactored `Agent::run` and stay green by virtue of this refactor
    //! being a strict no-behavior-change move.

    use super::*;
    use crate::decision::{ClaimSeed, MockDecide};
    use crate::evidence::{EvidenceId, EvidenceRecord};
    use crate::fs::AgentFs;
    use crate::storage::{AgentStorage, MemoryStorage};
    use crate::tools::ToolRegistry;
    use chrono::{DateTime, Utc};
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;

    fn ts() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-05-24T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn mandate() -> Mandate {
        Mandate::new("agent-core-test", Duration::from_millis(100), Some(4))
    }

    /// Build an `AgentFs` backed by an in-memory storage backend so the
    /// AgentCore seam tests stay hermetic, fast, and deterministic
    /// without touching the real filesystem. Matches the ticket
    /// directive ("`AgentCore`-level unit tests against `MemoryStorage`").
    async fn fixture() -> (AgentFs, Mandate) {
        let m = mandate();
        let storage: Arc<dyn AgentStorage> = Arc::new(MemoryStorage::new());
        let fs = AgentFs::new_with_storage(storage, "", &m).await.unwrap();
        (fs, m)
    }

    fn empty_bundle(m: &Mandate) -> ContextBundle {
        ContextBundle {
            mandate: m.clone(),
            triggers: vec![],
            recent_outputs: vec![],
            recent_evidence: vec![],
            open_claims: vec![],
            correction: None,
        }
    }

    // ---------- `decide` --------------------------------------------------

    #[tokio::test]
    async fn decide_returns_scripted_decision() {
        // The free function is a thin wrapper; the test exists to lock
        // the call-site shape stage 3.6+ will mirror.
        let m = mandate();
        let mock = MockDecide::new(vec![Decision::Idle {
            next_after: Duration::from_millis(7),
        }]);
        let got = decide(empty_bundle(&m), &mock).await.unwrap();
        assert_eq!(
            got,
            Decision::Idle {
                next_after: Duration::from_millis(7),
            }
        );
    }

    // ---------- `drain_triggers` -----------------------------------------

    #[tokio::test]
    async fn drain_triggers_packages_input_into_bundle() {
        // Today this is a passthrough to `assemble_context`. Locking the
        // call shape and the field-routing is enough — the deeper bundle
        // semantics are tested in `decision::tests`.
        let (fs, m) = fixture().await;
        let triggers = vec![Trigger::ScheduledWake];
        let bundle = drain_triggers(triggers.clone(), &fs, &m, None)
            .await
            .unwrap();
        assert_eq!(bundle.triggers, triggers);
        assert_eq!(bundle.mandate, m);
        assert!(bundle.correction.is_none());
    }

    #[tokio::test]
    async fn drain_triggers_threads_correction_through() {
        let (fs, m) = fixture().await;
        let corr = CorrectionContext::new("prior tick rejected: example");
        let bundle = drain_triggers(vec![], &fs, &m, Some(corr.clone()))
            .await
            .unwrap();
        assert_eq!(bundle.correction.as_ref(), Some(&corr));
    }

    // ---------- `dispatch` — Continue arm --------------------------------

    #[tokio::test]
    async fn dispatch_emit_output_with_resolvable_evidence_returns_continue() {
        let (fs, m) = fixture().await;
        // Seed one evidence record so the persist passes provenance.
        let ev_id = fs
            .record_evidence(EvidenceRecord::new(
                "echo",
                json!({"k": 1}),
                json!({"v": 1}),
                ts(),
            ))
            .await
            .unwrap();
        let decision = Decision::EmitOutput {
            content: "claim".into(),
            evidence: vec![ev_id],
        };
        let tools = ToolRegistry::new();
        let mut scheduler = Scheduler::new(m.idle_period);
        let outcome = dispatch(&fs, &tools, &mut scheduler, decision)
            .await
            .unwrap();
        assert!(matches!(outcome, DispatchOutcome::Continue));
        // Side effect happened (one output landed on disk).
        let outs = fs.list_recent_outputs(8).await.unwrap();
        assert_eq!(outs.len(), 1, "EmitOutput should have persisted an output");
    }

    #[tokio::test]
    async fn dispatch_idle_sets_scheduler_cadence_and_continues() {
        let (fs, m) = fixture().await;
        let tools = ToolRegistry::new();
        let mut scheduler = Scheduler::new(m.idle_period);
        let outcome = dispatch(
            &fs,
            &tools,
            &mut scheduler,
            Decision::Idle {
                next_after: Duration::from_millis(2500),
            },
        )
        .await
        .unwrap();
        assert!(matches!(outcome, DispatchOutcome::Continue));
        assert_eq!(scheduler.next_after(), Duration::from_millis(2500));
    }

    // ---------- `dispatch` — Retired arm ---------------------------------

    #[tokio::test]
    async fn dispatch_retire_writes_marker_and_returns_retired() {
        let (fs, m) = fixture().await;
        let tools = ToolRegistry::new();
        let mut scheduler = Scheduler::new(m.idle_period);
        let outcome = dispatch(
            &fs,
            &tools,
            &mut scheduler,
            Decision::Retire {
                reason: "done".into(),
            },
        )
        .await
        .unwrap();
        match outcome {
            DispatchOutcome::Retired(RetireReason(r)) => assert_eq!(r, "done"),
            other => panic!("expected Retired, got {other:?}"),
        }
        // `retirement.json` exists in storage.
        let marker = fs
            .storage()
            .get(&format!("{}retirement.json", fs.prefix()))
            .await
            .unwrap();
        assert!(
            marker.is_some(),
            "retirement marker should have been written"
        );
    }

    // ---------- `dispatch` — NeedsCorrection arm -------------------------

    #[tokio::test]
    async fn dispatch_emit_output_with_empty_evidence_returns_needs_correction() {
        let (fs, m) = fixture().await;
        let tools = ToolRegistry::new();
        let mut scheduler = Scheduler::new(m.idle_period);
        let outcome = dispatch(
            &fs,
            &tools,
            &mut scheduler,
            Decision::EmitOutput {
                content: "no evidence".into(),
                evidence: vec![],
            },
        )
        .await
        .unwrap();
        match outcome {
            DispatchOutcome::NeedsCorrection(desc) => {
                assert!(
                    desc.contains("evidence list is empty"),
                    "unexpected description: {desc}"
                );
            }
            other => panic!("expected NeedsCorrection, got {other:?}"),
        }
        // Provenance violation must NOT have produced an output on disk.
        let outs = fs.list_recent_outputs(8).await.unwrap();
        assert!(
            outs.is_empty(),
            "empty-evidence EmitOutput should not persist"
        );
    }

    #[tokio::test]
    async fn dispatch_call_tools_with_unknown_name_returns_needs_correction() {
        let (fs, m) = fixture().await;
        let tools = ToolRegistry::new(); // empty registry — every name unknown
        let mut scheduler = Scheduler::new(m.idle_period);
        let outcome = dispatch(
            &fs,
            &tools,
            &mut scheduler,
            Decision::CallTools {
                calls: vec![ToolCall::new(
                    "never_registered",
                    json!({}),
                    ClaimSeed::new("seed-1"),
                )],
            },
        )
        .await
        .unwrap();
        match outcome {
            DispatchOutcome::NeedsCorrection(desc) => {
                assert!(desc.contains("never_registered"), "got: {desc}");
            }
            other => panic!("expected NeedsCorrection, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dispatch_emit_output_with_unresolved_evidence_returns_needs_correction() {
        let (fs, m) = fixture().await;
        let tools = ToolRegistry::new();
        let mut scheduler = Scheduler::new(m.idle_period);
        // Construct a bogus evidence id that is not on disk.
        let bogus = EvidenceId::new("echo", &json!({"never": "written"}), &json!({"x": 0}));
        let outcome = dispatch(
            &fs,
            &tools,
            &mut scheduler,
            Decision::EmitOutput {
                content: "claim".into(),
                evidence: vec![bogus],
            },
        )
        .await
        .unwrap();
        assert!(matches!(outcome, DispatchOutcome::NeedsCorrection(_)));
    }

    // ---------- `dispatch` — ToolError arm -------------------------------

    /// Tool that always errors. Mirrors the spirit of the in-tree
    /// `FlakyTool` in `loop_smoke.rs` but inlined here so the AgentCore
    /// tests don't depend on the integration-test crate.
    struct ErroringTool {
        name: String,
    }

    #[async_trait::async_trait]
    impl crate::tools::Tool for ErroringTool {
        fn name(&self) -> &str {
            &self.name
        }

        async fn call(
            &self,
            _args: serde_json::Value,
        ) -> std::result::Result<serde_json::Value, anyhow::Error> {
            Err(anyhow::anyhow!("synthetic permanent failure"))
        }
    }

    #[tokio::test]
    async fn dispatch_call_tools_collects_per_call_failures() {
        let (fs, m) = fixture().await;
        let mut tools = ToolRegistry::new();
        tools
            .register(std::sync::Arc::new(ErroringTool {
                name: "errbomb".into(),
            }))
            .unwrap();
        let mut scheduler = Scheduler::new(m.idle_period);
        let outcome = dispatch(
            &fs,
            &tools,
            &mut scheduler,
            Decision::CallTools {
                calls: vec![
                    ToolCall::new("errbomb", json!({"i": 1}), ClaimSeed::new("a")),
                    ToolCall::new("errbomb", json!({"i": 2}), ClaimSeed::new("b")),
                ],
            },
        )
        .await
        .unwrap();
        match outcome {
            DispatchOutcome::ToolError { failures } => {
                assert_eq!(failures.len(), 2);
                assert_eq!(failures[0].tool, "errbomb");
                assert!(failures[0].error.contains("synthetic"));
                // Input order is preserved.
                assert_eq!(failures[0].args, json!({"i": 1}));
                assert_eq!(failures[1].args, json!({"i": 2}));
            }
            other => panic!("expected ToolError, got {other:?}"),
        }
    }

    // ---------- Adversarial: no spurious side effects --------------------

    #[tokio::test]
    async fn dispatch_idle_does_not_touch_outputs_or_evidence_or_retirement() {
        // The seam test the ticket calls out: an Idle decision dispatched
        // via `AgentCore::dispatch` must not write any of the three FS
        // surfaces a host would expect to remain untouched. The signal
        // source (mpsc), the async runtime race (`tokio::select!`), and
        // the in-process loop's correction/health state are *not* exercised
        // here — only `decide` + `dispatch` from the seam, via a scripted
        // `MockDecide`.
        let (fs, m) = fixture().await;
        let mock = MockDecide::new(vec![Decision::Idle {
            next_after: Duration::from_millis(50),
        }]);
        let bundle = drain_triggers(vec![Trigger::ScheduledWake], &fs, &m, None)
            .await
            .unwrap();
        let decision = decide(bundle, &mock).await.unwrap();
        let tools = ToolRegistry::new();
        let mut scheduler = Scheduler::new(m.idle_period);
        let outcome = dispatch(&fs, &tools, &mut scheduler, decision)
            .await
            .unwrap();
        assert!(matches!(outcome, DispatchOutcome::Continue));
        // FS surfaces untouched.
        assert!(fs.list_recent_outputs(8).await.unwrap().is_empty());
        assert!(fs.list_recent_evidence(8).await.unwrap().is_empty());
        let retirement = fs
            .storage()
            .get(&format!("{}retirement.json", fs.prefix()))
            .await
            .unwrap();
        assert!(
            retirement.is_none(),
            "idle decision must not write retirement marker"
        );
    }

    // ---------- `tool_failure_correction_text` ---------------------------
    //
    // The deep coverage of the corrective-text helper relocated here
    // with the helper itself (JAR2-57). Behaviour and assertions are
    // unchanged — only the file the tests live in moved.

    fn failure(tool: &str, args: serde_json::Value, error: &str) -> ToolFailure {
        ToolFailure {
            tool: tool.into(),
            args,
            error: error.into(),
        }
    }

    #[test]
    fn tool_failure_correction_text_quotes_tool_name_and_includes_args_and_error() {
        let s = tool_failure_correction_text(&[failure(
            "search_web",
            json!({"q": "what", "n": 3}),
            "503 Service Unavailable",
        )]);
        // Tool name is quoted so the model parses it as a token, not as
        // free text to summarize.
        assert!(
            s.contains("\"search_web\""),
            "tool name should be JSON-quoted, got: {s}"
        );
        // Args round-trip as JSON so the model sees exactly what it sent.
        // Key order is BTreeMap-sorted (see prompt.rs's determinism notes).
        assert!(
            s.contains("{\"n\":3,\"q\":\"what\"}"),
            "args should be rendered as JSON with sorted keys, got: {s}"
        );
        // Error string is preserved verbatim.
        assert!(
            s.contains("503 Service Unavailable"),
            "error should be preserved, got: {s}"
        );
        // Standing instruction at the end mirrors JAR2-19's "reply by
        // calling exactly one decision tool" framing so the model has a
        // clear next-step cue.
        assert!(
            s.contains("Reply with a different decision"),
            "should end with next-step cue, got: {s}"
        );
    }

    #[test]
    fn tool_failure_correction_text_handles_non_object_args() {
        // `call_tool`'s `args` is `serde_json::Value`; defensively the
        // helper must not assume an object — a model could supply a list
        // or even a primitive. Surface whatever it serializes to.
        let s = tool_failure_correction_text(&[failure("noop", json!([1, 2, 3]), "boom")]);
        assert!(s.contains("[1,2,3]"), "got: {s}");
    }

    #[test]
    fn tool_failure_correction_text_handles_null_args() {
        let s = tool_failure_correction_text(&[failure("noop", json!(null), "boom")]);
        assert!(s.contains("null"), "got: {s}");
    }

    /// JAR2-38: when the batch carries K>1 failures, the corrective text
    /// must enumerate each one (tool + args + error) so the model can
    /// target a retry without re-deriving the failure from a generic
    /// summary.
    #[test]
    fn tool_failure_correction_text_enumerates_batch_failures() {
        let s = tool_failure_correction_text(&[
            failure("read_a", json!({"path": "a.md"}), "ENOENT"),
            failure("read_b", json!({"path": "b.md"}), "ENOENT"),
            failure("read_c", json!({"path": "c.md"}), "permission denied"),
        ]);
        assert!(s.contains("3 parallel"), "should announce K count: {s}");
        for name in ["\"read_a\"", "\"read_b\"", "\"read_c\""] {
            assert!(s.contains(name), "missing tool {name}: {s}");
        }
        assert!(s.contains("permission denied"), "got: {s}");
        assert!(s.contains("Reply with a different decision"), "got: {s}");
    }
}

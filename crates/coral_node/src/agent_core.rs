//! Pure per-tick logic shared between the in-process `Agent::run` loop
//! and the workflow host. Three free functions, no internal async-runtime
//! concerns (no `tokio::select!`, channels, or `sleep`):
//! [`drain_triggers`] packages a drained `Vec<Trigger>` + FS reads into a
//! `ContextBundle`; [`decide`] calls into a `&dyn Decide`; [`dispatch`]
//! applies a `Decision` against the FS, tool registry, and scheduler and
//! returns a typed [`DispatchOutcome`]. The workflow host matches on
//! `Decision` directly and orchestrates activities itself, but its per-tick
//! result is isomorphic to `DispatchOutcome` so the shared post-dispatch
//! state machine (correction staging, budget accounting, health
//! transitions) can branch on the same enum.
//!
//! The four `DispatchOutcome` variants mirror the host's
//! `ApplyOutcome` shape and are kept distinct because budget accounting
//! depends on the distinction: `NeedsCorrection` consumes one
//! `FailureKind::Inference` slot; `ToolError { failures }` consumes K
//! `FailureKind::ToolCall` slots, one per failed call.

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
/// do next?" the in-process loop or the workflow can branch on.
///
/// * `Continue` — the tick produced no terminal or recoverable-failure
///   outcome; the host clears any correction state, marks the tick a
///   success, and proceeds to the next iteration.
/// * `Retired` — the agent emitted `Decision::Retire { reason }`; the
///   in-process executor has already written `retirement.json` to the FS
///   (the side-effect lives in `dispatch` because the host needs to exit
///   *after* the marker lands, not before). The variant carries the
///   `RetireReason` so the host can return it from `Agent::run`.
/// * `NeedsCorrection` — the decision parsed but the runtime cannot
///   satisfy it. The string is a human-readable failure description the
///   host threads into the next tick's `CorrectionContext` (and into the
///   `HealthIncident` retry trail on budget exhaustion).
/// * `ToolError { failures }` — the tool's internal retry policy
///   exhausted on one or more of K parallel calls. Successful sibling
///   calls in the same batch already had their evidence persisted before
///   this variant is constructed; the host does **not** unwind on
///   partial failure. Per-call accounting (K against budget) lives in
///   the host, not here.
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
/// must be `pub` for the workflow host to branch on the shared seam.
/// Both the in-process host and the workflow host read these fields
/// when staging the correction context and constructing the
/// `HealthIncident` retry trail.
#[derive(Debug, Clone)]
pub struct ToolFailure {
    pub tool: String,
    pub args: serde_json::Value,
    pub error: String,
}

/// Drain the already-collected triggers into a `ContextBundle` for the
/// current tick. In the in-process host the `Vec<Trigger>` comes from
/// `TriggerQueue::drain_ordered`; in the workflow host it comes from a
/// workflow-state buffer. Per-tick semantics are identical.
pub async fn drain_triggers(
    triggers: Vec<Trigger>,
    fs: &AgentFs,
    cfg: &Mandate,
    prior_correction: Option<CorrectionContext>,
) -> Result<ContextBundle> {
    assemble_context(fs, &triggers, cfg, prior_correction).await
}

/// Ask the [`Decide`] impl what to do this tick. Single shared call site
/// for the in-process loop and the workflow host's `decide_next_action`
/// activity.
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
/// **Side-effect semantics.** In the in-process host this is the
/// executor — it calls `fs.persist_output`, `tools.call`,
/// `fs.record_evidence`, etc. The workflow host matches on `Decision`
/// directly and orchestrates activities, so it does not call this
/// function; the shared piece is the returned `DispatchOutcome` shape.
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
        // Parent-child topology variants execute only on the workflow
        // host; reaching them here is a wiring bug.
        Decision::SpawnChild { .. } => {
            unimplemented!("in-process dispatch for parent-child decisions is not wired")
        }
        Decision::ReconcileChildren { .. } => {
            unimplemented!("in-process dispatch for parent-child decisions is not wired")
        }
        Decision::RetireChild { .. } => {
            unimplemented!("in-process dispatch for parent-child decisions is not wired")
        }
        Decision::ReplaceChild { .. } => {
            unimplemented!("in-process dispatch for parent-child decisions is not wired")
        }
    }
}

/// Dispatch the K calls in a `Decision::CallTools` together.
///
/// Order of operations matters for both correctness and determinism:
///
/// 1. **Pre-check every tool name.** If any call names a tool the registry
///    does not know, surface a single `NeedsCorrection` that lists every
///    missing name and dispatch *none* of the batch. "No tool registered"
///    is an inference-level correctable error, not a tool-call failure;
///    refusing to dispatch the sibling calls keeps evidence persistence
///    consistent with the rejection.
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
    // correction loop.
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
/// `CorrectionContext` staged after a tool-call exhaustion.
///
/// Accepts a batch of failed calls. For K=1 the wording is single-tool;
/// for K>1 the message lists every failed call so the model sees what
/// each sibling did and failed with.
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
    //! `crates/coral_node/tests/loop_smoke.rs` — those tests exercise the
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
    /// without touching the real filesystem.
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
        // The free function is a thin wrapper; the test locks the
        // call-site shape the workflow activity mirrors.
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
        // An Idle decision dispatched via `AgentCore::dispatch` must not
        // write any of the three FS surfaces a host would expect to
        // remain untouched. The signal source (mpsc), the async runtime
        // race (`tokio::select!`), and the in-process loop's
        // correction/health state are *not* exercised here — only
        // `decide` + `dispatch` from the seam, via a scripted
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
        // Standing instruction at the end gives the model a clear
        // next-step cue.
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

    /// When the batch carries K>1 failures, the corrective text must
    /// enumerate each one (tool + args + error) so the model can target a
    /// retry without re-deriving the failure from a generic summary.
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

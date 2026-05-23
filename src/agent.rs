//! `Agent` — the run loop that wires FS, triggers, decide, tools, and health.
//!
//! Each iteration of the loop:
//!
//! 1. **If a correction is pending**, skip the wait: the run loop is
//!    continuing a conversation with itself across an iteration boundary,
//!    not waiting on the world. Otherwise race the trigger queue against
//!    the scheduler's idle deadline. The race is `biased;` — when both
//!    arms are ready (the prior tick took longer than `idle_period` and a
//!    real trigger is also buffered), the queue arm wins, so
//!    `ScheduledWake` is pushed only when the queue is genuinely empty.
//! 2. Drain whatever is currently in the queue (may be empty when a
//!    correction is pending).
//! 3. **If a correction is pending**, this iteration is a continuation of
//!    the prior failed attempt: do **not** call [`HealthTracker::begin_tick`]
//!    — the per-tick retry budget must accumulate across the correction so
//!    exhaustion can mean what JAR2-19 says it means. Otherwise begin a
//!    fresh tick.
//! 4. Build a `ContextBundle` from the drained triggers, the recent FS
//!    state, and the pending correction (if any), and ask `Decide::decide`
//!    for a `Decision`.
//! 5. Dispatch the decision.
//! 6. **On [`ApplyOutcome::Continue`]**: clear `pending_correction` and
//!    mark the tick a success (`HealthTracker::mark_tick_success`) — this
//!    archives any prior Unhealthy incident on recovery.
//! 7. **On [`ApplyOutcome::Retire`]**: persist `retirement.json` and exit
//!    the loop with the reason.
//! 8. **On [`ApplyOutcome::NeedsCorrection`]**: the model emitted a
//!    `Decision` the runtime cannot satisfy (an unregistered tool, an
//!    unresolvable evidence id). We record the failure against the
//!    inference budget; if there is still room we stash the failure
//!    description in `pending_correction` and let the next iteration give
//!    the model a chance to self-correct. If the budget is exhausted we
//!    build a [`HealthIncident`] and transition the tracker to `Unhealthy`.
//!    The loop **does not halt** — the agent stays subscribed to its
//!    trigger queue per `health.rs`'s contract; a later successful tick
//!    recovers to `Healthy`.
//!
//! Decide-side `Err` (e.g. inference parse retries exhausted in
//! `LlmDecide`) is treated as inference-retry exhaustion at the run-loop
//! boundary: the tracker transitions to `Unhealthy` directly without
//! consulting the per-tick budget. The `LlmDecide` impl already did its
//! one allowed retry internally; spending another budget slot here would
//! double-count.
//!
//! # Why correction is agent-state, not a queue trigger
//!
//! An earlier draft expressed mid-correction continuation by self-injecting
//! a `Trigger::External { kind: SYNTHETIC_CORRECTION_KIND, ... }` into the
//! same queue external producers feed. That made "are we mid-correction?"
//! a property *derived* from queue contents, with two failure modes:
//!
//! * **External-producer race.** A trigger arriving between the inject and
//!   the next drain made `is_correction_only` false, which reset the
//!   per-tick budget and broke JAR2-19's accumulation contract.
//! * **Scheduler self-race.** If the prior tick took non-trivial time, the
//!   `select!` arm racing `wait_nonempty` against `sleep_until` could see
//!   both branches ready at once. Tokio picks pseudo-randomly; if the
//!   deadline branch won, `ScheduledWake` was pushed alongside the
//!   synthetic correction and the budget reset — even with zero external
//!   producers in the picture.
//!
//! Storing `pending_correction: Option<CorrectionContext>` directly on the
//! run loop makes the invariant a stored fact rather than a derived one.
//! The trigger queue stays the boundary with the outside world; corrections
//! stay agent-internal continuation state. See `decision::CorrectionContext`.
//!
//! # Type-parameter shape (deviation from the original ticket sketch)
//!
//! The bootstrap took `ToolRegistry` concretely (no `ToolDispatch` trait,
//! no abstraction-for-future-needs). That decision still holds: there is
//! still exactly one registry implementation, and `Decide` stays generic
//! because there are several impls in tree.
//!
//! # Provenance keep-alive
//!
//! Provenance violations (`FsError::EmptyEvidence`,
//! `FsError::EvidenceNotFound`) used to degrade to a `tracing::warn!` and
//! `Continue`. JAR2-19 routes them through the correction loop instead, so
//! the model gets a chance to self-correct on the next tick. The agent is
//! still kept alive on the failure (the original property), just via a
//! different mechanism.

use anyhow::Result;
use chrono::Utc;
use futures::future::join_all;
use serde_json::json;
use tokio::time::sleep_until;
use tracing::{debug, info_span, warn, Instrument};

use crate::decision::{assemble_context, CorrectionContext, Decide, Decision, ToolCall};
use crate::evidence::EvidenceId;
use crate::fs::{AgentFs, FsError};
use crate::health::{
    Attempt, FailingCall, FailureKind, HealthError, HealthIncident, HealthTracker,
};
use crate::mandate::Mandate;
use crate::scheduler::Scheduler;
use crate::tools::ToolRegistry;
use crate::trigger::Trigger;
use crate::trigger_queue::{SignalSink, TriggerQueue};

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use std::sync::{Arc, Mutex};

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use crate::decide_llm::llm_decide::{LlmDecide, TickTotals};
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use crate::model_client::CallStats;

/// Reason an agent retired, surfaced from `Agent::run`. Newtype around the
/// raw string so callers can distinguish a clean retirement from any other
/// `String` they might be holding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetireReason(pub String);

impl RetireReason {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The single-node agent. Owns the FS, the trigger queue, the model
/// adapter, the tool registry, the scheduler, and the health tracker.
pub struct Agent<D: Decide> {
    cfg: Mandate,
    fs: AgentFs,
    triggers: TriggerQueue,
    decide: D,
    tools: ToolRegistry,
    scheduler: Scheduler,
    sink: SignalSink,
    health: HealthTracker,
}

impl<D: Decide> Agent<D> {
    /// Wire an agent. The scheduler is seeded with `cfg.idle_period` so the
    /// first deadline arrives at the configured cadence. A fresh
    /// `TriggerQueue` is constructed; its `SignalSink` is retained so
    /// `signal()` can be called before `run()` consumes the agent.
    ///
    /// `health` is constructed by the caller (typically rooted at the same
    /// directory as `fs`) and passed in. The same construction-injection
    /// pattern as `decide`/`tools` keeps the wiring boundary clean and lets
    /// tests drive a tracker with deterministic timestamps and budgets.
    pub fn new(
        cfg: Mandate,
        fs: AgentFs,
        decide: D,
        tools: ToolRegistry,
        health: HealthTracker,
    ) -> Self {
        let scheduler = Scheduler::new(cfg.idle_period);
        let (triggers, sink) = TriggerQueue::new();
        Self {
            cfg,
            fs,
            triggers,
            decide,
            tools,
            scheduler,
            sink,
            health,
        }
    }

    /// Mint a clonable `SignalSink` an external producer can use to push
    /// triggers onto the queue. Safe to call before `run()` — the sink is
    /// stored on the agent at construction.
    pub fn signal(&self) -> SignalSink {
        self.sink.clone()
    }

    /// Run the loop until one of:
    /// - a `Decision::Retire` arrives (the normal path; `retirement.json`
    ///   is written and the reason is returned);
    /// - the `Mandate.max_ticks` safety cap is hit (also writes
    ///   `retirement.json`, with a synthesized `max_ticks (N) reached`
    ///   reason);
    /// - a non-recoverable error bubbles via `?`.
    ///
    /// `Unhealthy` transitions and pending corrections are **not** exit
    /// conditions — see the module doc for why.
    pub async fn run(self) -> Result<RetireReason> {
        let Agent {
            cfg,
            fs,
            mut triggers,
            decide,
            tools,
            mut scheduler,
            sink: _sink,
            mut health,
        } = self;

        // Continuation state. `Some` means the previous tick produced an
        // unsatisfiable `Decision`; this tick is a correction attempt.
        // Cleared on a successful Continue, on transition_to_unhealthy
        // (the tick that exhausts budget closes the correction window),
        // and on Retire.
        let mut pending_correction: Option<CorrectionContext> = None;

        // Retry trail accumulated across attempts in the *current* fresh
        // tick. Cleared whenever `begin_tick` runs (i.e. when no correction
        // is pending). Used to populate `HealthIncident` on budget
        // exhaustion.
        let mut retry_trail: Vec<Attempt> = Vec::new();

        let mut tick: u64 = 0;
        loop {
            // `Mandate.max_ticks` is a safety cap on loop iterations.
            // `None` means "run until `Retire`." Check before incrementing
            // so the cap is the count of ticks actually performed.
            if let Some(max) = cfg.max_ticks {
                if tick >= max {
                    let reason = format!("max_ticks ({}) reached", max);
                    fs.persist_retirement(&reason)?;
                    return Ok(RetireReason(reason));
                }
            }
            tick += 1;
            let span = info_span!("agent.tick", tick);
            let outcome = async {
                // Mid-correction: the loop is continuing where it left off,
                // so do not wait on the world. Drain whatever happens to be
                // queued (may be empty) and proceed. Fresh tick: race the
                // queue against the deadline as usual.
                let is_correction = pending_correction.is_some();
                if !is_correction {
                    // `biased;` makes tokio poll arms in declaration order
                    // rather than randomly. The queue arm is listed first
                    // so that when both are ready (the prior tick took
                    // longer than `idle_period`, leaving an elapsed
                    // deadline alongside buffered triggers), the queue
                    // wins and we don't push a spurious `ScheduledWake`
                    // onto a queue that already has work. `ScheduledWake`
                    // should mean "the idle period elapsed without other
                    // work" — a stronger semantic than tokio's default
                    // tie-break gives us — and pinning it here keeps the
                    // bundle deterministic across runs that share world
                    // state.
                    tokio::select! {
                        biased;
                        _ = triggers.wait_nonempty() => {}
                        _ = sleep_until(scheduler.next_deadline()) => {
                            triggers.push(Trigger::ScheduledWake);
                        }
                    }
                }

                let drained = triggers.drain_ordered();
                if !is_correction {
                    health.begin_tick();
                    retry_trail.clear();
                }
                debug!(count = drained.len(), is_correction, "drained triggers");

                let bundle =
                    assemble_context(&fs, &drained, &cfg, pending_correction.clone()).await?;
                let decision = match decide.decide(bundle).await {
                    Ok(d) => d,
                    Err(e) => {
                        // Decide-side Err: LlmDecide already did its
                        // one-shot internal retry (or this is `MockDecide`
                        // returning a script error). Treat as direct
                        // inference-retry exhaustion — go straight to
                        // Unhealthy without spending another budget slot.
                        warn!(error = %e, "decide returned Err; transitioning to Unhealthy");
                        let attempt = Attempt {
                            attempt: (retry_trail.len() as u32) + 1,
                            at: Utc::now(),
                            error: format!("{e:#}"),
                        };
                        retry_trail.push(attempt);
                        let incident = HealthIncident {
                            failing: FailingCall {
                                kind: FailureKind::Inference,
                                details: json!({
                                    "stage": "decide",
                                    "error": format!("{e:#}"),
                                }),
                            },
                            retry_trail: retry_trail.clone(),
                            last_error: format!("{e:#}"),
                            transitioned_at: Utc::now(),
                        };
                        health.transition_to_unhealthy(incident)?;
                        // Decide-Err closes the correction window: the
                        // Unhealthy transition replaces it as the
                        // operative state, and the next fresh tick should
                        // begin_tick from a clean slate.
                        pending_correction = None;
                        return Ok::<TickOutcome, anyhow::Error>(TickOutcome::Continue);
                    }
                };

                match dispatch(&fs, &tools, &mut scheduler, decision).await? {
                    ApplyOutcome::Continue => {
                        health.mark_tick_success(Utc::now())?;
                        retry_trail.clear();
                        pending_correction = None;
                        Ok(TickOutcome::Continue)
                    }
                    ApplyOutcome::Retire(reason) => Ok(TickOutcome::Retire(reason)),
                    ApplyOutcome::NeedsCorrection(desc) => {
                        let attempt = Attempt {
                            attempt: (retry_trail.len() as u32) + 1,
                            at: Utc::now(),
                            error: desc.clone(),
                        };
                        retry_trail.push(attempt);
                        match health.record_failure(FailureKind::Inference, &desc) {
                            Ok(()) => {
                                warn!(failure = %desc, "apply-time failure; staging correction");
                                pending_correction = Some(CorrectionContext::new(desc));
                                Ok(TickOutcome::Continue)
                            }
                            Err(HealthError::BudgetExhausted { kind }) => {
                                warn!(
                                    ?kind,
                                    "inference budget exhausted; transitioning to Unhealthy"
                                );
                                let incident = HealthIncident {
                                    failing: FailingCall {
                                        kind,
                                        details: json!({
                                            "stage": "apply",
                                            "error": desc,
                                        }),
                                    },
                                    retry_trail: retry_trail.clone(),
                                    last_error: desc,
                                    transitioned_at: Utc::now(),
                                };
                                health.transition_to_unhealthy(incident)?;
                                // Budget exhaustion closes the correction
                                // window; next fresh tick starts clean.
                                pending_correction = None;
                                Ok(TickOutcome::Continue)
                            }
                            Err(other) => Err(other.into()),
                        }
                    }
                    ApplyOutcome::ToolError { failures } => {
                        // JAR2-25: the tool's internal retry policy
                        // (`McpTool::call`) has already exhausted its
                        // `RetryPolicy::max_attempts` attempts before
                        // surfacing this error. Each exhausted call counts
                        // as one tick against `RetryBudget::max_tool`. The
                        // two bounds are deliberately distinct: per-call
                        // retries handle a single flaky tool invocation,
                        // per-tick budget handles "many tools breaking on
                        // one tick".
                        //
                        // JAR2-30: symmetric to the inference correction
                        // loop in `NeedsCorrection` above, we stage a
                        // `pending_correction` describing the failure
                        // (tool name, args summary, last error). The next
                        // tick threads it into the `ContextBundle` so the
                        // model can self-correct (try different args, a
                        // different tool, an `idle`, etc.). The shape of
                        // the corrective signal is the same `CorrectionContext`
                        // the inference path uses — reusing the existing
                        // mechanism rather than introducing a parallel
                        // trigger class, per the ticket's
                        // "no public Trigger variant explosion" guidance.
                        //
                        // JAR2-38: a tick that issues K parallel calls
                        // may surface K failures from one dispatch. Per
                        // the ticket's "K against the budget" default,
                        // each failed call consumes one
                        // `FailureKind::ToolCall` slot — so one bad tick
                        // can't burn an unbounded number of attempts
                        // through the noise floor. The loop below
                        // records every failure in order; if the budget
                        // exhausts partway through, the remaining
                        // failures still join the retry trail (so the
                        // `HealthIncident` archive captures the full
                        // batch) but stop spending budget slots that
                        // are no longer there. Budget accumulation
                        // across the correction continuation is
                        // symmetric to JAR2-30: the next iteration skips
                        // `begin_tick` while `pending_correction` is
                        // `Some`, so K failures here plus M more on the
                        // continuation count K+M against the same
                        // window.
                        let desc = tool_failure_correction_text(&failures);
                        let mut budget_exhausted: Option<FailureKind> = None;
                        let mut last_error = String::new();
                        for f in &failures {
                            let attempt = Attempt {
                                attempt: (retry_trail.len() as u32) + 1,
                                at: Utc::now(),
                                error: f.error.clone(),
                            };
                            retry_trail.push(attempt);
                            last_error = f.error.clone();
                            if budget_exhausted.is_some() {
                                continue;
                            }
                            match health.record_failure(FailureKind::ToolCall, &f.error) {
                                Ok(()) => {}
                                Err(HealthError::BudgetExhausted { kind }) => {
                                    budget_exhausted = Some(kind);
                                }
                                Err(other) => return Err(other.into()),
                            }
                        }
                        match budget_exhausted {
                            None => {
                                warn!(
                                    failures = failures.len(),
                                    first_tool = %failures.first().map(|f| f.tool.as_str()).unwrap_or(""),
                                    "tool call(s) exhausted retries; staging correction"
                                );
                                pending_correction = Some(CorrectionContext::new(desc));
                                Ok(TickOutcome::Continue)
                            }
                            Some(kind) => {
                                warn!(
                                    ?kind,
                                    failures = failures.len(),
                                    "tool-call budget exhausted; transitioning to Unhealthy"
                                );
                                let failures_json: Vec<serde_json::Value> = failures
                                    .iter()
                                    .map(|f| {
                                        json!({
                                            "tool": f.tool,
                                            "error": f.error,
                                        })
                                    })
                                    .collect();
                                let first_tool = failures
                                    .first()
                                    .map(|f| f.tool.clone())
                                    .unwrap_or_default();
                                let incident = HealthIncident {
                                    failing: FailingCall {
                                        kind,
                                        details: json!({
                                            "stage": "apply",
                                            "tool": first_tool,
                                            "error": last_error.clone(),
                                            "failures": failures_json,
                                        }),
                                    },
                                    retry_trail: retry_trail.clone(),
                                    last_error,
                                    transitioned_at: Utc::now(),
                                };
                                health.transition_to_unhealthy(incident)?;
                                // Budget exhaustion closes the correction
                                // window; the next fresh tick starts
                                // clean.
                                pending_correction = None;
                                Ok(TickOutcome::Continue)
                            }
                        }
                    }
                }
            }
            .instrument(span)
            .await?;

            if let TickOutcome::Retire(reason) = outcome {
                return Ok(reason);
            }
        }
    }
}

/// What a single tick decided. Either continue to the next tick or
/// terminate with a retirement reason.
enum TickOutcome {
    Continue,
    Retire(RetireReason),
}

/// Outcome of trying to apply one `Decision`.
///
/// `NeedsCorrection` is the JAR2-19 case: the decision parsed cleanly but
/// the runtime cannot satisfy it (unregistered tool, unresolvable
/// evidence). The contained string is the human-readable failure
/// description; it ends up in both the next bundle's
/// `CorrectionContext::failure` and the `HealthIncident` retry trail.
///
/// `ToolError` is the JAR2-25 case: the tool exists and was invoked, but
/// its internal retry policy (`McpTool::call` / equivalent) exhausted on
/// transient errors. The agent feeds this into the per-tick
/// `FailureKind::ToolCall` budget rather than the inference budget. Per
/// JAR2-30 it also stages a `CorrectionContext` describing the tool
/// call (name, args, error) so the model can self-correct on the next
/// tick — symmetric to the `NeedsCorrection` path.
///
/// JAR2-38: `ToolError` now carries a *batch* of failed calls because a
/// single `Decision::CallTools` may issue K tool calls in parallel.
/// Successful sibling calls in the same batch already had their
/// evidence persisted to disk before this outcome is constructed —
/// **we do not unwind on partial failure**. The corrective context
/// quotes each failed call's tool/args/error so the model can target a
/// retry; the per-call accounting against the
/// `FailureKind::ToolCall` budget is *one slot per failed call*
/// (K failures → K against budget), per JAR2-38's "K against budget"
/// default. The run-loop dispatch site (`agent::run`) implements that
/// accounting and documents the choice next to the call site.
enum ApplyOutcome {
    Continue,
    Retire(RetireReason),
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
#[derive(Debug, Clone)]
struct ToolFailure {
    tool: String,
    args: serde_json::Value,
    error: String,
}

/// Apply a single `Decision`. Recoverable apply-time failures surface as
/// [`ApplyOutcome::NeedsCorrection`]; everything else either continues,
/// retires, or bubbles via `?`.
async fn dispatch(
    fs: &AgentFs,
    tools: &ToolRegistry,
    scheduler: &mut Scheduler,
    decision: Decision,
) -> Result<ApplyOutcome> {
    match decision {
        Decision::CallTools { calls } => dispatch_call_tools(fs, tools, calls).await,
        Decision::EmitOutput { content, evidence } => {
            debug!(evidence_count = evidence.len(), "decision: emit_output");
            match fs.persist_output(&content, &evidence) {
                Ok(_) => Ok(ApplyOutcome::Continue),
                Err(e) => match e.downcast_ref::<FsError>() {
                    Some(FsError::EmptyEvidence) => Ok(ApplyOutcome::NeedsCorrection(
                        "emit_output: evidence list is empty (provenance contract)".into(),
                    )),
                    Some(FsError::EvidenceNotFound(id)) => {
                        let id: EvidenceId = id.clone();
                        Ok(ApplyOutcome::NeedsCorrection(format!(
                            "emit_output: evidence {id} not found on disk"
                        )))
                    }
                    _ => Err(e),
                },
            }
        }
        Decision::RewriteFs { ops } => {
            debug!(op_count = ops.len(), "decision: rewrite_fs");
            fs.apply_ops(ops)?;
            Ok(ApplyOutcome::Continue)
        }
        Decision::Idle { next_after } => {
            debug!(
                next_after_ms = next_after.as_millis() as u64,
                "decision: idle"
            );
            scheduler.set_next_after(next_after);
            Ok(ApplyOutcome::Continue)
        }
        Decision::Retire { reason } => {
            debug!(%reason, "decision: retire");
            fs.persist_retirement(&reason)?;
            Ok(ApplyOutcome::Retire(RetireReason(reason)))
        }
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
) -> Result<ApplyOutcome> {
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
        return Ok(ApplyOutcome::NeedsCorrection(format!(
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
                fs.record_evidence(ev)?;
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
        Ok(ApplyOutcome::Continue)
    } else {
        Ok(ApplyOutcome::ToolError { failures })
    }
}

/// Build the human-readable failure description for the
/// `CorrectionContext` staged after a tool-call exhaustion (JAR2-30).
///
/// JAR2-38: now accepts a batch of failed calls. For K=1 the wording
/// matches the original single-tool phrasing; for K>1 the message
/// lists every failed call so the model sees what each sibling did and
/// failed with.
///
/// Promoted to a named helper so the unit test below can assert against
/// the exact same string the run loop emits, and so a future cosmetic
/// change to the wording lands in one place.
fn tool_failure_correction_text(failures: &[ToolFailure]) -> String {
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

// ---------- JAR2-33: post-run / between-tick CallStats accessor ----------
//
// The accessor and its supporting `StatsHandle` are scoped to
// `Agent<LlmDecide>` — non-LLM `Decide` impls (test doubles, future
// non-model decision sources) don't need a stats surface, and per
// `JAR2-33` we kept the diff minimal by not extending the `Decide` trait
// with a stats method. The whole section is feature-gated on the
// `llm-*` features for the same reason `LlmDecide` itself is.

/// Cheap, clonable handle onto an `LlmDecide`'s per-tick `CallStats`
/// accumulator. Surfaces the same data as `LlmDecide::last_tick_calls` /
/// `last_tick_totals` but survives `Agent::run` consuming the `LlmDecide`,
/// so callers can read the most recent tick's stats after the run loop
/// retires.
///
/// **Update timing.** The underlying accumulator is reset at the start of
/// every `LlmDecide::decide` call and pushed once per upstream
/// `ModelClient::complete` call. A read in between two `decide`
/// invocations reflects the *previous* tick — there is no per-tick
/// history. Before the first `decide` runs the handle reports zero
/// calls and `TickTotals::default()`.
///
/// **Concurrency.** Internally an `Arc<Mutex<Vec<CallStats>>>`. The lock
/// is held only for the duration of the read; no `await` is performed
/// while holding it. Cloning the handle is `Arc::clone`-cheap.
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
#[derive(Clone)]
pub struct StatsHandle {
    inner: Arc<Mutex<Vec<CallStats>>>,
}

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
impl StatsHandle {
    /// Per-call stats for the most recent `decide()` invocation, in call
    /// order. Returns an owned `Vec` (clone of the inner storage) so the
    /// caller never has to hold the lock.
    pub fn last_tick_calls(&self) -> Vec<CallStats> {
        self.inner.lock().expect("stats mutex poisoned").clone()
    }

    /// Aggregate totals for the most recent `decide()` invocation.
    /// Returns `TickTotals::default()` before the first `decide` runs.
    pub fn last_tick_totals(&self) -> TickTotals {
        let stats = self.inner.lock().expect("stats mutex poisoned");
        TickTotals::from_calls(&stats)
    }
}

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
impl Agent<LlmDecide> {
    /// JAR2-33: capture a `StatsHandle` onto the inner `LlmDecide`'s
    /// per-tick `CallStats` accumulator. Cheap (one `Arc::clone`). The
    /// handle outlives the agent — `Agent::run` will consume `self` and
    /// drop the `LlmDecide`, but the `Arc<Mutex<...>>` storage remains
    /// reachable through any `StatsHandle` cloned out beforehand.
    ///
    /// Typical use in a test:
    /// ```ignore
    /// let agent = Agent::new(mandate, fs, decide, registry, health);
    /// let stats = agent.stats_handle();
    /// let RetireReason(_) = agent.run().await?;
    /// let calls = stats.last_tick_calls(); // last tick's CallStats
    /// ```
    pub fn stats_handle(&self) -> StatsHandle {
        StatsHandle {
            inner: self.decide.stats_handle(),
        }
    }

    /// Convenience: per-call stats for the most recent tick, via the
    /// inner `LlmDecide`. Useful between ticks from a borrow on the
    /// agent. Post-run callers must use `stats_handle()` to capture a
    /// handle before `.run()` consumes the agent.
    pub fn last_tick_calls(&self) -> Vec<CallStats> {
        self.decide.last_tick_calls()
    }

    /// Convenience: aggregate totals for the most recent tick. Same
    /// timing semantics as `StatsHandle::last_tick_totals`.
    pub fn last_tick_totals(&self) -> TickTotals {
        self.decide.last_tick_totals()
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for the JAR2-30 corrective-text helper.
    //!
    //! Integration coverage for the end-to-end exhaustion → correction →
    //! recovery flow lives in `tests/loop_smoke.rs`.

    use super::*;
    use serde_json::json;

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

    // ---------- JAR2-33: stats accessor unit tests ----------

    #[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
    mod stats_handle_tests {
        //! Unit tests for the JAR2-33 `StatsHandle` / `Agent<LlmDecide>`
        //! accessors. End-to-end coverage through `Agent::run` lives in
        //! the JAR2-21 fixture suites (see
        //! `tests/llm_fixture_anthropic.rs::unhealthy_then_recovery_cycle_via_agent_run`
        //! and the Cohere mirror).
        use super::*;
        use crate::model_client::{
            CallStats, CompleteOptions, CompleteRequest, CompleteResponse, ContentBlock,
            ModelClient, ModelError, ToolCall, Usage, Vendor,
        };
        use async_trait::async_trait;
        use serde_json::json;
        use std::sync::Mutex as StdMutex;

        /// Minimal scripted `ModelClient`: pops the next `CompleteResponse`
        /// from a queue on each `complete` call. Mirrors the pattern in
        /// `decide_llm::llm_decide::tests::MockModelClient` but lives here
        /// so the agent-side accessor tests don't reach into another
        /// module's private test surface.
        struct ScriptedClient {
            script: StdMutex<Vec<CompleteResponse>>,
        }

        #[async_trait]
        impl ModelClient for ScriptedClient {
            async fn complete(
                &self,
                _req: CompleteRequest,
            ) -> Result<CompleteResponse, ModelError> {
                let next = self
                    .script
                    .lock()
                    .unwrap()
                    .drain(..1)
                    .next()
                    .expect("ScriptedClient: script exhausted");
                Ok(next)
            }
        }

        fn resp(stats: CallStats, tool_call: ToolCall) -> CompleteResponse {
            CompleteResponse {
                content: vec![ContentBlock::ToolUse {
                    id: tool_call.id.clone(),
                    name: tool_call.name.clone(),
                    input: tool_call.arguments.clone(),
                }],
                tool_calls: vec![tool_call],
                usage: stats.usage,
                stats,
            }
        }

        fn idle_call() -> ToolCall {
            ToolCall {
                id: "toolu_idle".into(),
                name: "idle".into(),
                arguments: json!({"next_after": 1000}),
            }
        }

        fn stats(input: u32, output: u32, latency_ms: u64) -> CallStats {
            CallStats {
                usage: Usage {
                    input_tokens: input,
                    output_tokens: output,
                },
                latency_ms,
                vendor: Vendor::Anthropic,
                model: "test-model".into(),
            }
        }

        fn empty_bundle() -> crate::decision::ContextBundle {
            crate::decision::ContextBundle {
                mandate: Mandate::new("stats-test", std::time::Duration::from_secs(1), Some(1)),
                triggers: vec![],
                recent_outputs: vec![],
                recent_evidence: vec![],
                open_claims: vec![],
                correction: None,
            }
        }

        #[tokio::test]
        async fn stats_handle_zero_before_first_decide() {
            // A fresh `Agent<LlmDecide>` has run no ticks. The handle must
            // report no calls and a zero `TickTotals`.
            let client: Arc<dyn ModelClient> = Arc::new(ScriptedClient {
                script: StdMutex::new(vec![]),
            });
            let decide = LlmDecide::new(client, CompleteOptions::default());
            let tmp = tempfile::TempDir::new().unwrap();
            let mandate = Mandate::new("stats-test", std::time::Duration::from_millis(10), Some(1));
            let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate).unwrap();
            let registry = ToolRegistry::new();
            let health = HealthTracker::open(
                tmp.path(),
                crate::health::RetryBudget::default(),
                Utc::now(),
            )
            .unwrap();
            let agent = Agent::new(mandate, fs, decide, registry, health);

            let handle = agent.stats_handle();
            assert!(handle.last_tick_calls().is_empty());
            assert_eq!(handle.last_tick_totals(), TickTotals::default());
            // Pre-run convenience accessor should agree.
            assert!(agent.last_tick_calls().is_empty());
            assert_eq!(agent.last_tick_totals(), TickTotals::default());
        }

        #[tokio::test]
        async fn stats_handle_survives_after_run_consumes_agent() {
            // The core JAR2-33 promise: capture the handle pre-run, run
            // the agent to retirement (consuming `self`), then read the
            // most recent tick's stats off the handle.
            let s = stats(11, 7, 42);
            let client: Arc<dyn ModelClient> = Arc::new(ScriptedClient {
                script: StdMutex::new(vec![
                    // Tick 1: idle → loop continues.
                    resp(s.clone(), idle_call()),
                    // Tick 2: retire → loop exits.
                    resp(
                        stats(3, 2, 5),
                        ToolCall {
                            id: "toolu_retire".into(),
                            name: "retire".into(),
                            arguments: json!({"reason": "done"}),
                        },
                    ),
                ]),
            });
            let decide = LlmDecide::new(client, CompleteOptions::default());
            let tmp = tempfile::TempDir::new().unwrap();
            // Small idle_period + max_ticks cap so the test is fast and
            // bounded regardless of which retire path actually fires.
            let mandate = Mandate::new("stats-test", std::time::Duration::from_millis(1), Some(4));
            let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate).unwrap();
            let registry = ToolRegistry::new();
            let health = HealthTracker::open(
                tmp.path(),
                crate::health::RetryBudget::default(),
                Utc::now(),
            )
            .unwrap();
            let agent = Agent::new(mandate, fs, decide, registry, health);

            // Capture *before* run consumes the agent — this is the API
            // contract test (c) depends on.
            let stats_handle = agent.stats_handle();

            let RetireReason(reason) =
                tokio::time::timeout(std::time::Duration::from_secs(5), agent.run())
                    .await
                    .expect("agent retired")
                    .expect("run ok");
            assert_eq!(reason, "done");

            // Stats handle must reflect the *last* tick (the retire), not
            // the first tick (idle). LlmDecide resets its accumulator at
            // the start of every `decide`.
            let calls = stats_handle.last_tick_calls();
            assert_eq!(
                calls.len(),
                1,
                "last tick was a single-call retire decision"
            );
            assert_eq!(calls[0].usage.input_tokens, 3);
            assert_eq!(calls[0].usage.output_tokens, 2);
            assert_eq!(calls[0].latency_ms, 5);

            let totals = stats_handle.last_tick_totals();
            assert_eq!(totals.calls, 1);
            assert_eq!(totals.input_tokens, 3);
            assert_eq!(totals.output_tokens, 2);
            assert_eq!(totals.latency_ms, 5);
        }

        #[tokio::test]
        async fn stats_handle_is_cheap_to_clone() {
            // The handle must be cheap to clone and clones must share
            // storage — otherwise callers can't, say, hand a clone to a
            // tracing layer and read another clone post-run.
            let client: Arc<dyn ModelClient> = Arc::new(ScriptedClient {
                script: StdMutex::new(vec![resp(stats(50, 5, 99), idle_call())]),
            });
            let decide = LlmDecide::new(client, CompleteOptions::default());
            let tmp = tempfile::TempDir::new().unwrap();
            let mandate = Mandate::new("stats-test", std::time::Duration::from_secs(1), Some(1));
            let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate).unwrap();
            let registry = ToolRegistry::new();
            let health = HealthTracker::open(
                tmp.path(),
                crate::health::RetryBudget::default(),
                Utc::now(),
            )
            .unwrap();
            let agent = Agent::new(mandate, fs, decide, registry, health);

            let h1 = agent.stats_handle();
            let h2 = h1.clone();
            // Drive one tick directly via the inner `decide` (no run
            // loop) so the test exercises the `decide` reset semantics
            // without depending on agent scheduling.
            agent.decide.decide(empty_bundle()).await.unwrap();
            // Both clones see the same accumulator state.
            assert_eq!(h1.last_tick_calls(), h2.last_tick_calls());
            assert_eq!(h1.last_tick_totals(), h2.last_tick_totals());
            assert_eq!(h1.last_tick_totals().calls, 1);
            assert_eq!(h1.last_tick_totals().input_tokens, 50);
        }
    }
}

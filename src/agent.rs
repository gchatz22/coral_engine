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
use serde_json::json;
use tokio::time::sleep_until;
use tracing::{debug, info_span, warn, Instrument};

use crate::decision::{assemble_context, CorrectionContext, Decide, Decision};
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
enum ApplyOutcome {
    Continue,
    Retire(RetireReason),
    NeedsCorrection(String),
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
        Decision::CallTool { name, args, .. } => {
            debug!(tool = %name, "decision: call_tool");
            // Pre-check distinguishes "model picked a tool that doesn't
            // exist" (correctable inference error) from "the tool
            // existed and errored" (real call failure — JAR2-25's lane).
            if !tools.contains(&name) {
                return Ok(ApplyOutcome::NeedsCorrection(format!(
                    "call_tool: no tool registered under name {name:?}"
                )));
            }
            let ev = tools.call(&name, args).await?;
            fs.record_evidence(ev)?;
            Ok(ApplyOutcome::Continue)
        }
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

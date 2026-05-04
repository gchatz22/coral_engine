//! `Agent` — the run loop that wires FS, triggers, decide, and tools.
//!
//! This is the JAR2-8 integration ticket. The loop is the literal Rust
//! shape of `scratch/minimal_node_backend.md` § 4: race the trigger queue
//! against the scheduler's idle deadline, drain triggers, build a context
//! bundle, ask `Decide` what to do, and dispatch on the resulting
//! `Decision`.
//!
//! # Type-parameter shape (deviation from the ticket sketch)
//!
//! The ticket sketches `Agent<D: Decide, T>` with `T` as the tool registry.
//! In the bootstrap there is exactly one tool registry implementation
//! (`ToolRegistry`), no `ToolDispatch` trait, and a bare `T` with no bound
//! cannot call `.call()`. Introducing a one-impl trait purely to honor the
//! sketch would violate `DEVELOPMENT.md` § 2 ("no abstractions for
//! hypothetical future needs"). We take `ToolRegistry` concretely.
//! `Decide` stays generic because there are already two implementations in
//! tree (`MockDecide` plus the future real adapter) and the trait is
//! dyn-compatible by design.
//!
//! # Provenance keep-alive
//!
//! `persist_output` enforces the provenance contract by returning
//! `FsError::EmptyEvidence` or `FsError::EvidenceNotFound`. The acceptance
//! criterion is "agent does not exit" on these — they're the agent's bug,
//! not a runtime failure. The `EmitOutput` arm downcasts the `anyhow`
//! error and, if it is one of those two `FsError` variants, emits a
//! `tracing::warn!` and continues to the next tick. Real I/O errors and
//! every other variant still bubble.

use anyhow::Result;
use tokio::time::sleep_until;
use tracing::{debug, info_span, warn, Instrument};

use crate::decision::{assemble_context, Decide, Decision};
use crate::fs::{AgentFs, FsError};
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
/// adapter, the tool registry, and the scheduler.
pub struct Agent<D: Decide> {
    cfg: Mandate,
    fs: AgentFs,
    triggers: TriggerQueue,
    decide: D,
    tools: ToolRegistry,
    scheduler: Scheduler,
    sink: SignalSink,
}

impl<D: Decide> Agent<D> {
    /// Wire an agent. The scheduler is seeded with `cfg.idle_period` so the
    /// first deadline arrives at the configured cadence. A fresh
    /// `TriggerQueue` is constructed; its `SignalSink` is retained so
    /// `signal()` can be called before `run()` consumes the agent.
    pub fn new(cfg: Mandate, fs: AgentFs, decide: D, tools: ToolRegistry) -> Self {
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
        }
    }

    /// Mint a clonable `SignalSink` an external producer can use to push
    /// triggers onto the queue. Safe to call before `run()` — the sink is
    /// stored on the agent at construction.
    pub fn signal(&self) -> SignalSink {
        self.sink.clone()
    }

    /// Run the loop until a `Decision::Retire` arrives (or a non-provenance
    /// error bubbles).
    pub async fn run(self) -> Result<RetireReason> {
        let Agent {
            cfg,
            fs,
            mut triggers,
            decide,
            tools,
            mut scheduler,
            sink: _sink,
        } = self;

        let mut tick: u64 = 0;
        loop {
            // `Mandate.max_ticks` is a safety cap on loop iterations.
            // `None` means "run until `Retire`." Check before incrementing
            // so the cap is the count of ticks actually performed: with
            // `max_ticks = Some(N)`, exactly N ticks run before retirement.
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
                // Race the queue against the idle deadline. The unselected
                // future is dropped at the `}` of its arm, releasing the
                // `&mut triggers` borrow before the sleep arm body needs
                // it. Standard tokio idiom; see `select!` docs.
                tokio::select! {
                    _ = triggers.wait_nonempty() => {}
                    _ = sleep_until(scheduler.next_deadline()) => {
                        triggers.push(Trigger::ScheduledWake);
                    }
                }

                let drained = triggers.drain_ordered();
                debug!(count = drained.len(), "drained triggers");
                let bundle = assemble_context(&fs, &drained, &cfg).await?;
                let decision = decide.decide(bundle).await?;

                dispatch(&fs, &tools, &mut scheduler, decision).await
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

/// Apply a single `Decision`. Provenance violations from `EmitOutput`
/// degrade to a `tracing::warn!` and `Continue`; every other error path
/// bubbles via `?` so the loop terminates with a real failure rather than
/// silently swallowing it.
async fn dispatch(
    fs: &AgentFs,
    tools: &ToolRegistry,
    scheduler: &mut Scheduler,
    decision: Decision,
) -> Result<TickOutcome> {
    match decision {
        Decision::CallTool { name, args, .. } => {
            debug!(tool = %name, "decision: call_tool");
            let ev = tools.call(&name, args).await?;
            fs.record_evidence(ev)?;
            Ok(TickOutcome::Continue)
        }
        Decision::EmitOutput { content, evidence } => {
            debug!(evidence_count = evidence.len(), "decision: emit_output");
            match fs.persist_output(&content, &evidence) {
                Ok(_) => Ok(TickOutcome::Continue),
                Err(e) => match e.downcast_ref::<FsError>() {
                    Some(FsError::EmptyEvidence) | Some(FsError::EvidenceNotFound(_)) => {
                        warn!(error = %e, "provenance violation; agent staying alive");
                        Ok(TickOutcome::Continue)
                    }
                    _ => Err(e),
                },
            }
        }
        Decision::RewriteFs { ops } => {
            debug!(op_count = ops.len(), "decision: rewrite_fs");
            fs.apply_ops(ops)?;
            Ok(TickOutcome::Continue)
        }
        Decision::Idle { next_after } => {
            debug!(
                next_after_ms = next_after.as_millis() as u64,
                "decision: idle"
            );
            scheduler.set_next_after(next_after);
            Ok(TickOutcome::Continue)
        }
        Decision::Retire { reason } => {
            debug!(%reason, "decision: retire");
            fs.persist_retirement(&reason)?;
            Ok(TickOutcome::Retire(RetireReason(reason)))
        }
    }
}

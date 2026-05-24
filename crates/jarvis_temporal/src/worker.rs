//! Stage 3.2 (JAR2-58) — Jarvis worker registration helpers.
//!
//! Lives in the library so both the `worker` binary and integration
//! tests (in `tests/`) share the same registration call site. Drift
//! between "what the bin registers" and "what the test registers" would
//! be a class of bugs we shouldn't pay for.
//!
//! Stage 3.4–3.10 replaces [`NoopActivities`] with the real activity
//! set. [`build_worker`]'s body grows accordingly; its signature stays.

use anyhow::Result;
use temporalio_client::Client;
use temporalio_macros::activities;
use temporalio_sdk::{
    activities::{ActivityContext, ActivityError},
    Worker, WorkerOptions,
};
use temporalio_sdk_core::CoreRuntime;

use crate::workflow::AgentWorkflow;

/// Default task queue. Live tests override via `TEMPORAL_TASK_QUEUE`.
///
/// The `jarvis-agents` queue is the production default; we pick a stable
/// name early so deployment configs don't churn when the registration
/// surface grows.
pub const DEFAULT_TASK_QUEUE: &str = "jarvis-agents";

/// Dummy activity set. Stage 3.5–3.10 replaces this with the real one
/// (`assemble_context`, `decide_next_action`, `execute_tool`,
/// `persist_output`, `apply_fs_ops`, `persist_retirement`).
///
/// **Why it exists today.** Stage 3.2's acceptance is "worker registers
/// the workflow against a real Temporal Server"; the activity surface
/// being empty technically satisfies that, but registering one trivial
/// activity locks the `register_activities(value)` call-site against
/// SDK constraint § 3.4 of `temporal_rust_sdk_smoke.md` (macro impls on
/// the bare type — passing `Arc<NoopActivities>` is a type error).
/// Stage 3.5 onwards inherits a working call-site rather than discovering
/// the constraint mid-refactor.
pub struct NoopActivities;

#[activities]
impl NoopActivities {
    /// No-op activity. Real activities land in JAR2-61..66.
    #[activity]
    pub async fn noop(_ctx: ActivityContext, _input: ()) -> Result<(), ActivityError> {
        Ok(())
    }
}

/// Build a worker registering [`AgentWorkflow`] + [`NoopActivities`] on
/// the given task queue.
///
/// `Worker::new` returns `Box<dyn Error>` (not `Send + Sync`); we wrap
/// it via `anyhow::anyhow!("{e}")` so `?` works against `anyhow::Result`.
/// See `scratch/temporal_rust_sdk_smoke.md` § 3.5.
pub fn build_worker(runtime: &CoreRuntime, client: Client, task_queue: &str) -> Result<Worker> {
    let opts = WorkerOptions::new(task_queue)
        .register_workflow::<AgentWorkflow>()
        .register_activities(NoopActivities)
        .build();
    Worker::new(runtime, client, opts).map_err(|e| anyhow::anyhow!("Worker::new failed: {e}"))
}

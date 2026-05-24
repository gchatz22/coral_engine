//! Stage 3.2 (JAR2-58) — Jarvis worker registration helpers.
//! Stage 3.4 (JAR2-60) — replaces `NoopActivities` with [`AgentActivities`].
//!
//! Lives in the library so both the `worker` binary and integration
//! tests (in `tests/`) share the same registration call site.
//!
//! Stage 3.5–3.10 fills in real activity bodies inside
//! [`crate::activities::AgentActivities`]; the registration call here is
//! unchanged across those tickets.

use anyhow::Result;
use temporalio_client::Client;
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::CoreRuntime;

use crate::activities::AgentActivities;
use crate::workflow::AgentWorkflow;

/// Default task queue. Live tests override via `TEMPORAL_TASK_QUEUE`.
pub const DEFAULT_TASK_QUEUE: &str = "jarvis-agents";

/// Build a worker registering [`AgentWorkflow`] + [`AgentActivities`] on
/// the given task queue.
///
/// `Worker::new` returns `Box<dyn Error>` (not `Send + Sync`); we wrap
/// it via `anyhow::anyhow!("{e}")` so `?` works against `anyhow::Result`.
/// See `scratch/temporal_rust_sdk_smoke.md` § 3.5.
///
/// `register_activities` takes the bare value, not `Arc<T>` — smoke
/// § 3.4. The macro impls `ActivityImplementer for AgentActivities` and
/// `register_activities` wraps in `Arc` internally.
pub fn build_worker(runtime: &CoreRuntime, client: Client, task_queue: &str) -> Result<Worker> {
    let opts = WorkerOptions::new(task_queue)
        .register_workflow::<AgentWorkflow>()
        .register_activities(AgentActivities)
        .build();
    Worker::new(runtime, client, opts).map_err(|e| anyhow::anyhow!("Worker::new failed: {e}"))
}

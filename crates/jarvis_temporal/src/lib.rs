//! `jarvis_temporal` — Temporal-hosted agent workflow runtime.
//!
//! ## Library surface
//!
//! - [`workflow`] (JAR2-58, stage 3.2) — `AgentWorkflow` skeleton:
//!   workflow type, `AgentInput`/`AgentResult` shapes, URL-shaped
//!   workflow ID helper. The body continues-as-new once and exits;
//!   real loop body lives in JAR2-60.
//! - [`worker`] (JAR2-58) — shared registration helpers
//!   ([`worker::build_worker`], [`worker::NoopActivities`],
//!   [`worker::DEFAULT_TASK_QUEUE`]) used by both the `worker` binary
//!   and the integration tests under `tests/`.
//!
//! ## Binaries
//!
//! - `worker` (JAR2-58 onward) — Temporal worker that registers
//!   [`workflow::AgentWorkflow`] + a noop activity set. Run against a
//!   Temporal Server (`temporal server start-dev`).
//! - `temporal-smoke` (JAR2-41, stage 0.2) — primitive smoke binary
//!   exercised against the live SDK. Retained as a working reference
//!   until stage 3 is fully built out; deletes itself per
//!   `scratch/temporal_staged_plan.md` § 5 stage 0 when the real
//!   workflow surface is complete.

pub mod worker;
pub mod workflow;

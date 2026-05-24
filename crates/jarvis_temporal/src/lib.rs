//! `jarvis_temporal` — Temporal-hosted agent workflow runtime.
//!
//! ## Library surface
//!
//! - [`workflow`] (JAR2-58, JAR2-59, JAR2-60) — `AgentWorkflow`: the
//!   workflow type, signal/update surface, and the per-tick loop body
//!   that orchestrates the six activities below.
//! - [`activities`] (JAR2-60) — `AgentActivities`: the six activity
//!   stubs (`assemble_context`, `decide_next_action`, `execute_tool`,
//!   `persist_output`, `apply_fs_ops`, `persist_retirement`) the
//!   workflow body invokes. Bodies are canned `Ok(...)` placeholders;
//!   real bodies land in JAR2-61..66. Tests can script
//!   `decide_next_action` via [`activities::set_decision_script`].
//! - [`worker`] (JAR2-58, JAR2-60) — shared registration helpers
//!   ([`worker::build_worker`], [`worker::DEFAULT_TASK_QUEUE`]) used by
//!   both the `worker` binary and the integration tests under `tests/`.
//!
//! ## Binaries
//!
//! - `worker` (JAR2-58 onward) — Temporal worker that registers
//!   [`workflow::AgentWorkflow`] + [`activities::AgentActivities`]. Run
//!   against a Temporal Server (`temporal server start-dev`).
//! - `temporal-smoke` (JAR2-41, stage 0.2) — primitive smoke binary
//!   exercised against the live SDK. Retained as a working reference
//!   until stage 3 is fully built out; deletes itself per
//!   `scratch/temporal_staged_plan.md` § 5 stage 0 when the real
//!   workflow surface is complete.

pub mod activities;
pub mod worker;
pub mod workflow;

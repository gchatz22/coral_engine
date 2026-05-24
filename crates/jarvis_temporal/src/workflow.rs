//! Stage 3.2 (JAR2-58) — `AgentWorkflow` skeleton.
//!
//! This module defines the workflow type and input/output shapes that the
//! rest of stage 3 builds on. It deliberately ships **without** a real
//! body: the `run` method continues-as-new once and then exits cleanly on
//! the second run. The real loop body (drain triggers → assemble context →
//! decide → dispatch) lands in JAR2-60; signal handlers in JAR2-59;
//! activities in JAR2-61..66; real `Carryover` semantics in JAR2-67.
//!
//! ## What this ticket proves
//!
//! - The workflow registers and is reachable via the URL-shaped ID scheme
//!   `graphs/<graph_id>/agents/<agent_id>` (see [`agent_workflow_id`]).
//! - The `Worker::new` → register-workflow → register-activities pipeline
//!   wires up cleanly against the live SDK pinned in
//!   `crates/jarvis_temporal/Cargo.toml`.
//! - `ctx.continue_as_new(...)` fires from inside `AgentWorkflow::run` and
//!   the SDK reschedules a fresh run that reaches a clean `Ok` return.
//!
//! ## Placeholder types
//!
//! `AgentConfig`, `FsHandle`, `ParentRef`, and `Carryover` exist only so
//! `AgentInput` compiles with the field shape stage 3 needs. They are
//! intentionally opaque (`{}` structs) right now. Real shapes:
//!
//! - `AgentConfig` — JAR2-60+, sources from `jarvis_node` (`Mandate`, tool
//!   refs, model routing resolved at start).
//! - `FsHandle` — built from `<graph_id>/<agent_id>` per stage 2.5's
//!   storage trait (`scratch/agent_storage.md`).
//! - `ParentRef` — JAR2 stage 5 territory (parent-child topology). Present
//!   here only so `AgentInput.parent_handle: Option<ParentRef>` doesn't
//!   need a schema migration when stage 5 fills it in.
//! - `Carryover` — JAR2-67. Today it's an empty marker: presence on
//!   `AgentInput.carryover` is the only signal the workflow body reads
//!   (`None` = first run, `Some(_)` = post-continue-as-new). Real fields
//!   (trigger queue snapshot, scheduler cursor, child handles, last
//!   output id, mid-tick evidence) land in JAR2-67.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ContinueAsNewOptions, WorkflowContext, WorkflowResult};

/// Resolved agent configuration handed to the workflow at start.
///
/// **Placeholder.** Stage 3.2 only needs the type to exist; real fields
/// land in JAR2-60 (mandate, tool refs, model routing). The shape will be
/// driven by what `AgentCore` needs to make a `Decision`, sourced from the
/// structural DB + per-agent FS overrides per stage 1's
/// three-layer-resolution decision (`scratch/temporal_staged_plan.md`
/// § 8 decision 4).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConfig {}

/// Storage handle scoping the agent to its `<graph_id>/<agent_id>` prefix.
///
/// **Placeholder.** Stage 2.5 (`scratch/agent_storage.md`) ships the
/// `AgentStorage` trait + `AgentFs` facade with the prefix baked in;
/// stage 3 will plumb the `Arc<dyn AgentStorage>` + prefix through the
/// workflow input. Today the workflow body never reads it, so the shape
/// is empty.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsHandle {
    /// `<graph_id>/<agent_id>` — the prefix the storage trait scopes to.
    /// Populated for documentation and the live test only; the workflow
    /// body does not consume it yet.
    pub prefix: String,
}

/// Parent workflow reference for cross-workflow signal routing.
///
/// **Placeholder.** Stage 5 territory (parent → child topology + child →
/// parent signal path per `scratch/temporal_staged_plan.md` § 5 stage 5).
/// Field exists on `AgentInput` now so the input schema doesn't churn
/// when stage 5 fills it in.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentRef {}

/// Typed continue-as-new carryover.
///
/// **Placeholder.** JAR2-67 ships the real shape (trigger queue,
/// scheduler cursor, child handles, last output id, mid-tick evidence).
/// For stage 3.2 the only thing the workflow body cares about is
/// presence: `AgentInput.carryover.is_some()` means "this is a
/// continue-as-new run, exit cleanly"; `None` means "first run,
/// continue-as-new once to prove the wiring". That's structural, not
/// semantic — no real carryover state crosses the boundary.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Carryover {}

/// Input handed to `AgentWorkflow::run` at start (and at every
/// continue-as-new).
///
/// The four-field shape matches the stage 3.2 ticket exactly so JAR2-59
/// (signals), JAR2-60 (loop body), and JAR2-67 (real carryover) can fill
/// each field in without renaming.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentInput {
    /// Resolved configuration (mandate, tool refs, model routing).
    /// Resolved once at workflow start; in-flight changes arrive via
    /// `mandate_update` signals (JAR2-59).
    pub cfg: AgentConfig,
    /// `<graph_id>/<agent_id>` storage prefix for the per-agent FS.
    pub fs_handle: FsHandle,
    /// Parent reference for child-of-parent agents. `None` for root
    /// agents (stage 3 default).
    pub parent_handle: Option<ParentRef>,
    /// Continue-as-new carryover. `None` on the first run, `Some(_)`
    /// after a continue-as-new. Real shape lands in JAR2-67.
    pub carryover: Option<Carryover>,
}

/// Result returned by `AgentWorkflow::run` when the workflow exits cleanly
/// (retirement path — JAR2-60+). Today the empty body returns
/// `AgentResult::default()` after one continue-as-new round-trip.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentResult {}

/// Build the workflow ID for an agent.
///
/// URL-shaped scheme per `scratch/temporal_staged_plan.md` § 8 decision 2:
/// **`graphs/<graph_id>/agents/<agent_id>`** — mirrors the eventual HTTP
/// API (`GET /api/v1/graphs/<id>/agents/<id>/...`), stays flat within a
/// graph (parent-child topology lives in the structural DB, not in the
/// ID), and doesn't carry a leading slash (Temporal IDs don't).
pub fn agent_workflow_id(graph_id: &str, agent_id: &str) -> String {
    format!("graphs/{graph_id}/agents/{agent_id}")
}

/// The agent workflow.
///
/// Stage 3.2 body: if `input.carryover.is_none()`, continue-as-new with
/// `carryover = Some(Carryover::default())`; otherwise exit cleanly. This
/// proves the continue-as-new wiring with one round-trip and no real
/// agent logic. The real loop body lands in JAR2-60.
///
/// `#[derive(Default)]` is required by the SDK's `#[workflow]` macro —
/// see the smoke binary's `AgentLoopWorkflow` line 142 for the precedent.
#[workflow]
#[derive(Default)]
pub struct AgentWorkflow;

#[workflow_methods]
impl AgentWorkflow {
    /// Workflow entry point.
    ///
    /// Empty body: on the first run (`input.carryover.is_none()`), call
    /// `ctx.continue_as_new(...)` with a fresh `AgentInput` that carries
    /// `Some(Carryover::default())`. On the continue-as-new run, exit
    /// cleanly with `AgentResult::default()`. Real loop body lands in
    /// JAR2-60.
    ///
    /// **Deterministic.** No clocks, no random, no I/O — this body is
    /// pure on `input` per the SDK replay-determinism rule
    /// (`scratch/temporal_rust_sdk_smoke.md` § 2.5).
    ///
    /// **Tiny warm-up timer.** A 1ms `ctx.timer` runs before the
    /// continue-as-new decision so the workflow has at least one
    /// deterministic suspension point. The smoke binary's
    /// `ContinueAsNewSmokeWorkflow` (line 291 of `temporal_smoke.rs`)
    /// uses the same shape; copying it avoids a class of "empty workflow
    /// body" surprises the SDK has had in the past.
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: AgentInput,
    ) -> WorkflowResult<AgentResult> {
        // Single deterministic suspension point. See the smoke binary's
        // `ContinueAsNewSmokeWorkflow::run` for the precedent.
        ctx.timer(Duration::from_millis(1)).await;

        if input.carryover.is_none() {
            let next = AgentInput {
                carryover: Some(Carryover::default()),
                ..input
            };
            // `continue_as_new` returns `Err(WorkflowTermination::...)`;
            // the SDK terminates this run and schedules a fresh one with
            // the new input. The line below the call is unreachable on
            // the continue-as-new path. Smoke precedent:
            // `ContinueAsNewSmokeWorkflow::run` line 306 of
            // `temporal_smoke.rs`.
            ctx.continue_as_new(&next, ContinueAsNewOptions::default())?;
            unreachable!("continue_as_new should have terminated this run");
        }

        Ok(AgentResult::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_workflow_id_is_url_shaped() {
        // URL-shaped scheme per `temporal_staged_plan.md` § 8 decision 2.
        assert_eq!(
            agent_workflow_id("g1", "a1"),
            "graphs/g1/agents/a1",
            "workflow ID must be `graphs/<graph_id>/agents/<agent_id>` (no leading slash, no trailing slash)"
        );
    }

    #[test]
    fn agent_workflow_id_passes_components_through_unchanged() {
        // Today we don't escape or normalize. Capture today's behaviour
        // so a future move to validation is a deliberate, reviewable
        // change.
        assert_eq!(
            agent_workflow_id("graph-with-dashes", "agent_with_underscores"),
            "graphs/graph-with-dashes/agents/agent_with_underscores"
        );
        // Empty components survive — the structural DB layer will reject
        // bad IDs before we get here, so this layer stays mechanical.
        assert_eq!(agent_workflow_id("", ""), "graphs//agents/");
    }

    #[test]
    fn agent_input_default_has_no_carryover() {
        // The "first run, no continue-as-new yet" sentinel is
        // `carryover.is_none()`. Lock the default so JAR2-67 doesn't
        // accidentally flip it.
        let input = AgentInput::default();
        assert!(input.carryover.is_none());
        assert!(input.parent_handle.is_none());
    }

    #[test]
    fn agent_input_roundtrips_through_json() {
        // Temporal's payload codec round-trips workflow inputs through
        // serde. A JSON round-trip is a cheap, hermetic proxy that
        // catches `Serialize`/`Deserialize` derives missing on any of
        // the placeholder types.
        let input = AgentInput {
            cfg: AgentConfig::default(),
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            parent_handle: None,
            carryover: Some(Carryover::default()),
        };
        let json = serde_json::to_string(&input).expect("serialize AgentInput");
        let back: AgentInput = serde_json::from_str(&json).expect("deserialize AgentInput");
        assert_eq!(input, back);
    }

    #[test]
    fn agent_result_default_roundtrips_through_json() {
        let r = AgentResult::default();
        let json = serde_json::to_string(&r).expect("serialize AgentResult");
        let back: AgentResult = serde_json::from_str(&json).expect("deserialize AgentResult");
        assert_eq!(r, back);
    }
}

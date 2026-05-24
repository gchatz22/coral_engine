//! Stage 3.4 (JAR2-60) — activity surface for `AgentWorkflow`.
//!
//! Six activities, one for each branch the workflow loop body wants to
//! durably checkpoint:
//!
//! | activity              | invoked when                                             | real body lands in |
//! | --------------------- | -------------------------------------------------------- | ------------------ |
//! | `assemble_context`    | top of every tick (after drain)                          | JAR2-61            |
//! | `decide_next_action`  | after `assemble_context` returns a bundle                | JAR2-62            |
//! | `execute_tool`        | once per `ToolCall` in a `Decision::CallTools`           | JAR2-63            |
//! | `persist_output`      | `Decision::EmitOutput`                                   | JAR2-64            |
//! | `apply_fs_ops`        | `Decision::RewriteFs`                                    | JAR2-65            |
//! | `persist_retirement`  | `Decision::Retire` *or* the `retire` signal short-circuit | JAR2-66           |
//!
//! Every body here is a stub returning a canned `Ok(...)` so the workflow
//! loop runs end-to-end against `MockDecide`-style scripted decisions. The
//! input/output types are real — JAR2-61..66 subagents replace bodies
//! without touching the wire shape.
//!
//! As of JAR2-64, `persist_output` carries its real body: opens an
//! [`jarvis_node::fs::AgentFs`] over the process-wide [`AgentStorage`]
//! backend and delegates to `AgentFs::persist_output`. The body extracts
//! into the free helper [`persist_output_impl`] so hermetic tests can
//! exercise the FS-touching logic without an `ActivityContext` or the
//! `OnceLock` install path.
//!
//! ## Test injection
//!
//! `decide_next_action` consults a static `OnceLock<Mutex<VecDeque<Decision>>>`
//! before reaching for the installed [`Decide`] implementation. Tests
//! call [`set_decision_script`] before starting the workflow; the
//! activity pops from the script in order. This is the workflow-side
//! analogue of `agent_core`'s `MockDecide` — same scripted behaviour,
//! but reachable from inside an activity body (which must be a free
//! function over a value-typed registered instance per SDK constraint
//! § 3.4 of `temporal_rust_sdk_smoke.md`). When the script is empty
//! the activity falls through to `worker::decide_impl()` (JAR2-62) and
//! calls the installed `Decide::decide` once. Tests that don't install
//! a real `Decide` must script every decision the workflow body will
//! ask for — see `tests/workflow_loop.rs` for the canonical example.
//!
//! ## SDK constraints baked in
//!
//! - Each activity is a free `async fn` (not `&self`-receiver) per the
//!   `#[activities]` macro shape (see `bin/temporal_smoke.rs::SmokeActivities`
//!   line 76 and `examples/cancellation/workflows.rs::CancellationActivities`).
//! - First parameter is `ActivityContext`; second is the typed input.
//!   Return type is `Result<R, ActivityError>`. The `&self` form in the
//!   ticket sketch does not match the macro.
//! - `register_activities` takes the bare value, not `Arc<T>` (smoke § 3.4).
//!   `AgentActivities` is a unit struct; the macro impls
//!   `ActivityImplementer` for the bare type.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use jarvis_node::agent_core;
use jarvis_node::decision::{ContextBundle, CorrectionContext, Decide, Decision, FsOp, ToolCall};
use jarvis_node::evidence::EvidenceId;
use jarvis_node::fs::AgentFs;
use jarvis_node::mandate::{Mandate, OutputId};
use jarvis_node::model_client::ModelError;
use jarvis_node::storage::AgentStorage;
use jarvis_node::trigger::{HumanOp, MandatePatch, Trigger};
use serde::{Deserialize, Serialize};
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use temporalio_sdk::ApplicationFailure;

use crate::worker::agent_storage;
use crate::workflow::{AgentConfig, FsHandle};

// ---------------------------------------------------------------------------
// Input / output types
//
// Real fields chosen against the JAR2-61..66 target shapes
// (`scratch/temporal_staged_plan.md` § 5 stages 3.5–3.10). Stubs ignore the
// inputs and return canned outputs; the real bodies will plumb FS reads /
// LLM calls / tool dispatch / FS writes through these payloads.

/// Input to [`AgentActivities::assemble_context`]. Carries the per-tick
/// drained signal buckets (`triggers`, `human_ops`, `mandate_patches`) plus
/// the resolved [`Mandate`] + FS handle + prior-tick correction so the
/// activity can call into [`jarvis_node::agent_core::drain_triggers`].
///
/// JAR2-61 promoted the prior `cfg: AgentConfig` placeholder to a real
/// `mandate: Mandate` — `drain_triggers` requires a concrete `&Mandate`
/// to seed the `ContextBundle` and to write `mandate.json` on first FS
/// open. The other activity inputs (`ExecuteToolInput`, `PersistOutputInput`)
/// still carry the `AgentConfig` placeholder; siblings JAR2-62..66 will
/// promote each as their real bodies need it. No `Default` derive — the
/// real `Mandate` has no `Default` and the placeholder construction lives
/// at the workflow-body call site.
///
/// `mandate_patches` are surfaced here so JAR2-61 can apply them to the
/// per-agent FS before assembling the bundle (the workflow body itself
/// must not touch FS — see `scratch/temporal_staged_plan.md` § 2.5
/// "Drain triggers (typed, ordered)" and the JAR2-60 ticket's notes on
/// the drain/assemble merge in `agent_core`). Today the activity logs the
/// patch count and drops them on the floor; stage 6 wires the consumption.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssembleContextInput {
    pub mandate: Mandate,
    pub fs_handle: FsHandle,
    pub triggers: Vec<Trigger>,
    /// Human overrides drained alongside `triggers`. JAR2-61 folds these
    /// into the `Trigger::HumanOverride` taxonomy before calling
    /// `drain_triggers`, appending them after the regular triggers so the
    /// ordering matches the in-process loop (which sees the same signal
    /// stream serialized through one mpsc receiver).
    pub human_ops: Vec<HumanOp>,
    /// Mandate patches drained from the workflow's `pending_mandate_patches`
    /// bucket. Stage 6 owns the consumption (apply patch → write FS →
    /// re-resolve routing); the activity just records the count today.
    pub mandate_patches: Vec<MandatePatch>,
    /// Correction context staged by the previous tick — `Some` when the
    /// previous `DispatchOutcome` was `NeedsCorrection` or `ToolError`.
    /// `None` on the first tick of a run.
    pub prior_correction: Option<CorrectionContext>,
}

/// Output of [`AgentActivities::assemble_context`]. Real body returns the
/// fully-populated [`ContextBundle`] from `agent_core::drain_triggers`;
/// stub returns an empty bundle with a placeholder mandate so the
/// downstream `decide_next_action` activity has something to serialize.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssembleContextOutput {
    pub bundle: ContextBundle,
}

/// Input to [`AgentActivities::decide_next_action`]. Real body wraps
/// `LlmDecide::decide(bundle)`; stub consults the test script and falls
/// back to a canned `Idle`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DecideInput {
    pub bundle: ContextBundle,
}

/// Input to [`AgentActivities::execute_tool`]. One activity invocation per
/// `ToolCall` — the workflow body fans out via `workflows::join_all` so a
/// partial parallel batch survives a worker crash (only in-flight calls
/// re-execute on retry; completed ones already wrote their outcome to
/// workflow history). See `scratch/temporal_staged_plan.md` § 2.5 +
/// JAR2-60 ticket § "SDK constraints baked in" item 2.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecuteToolInput {
    pub cfg: AgentConfig,
    pub fs_handle: FsHandle,
    pub call: ToolCall,
}

/// Result of a single `execute_tool` activity invocation. Mirrors the
/// shape `agent_core::dispatch_call_tools` already produces — successful
/// calls carry an `EvidenceId`; failed calls carry a structured
/// [`ToolCallFailure`] the workflow can fold into next-tick correction
/// context.
///
/// **Why this mirrors `agent_core::ToolFailure` but isn't it.**
/// `agent_core::ToolFailure` doesn't derive `Serialize`/`Deserialize` — and
/// it must not, in this ticket: that crate is out of scope per JAR2-60
/// guardrail 1. We carry the same three fields here so JAR2-63 can
/// translate one to the other when wiring the real body.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ToolCallOutcome {
    Success { evidence_id: EvidenceId },
    Failure { failure: ToolCallFailure },
}

/// Mirror of `jarvis_node::agent_core::ToolFailure` with serde derives so
/// the value crosses the workflow ↔ activity boundary via Temporal's
/// payload codec. JAR2-63's real `execute_tool` body converts the
/// `agent_core::ToolFailure` from `dispatch_call_tools` into this shape.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolCallFailure {
    pub tool: String,
    pub args: serde_json::Value,
    pub error: String,
}

/// Input to [`AgentActivities::persist_output`]. Real body calls
/// `AgentFs::persist_output` and returns the minted `OutputId`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistOutputInput {
    pub cfg: AgentConfig,
    pub fs_handle: FsHandle,
    pub content: String,
    pub evidence: Vec<EvidenceId>,
}

/// Input to [`AgentActivities::apply_fs_ops`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplyFsOpsInput {
    pub fs_handle: FsHandle,
    pub ops: Vec<FsOp>,
}

/// Input to [`AgentActivities::persist_retirement`]. Carries the reason so
/// retirement is auditable on disk.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistRetirementInput {
    pub fs_handle: FsHandle,
    pub reason: String,
}

// ---------------------------------------------------------------------------
// Test-injectable decision script
//
// Lives outside the impl block because activity bodies are free functions
// over a value-typed registered instance (smoke § 3.4) — external
// observation/control of the registered `AgentActivities` value isn't
// available, so a process-wide static is the SDK-blessed workaround. The
// scripted decide_next_action activity consults this before returning the
// fallback `Decision::Idle`.

static DECISION_SCRIPT: OnceLock<Mutex<VecDeque<Decision>>> = OnceLock::new();

fn script_handle() -> &'static Mutex<VecDeque<Decision>> {
    DECISION_SCRIPT.get_or_init(|| Mutex::new(VecDeque::new()))
}

/// Install a script of decisions the [`AgentActivities::decide_next_action`]
/// stub returns, in order. Tests call this *before* starting the workflow.
///
/// When the script is empty, the activity falls back to its canned
/// `Decision::Idle { next_after: 1s }` so a misconfigured test doesn't
/// hang. To reset between tests, pass an empty `Vec`.
pub fn set_decision_script(script: Vec<Decision>) {
    let mut q = script_handle()
        .lock()
        .expect("DECISION_SCRIPT mutex poisoned");
    *q = script.into();
}

/// Pop the next scripted decision, or `None` if the script is empty.
fn pop_scripted_decision() -> Option<Decision> {
    script_handle()
        .lock()
        .expect("DECISION_SCRIPT mutex poisoned")
        .pop_front()
}

// ---------------------------------------------------------------------------
// Activity body helpers
//
// Free functions extracted from the activity bodies so hermetic tests can
// exercise the FS-touching logic without constructing an `ActivityContext`
// (which has no `Default` impl and a non-trivial Core-tied constructor) or
// installing the process-wide `OnceLock<AgentStorage>` (which would race
// the `worker::install_then_access_*` test that already installs it in
// the lib test binary). The activity body is a 3-line wrapper around
// these helpers; the helpers carry the real shape.

/// Stage 3.8 helper — open an `AgentFs` over `storage` at `prefix` and
/// persist `content` as an output whose provenance trail is `evidence`.
/// Returns the minted `OutputId` (a fresh ULID — see the
/// `persist_output` doc comment for the idempotency caveat).
///
/// `AgentFs::persist_output` rejects:
/// - Empty `evidence` (`FsError::EmptyEvidence`).
/// - Any cited id whose `evidence/<id>.json` is absent
///   (`FsError::EvidenceNotFound`).
///
/// Both errors propagate via `?` through `anyhow::Error` →
/// `ActivityError::Application`. The workflow body's next-tick
/// correction-context staging (JAR2-60 `dispatch_call_tools`) is the
/// agent-loop's mechanism for surfacing these failures to the LLM; the
/// activity itself just reports.
pub(crate) async fn persist_output_impl(
    storage: Arc<dyn AgentStorage>,
    prefix: &str,
    content: &str,
    evidence: &[EvidenceId],
) -> anyhow::Result<OutputId> {
    // Placeholder mandate matches `assemble_context`'s stub — `AgentFs`
    // only writes `mandate.json` when absent, so the real mandate
    // persisted by JAR2-61's `assemble_context` (or a prior boot of
    // this same agent) is not clobbered when this activity opens the
    // FS to persist an output.
    let mandate = Mandate::new("", Duration::ZERO, None);
    let fs = AgentFs::new_with_storage(storage, prefix, &mandate).await?;
    let output = fs.persist_output(content, evidence).await?;
    Ok(output.id)
}

// ---------------------------------------------------------------------------
// Activity impl
//
// `AgentActivities` is the new value-typed activity bundle replacing
// JAR2-58's `NoopActivities`. The macro impls
// `ActivityImplementer for AgentActivities`; `register_activities` wraps
// in `Arc` internally (smoke § 3.4 — passing `Arc<AgentActivities>` is a
// type error).
//
// Every body is a stub returning canned `Ok(...)`. The real bodies land
// in JAR2-61..66; each one will:
//
// - `assemble_context` (JAR2-61): open the per-agent `AgentFs` from
//   `fs_handle`, apply any drained `mandate_patches`, fold `human_ops`
//   into the `Trigger` stream, call `agent_core::drain_triggers`.
// - `decide_next_action` (JAR2-62): construct an `LlmDecide` from `cfg`
//   (model routing, system prompt), call `.decide(bundle)`.
// - `execute_tool` (JAR2-63): resolve `cfg.tools` against the registry,
//   dispatch one `ToolCall`, record_evidence on success.
// - `persist_output` (JAR2-64): re-open `AgentFs`, call `persist_output`.
// - `apply_fs_ops` (JAR2-65): re-open `AgentFs`, call `apply_ops`.
// - `persist_retirement` (JAR2-66): re-open `AgentFs`, call `persist_retirement`.

/// Activity bundle registered on the worker. Replaces JAR2-58's
/// `NoopActivities` — the bare value passes through `register_activities`
/// unchanged (smoke § 3.4).
pub struct AgentActivities;

#[activities]
impl AgentActivities {
    /// Stage 3.5 (JAR2-61). Build a per-tick [`AgentFs`] over the
    /// worker-shared `AgentStorage` (JAR2-69) at the input's prefix, fold
    /// drained `human_ops` into the `Trigger::HumanOverride` taxonomy,
    /// then delegate to [`agent_core::drain_triggers`] for the
    /// FS-assemble that yields the warm `ContextBundle`.
    ///
    /// **Mandate patches.** Drained off the `mandate_update` signal
    /// queue and surfaced on the input for stage 6 — the activity logs
    /// the count and drops them today. Wiring the consumption (apply
    /// patch → re-resolve routing → re-open FS) is JAR2-67+ territory.
    ///
    /// **FS open is idempotent** — `AgentFs::new_with_storage` only
    /// writes `mandate.json` when absent, so passing the workflow's
    /// mandate through on every tick is correct. The cost is one storage
    /// `get` per tick + a one-time put on first open per agent.
    ///
    /// **`tokio` async is fine here** — activity bodies live outside
    /// workflow-replay determinism rules; the workflow itself is the
    /// piece that may only use `temporalio_sdk::workflows::*` primitives.
    #[activity]
    pub async fn assemble_context(
        _ctx: ActivityContext,
        input: AssembleContextInput,
    ) -> Result<AssembleContextOutput, ActivityError> {
        let storage = crate::worker::agent_storage();
        let fs = AgentFs::new_with_storage(storage, input.fs_handle.prefix.clone(), &input.mandate)
            .await?;

        // Fold drained `human_ops` into the trigger stream as
        // `Trigger::HumanOverride { op }`. Appended after the regular
        // triggers so ordering matches the in-process loop (which sees
        // every signal serialized through one mpsc receiver in arrival
        // order).
        let mut triggers = input.triggers;
        triggers.extend(
            input
                .human_ops
                .into_iter()
                .map(|op| Trigger::HumanOverride { op }),
        );

        if !input.mandate_patches.is_empty() {
            tracing::debug!(
                count = input.mandate_patches.len(),
                "assemble_context: dropping mandate_patches (stage 6 territory)"
            );
        }

        let bundle =
            agent_core::drain_triggers(triggers, &fs, &input.mandate, input.prior_correction)
                .await?;
        Ok(AssembleContextOutput { bundle })
    }

    /// Stage 3.6 (JAR2-62). Wraps the process-wide [`Decide`] impl
    /// installed via [`crate::worker::install_decide`] (typically an
    /// `LlmDecide` over a vendor `ModelClient`).
    ///
    /// **Script-first.** The activity consults the test-injected
    /// [`DECISION_SCRIPT`] *before* reaching for the installed
    /// implementation. This is load-bearing: the live `workflow_loop`
    /// test scripts every decision the workflow will ask for, and a
    /// real LLM call would defeat both the test's determinism and the
    /// CI envelope (no API keys, no network). The static-script
    /// injection path predates JAR2-62 and must keep working — see
    /// `tests/workflow_loop.rs` for the call site.
    ///
    /// **Error classification.** When the installed `Decide`
    /// implementation returns an `anyhow::Error`, the activity
    /// classifies it by downcasting to `&ModelError` (the typed error
    /// the `LlmDecide` adapter passes through from
    /// `ModelClient::complete`):
    ///
    /// - `ModelError::Transport` / `ModelError::RateLimit` →
    ///   **retryable**. The Temporal worker will reschedule the
    ///   activity per the workflow-side `ActivityOptions::retry_policy`
    ///   (default Temporal policy today; per-activity tuning is a
    ///   follow-up — see PR summary).
    /// - `ModelError::Auth` / `ModelError::Parse` /
    ///   `ModelError::Other` → **non-retryable**. Bad credentials,
    ///   malformed responses, and vendor-specific 4xxs don't get
    ///   better by retrying.
    /// - Downcast fails (e.g. `LlmDecide`'s "parse failed on all N
    ///   attempts" `anyhow!` after exhausting the inner correction
    ///   loop) → **non-retryable**. Validation failures bubble as
    ///   activity-layer failures so the workflow body can stage a
    ///   correction context on the next tick rather than retrying the
    ///   same broken decision in place (guardrail 3 of the ticket).
    ///
    /// **Heartbeats** are deliberately omitted in this revision. The
    /// activity timeout is 30s (`workflow::ACTIVITY_TIMEOUT`), which
    /// comfortably brackets a normal LLM call (sub-10s for short
    /// prompts); a long-running streaming variant would need
    /// heartbeats, but the batch-shape `ModelClient` doesn't.
    #[activity]
    pub async fn decide_next_action(
        _ctx: ActivityContext,
        input: DecideInput,
    ) -> Result<Decision, ActivityError> {
        // Script-first (guardrail 5). If a scripted decision is
        // queued, return it without touching the installed `Decide`.
        if let Some(d) = pop_scripted_decision() {
            return Ok(d);
        }

        let decide = crate::worker::decide_impl();
        decide_with(decide.as_ref(), input)
            .await
            .map_err(classify_decide_error)
    }

    /// Stage 3.7 (JAR2-63). Real body: dispatches one `ToolCall` through
    /// the process-wide [`ToolRegistry`] (installed at worker boot via
    /// [`crate::worker::install_tool_registry`]) and, on success,
    /// persists the resulting `EvidenceRecord` via the per-agent
    /// `AgentFs` facade backed by the installed
    /// [`crate::worker::agent_storage`].
    ///
    /// One activity invocation per `ToolCall`; the workflow body fans
    /// out N calls via `workflows::join_all` and stages a
    /// `CorrectionContext` for next tick when any of them surface as
    /// `Failure`. See [`crate::workflow::dispatch_call_tools`].
    ///
    /// **Retry layering.** Tool calls themselves are dispatched
    /// single-shot from this activity — `McpTool` (the production
    /// `ToolRegistry` entry built by `register_mcp_server_with_policy`)
    /// already runs its own `RetryPolicy` loop inside `Tool::call`
    /// (`crates/jarvis_node/src/mcp/tool.rs` `call_with_retry`). Adding
    /// a second retry loop here would compound those retries
    /// multiplicatively. The per-call surface this activity returns —
    /// `Success { evidence_id }` or `Failure { failure }` — already
    /// matches the in-process `agent_core::dispatch_call_tools`
    /// post-retry shape. The outer Temporal retry on activity errors
    /// (heartbeat timeout, worker crash) stays safe because evidence
    /// is content-addressed: a retried activity invocation with the
    /// same `(tool, args, result)` triple resolves to the same
    /// `EvidenceId` and `AgentFs::record_evidence` is idempotent via
    /// `put_if_absent` (`crates/jarvis_node/src/fs.rs`).
    ///
    /// **Tool error → Failure (not ActivityError).** A tool that
    /// errors after its own retry exhaustion does **not** surface as
    /// `ActivityError` (which would trip Temporal's outer retry —
    /// pointless work, given the inner retry already gave up). It
    /// returns `Ok(ToolCallOutcome::Failure { failure })` so the
    /// workflow body folds it into a `CorrectionContext` and the next
    /// tick's LLM sees the failure. This mirrors the in-process
    /// `DispatchOutcome::ToolError` semantics from
    /// `agent_core::dispatch_call_tools` (`scratch/temporal_staged_plan.md`
    /// § 2.5; JAR2-38). A tool name unknown to the registry takes the
    /// same path; the in-process loop returned `NeedsCorrection` for
    /// that case via a batch-wide pre-check, but at the per-call
    /// granularity the unknown-name failure is observationally
    /// identical to any other call-time error from the LLM's
    /// perspective.
    ///
    /// **Mandate placeholder.** `ExecuteToolInput.cfg` is the
    /// JAR2-60-era `AgentConfig {}` empty struct; promotion to
    /// `Mandate` lands later in the stack. `AgentFs::new_with_storage`
    /// still wants a `&Mandate` to seed an `mandate.json` write on
    /// first open — but that write is idempotent (read-then-PUT-only-
    /// if-absent), so a placeholder mandate here cannot corrupt
    /// whatever the agent's mandate-bearing path (e.g. JAR2-61's
    /// `assemble_context`) wrote first. Matches the same trick the
    /// `assemble_context` stub uses to construct its placeholder
    /// `ContextBundle.mandate`.
    ///
    /// **Heartbeats deferred.** `ActivityContext::record_heartbeat`
    /// exists on the pinned SDK (verified against
    /// `temporalio-sdk-0.4.0/src/activities.rs:170`), but with
    /// today's bootstrap tool surface — `EchoTool` (microseconds) and
    /// `McpTool` (sub-second under the default retry policy of
    /// 3×50ms) — neither approaches the workflow-set 30s
    /// start-to-close timeout. Add a heartbeat loop here when a
    /// tool's expected duration approaches or exceeds the timeout
    /// (e.g. JAR2-68's MCP-server wiring with a long-running fetch).
    #[activity]
    pub async fn execute_tool(
        _ctx: ActivityContext,
        input: ExecuteToolInput,
    ) -> Result<ToolCallOutcome, ActivityError> {
        let registry = crate::worker::tool_registry();
        // One-shot dispatch — the tool implementation owns its retry
        // policy (see McpTool::call). Wrapping in another retry here
        // would compound them multiplicatively.
        let call_result = registry
            .call(&input.call.name, input.call.args.clone())
            .await;
        match call_result {
            Ok(record) => {
                // Persist evidence via the per-agent AgentFs facade.
                // Construction is idempotent against the prefix's
                // mandate file (read-then-PUT-only-if-absent), so the
                // placeholder mandate below cannot overwrite a real
                // mandate already on disk.
                let storage = crate::worker::agent_storage();
                let placeholder_mandate = Mandate::new("", Duration::ZERO, None);
                let fs = AgentFs::new_with_storage(
                    storage,
                    input.fs_handle.prefix.clone(),
                    &placeholder_mandate,
                )
                .await
                .map_err(|e| ActivityError::from(anyhow::anyhow!("agent_fs open failed: {e:#}")))?;
                let evidence_id = fs.record_evidence(record).await.map_err(|e| {
                    ActivityError::from(anyhow::anyhow!("record_evidence failed: {e:#}"))
                })?;
                Ok(ToolCallOutcome::Success { evidence_id })
            }
            Err(e) => Ok(ToolCallOutcome::Failure {
                failure: ToolCallFailure {
                    tool: input.call.name.clone(),
                    args: input.call.args.clone(),
                    error: format!("{e:#}"),
                },
            }),
        }
    }

    /// Stage 3.8 (JAR2-64). Real body: opens an [`AgentFs`] over the
    /// process-wide [`AgentStorage`] backend (installed by the worker
    /// boot or a test harness) at the agent's prefix, then delegates to
    /// [`AgentFs::persist_output`] — which enforces the provenance
    /// contract from JAR2-4 (every cited `EvidenceId` must resolve to a
    /// file in `evidence/`) and updates the outputs tail-index from
    /// JAR2-54.
    ///
    /// **Mandate placeholder.** `AgentFs::new_with_storage` only writes
    /// `mandate.json` if the file is absent; the placeholder here is a
    /// no-op when JAR2-61's `assemble_context` has already persisted the
    /// real mandate. Sibling JAR2-61 will swap `PersistOutputInput.cfg`
    /// to the real `Mandate` shape; until that lands, this body matches
    /// the same placeholder `assemble_context` uses today.
    ///
    /// **Idempotency caveat.** Per the JAR2-64 ticket the activity is
    /// "idempotent for free" because `OutputId` was assumed to be
    /// content-addressed — but `OutputId::new()` in `jarvis_node::mandate`
    /// is a random ULID, so a Temporal retry of a successful FS write +
    /// failed activity ack will mint a fresh id and write a second
    /// file. Out of scope for JAR2-64 (smallest correct diff: swap the
    /// body, not redesign `OutputId`); flagged as a follow-up in the PR
    /// summary.
    #[activity]
    pub async fn persist_output(
        _ctx: ActivityContext,
        input: PersistOutputInput,
    ) -> Result<OutputId, ActivityError> {
        let storage = agent_storage();
        let id = persist_output_impl(
            storage,
            &input.fs_handle.prefix,
            &input.content,
            &input.evidence,
        )
        .await?;
        Ok(id)
    }

    /// Stage 3.9 (JAR2-65). Stub is a no-op. JAR2-65 replaces the body
    /// with `AgentFs::apply_ops`.
    #[activity]
    pub async fn apply_fs_ops(
        _ctx: ActivityContext,
        _input: ApplyFsOpsInput,
    ) -> Result<(), ActivityError> {
        Ok(())
    }

    /// Stage 3.10 (JAR2-66). Real body — write `retirement.json` via
    /// [`AgentFs::persist_retirement`] using a deterministic timestamp
    /// drawn from `ctx.info().scheduled_time`.
    ///
    /// # Why `AgentFs::attach` (not `new_with_storage`)
    ///
    /// `new_with_storage` reads-or-writes `mandate.json` to confirm the
    /// per-agent FS is initialized. At the retirement-signal short-
    /// circuit (workflow.rs `Decision::Retire` arm or the `retire`
    /// signal short-circuit ahead of `assemble_context`) no `Mandate`
    /// is in scope — the workflow body never loaded one. `attach` is
    /// the strictly weaker constructor that skips the mandate write
    /// and the tail-index reconciliation. The retirement path writes
    /// exactly one key (`retirement.json`) and exits, so neither side
    /// effect is required.
    ///
    /// # Why `scheduled_time` (not `Utc::now()`)
    ///
    /// `Utc::now()` inside an activity body is wall-clock time at
    /// execution. If the activity fails and Temporal retries it, the
    /// retry attempt's `Utc::now()` differs from the first attempt's.
    /// Two attempts that both reach the `put` call would write
    /// different bytes to `retirement.json` — defeating the workflow-
    /// replay byte-identicality property the rest of the kernel
    /// promises. `ctx.info().scheduled_time` is stamped from workflow
    /// history (when the workflow *scheduled* the activity), so it is
    /// stable across retries.
    ///
    /// Fallback path: if `scheduled_time` is `None` (test harnesses
    /// that bypass the worker's activity-info plumbing, or an SDK that
    /// hasn't filled it in), we synthesize `Utc::now()` so the body
    /// still completes. This costs the replay-determinism property in
    /// that edge case; loud telemetry would make sense once
    /// observability lands (out of scope here per JAR2-66 guardrail 1).
    #[activity]
    pub async fn persist_retirement(
        ctx: ActivityContext,
        input: PersistRetirementInput,
    ) -> Result<(), ActivityError> {
        persist_retirement_inner(&input, ctx.info().scheduled_time).await?;
        Ok(())
    }
}

/// Body of [`AgentActivities::persist_retirement`], factored out so the
/// hermetic test in this file can call it without constructing an
/// `ActivityContext` (whose `pub fn new(...)` takes an `Arc<CoreWorker>`
/// — only reachable from inside a worker).
///
/// Sources the storage backend from [`agent_storage`] (the process-wide
/// `OnceLock` installed at worker boot per JAR2-69) and the timestamp
/// from `scheduled_time` — both load-bearing for the activity contract.
/// See the activity-method doc for why `scheduled_time` and not
/// `Utc::now()`.
pub async fn persist_retirement_inner(
    input: &PersistRetirementInput,
    scheduled_time: Option<std::time::SystemTime>,
) -> anyhow::Result<()> {
    let storage = agent_storage();
    let fs = AgentFs::attach(storage, &input.fs_handle.prefix);
    let retired_at: DateTime<Utc> = scheduled_time
        .map(DateTime::<Utc>::from)
        .unwrap_or_else(Utc::now);
    fs.persist_retirement(&input.reason, retired_at).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// JAR2-62 helpers — pulled out of the activity body so unit tests can call
// the inner shape directly without faking an `ActivityContext`. The split
// also keeps the `#[activity]`-decorated body short.

/// Call the supplied [`Decide`] with the activity's input. Separated
/// from [`AgentActivities::decide_next_action`] so the hermetic test in
/// this module can exercise the wiring against an arbitrary `Decide`
/// (typically a `MockDecide`) without going through the
/// `worker::decide_impl()` static — the unit test injects its own
/// dependency.
async fn decide_with(decide: &dyn Decide, input: DecideInput) -> anyhow::Result<Decision> {
    decide.decide(input.bundle).await
}

/// Map an `anyhow::Error` from `Decide::decide` to a Temporal
/// [`ActivityError`] with retryability flagged per the categorization
/// rules in [`AgentActivities::decide_next_action`].
///
/// The downcast to `&ModelError` is the contract `LlmDecide` exposes:
/// its `model_err_to_anyhow` helper wraps the typed `ModelError` via
/// `anyhow::Error::new` (see `decide_llm/llm_decide.rs::model_err_to_anyhow`),
/// so the source chain preserves the category. Non-`ModelError` causes
/// — `LlmDecide`'s "parse failed on all attempts" `anyhow!`, or any
/// other `Decide` impl's bespoke error — fall through to the
/// non-retryable default. That matches guardrail 3 of the ticket:
/// validation failures don't retry at the activity layer, they become
/// correction contexts in the next workflow tick.
fn classify_decide_error(err: anyhow::Error) -> ActivityError {
    let retryable = matches!(
        err.downcast_ref::<ModelError>(),
        Some(ModelError::Transport(_)) | Some(ModelError::RateLimit(_))
    );
    let failure = if retryable {
        ApplicationFailure::new(err)
    } else {
        ApplicationFailure::non_retryable(err)
    };
    ActivityError::application(failure)
}

// Compile-time witness that `crate::worker::decide_impl()` returns
// exactly `Arc<dyn Decide>`. Catches any future refactor that changes
// the worker-side signature out from under us — the activity body
// passes the result through `Arc::as_ref` to `decide_with`, which
// only works if the function returns an `Arc`-shaped trait object.
// The closure is never invoked; `let _ = ...` only references the
// function item, so the static analysis fires at compile time and
// nothing runs at startup. (Important: invoking `decide_impl()` here
// would panic when no `Decide` is installed.)
const _: fn() = || {
    fn assert_arc_dyn_decide() -> Arc<dyn Decide> {
        crate::worker::decide_impl()
    }
    let _ = assert_arc_dyn_decide;
};

#[cfg(test)]
mod tests {
    //! Hermetic unit coverage for the activity surface.
    //!
    //! The activity bodies are stubs; tests assert (a) the
    //! `set_decision_script` / pop pair round-trips a scripted decision,
    //! (b) the canned-fallback fires when the script is empty, and (c)
    //! every input/output type round-trips through serde (Temporal's
    //! payload codec uses serde under the hood). The live tests in
    //! `tests/workflow_loop.rs` exercise the activities through the real
    //! workflow against a Temporal Server.

    use super::*;
    use jarvis_node::decision::MockDecide;
    use serde_json::json;

    // Serializes the two tests below that mutate the process-wide
    // `DECISION_SCRIPT` static. Without this they race under cargo's
    // default parallel runner (CI hit it; locally they happened to
    // schedule far enough apart to pass).
    static SCRIPT_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Build an empty `ContextBundle` for tests that exercise the
    /// activity body. `Mandate::new("", Duration::ZERO, None)` is the
    /// cheapest valid construction (mirrors the stub fallback in
    /// `assemble_context`).
    fn empty_bundle() -> ContextBundle {
        ContextBundle {
            mandate: Mandate::new("", Duration::ZERO, None),
            triggers: Vec::new(),
            recent_outputs: Vec::new(),
            recent_evidence: Vec::new(),
            open_claims: Vec::new(),
            correction: None,
        }
    }

    #[test]
    fn decision_script_round_trips_in_order() {
        let _g = SCRIPT_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        // Reset (subsequent tests inherit process state).
        set_decision_script(vec![]);

        set_decision_script(vec![
            Decision::Idle {
                next_after: Duration::from_millis(100),
            },
            Decision::Retire {
                reason: "test".into(),
            },
        ]);
        let first = pop_scripted_decision();
        assert!(matches!(
            first,
            Some(Decision::Idle {
                next_after,
            }) if next_after == Duration::from_millis(100)
        ));
        let second = pop_scripted_decision();
        assert!(matches!(second, Some(Decision::Retire { reason }) if reason == "test"));
        // Drained — falls back to None.
        assert!(pop_scripted_decision().is_none());
    }

    #[test]
    fn decision_script_resets_between_tests() {
        let _g = SCRIPT_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        set_decision_script(vec![Decision::Retire {
            reason: "first".into(),
        }]);
        set_decision_script(vec![Decision::Idle {
            next_after: Duration::from_secs(5),
        }]);
        // Second `set_decision_script` replaces, not appends.
        let got = pop_scripted_decision();
        assert!(
            matches!(got, Some(Decision::Idle { next_after }) if next_after == Duration::from_secs(5))
        );
        assert!(pop_scripted_decision().is_none());
    }

    #[test]
    fn assemble_context_input_empty_buckets_pin_shape() {
        // JAR2-61 dropped the `Default` derive on `AssembleContextInput`
        // when promoting `cfg: AgentConfig` → `mandate: Mandate` (the
        // real `Mandate` has no `Default`). The empty-bucket invariant
        // is preserved via explicit construction so a future refactor
        // that adds a non-`Default` field has to think about the bucket
        // init the same way.
        let i = AssembleContextInput {
            mandate: Mandate::new("", Duration::ZERO, None),
            fs_handle: FsHandle::default(),
            triggers: Vec::new(),
            human_ops: Vec::new(),
            mandate_patches: Vec::new(),
            prior_correction: None,
        };
        assert!(i.triggers.is_empty());
        assert!(i.human_ops.is_empty());
        assert!(i.mandate_patches.is_empty());
        assert!(i.prior_correction.is_none());
    }

    #[test]
    fn assemble_context_input_round_trips_through_json() {
        let i = AssembleContextInput {
            mandate: Mandate::new("test", Duration::from_millis(100), Some(4)),
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            triggers: vec![Trigger::ScheduledWake],
            human_ops: vec![HumanOp::new(json!({"action": "pause"}))],
            mandate_patches: vec![MandatePatch::new(json!({"model": "x"}))],
            prior_correction: Some(CorrectionContext::new("prior failure")),
        };
        let s = serde_json::to_string(&i).unwrap();
        let back: AssembleContextInput = serde_json::from_str(&s).unwrap();
        assert_eq!(i, back);
    }

    #[test]
    fn tool_call_outcome_round_trips_through_json() {
        let id = EvidenceId::new("t", &json!({"a": 1}), &json!({"r": 1}));
        let oc = ToolCallOutcome::Success {
            evidence_id: id.clone(),
        };
        let s = serde_json::to_string(&oc).unwrap();
        assert!(s.contains("\"outcome\":\"success\""), "wire shape: {s}");
        let _back: ToolCallOutcome = serde_json::from_str(&s).unwrap();

        let f = ToolCallFailure {
            tool: "errbomb".into(),
            args: json!({"x": 1}),
            error: "boom".into(),
        };
        let oc2 = ToolCallOutcome::Failure { failure: f };
        let s2 = serde_json::to_string(&oc2).unwrap();
        assert!(s2.contains("\"outcome\":\"failure\""), "wire shape: {s2}");
        let _back2: ToolCallOutcome = serde_json::from_str(&s2).unwrap();
    }

    #[test]
    fn execute_tool_input_round_trips_through_json() {
        use jarvis_node::decision::ClaimSeed;
        let i = ExecuteToolInput {
            cfg: AgentConfig::default(),
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            call: ToolCall::new("echo", json!({"msg": "hi"}), ClaimSeed::new("s")),
        };
        let s = serde_json::to_string(&i).unwrap();
        let _back: ExecuteToolInput = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn persist_output_input_round_trips_through_json() {
        let id = EvidenceId::new("t", &json!({}), &json!({}));
        let i = PersistOutputInput {
            cfg: AgentConfig::default(),
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            content: "claim".into(),
            evidence: vec![id],
        };
        let s = serde_json::to_string(&i).unwrap();
        let _back: PersistOutputInput = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn apply_fs_ops_input_round_trips_through_json() {
        let i = ApplyFsOpsInput {
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            ops: vec![FsOp::WriteFile {
                path: "n/x.md".into(),
                content: "hi".into(),
            }],
        };
        let s = serde_json::to_string(&i).unwrap();
        let _back: ApplyFsOpsInput = serde_json::from_str(&s).unwrap();
    }

    #[test]
    fn persist_retirement_input_round_trips_through_json() {
        let i = PersistRetirementInput {
            fs_handle: FsHandle {
                prefix: "g1/a1".into(),
            },
            reason: "done".into(),
        };
        let s = serde_json::to_string(&i).unwrap();
        let _back: PersistRetirementInput = serde_json::from_str(&s).unwrap();
    }

    // ---------- JAR2-62: decide_with + classify_decide_error -------------

    /// Bespoke `Decide` impl that returns the supplied error verbatim on
    /// every `decide` call. Lets us drive the activity body's error
    /// classification path without standing up a full `LlmDecide` over
    /// a `MockModelClient` (cross-crate; lives in `decide_llm` tests).
    struct ErrDecide {
        make_err: fn() -> anyhow::Error,
    }

    #[async_trait::async_trait]
    impl Decide for ErrDecide {
        async fn decide(&self, _ctx: ContextBundle) -> anyhow::Result<Decision> {
            Err((self.make_err)())
        }
    }

    /// Happy path: `decide_with` forwards the bundle to the trait
    /// method and returns the trait's decision verbatim. Uses
    /// `MockDecide` (the in-tree scripted impl from
    /// `jarvis_node::decision`) so this test never touches a real
    /// vendor or its features.
    #[tokio::test]
    async fn decide_with_returns_trait_decision_on_success() {
        let want = Decision::Idle {
            next_after: Duration::from_millis(250),
        };
        let decide: Arc<dyn Decide> = Arc::new(MockDecide::new(vec![want.clone()]));
        let input = DecideInput {
            bundle: empty_bundle(),
        };
        let got = decide_with(decide.as_ref(), input).await.unwrap();
        assert_eq!(got, want);
    }

    /// Transport failures classify as retryable. The activity body
    /// surfaces this via `ActivityError::Application(_)` carrying an
    /// `ApplicationFailure` with `is_non_retryable() == false`, so
    /// Temporal's default retry policy will reschedule.
    #[test]
    fn classify_decide_error_transport_is_retryable() {
        let err = anyhow::Error::new(ModelError::Transport("DNS failure".into()));
        let activity_err = classify_decide_error(err);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(
            !failure.is_non_retryable(),
            "Transport errors must be retryable"
        );
    }

    /// Rate-limit failures classify as retryable. Same shape as
    /// Transport; vendor-side backoff handling lives outside the
    /// activity (and is out of scope for JAR2-62 — see PR summary
    /// about the missing per-activity retry policy).
    #[test]
    fn classify_decide_error_rate_limit_is_retryable() {
        let err = anyhow::Error::new(ModelError::RateLimit("slow down".into()));
        let activity_err = classify_decide_error(err);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(
            !failure.is_non_retryable(),
            "RateLimit errors must be retryable"
        );
    }

    /// Auth failures classify as non-retryable. Bad credentials don't
    /// fix themselves; surface to the workflow body as a terminal
    /// activity failure on the first attempt.
    #[test]
    fn classify_decide_error_auth_is_non_retryable() {
        let err = anyhow::Error::new(ModelError::Auth("ANTHROPIC_API_KEY missing".into()));
        let activity_err = classify_decide_error(err);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(
            failure.is_non_retryable(),
            "Auth errors must be non-retryable"
        );
    }

    /// `Parse` and `Other` failures classify as non-retryable. Bad
    /// response shapes and vendor-specific 4xxs don't get better by
    /// retrying; the workflow body's next-tick correction is the right
    /// place to surface them.
    #[test]
    fn classify_decide_error_parse_and_other_are_non_retryable() {
        for err in [
            anyhow::Error::new(ModelError::Parse("bad JSON".into())),
            anyhow::Error::new(ModelError::Other("4xx".into())),
        ] {
            let activity_err = classify_decide_error(err);
            let ActivityError::Application(failure) = activity_err else {
                panic!("expected ActivityError::Application");
            };
            assert!(
                failure.is_non_retryable(),
                "Parse/Other errors must be non-retryable"
            );
        }
    }

    /// Non-`ModelError` causes (e.g. `LlmDecide`'s parse-exhaustion
    /// `anyhow!(...)`) classify as non-retryable. Guardrail 3:
    /// validation failures don't retry at the activity layer; they
    /// become correction contexts on the next workflow tick.
    #[test]
    fn classify_decide_error_non_model_error_is_non_retryable() {
        let err = anyhow::anyhow!("LlmDecide: parse failed on all 2 attempt(s)");
        let activity_err = classify_decide_error(err);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(
            failure.is_non_retryable(),
            "non-ModelError causes must be non-retryable"
        );
    }

    /// End-to-end of the `decide_with` + `classify_decide_error` pair:
    /// when the trait returns a transport-flavored error, the call
    /// site lands on a retryable `ApplicationFailure`. Closes the loop
    /// with the same shape the `#[activity]` body uses.
    #[tokio::test]
    async fn decide_with_then_classify_transport_yields_retryable_failure() {
        let decide: Arc<dyn Decide> = Arc::new(ErrDecide {
            make_err: || anyhow::Error::new(ModelError::Transport("downstream 503".into())),
        });
        let input = DecideInput {
            bundle: empty_bundle(),
        };
        let raw = decide_with(decide.as_ref(), input).await.unwrap_err();
        let activity_err = classify_decide_error(raw);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(!failure.is_non_retryable());
    }

    /// End-to-end with a parse-exhaustion `anyhow!` (the canonical
    /// `LlmDecide` validation-failure shape). The activity layer must
    /// surface this as non-retryable so the workflow body's correction
    /// path takes over on the next tick.
    #[tokio::test]
    async fn decide_with_then_classify_validation_yields_non_retryable_failure() {
        let decide: Arc<dyn Decide> = Arc::new(ErrDecide {
            make_err: || anyhow::anyhow!("LlmDecide: parse failed on all 2 attempts"),
        });
        let input = DecideInput {
            bundle: empty_bundle(),
        };
        let raw = decide_with(decide.as_ref(), input).await.unwrap_err();
        let activity_err = classify_decide_error(raw);
        let ActivityError::Application(failure) = activity_err else {
            panic!("expected ActivityError::Application");
        };
        assert!(failure.is_non_retryable());
    }

    // -----------------------------------------------------------------
    // JAR2-64 — `persist_output_impl` hermetic coverage.
    //
    // The tests below exercise the activity-body logic through the
    // extracted free helper so they don't need an `ActivityContext`
    // (no `Default` impl, non-trivial Core-tied construction) or the
    // process-wide `OnceLock<AgentStorage>` install path (which
    // `worker::install_then_access_*` already touches in the same
    // test binary). Each test creates its own `MemoryStorage` and
    // exercises the storage-prefix shape `<graph_id>/<agent_id>/`.

    use chrono::Utc;
    use jarvis_node::evidence::EvidenceRecord;
    use jarvis_node::fs::FsError;
    use jarvis_node::storage::MemoryStorage;

    /// Plant an evidence record under `prefix` so a subsequent
    /// `persist_output_impl` referencing the returned id passes the
    /// provenance check. Shared between the happy-path test and the
    /// failure tests so the planting shape doesn't drift between them.
    async fn plant_evidence(
        storage: Arc<dyn jarvis_node::storage::AgentStorage>,
        prefix: &str,
        tool: &str,
        args: serde_json::Value,
        result: serde_json::Value,
    ) -> EvidenceId {
        // Open an `AgentFs` over the *same* storage Arc + prefix the
        // activity body will open against. This is the load-bearing
        // shape: a separate `MemoryStorage` instance would never share
        // evidence with the activity's view because `MemoryStorage` is
        // in-process state, not a connected backend.
        let mandate = Mandate::new("plant", Duration::from_millis(0), None);
        let fs = AgentFs::new_with_storage(storage, prefix, &mandate)
            .await
            .expect("open planting AgentFs");
        let rec = EvidenceRecord::new(tool, args, result, Utc::now());
        fs.record_evidence(rec).await.expect("plant evidence")
    }

    #[tokio::test]
    async fn persist_output_impl_writes_output_with_resolved_evidence() {
        let storage: Arc<dyn jarvis_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let prefix = "graphs/g1/agents/a1/";

        // Plant two evidence records the output will cite.
        let id_a = plant_evidence(
            storage.clone(),
            prefix,
            "tool_a",
            json!({"q": "alpha"}),
            json!({"r": 1}),
        )
        .await;
        let id_b = plant_evidence(
            storage.clone(),
            prefix,
            "tool_b",
            json!({"q": "beta"}),
            json!({"r": 2}),
        )
        .await;
        assert_ne!(id_a, id_b);

        let out_id = persist_output_impl(
            storage.clone(),
            prefix,
            "claim X",
            &[id_a.clone(), id_b.clone()],
        )
        .await
        .expect("persist_output_impl ok");

        // Inspect what landed via a fresh `AgentFs` view over the same
        // storage. `list_recent_outputs` exercises the tail-index path
        // from JAR2-54 too — proving the activity body inherits that
        // wiring for free.
        let mandate = Mandate::new("inspect", Duration::from_millis(0), None);
        let fs = AgentFs::new_with_storage(storage, prefix, &mandate)
            .await
            .unwrap();
        let outs = fs.list_recent_outputs(8).await.expect("list outputs");
        assert_eq!(outs.len(), 1, "expected exactly one output on disk");
        let on_disk = &outs[0];
        assert_eq!(
            on_disk.id, out_id,
            "OutputId returned must match on-disk file"
        );
        assert_eq!(on_disk.content, "claim X");
        assert!(
            on_disk.evidence.contains(&id_a) && on_disk.evidence.contains(&id_b),
            "output must cite both planted evidence ids"
        );
    }

    #[tokio::test]
    async fn persist_output_impl_rejects_unresolved_evidence_id() {
        let storage: Arc<dyn jarvis_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let prefix = "graphs/g1/agents/a-missing/";

        // Reference an evidence id that was never planted — the
        // `AgentFs::persist_output` provenance check fires.
        let bogus = EvidenceId::new("tool_x", &json!({}), &json!({"never": "written"}));
        let err = persist_output_impl(storage.clone(), prefix, "claim Y", &[bogus.clone()])
            .await
            .expect_err("must fail on unresolved evidence id");
        let typed = err.downcast_ref::<FsError>().expect("typed FsError");
        match typed {
            FsError::EvidenceNotFound(missing) => assert_eq!(missing, &bogus),
            other => panic!("expected EvidenceNotFound, got {other:?}"),
        }

        // No output written.
        let mandate = Mandate::new("inspect", Duration::from_millis(0), None);
        let fs = AgentFs::new_with_storage(storage, prefix, &mandate)
            .await
            .unwrap();
        let outs = fs.list_recent_outputs(8).await.unwrap();
        assert!(outs.is_empty(), "no output should have been written");
    }

    #[tokio::test]
    async fn persist_output_impl_rejects_empty_evidence_list() {
        // Provenance contract from JAR2-4: an output with no evidence
        // is rejected before the file write. The activity body
        // inherits this — Temporal sees the error, the workflow's
        // next-tick correction-context staging gets the message.
        let storage: Arc<dyn jarvis_node::storage::AgentStorage> = Arc::new(MemoryStorage::new());
        let prefix = "graphs/g1/agents/a-empty/";

        let err = persist_output_impl(storage, prefix, "claim Z", &[])
            .await
            .expect_err("must fail on empty evidence");
        let typed = err.downcast_ref::<FsError>().expect("typed FsError");
        assert!(matches!(typed, FsError::EmptyEvidence));
    }
}

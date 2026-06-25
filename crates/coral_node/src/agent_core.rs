//! Per-cycle execution primitives shared between the in-process
//! `Agent::run` loop and the workflow host: [`build_seed`] (read FS state
//! into a thin orienting [`Seed`]), [`decide`] (call a `&dyn Decide` with
//! the accumulating [`Session`]), and [`execute_step`] (run one repertoire
//! [`Decision`] against the FS + tools and return a typed [`StepOutcome`]
//! carrying the observation to feed back into the session).
//!
//! These are the *substance* of one inner-loop step; the loop skeleton
//! itself — seed once, then `decide → execute → observe` until the model
//! chooses `Idle` — lives in each host (`agent.rs` for in-process, the
//! workflow body for Temporal) because the two genuinely differ: the
//! in-process host calls these directly, while the Temporal host wraps each
//! in a journaled activity and bounds history with continue-as-new. The
//! shared pieces (session accumulation, terminal detection via
//! [`Decision::idle_after`], the [`CYCLE_RUNAWAY_FUSE`]) keep the two
//! skeletons from drifting.
//!
//! `Idle` is never executed here — it is the sole terminal step the host
//! detects and acts on (set cadence, end the cycle). Parent-child topology
//! variants execute only on the workflow host; reaching them in-process is
//! a wiring bug.
//!
//! A [`StepOutcome`] carries both the observation (always — success or
//! failure — so the next step can adapt within the same cycle) and an
//! optional [`StepFailure`] the host accounts against the per-cycle retry
//! budget: `NeedsCorrection` is one `FailureKind::Inference` slot;
//! `ToolError` is K `FailureKind::ToolCall` slots, one per failed call.

use anyhow::Result;
use futures::future::join_all;
use tracing::debug;

use crate::decision::{Decide, Decision, FsIndex, Observation, Seed, Session, ToolCall};
use crate::fs::{AgentFs, FsError};
use crate::mandate::Mandate;
use crate::tools::ToolRegistry;
use crate::trigger::Trigger;

/// Runaway fuse for one cycle's inner loop, counted in repertoire steps
/// across any continue-as-new rollovers within the same logical cycle. Set
/// far above any real task — a multi-thousand-step cycle stays well under —
/// so it never bites legitimate work; it exists only to stop a model that
/// never chooses `Idle` from looping forever. On hit the host force-idles
/// the cycle and logs loudly: a mandate that never converges should be
/// decomposed. Superseded by the per-cycle token budget when that lands.
pub const CYCLE_RUNAWAY_FUSE: usize = 50_000;

/// Number of recent output filenames to surface in the cycle seed's index.
/// Pointers only — the model reads the bodies it wants via `Read`.
const SEED_INDEX_OUTPUTS: usize = 32;

/// Cap on note filenames in the seed index. `notes/` has no recency sidecar,
/// so this is a lexicographic tail — the bound is what matters: a thin seed
/// must stay a small constant even for an agent with thousands of notes. The
/// model can always `list notes/` for the full set.
const SEED_INDEX_NOTES: usize = 32;

/// The result of executing one repertoire [`Decision`].
///
/// `observation` is always present and is what the next step reasons over —
/// on success it's the action's product (a file body, a listing, "output
/// persisted"); on a recoverable failure it's the error rendered for the
/// model to adapt to *within the same cycle*. There is no cross-cycle
/// correction state: the failure is just an observation.
///
/// `failure` is `Some` when the step failed recoverably and the host should
/// account it against the per-cycle retry budget. `None` is a clean step.
#[derive(Debug)]
pub struct StepOutcome {
    pub observation: Observation,
    pub failure: Option<StepFailure>,
}

/// A recoverable step failure the host accounts against the retry budget.
///
/// * `NeedsCorrection` — the decision parsed but the runtime cannot satisfy
///   it (an unknown tool, an unresolvable evidence id, a missing file). One
///   `FailureKind::Inference` slot.
/// * `ToolError` — the tool's internal retry policy exhausted on one or more
///   of K parallel calls. Successful sibling calls in the same batch already
///   had their evidence persisted; the host does **not** unwind on partial
///   failure. K `FailureKind::ToolCall` slots, one per failed call.
#[derive(Debug)]
pub enum StepFailure {
    NeedsCorrection(String),
    ToolError(Vec<ToolFailure>),
}

impl StepOutcome {
    /// A clean step whose observation is `content`.
    fn ok(content: impl Into<String>) -> Self {
        Self {
            observation: Observation::ok(content),
            failure: None,
        }
    }

    /// A recoverable `NeedsCorrection` failure: the same message is both the
    /// model-facing observation and the budget-accounted failure.
    fn needs_correction(msg: impl Into<String>) -> Self {
        let msg = msg.into();
        Self {
            observation: Observation::err(msg.clone()),
            failure: Some(StepFailure::NeedsCorrection(msg)),
        }
    }

    /// A tool-call batch failure: the observation is the enumerated
    /// failure text the model adapts to; the failures ride the budget.
    fn tool_error(failures: Vec<ToolFailure>) -> Self {
        let text = tool_failure_correction_text(&failures);
        Self {
            observation: Observation::err(text),
            failure: Some(StepFailure::ToolError(failures)),
        }
    }
}

/// One failed call in a `Decision::CallTools` batch. `tool` and `args`
/// echo what the model asked for so the failure observation can tell the
/// model exactly what failed; `error` is the underlying error string from
/// `ToolRegistry::call`.
///
/// `pub` because it leaks through [`StepFailure::ToolError`], which the
/// host reads when constructing the `HealthIncident` retry trail.
#[derive(Debug, Clone)]
pub struct ToolFailure {
    pub tool: String,
    pub args: serde_json::Value,
    pub error: String,
}

/// Build the thin orienting [`Seed`] for a fresh cycle: the mandate, the
/// triggers that woke the agent, and a pointers-only index of `notes/` and
/// recent `outputs/`. File *contents* are pulled on demand via the FS-nav
/// steps, so the seed stays a small constant rather than a tuned window.
///
/// In the in-process host the `Vec<Trigger>` comes from
/// `TriggerQueue::drain_ordered`; in the workflow host it comes from a
/// workflow-state buffer. Per-cycle semantics are identical.
pub async fn build_seed(fs: &AgentFs, triggers: Vec<Trigger>, cfg: &Mandate) -> Result<Seed> {
    let mut notes = fs.list_dir("notes/").await?;
    if notes.len() > SEED_INDEX_NOTES {
        let start = notes.len() - SEED_INDEX_NOTES;
        notes.drain(..start);
    }
    let outputs = fs.recent_output_filenames(SEED_INDEX_OUTPUTS).await?;
    debug!(
        notes = notes.len(),
        outputs = outputs.len(),
        triggers = triggers.len(),
        "build_seed index sizes"
    );
    Ok(Seed::new(cfg.clone(), triggers, FsIndex { notes, outputs }))
}

/// Ask the [`Decide`] impl for the next step given the accumulating
/// `session`. Single shared call site for the in-process loop and the
/// workflow host's `decide_step` activity.
pub async fn decide<D: Decide + ?Sized>(session: &Session, d: &D) -> Result<Decision> {
    d.decide(session).await
}

/// Execute one **repertoire** `Decision` against the FS + tools and return
/// a typed [`StepOutcome`] (the observation to feed back into the session,
/// plus any budget-accounted failure).
///
/// `Idle` is the terminal step and is never passed here — the host detects
/// it via [`Decision::idle_after`] and ends the cycle. Recoverable failures
/// (empty/unresolvable evidence, unknown tool, missing file, exhausted tool
/// retries) come back as a `StepOutcome` with `failure: Some(..)`;
/// everything else either succeeds or bubbles a real error via `?`.
///
/// **Side-effect semantics.** In the in-process host this is the executor —
/// it calls `fs.persist_output`, `tools.call`, `fs.read_file`, etc. The
/// workflow host wraps the same primitive in a journaled activity; the
/// shared piece is the returned `StepOutcome` shape.
pub async fn execute_step(
    fs: &AgentFs,
    tools: &ToolRegistry,
    action: &Decision,
) -> Result<StepOutcome> {
    match action {
        Decision::CallTools { calls } => execute_call_tools(fs, tools, calls).await,
        Decision::EmitOutput { content, evidence } => {
            debug!(evidence_count = evidence.len(), "step: emit_output");
            match fs.persist_output(content, evidence).await {
                Ok(_) => Ok(StepOutcome::ok("output persisted")),
                Err(e) => match e.downcast_ref::<FsError>() {
                    Some(FsError::EmptyEvidence) => Ok(StepOutcome::needs_correction(
                        "emit_output: evidence list is empty (provenance contract)",
                    )),
                    Some(FsError::EvidenceNotFound(id)) => Ok(StepOutcome::needs_correction(
                        format!("emit_output: evidence {id} not found on disk"),
                    )),
                    _ => Err(e),
                },
            }
        }
        Decision::RewriteFs { ops } => {
            debug!(op_count = ops.len(), "step: rewrite_fs");
            fs.apply_ops(ops.clone()).await?;
            Ok(StepOutcome::ok("notes updated"))
        }
        Decision::Read { path } => {
            debug!(path = path.as_str(), "step: read");
            match fs.read_file(path).await {
                Ok(body) => Ok(StepOutcome::ok(body)),
                Err(e) => match e.downcast_ref::<FsError>() {
                    Some(FsError::FileNotFound(_)) => {
                        Ok(StepOutcome::needs_correction(format!("read: {e:#}")))
                    }
                    _ => Err(e),
                },
            }
        }
        Decision::List { path } => {
            debug!(path = path.as_str(), "step: list");
            let names = fs.list_dir(path).await?;
            let body = if names.is_empty() {
                format!("(empty: {path})")
            } else {
                names.join("\n")
            };
            Ok(StepOutcome::ok(body))
        }
        Decision::Search { query, path } => {
            debug!(query = query.as_str(), "step: search");
            let hits = fs.search(query, path.as_deref()).await?;
            let body = if hits.is_empty() {
                format!("(no matches for {query:?})")
            } else {
                hits.into_iter()
                    .map(|(file, line)| format!("{file}: {line}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            Ok(StepOutcome::ok(body))
        }
        // Terminal — handled by the cycle driver, never executed here.
        Decision::Idle { .. } => {
            unreachable!("Idle is terminal; the host detects it before calling execute_step")
        }
        // Parent-child topology variants execute only on the workflow
        // host; reaching them here is a wiring bug.
        Decision::SpawnChild { .. }
        | Decision::ReconcileChildren { .. }
        | Decision::RetireChild { .. }
        | Decision::ReplaceChild { .. } => {
            unimplemented!("in-process execution for parent-child topology is not wired")
        }
    }
}

/// Execute the K calls in a `Decision::CallTools` together.
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
///    error. The per-cycle K-against-budget accounting needs every
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
///    The model can cite a partial-success evidence id on a later step;
///    rolling back would discard load-bearing observations of the world.
///    The failure observation describes only the failures so the model
///    knows what to retry.
async fn execute_call_tools(
    fs: &AgentFs,
    tools: &ToolRegistry,
    calls: &[ToolCall],
) -> Result<StepOutcome> {
    debug!(count = calls.len(), "step: call_tools");

    // Step 1: pre-check tool-name registration for every call. A single
    // unknown name takes the whole batch through the inference
    // correction path.
    let unknown: Vec<&str> = calls
        .iter()
        .filter(|c| !tools.contains(&c.name))
        .map(|c| c.name.as_str())
        .collect();
    if !unknown.is_empty() {
        return Ok(StepOutcome::needs_correction(format!(
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
    // input order, collect failures into a batch outcome. The observation
    // summarises both: what succeeded and what failed.
    let mut failures: Vec<ToolFailure> = Vec::new();
    let mut succeeded = 0usize;
    for (i, result) in results.into_iter().enumerate() {
        let call = &calls[i];
        match result {
            Ok(ev) => {
                fs.record_evidence(ev).await?;
                succeeded += 1;
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
        Ok(StepOutcome::ok(format!(
            "{succeeded} tool call(s) succeeded; evidence recorded"
        )))
    } else {
        Ok(StepOutcome::tool_error(failures))
    }
}

/// Build the human-readable failure observation appended to the session
/// after a tool-call exhaustion — the in-cycle signal the model adapts to.
///
/// Accepts a batch of failed calls. For K=1 the wording is single-tool;
/// for K>1 the message lists every failed call so the model sees what
/// each sibling did and failed with.
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

#[cfg(test)]
mod tests {
    //! Unit tests for the per-cycle execution primitives.
    //!
    //! Each runs against [`crate::storage::MemoryStorage`] (wired through
    //! [`AgentFs::new_with_storage`] so the primitive exercises the same
    //! facade the in-process loop uses — only the backend is swapped).
    //!
    //! End-to-end coverage of the cycle loop and the budget-accounting
    //! state machine that sits *on top of* these primitives lives in
    //! `crates/coral_node/tests/loop_smoke.rs` — those tests exercise the
    //! reshaped `Agent::run`.

    use super::*;
    use crate::decision::{ClaimSeed, FsOp, MockDecide};
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
    /// primitive tests stay hermetic, fast, and deterministic without
    /// touching the real filesystem.
    async fn fixture() -> (AgentFs, Mandate) {
        let m = mandate();
        let storage: Arc<dyn AgentStorage> = Arc::new(MemoryStorage::new());
        let fs = AgentFs::new_with_storage(storage, "", &m).await.unwrap();
        (fs, m)
    }

    /// Seed one evidence record + one output so output-index / cite tests
    /// have something to read. Returns the output's on-disk filename.
    async fn seed_one_output(fs: &AgentFs) -> String {
        let ev = fs
            .record_evidence(EvidenceRecord::new(
                "echo",
                json!({"k": 1}),
                json!({"v": 1}),
                ts(),
            ))
            .await
            .unwrap();
        let out = fs.persist_output("a claim", &[ev]).await.unwrap();
        format!("{}.json", out.id)
    }

    // ---------- `decide` --------------------------------------------------

    #[tokio::test]
    async fn decide_returns_scripted_decision() {
        // The free function is a thin wrapper; the test locks the
        // call-site shape the workflow activity mirrors.
        let mock = MockDecide::new(vec![Decision::Idle {
            next_after: Duration::from_millis(7),
        }]);
        let seed = Seed::new(mandate(), vec![], FsIndex::default());
        let session = Session::new(seed);
        let got = decide(&session, &mock).await.unwrap();
        assert_eq!(
            got,
            Decision::Idle {
                next_after: Duration::from_millis(7),
            }
        );
    }

    // ---------- `build_seed` ---------------------------------------------

    #[tokio::test]
    async fn build_seed_is_thin_and_indexes_notes_and_outputs() {
        let (fs, m) = fixture().await;
        fs.apply_ops(vec![FsOp::WriteFile {
            path: "notes/plan.md".into(),
            content: "the plan".into(),
        }])
        .await
        .unwrap();
        let out_file = seed_one_output(&fs).await;

        let triggers = vec![Trigger::ScheduledWake];
        let seed = build_seed(&fs, triggers.clone(), &m).await.unwrap();
        assert_eq!(seed.triggers, triggers);
        assert_eq!(seed.mandate, m);
        // Index carries pointers (filenames), never bodies.
        assert_eq!(seed.index.notes, vec!["plan.md".to_string()]);
        assert_eq!(seed.index.outputs, vec![out_file]);
    }

    #[tokio::test]
    async fn build_seed_caps_notes_index_to_keep_the_seed_thin() {
        let (fs, m) = fixture().await;
        // Write more notes than the cap so the thin-seed bound bites.
        for i in 0..(SEED_INDEX_NOTES + 10) {
            fs.apply_ops(vec![FsOp::WriteFile {
                path: format!("notes/n-{i:03}.md"),
                content: "x".into(),
            }])
            .await
            .unwrap();
        }
        let seed = build_seed(&fs, vec![], &m).await.unwrap();
        assert_eq!(
            seed.index.notes.len(),
            SEED_INDEX_NOTES,
            "notes index must be capped so the seed stays a small constant"
        );
    }

    // ---------- `execute_step` — clean steps -----------------------------

    #[tokio::test]
    async fn execute_emit_output_with_resolvable_evidence_succeeds() {
        let (fs, _m) = fixture().await;
        let ev_id = fs
            .record_evidence(EvidenceRecord::new(
                "echo",
                json!({"k": 1}),
                json!({"v": 1}),
                ts(),
            ))
            .await
            .unwrap();
        let tools = ToolRegistry::new();
        let outcome = execute_step(
            &fs,
            &tools,
            &Decision::EmitOutput {
                content: "claim".into(),
                evidence: vec![ev_id],
            },
        )
        .await
        .unwrap();
        assert!(outcome.failure.is_none());
        assert!(outcome.observation.ok);
        let outs = fs.list_recent_outputs(8).await.unwrap();
        assert_eq!(outs.len(), 1, "EmitOutput should have persisted an output");
    }

    #[tokio::test]
    async fn execute_read_returns_file_body_as_observation() {
        let (fs, _m) = fixture().await;
        fs.apply_ops(vec![FsOp::WriteFile {
            path: "notes/a.md".into(),
            content: "hello body".into(),
        }])
        .await
        .unwrap();
        let tools = ToolRegistry::new();
        let outcome = execute_step(
            &fs,
            &tools,
            &Decision::Read {
                path: "notes/a.md".into(),
            },
        )
        .await
        .unwrap();
        assert!(outcome.failure.is_none());
        assert_eq!(outcome.observation.content, "hello body");
    }

    #[tokio::test]
    async fn execute_read_missing_file_is_a_recoverable_correction() {
        let (fs, _m) = fixture().await;
        let tools = ToolRegistry::new();
        let outcome = execute_step(
            &fs,
            &tools,
            &Decision::Read {
                path: "notes/nope.md".into(),
            },
        )
        .await
        .unwrap();
        assert!(!outcome.observation.ok);
        assert!(matches!(
            outcome.failure,
            Some(StepFailure::NeedsCorrection(_))
        ));
    }

    #[tokio::test]
    async fn execute_list_lists_directory_entries() {
        let (fs, _m) = fixture().await;
        for f in ["a.md", "b.md"] {
            fs.apply_ops(vec![FsOp::WriteFile {
                path: format!("notes/{f}"),
                content: "x".into(),
            }])
            .await
            .unwrap();
        }
        let tools = ToolRegistry::new();
        let outcome = execute_step(
            &fs,
            &tools,
            &Decision::List {
                path: "notes/".into(),
            },
        )
        .await
        .unwrap();
        assert!(outcome.failure.is_none());
        assert!(outcome.observation.content.contains("a.md"));
        assert!(outcome.observation.content.contains("b.md"));
    }

    #[tokio::test]
    async fn execute_search_finds_matching_content() {
        let (fs, _m) = fixture().await;
        fs.apply_ops(vec![FsOp::WriteFile {
            path: "notes/find.md".into(),
            content: "the answer is tsmc capacity".into(),
        }])
        .await
        .unwrap();
        let tools = ToolRegistry::new();
        let hit = execute_step(
            &fs,
            &tools,
            &Decision::Search {
                query: "tsmc".into(),
                path: None,
            },
        )
        .await
        .unwrap();
        assert!(hit.failure.is_none());
        assert!(hit.observation.content.contains("notes/find.md"));

        let miss = execute_step(
            &fs,
            &tools,
            &Decision::Search {
                query: "no-such-token".into(),
                path: Some("notes/".into()),
            },
        )
        .await
        .unwrap();
        assert!(miss.failure.is_none());
        assert!(miss.observation.content.contains("no matches"));
    }

    // ---------- `execute_step` — recoverable failures --------------------

    #[tokio::test]
    async fn execute_emit_output_with_empty_evidence_needs_correction() {
        let (fs, _m) = fixture().await;
        let tools = ToolRegistry::new();
        let outcome = execute_step(
            &fs,
            &tools,
            &Decision::EmitOutput {
                content: "no evidence".into(),
                evidence: vec![],
            },
        )
        .await
        .unwrap();
        match outcome.failure {
            Some(StepFailure::NeedsCorrection(desc)) => {
                assert!(
                    desc.contains("evidence list is empty"),
                    "unexpected description: {desc}"
                );
            }
            other => panic!("expected NeedsCorrection, got {other:?}"),
        }
        // The failure is also the observation so the model adapts in-cycle.
        assert!(!outcome.observation.ok);
        // Provenance violation must NOT have produced an output on disk.
        assert!(fs.list_recent_outputs(8).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn execute_call_tools_with_unknown_name_needs_correction() {
        let (fs, _m) = fixture().await;
        let tools = ToolRegistry::new(); // empty registry — every name unknown
        let outcome = execute_step(
            &fs,
            &tools,
            &Decision::CallTools {
                calls: vec![ToolCall::new(
                    "never_registered",
                    json!({}),
                    ClaimSeed::new("seed-1"),
                )],
            },
        )
        .await
        .unwrap();
        match outcome.failure {
            Some(StepFailure::NeedsCorrection(desc)) => {
                assert!(desc.contains("never_registered"), "got: {desc}");
            }
            other => panic!("expected NeedsCorrection, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_emit_output_with_unresolved_evidence_needs_correction() {
        let (fs, _m) = fixture().await;
        let tools = ToolRegistry::new();
        let bogus = EvidenceId::new("echo", &json!({"never": "written"}), &json!({"x": 0}));
        let outcome = execute_step(
            &fs,
            &tools,
            &Decision::EmitOutput {
                content: "claim".into(),
                evidence: vec![bogus],
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            outcome.failure,
            Some(StepFailure::NeedsCorrection(_))
        ));
    }

    /// Tool that always errors. Mirrors the spirit of the in-tree
    /// `FlakyTool` in `loop_smoke.rs` but inlined here so these tests
    /// don't depend on the integration-test crate.
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
    async fn execute_call_tools_collects_per_call_failures() {
        let (fs, _m) = fixture().await;
        let mut tools = ToolRegistry::new();
        tools
            .register(std::sync::Arc::new(ErroringTool {
                name: "errbomb".into(),
            }))
            .unwrap();
        let outcome = execute_step(
            &fs,
            &tools,
            &Decision::CallTools {
                calls: vec![
                    ToolCall::new("errbomb", json!({"i": 1}), ClaimSeed::new("a")),
                    ToolCall::new("errbomb", json!({"i": 2}), ClaimSeed::new("b")),
                ],
            },
        )
        .await
        .unwrap();
        match outcome.failure {
            Some(StepFailure::ToolError(failures)) => {
                assert_eq!(failures.len(), 2);
                assert_eq!(failures[0].tool, "errbomb");
                assert!(failures[0].error.contains("synthetic"));
                // Input order is preserved.
                assert_eq!(failures[0].args, json!({"i": 1}));
                assert_eq!(failures[1].args, json!({"i": 2}));
            }
            other => panic!("expected ToolError, got {other:?}"),
        }
        // The failure observation enumerates the failures for the model.
        assert!(!outcome.observation.ok);
        assert!(outcome.observation.content.contains("errbomb"));
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

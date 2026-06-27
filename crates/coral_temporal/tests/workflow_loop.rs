//! Live integration test for the `AgentWorkflow` loop body.
//!
//! Env-gated behind `TEMPORAL_LIVE_TEST=1`. Drives the workflow against a
//! real Temporal Server with a scripted `decide_next_action` activity
//! and asserts the per-tick dispatch shape (Idle / CallTools / EmitOutput /
//! RewriteFs / Retire) via the workflow's history events and the
//! resulting FS artifacts.

use std::env;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowFetchHistoryOptions,
    WorkflowGetResultOptions, WorkflowStartOptions,
};
use temporalio_common::protos::temporal::api::history::v1::history_event::Attributes;
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use coral_node::agent_ref::{AgentId, GraphId};
use coral_node::decision::{
    ClaimSeed, Decision, FsIndex, FsOp, Observation, Seed, Session, ToolCall,
};
use coral_node::evidence::EvidenceRecord;
use coral_node::fs::AgentFs;
use coral_node::mandate::Mandate;
use coral_node::storage::{AgentStorage, MemoryStorage};
use coral_node::tools::{Tool, ToolRegistry};
use coral_temporal::activities::set_decision_script;
use coral_temporal::worker::{build_worker, install_agent_storage, install_tool_registry};
use coral_temporal::workflow::{
    agent_workflow_id, AgentInput, AgentResult, AgentWorkflow, Carryover,
};
use uuid::Uuid;

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// Shared in-memory storage handed to the activity bodies via
/// `agent_storage()` and to the test's post-run assertions. The install
/// hooks panic on double-install, so all tests in this binary share
/// one storage + one tool registry.
static SHARED_STORAGE: OnceLock<Arc<MemoryStorage>> = OnceLock::new();
static INIT: std::sync::Once = std::sync::Once::new();

/// Serializes the two live tests in this binary: both mutate the
/// process-wide [`set_decision_script`] queue and share the installed
/// `AgentStorage` + `ToolRegistry`. Held for the full test body,
/// including the worker run + history fetch.
static LIVE_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

const SUCCEEDING_TOOL_NAMES: &[&str] = &["tool_a", "tool_b", "tool_c"];
const FAILING_TOOL_NAME: &str = "errbomb";

/// Every tool the scripted agents call, as the `Mandate.tools` grant. Dispatch
/// is scoped per agent, so a tool must be both registered (with a recorded
/// owner) and granted on the mandate for a call to reach it.
fn assigned_tools() -> Vec<String> {
    SUCCEEDING_TOOL_NAMES
        .iter()
        .map(|s| s.to_string())
        .chain(std::iter::once(FAILING_TOOL_NAME.to_string()))
        .collect()
}

/// `Tool` impl with a configurable `name()` so the workflow_loop test
/// can register three distinct names that all dispatch to the same
/// in-memory body.
struct AliasEchoTool {
    name: String,
}

#[async_trait]
impl Tool for AliasEchoTool {
    fn name(&self) -> &str {
        &self.name
    }
    async fn call(&self, args: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        Ok(serde_json::json!({"echoed": args, "from": self.name}))
    }
}

/// `Tool` impl that always errors. Used to assert that a single failing
/// call produces a `ToolCallOutcome::Failure` while the sibling calls'
/// evidence persists.
struct FailingTool {
    name: String,
}

#[async_trait]
impl Tool for FailingTool {
    fn name(&self) -> &str {
        &self.name
    }
    async fn call(&self, _args: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        Err(anyhow::anyhow!("synthetic permanent failure (FailingTool)"))
    }
}

/// One-shot install of the process-wide `AgentStorage` + `ToolRegistry`
/// the activity bodies reach for. Idempotent via `std::sync::Once`.
fn ensure_installed() -> Arc<MemoryStorage> {
    INIT.call_once(|| {
        let storage: Arc<MemoryStorage> = Arc::new(MemoryStorage::new());
        SHARED_STORAGE
            .set(Arc::clone(&storage))
            .expect("SHARED_STORAGE set exactly once");
        let dyn_storage: Arc<dyn AgentStorage> = storage;
        install_agent_storage(dyn_storage);

        let mut reg = ToolRegistry::new();
        for name in SUCCEEDING_TOOL_NAMES {
            reg.register(Arc::new(AliasEchoTool {
                name: (*name).into(),
            }))
            .expect("register alias echo tool");
            // Dispatch is scoped per agent; these tests assign each tool to
            // itself (def id == advertised name) and grant them on the
            // scripted mandate (see `assigned_tools`).
            reg.record_owner(name, name);
        }
        reg.register(Arc::new(FailingTool {
            name: FAILING_TOOL_NAME.into(),
        }))
        .expect("register failing tool");
        reg.record_owner(FAILING_TOOL_NAME, FAILING_TOOL_NAME);
        install_tool_registry(Arc::new(reg));
    });
    SHARED_STORAGE.get().cloned().expect("storage installed")
}

fn run_suffix() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().to_string())
        .unwrap_or_else(|_| "no-suffix".into())
}

async fn build_client() -> Result<Client> {
    let address = env::var("TEMPORAL_ADDRESS").unwrap_or_else(|_| DEFAULT_ADDRESS.into());
    let namespace = env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| DEFAULT_NAMESPACE.into());
    let url = Url::parse(&address).context("parsing TEMPORAL_ADDRESS")?;
    let connection_options = ConnectionOptions::new(url).build();
    let connection = Connection::connect(connection_options)
        .await
        .context("connecting to Temporal Server (is `temporal server start-dev` running?)")?;
    let client_options = ClientOptions::new(namespace.clone()).build();
    let client = Client::new(connection, client_options).context("building Temporal client")?;
    Ok(client)
}

/// Live test: scripts a single cycle of CallTools(3) → EmitOutput →
/// RewriteFs → Idle (capped at `step_cap=1`), then asserts the workflow
/// history shows the expected parallel tool dispatch + persist_retirement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// The `LIVE_TEST_GUARD` lock is held across `await`s on purpose: it
// serializes two live tests that both mutate process-wide state
// (`DECISION_SCRIPT`, the installed `AgentStorage` / `ToolRegistry`).
// Releasing the lock before `run_live_test().await` would defeat the
// serialization. Async-aware (`tokio::sync::Mutex`) would work too,
// but std's sync Mutex in a `static` context is the smaller diff and
// contention is at most "wait for the sibling test" (seconds).
#[allow(clippy::await_holding_lock)]
async fn workflow_loop_runs_idle_then_calltools_then_retire() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping workflow_loop_runs_idle_then_calltools_then_retire; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }

    let _guard = LIVE_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
    run_live_test().await.expect("live workflow_loop test");
}

async fn run_live_test() -> Result<()> {
    let suffix = run_suffix();
    let task_queue = format!("coral-agents-loop-test-{suffix}");

    // Per-run agent prefix the workflow's `FsHandle` will scope to.
    // Embedding the run suffix keeps reruns against the shared
    // `MemoryStorage` from colliding on `<prefix>/retirement.json`,
    // `<prefix>/outputs/...`, and `<prefix>/notes/...`.
    let agent_prefix = format!("graphs/g-loop-test/agents/a-loop-test-{suffix}");
    let driver_prefix = agent_prefix.clone();

    let storage = ensure_installed();

    // Plant one evidence record under the workflow's FS prefix so the
    // scripted `Decision::EmitOutput`'s provenance check resolves. The
    // planting `AgentFs` must share the same `Arc<dyn AgentStorage>` the
    // worker hands to activities — a fresh `MemoryStorage` would not
    // share state.
    let plant_mandate = Mandate::new("plant", Duration::from_millis(0), None);
    let plant_storage: Arc<dyn AgentStorage> = storage.clone();
    let plant_fs = AgentFs::new_with_storage(plant_storage, &agent_prefix, &plant_mandate)
        .await
        .context("open planting AgentFs")?;
    let planted_id = plant_fs
        .record_evidence(EvidenceRecord::new(
            "tool_seed",
            serde_json::json!({"k": "v"}),
            serde_json::json!({"hit": true}),
            Utc::now(),
        ))
        .await
        .context("plant evidence for EmitOutput")?;

    // Install the scripted cycle BEFORE the worker starts. One cycle of
    // four steps: CallTools(3 parallel) → EmitOutput → RewriteFs → Idle
    // (the sole terminal that ends the cycle), then the `step_cap=1` cap
    // stops the loop at the top of cycle 1 (agents never self-terminate).
    set_decision_script(vec![
        Decision::CallTools {
            calls: vec![
                ToolCall::new("tool_a", serde_json::json!({"i": 1}), ClaimSeed::new("s-a")),
                ToolCall::new("tool_b", serde_json::json!({"i": 2}), ClaimSeed::new("s-b")),
                ToolCall::new("tool_c", serde_json::json!({"i": 3}), ClaimSeed::new("s-c")),
            ],
        },
        Decision::EmitOutput {
            content: "workflow_loop test: scripted output".into(),
            evidence: vec![planted_id.clone()],
        },
        Decision::RewriteFs {
            ops: vec![FsOp::WriteFile {
                path: "notes/loop-test.md".into(),
                content: "from workflow_loop live test".into(),
            }],
        },
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
    ]);

    let telemetry_options = TelemetryOptions::builder().build();
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(telemetry_options)
            .build()
            .map_err(|e| anyhow::anyhow!("RuntimeOptions build failed: {e}"))?,
    )?;
    let client = build_client().await?;
    let mut worker = build_worker(&runtime, client.clone(), &task_queue)?;
    let shutdown = worker.shutdown_handle();

    let driver_task_queue = task_queue.clone();
    let driver_storage = storage.clone();
    let driver_planted_id = planted_id.clone();
    let driver = tokio::spawn(async move {
        let workflow_id = format!(
            "{}-{suffix}",
            agent_workflow_id("g-loop-test", "a-loop-test")
        );
        eprintln!("workflow_loop: starting workflow_id={workflow_id} on {driver_task_queue}");
        // Use a guard so `shutdown()` runs whether `drive` returns Ok,
        // returns Err, or panics — otherwise an assertion panic leaves
        // `worker.run()` hanging and the timeout fires instead of the
        // actual assertion message.
        struct ShutdownGuard<F: Fn()>(F);
        impl<F: Fn()> Drop for ShutdownGuard<F> {
            fn drop(&mut self) {
                (self.0)();
            }
        }
        let _guard = ShutdownGuard(shutdown);
        drive(
            client,
            &driver_task_queue,
            &workflow_id,
            &driver_prefix,
            driver_storage,
            driver_planted_id,
        )
        .await
    });

    // 60-second timeout catches stalls; the workflow completes in <2s
    // on a healthy local server.
    let worker_result = tokio::time::timeout(Duration::from_secs(60), worker.run())
        .await
        .map_err(|_| anyhow::anyhow!("worker.run() timed out (60s)"))?
        .map_err(|e| anyhow::anyhow!("worker.run() exited with error: {e}"));
    let driver_result = driver.await.context("driver task panicked")?;

    worker_result?;
    driver_result?;
    Ok(())
}

async fn drive(
    client: Client,
    task_queue: &str,
    workflow_id: &str,
    agent_prefix: &str,
    storage: Arc<MemoryStorage>,
    planted_id: coral_node::evidence::EvidenceId,
) -> Result<()> {
    // Build an `AgentInput` that scopes the per-agent FS to a per-run
    // prefix — the workflow body passes it into every activity input,
    // so `persist_retirement` writes to `<prefix>/retirement.json`,
    // `persist_output` to `<prefix>/outputs/<ulid>.json`, and
    // `apply_fs_ops` to `<prefix>/notes/loop-test.md`.
    let mut input = AgentInput::new_for_test(
        GraphId::new(Uuid::new_v4()),
        AgentId::new(Uuid::new_v4()),
        "workflow-loop-test",
    );
    input.fs_handle = coral_temporal::workflow::FsHandle {
        prefix: agent_prefix.into(),
    };
    input.mandate.tools = assigned_tools();
    // The loop runs the one scripted cycle (4 steps), then the cap stops it.
    input.mandate.step_cap = Some(1);
    let handle = client
        .start_workflow(
            AgentWorkflow::run,
            input,
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow)")?;

    // Workflow runs the scripted CallTools(3) → EmitOutput → RewriteFs →
    // Idle cycle on its own, then the step_cap cap stops it; no
    // client-side signals needed. Each step calls `decide_step` which pops
    // the next scripted decision.
    let result: AgentResult = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result")?;
    eprintln!("workflow_loop: workflow returned {result:?}");
    let AgentResult::Retired { reason } = result;
    assert_eq!(
        reason, "step_cap (1) reached",
        "workflow returned wrong retire reason: {reason:?}"
    );

    // History assertions — count `ActivityTaskScheduled` events by
    // activity type. The scripted sequence guarantees:
    //
    // - exactly 3 `execute_tool` schedules (one per `ToolCall`);
    // - exactly 1 `persist_output` schedule (from `EmitOutput`);
    // - exactly 1 `persist_retirement` schedule (from the `step_cap` cap).
    //
    // `WorkflowFetchHistoryOptions::default()` leaves `event_filter_type`
    // at the proto enum's zero value (Unspecified), which the server
    // reads as "give me close events only". Build the options
    // explicitly so we get the full event stream for assertions.
    let history = handle
        .fetch_history(WorkflowFetchHistoryOptions::builder().build())
        .await
        .context("fetch_history")?;
    eprintln!(
        "workflow_loop: fetched {} history events",
        history.events().len()
    );
    // Activity type names are namespaced by the `#[activities]` macro
    // as `AgentActivities::<fn_name>`. Match on the unqualified suffix
    // so the assertion stays robust if the macro ever drops the prefix.
    let mut execute_tool_schedules = 0usize;
    let mut persist_output_schedules = 0usize;
    let mut persist_retirement_schedules = 0usize;
    let mut apply_fs_ops_schedules = 0usize;
    // A retiring workflow must NOT emit a
    // `WorkflowExecutionContinuedAsNew` event — counting CAN events is
    // the most direct end-to-end assertion that the CAN check sits
    // after the retirement-return arms in `AgentWorkflow::run`.
    let mut continued_as_new_events = 0usize;
    let mut all_activity_type_names: Vec<String> = Vec::new();
    for ev in history.events() {
        match &ev.attributes {
            Some(Attributes::ActivityTaskScheduledEventAttributes(a)) => {
                if let Some(ty) = &a.activity_type {
                    all_activity_type_names.push(ty.name.clone());
                    let unqualified = ty.name.rsplit("::").next().unwrap_or(ty.name.as_str());
                    match unqualified {
                        "execute_tool" => execute_tool_schedules += 1,
                        "persist_output" => persist_output_schedules += 1,
                        "persist_retirement" => persist_retirement_schedules += 1,
                        "apply_fs_ops" => apply_fs_ops_schedules += 1,
                        _ => {}
                    }
                }
            }
            Some(Attributes::WorkflowExecutionContinuedAsNewEventAttributes(_)) => {
                continued_as_new_events += 1;
            }
            _ => {}
        }
    }
    eprintln!("workflow_loop: observed activity-type names: {all_activity_type_names:?}");
    eprintln!(
        "workflow_loop: execute_tool={execute_tool_schedules}, \
         persist_output={persist_output_schedules}, \
         apply_fs_ops={apply_fs_ops_schedules}, \
         persist_retirement={persist_retirement_schedules}"
    );
    assert_eq!(
        execute_tool_schedules, 3,
        "expected 3 parallel execute_tool activity invocations, got {execute_tool_schedules}"
    );
    assert!(
        persist_output_schedules >= 1,
        "expected at least 1 persist_output invocation, got {persist_output_schedules}"
    );
    assert!(
        apply_fs_ops_schedules >= 1,
        "expected at least 1 apply_fs_ops invocation, got {apply_fs_ops_schedules}"
    );
    assert!(
        persist_retirement_schedules >= 1,
        "expected at least 1 persist_retirement invocation, got {persist_retirement_schedules}"
    );
    assert_eq!(
        continued_as_new_events, 0,
        "a retiring workflow must NOT emit a \
         WorkflowExecutionContinuedAsNew event (the CAN check sits after \
         the retirement-return arms in run()), got {continued_as_new_events}"
    );

    // The `persist_retirement` activity body wrote
    // `<prefix>/retirement.json` via the shared MemoryStorage. Assert
    // the file lands with the scripted reason and a UTC-shaped
    // timestamp.
    let key = format!("{agent_prefix}/retirement.json");
    let bytes = storage
        .get(&key)
        .await
        .context("MemoryStorage::get on retirement.json")?
        .ok_or_else(|| anyhow::anyhow!("retirement.json absent at key {key}"))?;
    let v: serde_json::Value =
        serde_json::from_slice(&bytes).context("retirement.json is not JSON")?;
    let reason_on_disk = v
        .get("reason")
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow::anyhow!("retirement.json missing reason"))?;
    assert_eq!(
        reason_on_disk, "step_cap (1) reached",
        "retirement.json carries wrong reason: {reason_on_disk:?}"
    );
    let retired_at = v
        .get("retired_at")
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow::anyhow!("retirement.json missing retired_at"))?;
    assert!(
        retired_at.ends_with("+00:00") || retired_at.ends_with('Z'),
        "retired_at not UTC-shaped: {retired_at:?}"
    );

    // Each successful tool call's evidence record must land in the
    // per-agent FS via `AgentFs::record_evidence`. With three
    // succeeding tools the three sha256-keyed evidence files are
    // distinct (each tool name is part of the canonical-JSON hash,
    // see `EvidenceId::new`). Plus one planted evidence (tool_seed) → 4
    // total. Read directly via the storage backend rather than
    // constructing an `AgentFs` to avoid re-running tail reconciliation.
    // Evidence keys live at `<agent_prefix>/evidence/<sha>.json` (see
    // `AgentFs::evidence_key`), so scope the list to the per-agent
    // prefix.
    let evidence_prefix = format!("{agent_prefix}/evidence/");
    let page = storage
        .list(&evidence_prefix, None, usize::MAX)
        .await
        .context("listing evidence/ from shared storage")?;
    let evidence_records: Vec<_> = page
        .keys
        .iter()
        .filter(|k| k.ends_with(".json") && !k.ends_with("/_tail.json"))
        .collect();
    eprintln!("workflow_loop: post-run evidence keys: {evidence_records:?}");
    assert_eq!(
        evidence_records.len(),
        4,
        "expected 4 evidence files (3 tool calls + 1 planted seed), got {evidence_records:?}"
    );

    // FS assertions: open a fresh `AgentFs` over the same process-wide
    // storage the worker hands to activities and verify the scripted
    // `EmitOutput` actually landed at `outputs/<ulid>.json` with the
    // scripted content + the planted evidence id.
    let inspect_mandate = Mandate::new("inspect", Duration::from_millis(0), None);
    let inspect_storage: Arc<dyn AgentStorage> = storage.clone();
    let inspect_fs = AgentFs::new_with_storage(inspect_storage, agent_prefix, &inspect_mandate)
        .await
        .context("open inspecting AgentFs")?;
    let outs = inspect_fs
        .list_recent_outputs(8)
        .await
        .context("list_recent_outputs")?;
    assert_eq!(
        outs.len(),
        1,
        "expected exactly one output on disk after EmitOutput; got {}: {outs:?}",
        outs.len()
    );
    let on_disk = &outs[0];
    assert_eq!(
        on_disk.content, "workflow_loop test: scripted output",
        "output content must match scripted EmitOutput"
    );
    assert!(
        on_disk.evidence.contains(&planted_id),
        "output must cite the planted evidence id, got {:?}",
        on_disk.evidence
    );
    eprintln!(
        "workflow_loop: output landed at outputs/{}.json with {} evidence id(s)",
        on_disk.id,
        on_disk.evidence.len()
    );

    // The `RewriteFs` step writes `<prefix>/notes/loop-test.md`. Pull
    // it from the same shared `MemoryStorage` backend the activity
    // wrote into.
    let notes_key = format!("{agent_prefix}/notes/loop-test.md");
    let blob = storage
        .get(&notes_key)
        .await
        .with_context(|| format!("storage.get({notes_key}) after live RewriteFs"))?
        .ok_or_else(|| anyhow::anyhow!("expected {notes_key} on disk after RewriteFs decision"))?;
    let body = std::str::from_utf8(blob.as_ref()).context("notes/loop-test.md body utf-8")?;
    assert_eq!(body, "from workflow_loop live test");

    Ok(())
}

/// Partial-batch survival test. Scripts `Decision::CallTools` with one
/// failing tool + two succeeding tools and asserts:
///
/// 1. All three `execute_tool` activities scheduled (parallel
///    `join_all` doesn't short-circuit on the first failure).
/// 2. Two new evidence files land in storage (the two succeeding
///    calls).
/// 3. The workflow continues — the failed call is folded into a
///    `CorrectionContext` for the next tick, not bubbled as an
///    `ActivityError`, so the workflow still observes the subsequent
///    scripted `Retire`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// See sibling test for the same-lock-across-await rationale.
#[allow(clippy::await_holding_lock)]
async fn workflow_loop_survives_partial_batch_failure() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping workflow_loop_survives_partial_batch_failure; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }

    let _guard = LIVE_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
    run_partial_failure_test()
        .await
        .expect("live partial-failure test");
}

async fn run_partial_failure_test() -> Result<()> {
    let suffix = run_suffix();
    let task_queue = format!("coral-agents-loop-pf-test-{suffix}");
    let storage = ensure_installed();

    // Snapshot the pre-run evidence-key set so the post-run assertion
    // counts only the new files this test's CallTools produced (the
    // earlier `run_live_test` invocation in the same binary already
    // wrote three).
    let pre_run_count = storage
        .list("evidence/", None, usize::MAX)
        .await
        .context("pre-run evidence list")?
        .keys
        .iter()
        .filter(|k| k.ends_with(".json") && !k.ends_with("/_tail.json"))
        .count();

    // Script: one cycle of CallTools(one failing + two distinct succeeding
    // calls) → Idle, then the `step_cap=1` cap stops the loop. The
    // succeeding calls use args distinct from run_live_test's so their
    // EvidenceIds (content-addressed on (tool, args, result)) don't collide
    // with the pre-run records.
    set_decision_script(vec![
        Decision::CallTools {
            calls: vec![
                ToolCall::new(
                    "tool_a",
                    serde_json::json!({"pf": 1}),
                    ClaimSeed::new("pf-a"),
                ),
                ToolCall::new(
                    FAILING_TOOL_NAME,
                    serde_json::json!({"pf": "boom"}),
                    ClaimSeed::new("pf-fail"),
                ),
                ToolCall::new(
                    "tool_b",
                    serde_json::json!({"pf": 2}),
                    ClaimSeed::new("pf-b"),
                ),
            ],
        },
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
    ]);

    let telemetry_options = TelemetryOptions::builder().build();
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(telemetry_options)
            .build()
            .map_err(|e| anyhow::anyhow!("RuntimeOptions build failed: {e}"))?,
    )?;
    let client = build_client().await?;
    let mut worker = build_worker(&runtime, client.clone(), &task_queue)?;
    let shutdown = worker.shutdown_handle();

    let driver_task_queue = task_queue.clone();
    let driver = tokio::spawn(async move {
        let workflow_id = format!(
            "{}-pf-{suffix}",
            agent_workflow_id("g-loop-pf-test", "a-loop-pf-test")
        );
        eprintln!("workflow_loop_pf: starting workflow_id={workflow_id} on {driver_task_queue}");
        struct ShutdownGuard<F: Fn()>(F);
        impl<F: Fn()> Drop for ShutdownGuard<F> {
            fn drop(&mut self) {
                (self.0)();
            }
        }
        let _guard = ShutdownGuard(shutdown);
        drive_partial(client, &driver_task_queue, &workflow_id).await
    });

    let worker_result = tokio::time::timeout(Duration::from_secs(60), worker.run())
        .await
        .map_err(|_| anyhow::anyhow!("worker.run() timed out (60s)"))?
        .map_err(|e| anyhow::anyhow!("worker.run() exited with error: {e}"));
    let driver_result = driver.await.context("driver task panicked")?;

    worker_result?;
    driver_result?;

    // Post-run: exactly two new evidence files (the two succeeding
    // calls). The failing call must NOT have written evidence.
    let post_run_count = storage
        .list("evidence/", None, usize::MAX)
        .await
        .context("post-run evidence list")?
        .keys
        .iter()
        .filter(|k| k.ends_with(".json") && !k.ends_with("/_tail.json"))
        .count();
    assert_eq!(
        post_run_count - pre_run_count,
        2,
        "expected 2 new evidence files (succeeding calls only); pre={pre_run_count} post={post_run_count}"
    );
    Ok(())
}

async fn drive_partial(client: Client, task_queue: &str, workflow_id: &str) -> Result<()> {
    let mut input = AgentInput::new_for_test(
        GraphId::new(Uuid::new_v4()),
        AgentId::new(Uuid::new_v4()),
        "workflow-loop-pf-test",
    );
    input.mandate.tools = assigned_tools();
    // The loop runs the one scripted cycle (2 steps), then the cap stops it.
    input.mandate.step_cap = Some(1);
    let handle = client
        .start_workflow(
            AgentWorkflow::run,
            input,
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow) [partial]")?;

    let result: AgentResult = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result [partial]")?;
    let AgentResult::Retired { reason } = result;
    assert_eq!(
        reason, "step_cap (1) reached",
        "workflow returned wrong retire reason in partial-failure test: {reason:?}"
    );

    // History assertions: still 3 execute_tool schedules — the
    // workflow's `join_all` fans out all three, and the failing
    // call returns `Ok(ToolCallOutcome::Failure)` rather than
    // bubbling an ActivityError, so no schedule was skipped.
    let history = handle
        .fetch_history(WorkflowFetchHistoryOptions::builder().build())
        .await
        .context("fetch_history [partial]")?;
    let mut execute_tool_schedules = 0usize;
    for ev in history.events() {
        if let Some(Attributes::ActivityTaskScheduledEventAttributes(a)) = &ev.attributes {
            if let Some(ty) = &a.activity_type {
                let unqualified = ty.name.rsplit("::").next().unwrap_or(ty.name.as_str());
                if unqualified == "execute_tool" {
                    execute_tool_schedules += 1;
                }
            }
        }
    }
    assert_eq!(
        execute_tool_schedules, 3,
        "partial-failure: expected 3 execute_tool schedules, got {execute_tool_schedules}"
    );
    Ok(())
}

/// Regression for the silently-ignored `step_cap` on the Temporal path: an
/// agent with `step_cap=2` whose script never stops on its own must still
/// stop at the cap with the in-process retire wording. (Agents never
/// self-terminate; `step_cap` is the cap.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::await_holding_lock)]
async fn workflow_loop_enforces_step_cap() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping workflow_loop_enforces_step_cap; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    let _guard = LIVE_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());

    let mandate = Mandate::new("max-ticks regression", Duration::from_millis(20), Some(2));
    // Buffer of Idles longer than the cap so the cap — not script
    // exhaustion — is what stops the loop.
    let script = vec![
        Decision::Idle {
            next_after: Duration::from_millis(20),
        };
        6
    ];

    let reason = run_stop_contract_test("maxticks", mandate, script)
        .await
        .expect("step_cap regression live test");
    assert_eq!(
        reason, "step_cap (2) reached",
        "agent must retire at the cap with the in-process wording, got {reason:?}"
    );
}

/// Drive an `AgentWorkflow` with `mandate` + `script` against a live server
/// and return the `AgentResult::Retired` reason. No tool calls / outputs, so
/// no evidence planting is needed.
async fn run_stop_contract_test(
    label: &str,
    mandate: Mandate,
    script: Vec<Decision>,
) -> Result<String> {
    let suffix = run_suffix();
    let task_queue = format!("coral-agents-stop-{label}-{suffix}");
    ensure_installed();
    set_decision_script(script);

    let telemetry_options = TelemetryOptions::builder().build();
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(telemetry_options)
            .build()
            .map_err(|e| anyhow::anyhow!("RuntimeOptions build failed: {e}"))?,
    )?;
    let client = build_client().await?;
    let mut worker = build_worker(&runtime, client.clone(), &task_queue)?;
    let shutdown = worker.shutdown_handle();

    let driver_task_queue = task_queue.clone();
    let driver_label = label.to_string();
    let driver = tokio::spawn(async move {
        let workflow_id = format!(
            "{}-{driver_label}-{suffix}",
            agent_workflow_id("g-stop-test", "a-stop-test")
        );
        struct ShutdownGuard<F: Fn()>(F);
        impl<F: Fn()> Drop for ShutdownGuard<F> {
            fn drop(&mut self) {
                (self.0)();
            }
        }
        let _guard = ShutdownGuard(shutdown);

        let mut input = AgentInput::new_for_test(
            GraphId::new(Uuid::new_v4()),
            AgentId::new(Uuid::new_v4()),
            "stop-contract-test",
        );
        input.fs_handle = coral_temporal::workflow::FsHandle {
            prefix: format!("graphs/g-stop-test/agents/a-stop-{driver_label}-{suffix}"),
        };
        input.mandate = mandate;

        let handle = client
            .start_workflow(
                AgentWorkflow::run,
                input,
                WorkflowStartOptions::new(&driver_task_queue, &workflow_id).build(),
            )
            .await
            .context("start_workflow(AgentWorkflow) [stop-contract]")?;
        let result: AgentResult = handle
            .get_result(WorkflowGetResultOptions::default())
            .await
            .context("AgentWorkflow.get_result [stop-contract]")?;
        let AgentResult::Retired { reason } = result;
        Ok::<String, anyhow::Error>(reason)
    });

    let worker_result = tokio::time::timeout(Duration::from_secs(60), worker.run())
        .await
        .map_err(|_| anyhow::anyhow!("worker.run() timed out (60s)"))?
        .map_err(|e| anyhow::anyhow!("worker.run() exited with error: {e}"));
    let reason = driver.await.context("driver task panicked")??;
    worker_result?;
    Ok(reason)
}

/// Resume half of A+C (mid-cycle continue-as-new). The *suspend* trigger
/// (`continue_as_new_suggested`) can't be forced hermetically, but the
/// replay-risky *resume* path is just a workflow started with a crafted
/// carryover, so it is fully deterministic. Start `AgentWorkflow` with
/// `carryover.in_flight = Some(session_with_2_steps)` + `tick = 3` and a
/// continuation script of a single `Idle`, then assert the run RESUMED the
/// cycle rather than starting fresh:
///
/// - no `build_seed` schedule (resume skips the seed-building path),
/// - no `execute_tool` schedule (the carried `CallTools` step is NOT
///   re-executed),
/// - the continuation decision logs at `decisions/3-2.jsonl` (step derived
///   from `session.len() == 2`), and `decisions/3-0.jsonl` is absent (no
///   restart-at-zero clobber),
/// - the cycle idles and the loop retires at `step_cap=4` — i.e. `tick` went
///   3 → 4 (one cycle), not 3 → 3+N.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::await_holding_lock)]
async fn workflow_loop_resumes_in_flight_cycle_from_carryover() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping workflow_loop_resumes_in_flight_cycle_from_carryover; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    let _guard = LIVE_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
    run_resume_test().await.expect("live resume test");
}

/// Build the 2-step in-flight session the resume test carries. The steps are
/// synthetic `(action, observation)` pairs — resume never re-runs them, so a
/// real `CallTools` action here proves the no-re-execution invariant (its
/// `execute_tool` activity must not fire on the resumed run).
fn carried_session() -> Session {
    let seed = Seed::new(
        Mandate::new("resume-me", Duration::from_millis(20), Some(4)),
        Vec::new(),
        FsIndex::default(),
    );
    let mut session = Session::new(seed);
    session.push(
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "tool_a",
                serde_json::json!({"carried": true}),
                ClaimSeed::new("carried-a"),
            )],
        },
        Observation::ok("carried: 1 tool call succeeded"),
    );
    session.push(
        Decision::Read {
            path: "notes/carried.md".into(),
        },
        Observation::ok("carried note body"),
    );
    session
}

async fn run_resume_test() -> Result<()> {
    let suffix = run_suffix();
    let task_queue = format!("coral-agents-resume-test-{suffix}");
    let storage = ensure_installed();

    // The resumed cycle needs exactly one more decision to terminate.
    set_decision_script(vec![Decision::Idle {
        next_after: Duration::from_millis(20),
    }]);

    let telemetry_options = TelemetryOptions::builder().build();
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(telemetry_options)
            .build()
            .map_err(|e| anyhow::anyhow!("RuntimeOptions build failed: {e}"))?,
    )?;
    let client = build_client().await?;
    let mut worker = build_worker(&runtime, client.clone(), &task_queue)?;
    let shutdown = worker.shutdown_handle();

    let agent_prefix = format!("graphs/g-resume-test/agents/a-resume-{suffix}");
    let driver_task_queue = task_queue.clone();
    let driver_storage = storage.clone();
    let driver = tokio::spawn(async move {
        let workflow_id = format!(
            "{}-{suffix}",
            agent_workflow_id("g-resume-test", "a-resume")
        );
        eprintln!(
            "workflow_loop_resume: starting workflow_id={workflow_id} on {driver_task_queue}"
        );
        struct ShutdownGuard<F: Fn()>(F);
        impl<F: Fn()> Drop for ShutdownGuard<F> {
            fn drop(&mut self) {
                (self.0)();
            }
        }
        let _guard = ShutdownGuard(shutdown);
        drive_resume(
            client,
            &driver_task_queue,
            &workflow_id,
            &agent_prefix,
            driver_storage,
        )
        .await
    });

    let worker_result = tokio::time::timeout(Duration::from_secs(60), worker.run())
        .await
        .map_err(|_| anyhow::anyhow!("worker.run() timed out (60s)"))?
        .map_err(|e| anyhow::anyhow!("worker.run() exited with error: {e}"));
    let driver_result = driver.await.context("driver task panicked")?;

    worker_result?;
    driver_result?;
    Ok(())
}

async fn drive_resume(
    client: Client,
    task_queue: &str,
    workflow_id: &str,
    agent_prefix: &str,
    storage: Arc<MemoryStorage>,
) -> Result<()> {
    let mut input = AgentInput::new_for_test(
        GraphId::new(Uuid::new_v4()),
        AgentId::new(Uuid::new_v4()),
        "workflow-loop-resume-test",
    );
    input.fs_handle = coral_temporal::workflow::FsHandle {
        prefix: agent_prefix.into(),
    };
    input.mandate.tools = assigned_tools();
    // tick=3 carried; step_cap=4 so the resumed cycle runs (3 < 4) and the
    // loop retires on the NEXT iteration once the cycle bumps tick to 4.
    input.mandate.step_cap = Some(4);
    input.carryover = Some(Carryover {
        in_flight: Some(carried_session()),
        tick: 3,
        ..Carryover::default()
    });

    let handle = client
        .start_workflow(
            AgentWorkflow::run,
            input,
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow) [resume]")?;

    let result: AgentResult = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result [resume]")?;
    let AgentResult::Retired { reason } = result;
    assert_eq!(
        reason, "step_cap (4) reached",
        "resumed cycle must bump tick 3 → 4 (one cycle) then retire at the cap, got {reason:?}"
    );

    // History: the resume path skips `build_seed` and must not re-execute the
    // carried `CallTools` step.
    let history = handle
        .fetch_history(WorkflowFetchHistoryOptions::builder().build())
        .await
        .context("fetch_history [resume]")?;
    let mut build_seed_schedules = 0usize;
    let mut execute_tool_schedules = 0usize;
    let mut persist_retirement_schedules = 0usize;
    for ev in history.events() {
        if let Some(Attributes::ActivityTaskScheduledEventAttributes(a)) = &ev.attributes {
            if let Some(ty) = &a.activity_type {
                match ty.name.rsplit("::").next().unwrap_or(ty.name.as_str()) {
                    "build_seed" => build_seed_schedules += 1,
                    "execute_tool" => execute_tool_schedules += 1,
                    "persist_retirement" => persist_retirement_schedules += 1,
                    _ => {}
                }
            }
        }
    }
    assert_eq!(
        build_seed_schedules, 0,
        "resume must skip build_seed (the cycle is already in flight), got {build_seed_schedules}"
    );
    assert_eq!(
        execute_tool_schedules, 0,
        "carried CallTools step must NOT be re-executed on resume, got {execute_tool_schedules}"
    );
    assert!(
        persist_retirement_schedules >= 1,
        "resumed run must retire at the step_cap, got {persist_retirement_schedules}"
    );

    // The continuation decision logs at `<tick>-<step>` = `3-2` (step derived
    // from the carried `session.len() == 2`), proving the decision stream
    // continued rather than restarting at `3-0`.
    let resumed_key = format!("{agent_prefix}/decisions/3-2.jsonl");
    assert!(
        storage
            .get(&resumed_key)
            .await
            .context("get resumed decision-log key")?
            .is_some(),
        "expected resumed decision log at {resumed_key} (step derived from session.len()==2)"
    );
    let clobber_key = format!("{agent_prefix}/decisions/3-0.jsonl");
    assert!(
        storage
            .get(&clobber_key)
            .await
            .context("get would-be-clobber decision-log key")?
            .is_none(),
        "resume must not restart the decision stream at {clobber_key}"
    );

    Ok(())
}

//! Stage 3.4 (JAR2-60) — `AgentWorkflow` loop body live integration test.
//!
//! Env-gated behind `TEMPORAL_LIVE_TEST=1`. Drives the workflow against a
//! real Temporal Server with a scripted `decide_next_action` activity
//! (via [`jarvis_temporal::activities::set_decision_script`]) and asserts
//! the per-tick dispatch shape end-to-end:
//!
//! 1. `Decision::Idle` → loop continues to next tick (history shows the
//!    `Decision::Idle`-producing `decide_next_action` invocation).
//! 2. `Decision::CallTools { calls: [A, B, C] }` → 3 parallel
//!    `execute_tool` activity invocations (asserted by counting
//!    `ActivityTaskScheduled` events with the right activity type).
//! 3. `Decision::Retire { reason }` → `persist_retirement` activity
//!    fires, workflow returns `AgentResult::Retired { reason }`.
//!
//! ## Why a scripted activity (and not a `MockDecide` injected via cfg)
//!
//! The SDK's `register_activities` takes a value-typed bundle (smoke
//! § 3.4) and the workflow code is replayed by the worker; we cannot
//! reach into the registered `AgentActivities` instance to swap in a
//! `MockDecide`. The static `OnceLock<Mutex<VecDeque<Decision>>>` in
//! `jarvis_temporal::activities` is the SDK-blessed workaround — the
//! same one the smoke binary uses for its
//! `ACTIVITY_INVOCATIONS: AtomicUsize`.
//!
//! ## History assertions
//!
//! After `get_result`, the test calls
//! `Client::list_workflow_history(...)` (the SDK's iteration API) and
//! counts:
//!
//! - `ActivityTaskScheduled` events with activity-type `execute_tool` —
//!   asserted >= 3 (one per scripted `ToolCall`).
//! - `ActivityTaskScheduled` events with activity-type
//!   `persist_retirement` — asserted >= 1.
//!
//! Both invariants are necessary; together they prove the
//! `CallTools` → `join_all` → `persist_retirement` path executed.

use std::env;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_trait::async_trait;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowFetchHistoryOptions,
    WorkflowGetResultOptions, WorkflowStartOptions,
};
use temporalio_common::protos::temporal::api::history::v1::history_event::Attributes;
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use jarvis_node::decision::{ClaimSeed, Decision, ToolCall};
use jarvis_node::storage::{AgentStorage, MemoryStorage};
use jarvis_node::tools::{Tool, ToolRegistry};
use jarvis_temporal::activities::set_decision_script;
use jarvis_temporal::worker::{build_worker, install_agent_storage, install_tool_registry};
use jarvis_temporal::workflow::{agent_workflow_id, AgentInput, AgentResult, AgentWorkflow};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// JAR2-63: shared in-memory storage backend handed to both the
/// `execute_tool` activity (via `agent_storage()`), the JAR2-66
/// `persist_retirement` activity (same path), and the test's post-run
/// evidence + retirement-file assertions. `OnceLock` because the
/// install hooks panic on double-install — every test in this binary
/// shares one storage + one tool registry.
static SHARED_STORAGE: OnceLock<Arc<MemoryStorage>> = OnceLock::new();
static INIT: std::sync::Once = std::sync::Once::new();

/// Serializes the two live tests in this binary. Cargo's default test
/// runner schedules tests in a binary in parallel; both live tests
/// here mutate the process-wide [`set_decision_script`] queue and
/// share one installed `AgentStorage` + `ToolRegistry`, so running
/// them concurrently would have one test pop the other's scripted
/// decisions and produce nonsense workflow histories. The mutex is
/// held for the full duration of each test body, including the
/// worker run + history fetch.
static LIVE_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// JAR2-63: per-test name for the `succeeding` and `failing` tools so
/// the partial-failure test can route a single call to a tool that
/// always errors while sibling calls succeed.
const SUCCEEDING_TOOL_NAMES: &[&str] = &["tool_a", "tool_b", "tool_c"];
const FAILING_TOOL_NAME: &str = "errbomb";

/// Test double: `Tool` impl that wraps an arbitrary name and echoes its
/// args under a fixed key. Mirror of `jarvis_node::tools::EchoTool` but
/// with a configurable `name()` so the workflow_loop test can register
/// three distinct names (`tool_a`/`tool_b`/`tool_c`) that all dispatch
/// to the same in-memory body. Lives in the test crate (not promoted to
/// `jarvis_node::tools` test surface) per the "smallest correct diff"
/// rule — production code has no use for an alias-able echo.
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

/// Test double: `Tool` impl that always errors. Used by the partial-
/// failure test to assert that a single failing call produces a
/// `ToolCallOutcome::Failure` while the sibling calls' evidence
/// persists.
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
/// the JAR2-63 activity body reaches for. Idempotent via `std::sync::Once`.
/// Subsumes JAR2-66's `ensure_installed` (storage-only) by
/// also installing a `ToolRegistry`; JAR2-66's retirement assertions
/// reach for the same `SHARED_STORAGE` via the returned `Arc`.
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
        }
        reg.register(Arc::new(FailingTool {
            name: FAILING_TOOL_NAME.into(),
        }))
        .expect("register failing tool");
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

/// Live test: scripts the decide_next_action activity through Idle →
/// CallTools(3) → Retire, then asserts the workflow history shows the
/// expected parallel tool dispatch + persist_retirement.
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
    let task_queue = format!("jarvis-agents-loop-test-{suffix}");
    // JAR2-63: install the AgentStorage + ToolRegistry the
    // execute_tool activity body reaches for. Idempotent: the second
    // test in this binary reuses the same install.
    let _storage = ensure_installed();

    // JAR2-66 (main) added `ensure_installed()` below
    // (line ~171) which installs the process-wide `MemoryStorage` for
    // every per-tick `worker::agent_storage()` lookup the real
    // assemble_context body needs. JAR2-61's earlier `catch_unwind`-wrapped
    // install at this site is dropped on rebase — the helper covers it.

    // Install the scripted decision sequence BEFORE the worker starts —
    // by the time the first `decide_next_action` activity body fires,
    // the script is in place. Sequence covers the three cases the
    // ticket calls out: Idle → CallTools(3 parallel) → Retire.
    set_decision_script(vec![
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
        Decision::CallTools {
            calls: vec![
                ToolCall::new("tool_a", serde_json::json!({"i": 1}), ClaimSeed::new("s-a")),
                ToolCall::new("tool_b", serde_json::json!({"i": 2}), ClaimSeed::new("s-b")),
                ToolCall::new("tool_c", serde_json::json!({"i": 3}), ClaimSeed::new("s-c")),
            ],
        },
        Decision::Retire {
            reason: "workflow_loop test: scripted retire".into(),
        },
    ]);

    // JAR2-66: the `persist_retirement` activity body calls
    // `worker::agent_storage()`, which panics if no backend has been
    // installed. Pre-JAR2-66 the activity was a no-op stub so this test
    // worked without the install. Wire a `MemoryStorage` here so the
    // worker can serve the activity, and keep the typed Arc clone so
    // the post-workflow assertion below can `get` the resulting
    // `retirement.json`.
    let storage = ensure_installed();

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
    // Per-run agent prefix the workflow's `FsHandle` will scope to.
    // Embedding the run suffix keeps reruns of this test against the
    // same shared `MemoryStorage` from colliding on
    // `<prefix>retirement.json`.
    let agent_prefix = format!("graphs/g-loop-test/agents/a-loop-test-{suffix}");
    let driver_prefix = agent_prefix.clone();
    let driver_storage = storage.clone();
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
        )
        .await
    });

    // 60-second timeout matches JAR2-58's test ceiling; the workflow
    // completes in <2s on a healthy local server.
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
) -> Result<()> {
    // Build an `AgentInput` that scopes the per-agent FS to a per-run
    // prefix — the workflow body passes it into every activity input,
    // and JAR2-66's `persist_retirement` writes to
    // `<prefix>/retirement.json` (the file we assert exists below).
    let input = AgentInput {
        fs_handle: jarvis_temporal::workflow::FsHandle {
            prefix: agent_prefix.into(),
        },
        ..AgentInput::default()
    };
    let handle = client
        .start_workflow(
            AgentWorkflow::run,
            input,
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow)")?;

    // Workflow runs through the scripted Idle → CallTools(3) → Retire
    // sequence on its own; no client-side signals needed. Each tick
    // calls `decide_next_action` which pops the next scripted decision.
    let result: AgentResult = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result")?;
    eprintln!("workflow_loop: workflow returned {result:?}");
    let AgentResult::Retired { reason } = result;
    assert!(
        reason.contains("scripted retire"),
        "workflow returned wrong retire reason: {reason:?}"
    );

    // History assertions — count `ActivityTaskScheduled` events by
    // activity type. The scripted sequence guarantees:
    //
    // - exactly 3 `execute_tool` schedules (one per `ToolCall` in the
    //   CallTools batch);
    // - exactly 1 `persist_retirement` schedule (from the `Retire` arm).
    //
    // (assemble_context + decide_next_action each fire once per tick;
    // we don't assert on those because the loop semantics may schedule
    // more if the SDK retries or replays — the *parallel-batch* and
    // *retire* invariants are the load-bearing ones for JAR2-60.)
    // Use the SDK's `fetch_history` (paginates + returns flattened
    // events) rather than calling the raw gRPC API by hand.
    //
    // `WorkflowFetchHistoryOptions::default()` leaves `event_filter_type`
    // at the proto enum's zero value (Unspecified), which the server
    // reads as "give me close events only". The builder default
    // (`AllEvent`) is what we actually want for assertion purposes, so
    // build the options explicitly.
    let history = handle
        .fetch_history(WorkflowFetchHistoryOptions::builder().build())
        .await
        .context("fetch_history")?;
    eprintln!(
        "workflow_loop: fetched {} history events",
        history.events().len()
    );
    // Activity type names are namespaced by the `#[activities]` macro
    // as `AgentActivities::<fn_name>`, observed via the SDK's
    // registration shape. Match on the unqualified suffix so the
    // assertion stays robust if the macro ever drops the prefix.
    let mut execute_tool_schedules = 0usize;
    let mut persist_retirement_schedules = 0usize;
    let mut all_activity_type_names: Vec<String> = Vec::new();
    for ev in history.events() {
        if let Some(Attributes::ActivityTaskScheduledEventAttributes(a)) = &ev.attributes {
            if let Some(ty) = &a.activity_type {
                all_activity_type_names.push(ty.name.clone());
                let unqualified = ty.name.rsplit("::").next().unwrap_or(ty.name.as_str());
                match unqualified {
                    "execute_tool" => execute_tool_schedules += 1,
                    "persist_retirement" => persist_retirement_schedules += 1,
                    _ => {}
                }
            }
        }
    }
    eprintln!("workflow_loop: observed activity-type names: {all_activity_type_names:?}");
    eprintln!(
        "workflow_loop: execute_tool schedules = {execute_tool_schedules}, \
         persist_retirement schedules = {persist_retirement_schedules}"
    );
    assert_eq!(
        execute_tool_schedules, 3,
        "expected 3 parallel execute_tool activity invocations, got {execute_tool_schedules}"
    );
    assert!(
        persist_retirement_schedules >= 1,
        "expected at least 1 persist_retirement invocation, got {persist_retirement_schedules}"
    );

    // JAR2-66: the real `persist_retirement` activity body wrote
    // `<prefix>/retirement.json` via the shared MemoryStorage. Assert
    // the file lands with the scripted reason and a UTC-shaped
    // timestamp. Sourcing the bytes via the typed `Arc<MemoryStorage>`
    // we kept from the install — same backend the worker's activity
    // body wrote into through `worker::agent_storage()`.
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
    assert!(
        reason_on_disk.contains("scripted retire"),
        "retirement.json carries wrong reason: {reason_on_disk:?}"
    );
    let retired_at = v
        .get("retired_at")
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow::anyhow!("retirement.json missing retired_at"))?;
    // chrono RFC 3339 form. The activity body sources the timestamp
    // from `ctx.info().scheduled_time` (deterministic from workflow
    // history) so a worker retry produces byte-identical bytes —
    // JAR2-66 § "Why scheduled_time" in activities.rs.
    assert!(
        retired_at.ends_with("+00:00") || retired_at.ends_with('Z'),
        "retired_at not UTC-shaped: {retired_at:?}"
    );

    // JAR2-63: each successful tool call's evidence record must land
    // in the per-agent FS via `AgentFs::record_evidence`. With three
    // succeeding tools (tool_a/tool_b/tool_c, all AliasEchoTool) the
    // three sha256-keyed evidence files are distinct (each tool name
    // is part of the canonical-JSON hash, see `EvidenceId::new`).
    // Read directly via the storage backend rather than constructing
    // an `AgentFs` because `AgentFs::new_with_storage` would re-run
    // tail reconciliation — pointless work that obscures the
    // assertion.
    let page = storage
        .list("evidence/", None, usize::MAX)
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
        3,
        "expected 3 distinct evidence files (one per successful tool call), got {evidence_records:?}"
    );
    Ok(())
}

/// JAR2-63 partial-batch survival test (ticket § verification step 6).
/// Scripts `Decision::CallTools` with one failing tool + two succeeding
/// tools and asserts:
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
    let task_queue = format!("jarvis-agents-loop-pf-test-{suffix}");
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

    // Script: Idle → CallTools(one failing + two distinct succeeding
    // calls) → Retire. The succeeding calls use args distinct from
    // run_live_test's so their EvidenceIds (content-addressed on
    // (tool, args, result)) don't collide with the pre-run records.
    set_decision_script(vec![
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
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
        Decision::Retire {
            reason: "partial-failure test: scripted retire".into(),
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
    let handle = client
        .start_workflow(
            AgentWorkflow::run,
            AgentInput::default(),
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow) [partial]")?;

    let result: AgentResult = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result [partial]")?;
    let AgentResult::Retired { reason } = result;
    assert!(
        reason.contains("partial-failure test"),
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

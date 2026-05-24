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
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowFetchHistoryOptions,
    WorkflowGetResultOptions, WorkflowStartOptions,
};
use temporalio_common::protos::temporal::api::history::v1::history_event::Attributes;
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use jarvis_node::decision::{ClaimSeed, Decision, ToolCall};
use jarvis_node::storage::{AgentStorage, MemoryStorage};
use jarvis_temporal::activities::set_decision_script;
use jarvis_temporal::worker::{build_worker, install_agent_storage};
use jarvis_temporal::workflow::{agent_workflow_id, AgentInput, AgentResult, AgentWorkflow};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// Process-wide handle to the `MemoryStorage` the test worker uses for
/// every activity body that reaches for `worker::agent_storage()`
/// (JAR2-66's `persist_retirement` is the first one). Installed once
/// per test process via [`install_or_reuse_test_storage`] — the
/// underlying `OnceLock` in `worker.rs::AGENT_STORAGE` panics on
/// double-install by design.
static SHARED_TEST_STORAGE: OnceLock<Arc<MemoryStorage>> = OnceLock::new();

/// Install a process-wide `MemoryStorage` the first time this is
/// called and return a concretely-typed `Arc<MemoryStorage>` clone for
/// post-workflow key inspection. Subsequent calls (e.g. a second test
/// in this binary) reuse the same backend.
fn install_or_reuse_test_storage() -> Arc<MemoryStorage> {
    SHARED_TEST_STORAGE
        .get_or_init(|| {
            let storage: Arc<MemoryStorage> = Arc::new(MemoryStorage::new());
            install_agent_storage(storage.clone() as Arc<dyn AgentStorage>);
            storage
        })
        .clone()
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
async fn workflow_loop_runs_idle_then_calltools_then_retire() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping workflow_loop_runs_idle_then_calltools_then_retire; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }

    run_live_test().await.expect("live workflow_loop test");
}

async fn run_live_test() -> Result<()> {
    let suffix = run_suffix();
    let task_queue = format!("jarvis-agents-loop-test-{suffix}");

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
    let storage = install_or_reuse_test_storage();

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

    // mandate.json must NOT have been written by the retirement path
    // — `AgentFs::attach` (used by the activity body) skips it. The
    // workflow doesn't currently call `assemble_context` against
    // `new_with_storage` either (stubs ignore storage today), so the
    // key should be absent. Once JAR2-61's real `assemble_context`
    // lands and reads/writes mandate.json, this assertion will need to
    // move to a more targeted shape; flagging here so the next ticket
    // doesn't trip on it silently.
    let mandate_key = format!("{agent_prefix}/mandate.json");
    let mandate = storage
        .get(&mandate_key)
        .await
        .context("MemoryStorage::get on mandate.json")?;
    assert!(
        mandate.is_none(),
        "retirement path must not materialise mandate.json (got {} bytes)",
        mandate.as_ref().map(|b| b.len()).unwrap_or(0),
    );

    Ok(())
}

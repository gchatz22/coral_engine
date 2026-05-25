//! Stage 3.2 (JAR2-58) — `AgentWorkflow` live integration test.
//!
//! Env-gated behind `TEMPORAL_LIVE_TEST=1`. When the env var is absent
//! the test no-ops cleanly so the default `cargo test` stays hermetic.
//! When set, the test:
//!
//! 1. Connects to a Temporal Server at `TEMPORAL_ADDRESS` (default
//!    `http://localhost:7233`).
//! 2. Starts an in-process worker registering `AgentWorkflow` +
//!    `AgentActivities` on a unique task queue (suffixed by epoch-ms so
//!    parallel test runs don't collide).
//! 3. Starts `AgentWorkflow` with an `AgentInput::new_for_test(..)`
//!    (JAR2-80 dropped `Default` — see workflow.rs) under the
//!    URL-shaped workflow ID `graphs/<graph_id>/agents/<agent_id>`.
//! 4. Sends a `retire` signal after a short delay.
//! 5. Awaits the workflow result. The new JAR2-60 loop body short-
//!    circuits to the retirement path when the `retire` signal lands,
//!    so a successful `get_result` is proof that the loop drained the
//!    bucket and the `persist_retirement` activity completed.
//!
//! ## JAR2-60 adaptation (kept honest, not weakened)
//!
//! The JAR2-58 placeholder body terminated on its own (continue-as-new
//! once, then time out). The JAR2-60 loop runs indefinitely against the
//! stub `Decision::Idle { 1s }` fallback; it terminates only on a
//! `Decision::Retire` (from `decide_next_action`) or the `retire`
//! signal. The original test's intent ("wiring works end-to-end → the
//! workflow exits cleanly") is preserved by sending `retire`; the path
//! to exit shifts from "post-CAN timer ceiling" to "retire signal
//! observed by the loop's `wait_condition` predicate". See JAR2-60 PR
//! body for the conflict surfacing.
//!
//! ## SDK constraints (see `scratch/temporal_rust_sdk_smoke.md`)
//!
//! - The `Worker` is not `Send`, so it runs on the test's main task; the
//!   workflow driver runs on a `tokio::spawn`-ed task and uses the
//!   worker's `shutdown_handle()` to ask `worker.run()` to return after
//!   the assertion. Same shape as `temporal_smoke.rs::run` (§ 3.1).
//! - `Worker::new` returns `Box<dyn Error>` (not `Send + Sync`) — wrapped
//!   via `anyhow::anyhow!("{e}")` inside
//!   `jarvis_temporal::worker::build_worker` (§ 3.5).

use std::env;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use jarvis_node::agent_ref::{AgentId, GraphId};
use jarvis_node::decision::Decision;
use jarvis_node::storage::{AgentStorage, MemoryStorage};
use jarvis_node::tools::{EchoTool, ToolRegistry};
use jarvis_temporal::activities::set_decision_script;
use jarvis_temporal::worker::{build_worker, install_agent_storage, install_tool_registry};
use jarvis_temporal::workflow::{agent_workflow_id, AgentInput, AgentWorkflow};
use uuid::Uuid;

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// Suffix derived from epoch-millis so iterative test runs don't collide
/// on workflow IDs or task queues. Matches the smoke binary's pattern
/// (`temporal_smoke.rs::run_suffix`).
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
    let client_options = ClientOptions::new(namespace).build();
    let client = Client::new(connection, client_options).context("building Temporal client")?;
    Ok(client)
}

/// The live test. Runs an in-process worker + workflow client, sends a
/// `retire` signal, and asserts the workflow runs to completion via the
/// JAR2-60 loop's retirement-signal short-circuit. Multi-threaded
/// runtime because the worker and the driver task need to run
/// concurrently.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_skeleton_continues_as_new_and_exits() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping workflow_skeleton_continues_as_new_and_exits; set \
             TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }

    run_live_test().await.expect("live workflow_skeleton test");
}

/// JAR2-68: install a process-wide `AgentStorage` + `ToolRegistry` once
/// per process. Required because JAR2-66's `persist_retirement` activity
/// body (fired by the retire-signal short-circuit) reaches for
/// `agent_storage()`. Pre-JAR2-66 the stub didn't need this.
fn ensure_installed_for_skeleton_test() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let storage: Arc<dyn AgentStorage> = Arc::new(MemoryStorage::new());
        install_agent_storage(storage);
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(EchoTool)).expect("register EchoTool");
        install_tool_registry(Arc::new(reg));
    });
}

async fn run_live_test() -> Result<()> {
    ensure_installed_for_skeleton_test();
    // JAR2-68: install a long-Idle script so `decide_next_action` returns
    // without reaching for the (un-installed) live `Decide` impl. See
    // the matching note in `tests/signal_handlers.rs::run_live_test`.
    set_decision_script(vec![Decision::Idle {
        next_after: Duration::from_secs(60),
    }]);
    let suffix = run_suffix();
    let task_queue = format!("jarvis-agents-test-{suffix}");

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

    // Driver task: starts the workflow, awaits its result, asks the
    // worker to shut down. The worker stays on the main task because
    // `Worker` is not `Send`.
    let driver_task_queue = task_queue.clone();
    let driver = tokio::spawn(async move {
        let workflow_id = format!("{}-{suffix}", agent_workflow_id("g-test", "a-test"));
        eprintln!("workflow_skeleton: starting workflow_id={workflow_id} on {driver_task_queue}");
        let result = drive(client, &driver_task_queue, &workflow_id).await;
        // Always trigger shutdown so `worker.run()` returns and the
        // main task can exit, even if the driver errored.
        shutdown();
        result
    });

    // 60-second timeout: live tests against a local Temporal Server
    // typically complete in <2s; the longer ceiling catches stalls
    // (e.g. worker fails to register) without hanging CI forever.
    let worker_result = tokio::time::timeout(Duration::from_secs(60), worker.run())
        .await
        .map_err(|_| anyhow::anyhow!("worker.run() timed out (60s)"))?
        .map_err(|e| anyhow::anyhow!("worker.run() exited with error: {e}"));
    let driver_result = driver.await.context("driver task panicked")?;

    // Surface worker errors first — a worker death is the root cause
    // and the driver's failure would be downstream.
    worker_result?;
    driver_result?;
    Ok(())
}

async fn drive(client: Client, task_queue: &str, workflow_id: &str) -> Result<()> {
    // JAR2-80: `AgentInput::Default` was dropped; use the explicit
    // test constructor with synthetic identity. The skeleton smoke
    // doesn't read these fields — it just exercises the retire path.
    let input = AgentInput::new_for_test(
        GraphId::new(Uuid::new_v4()),
        AgentId::new(Uuid::new_v4()),
        "skeleton-test",
    );
    let handle = client
        .start_workflow(
            AgentWorkflow::run,
            input,
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow)")?;

    // The JAR2-60 loop body runs until either `Decision::Retire` or the
    // `retire` signal arrives. With the stubbed `decide_next_action`
    // returning `Idle { 1s }` (no script installed for this test), the
    // signal is what terminates the workflow. The short sleep gives
    // the worker time to register and start the first iteration; the
    // SDK queues signals that arrive before the workflow registers, so
    // strictly speaking we don't need it — but it keeps the eprintln
    // order legible during local debugging.
    tokio::time::sleep(Duration::from_millis(250)).await;
    handle
        .signal(
            AgentWorkflow::retire,
            "workflow_skeleton test: shutdown".to_string(),
            WorkflowSignalOptions::default(),
        )
        .await
        .context("signal AgentWorkflow::retire")?;
    eprintln!("workflow_skeleton: sent retire signal");

    // Receiving any `Ok` here is proof that the loop body observed the
    // retirement signal and the `persist_retirement` activity
    // completed. Type inference flows from the workflow's
    // `WorkflowResult<AgentResult>` return.
    let _result: jarvis_temporal::workflow::AgentResult = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result")?;
    eprintln!("workflow_skeleton: workflow {workflow_id} terminated cleanly via retire signal");
    Ok(())
}

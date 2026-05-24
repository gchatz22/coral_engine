//! Stage 3.2 (JAR2-58) — `AgentWorkflow` live integration test.
//!
//! Env-gated behind `TEMPORAL_LIVE_TEST=1`. When the env var is absent
//! the test no-ops cleanly so the default `cargo test` stays hermetic.
//! When set, the test:
//!
//! 1. Connects to a Temporal Server at `TEMPORAL_ADDRESS` (default
//!    `http://localhost:7233`).
//! 2. Starts an in-process worker registering `AgentWorkflow` +
//!    `NoopActivities` on a unique task queue (suffixed by epoch-ms so
//!    parallel test runs don't collide).
//! 3. Starts `AgentWorkflow` with `AgentInput::default()` (carryover =
//!    None) under the URL-shaped workflow ID
//!    `graphs/<graph_id>/agents/<agent_id>` (suffixed with epoch-ms to
//!    avoid `WorkflowExecutionAlreadyStarted` between iterative runs).
//! 4. Awaits the workflow result. The body returns `Ok(_)` ONLY on the
//!    continue-as-new run, so any successful `get_result` is proof that
//!    continue-as-new fired at least once and the post-CAN run
//!    terminated cleanly. (No history-event query needed — the
//!    if/else in `AgentWorkflow::run` makes the invariant structural.)
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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use jarvis_temporal::worker::build_worker;
use jarvis_temporal::workflow::{agent_workflow_id, AgentInput, AgentWorkflow};

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

/// The live test. Runs an in-process worker + workflow client and
/// asserts the workflow runs to completion (which structurally proves
/// continue-as-new fired). Multi-threaded runtime because the worker
/// and the driver task need to run concurrently.
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

async fn run_live_test() -> Result<()> {
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
    let handle = client
        .start_workflow(
            AgentWorkflow::run,
            AgentInput::default(),
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow)")?;

    // The body returns `Ok(_)` only on the continue-as-new run; the
    // first run terminates via `continue_as_new` (which returns
    // `Err(WorkflowTermination::...)` to the SDK). So receiving any
    // `Ok` here is structural proof that continue-as-new fired and the
    // post-CAN run terminated cleanly.
    // Type inference flows from the workflow's `WorkflowResult<AgentResult>`
    // return — `get_result()` takes no generic argument.
    let _result: jarvis_temporal::workflow::AgentResult = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result")?;
    eprintln!("workflow_skeleton: workflow {workflow_id} terminated cleanly post-CAN");
    Ok(())
}

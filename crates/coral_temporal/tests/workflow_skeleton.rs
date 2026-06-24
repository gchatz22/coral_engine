//! `AgentWorkflow` live integration test.
//!
//! Env-gated behind `TEMPORAL_LIVE_TEST=1`; no-ops without it. Starts a
//! worker registering `AgentWorkflow` + `AgentActivities`, starts the
//! workflow, sends `retire`, awaits the result. The loop body short-
//! circuits to the retirement path when the signal lands; a successful
//! `get_result` is proof the loop drained the bucket and
//! `persist_retirement` completed.

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

use coral_node::agent_ref::{AgentId, GraphId};
use coral_node::decision::Decision;
use coral_node::storage::{AgentStorage, MemoryStorage};
use coral_node::tools::{EchoTool, ToolRegistry};
use coral_temporal::activities::set_decision_script;
use coral_temporal::worker::{build_worker, install_agent_storage, install_tool_registry};
use coral_temporal::workflow::{agent_workflow_id, AgentInput, AgentWorkflow};
use uuid::Uuid;

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// Suffix derived from epoch-millis so iterative test runs don't collide
/// on workflow IDs or task queues.
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
/// loop's retirement-signal short-circuit. Multi-threaded runtime
/// because the worker and the driver task need to run concurrently.
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

/// Install a process-wide `AgentStorage` + `ToolRegistry` once per
/// process. Required because the `persist_retirement` activity body
/// (fired by the retire-signal short-circuit) reaches for
/// `agent_storage()`.
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
    // Install a long-Idle script so `decide_next_action` returns
    // without reaching for the (un-installed) live `Decide` impl.
    set_decision_script(vec![Decision::Idle {
        next_after: Duration::from_secs(60),
    }]);
    let suffix = run_suffix();
    let task_queue = format!("coral-agents-test-{suffix}");

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

    // The loop body runs until the `retire` signal (or the `max_ticks`
    // cap) arrives. The signal terminates the workflow; the short sleep
    // gives the worker time to register and start the first iteration so
    // the eprintln order is legible during local debugging.
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
    let _result: coral_temporal::workflow::AgentResult = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result")?;
    eprintln!("workflow_skeleton: workflow {workflow_id} terminated cleanly via retire signal");
    Ok(())
}

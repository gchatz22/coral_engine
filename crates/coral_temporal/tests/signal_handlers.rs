//! `AgentWorkflow` signal/update integration test.
//!
//! Env-gated behind `TEMPORAL_LIVE_TEST=1`; no-ops without it. Sends one
//! signal of each variant (`external_signal`, `human_override`,
//! `mandate_update`, `retire`), then calls `inspect_state` and asserts
//! the `retirement_request` landed and the three non-retire signals
//! were observed via the cumulative counters. The workflow exits via
//! the retirement short-circuit. Capped at 60s for stall detection.

use std::env;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowExecuteUpdateOptions,
    WorkflowGetResultOptions, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use coral_node::agent_ref::{AgentId, GraphId};
use coral_node::decision::Decision;
use coral_node::storage::{AgentStorage, MemoryStorage};
use coral_node::tools::{EchoTool, ToolRegistry};
use coral_node::trigger::{HumanOp, MandatePatch, Trigger};
use coral_temporal::activities::set_decision_script;
use coral_temporal::worker::{build_worker, install_agent_storage, install_tool_registry};
use coral_temporal::workflow::{agent_workflow_id, AgentInput, AgentSnapshot, AgentWorkflow};
use uuid::Uuid;

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signal_handlers_round_trip_and_inspect_state_returns_snapshot() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping signal_handlers_round_trip_and_inspect_state_returns_snapshot; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }

    run_live_test().await.expect("live signal_handlers test");
}

/// Install a process-wide `AgentStorage` + `ToolRegistry` once per
/// process. Required because `persist_retirement` and
/// `append_decision_log` activity bodies reach for `agent_storage()` —
/// even on the retire-signal short-circuit path, `persist_retirement`
/// runs before the workflow returns.
fn ensure_installed_for_signal_handlers_test() {
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
    ensure_installed_for_signal_handlers_test();
    // Install a long-Idle script so `decide_next_action` returns without
    // falling through to the (un-installed) live `Decide` impl. The
    // retire-signal short-circuit fires before `decide_next_action` most
    // of the time, but `INITIAL_NEXT_WAKE` (1ms) can let one tick sneak
    // in between worker start and the test's signals; that tick's
    // `decide_next_action` would otherwise panic on `decide_impl()`.
    set_decision_script(vec![Decision::Idle {
        next_after: Duration::from_secs(60),
    }]);
    let suffix = run_suffix();
    let task_queue = format!("coral-agents-signal-test-{suffix}");

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
            "{}-{suffix}",
            agent_workflow_id("g-signal-test", "a-signal-test")
        );
        eprintln!("signal_handlers: starting workflow_id={workflow_id} on {driver_task_queue}");
        let result = drive(client, &driver_task_queue, &workflow_id).await;
        // Always trigger shutdown so `worker.run()` returns and the
        // main task can exit, even if the driver errored.
        shutdown();
        result
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

async fn drive(client: Client, task_queue: &str, workflow_id: &str) -> Result<()> {
    let input = AgentInput::new_for_test(
        GraphId::new(Uuid::new_v4()),
        AgentId::new(Uuid::new_v4()),
        "signal-handlers-test",
    );
    let handle = client
        .start_workflow(
            AgentWorkflow::run,
            input,
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow)")?;

    // ---- Send all four signals as quickly as possible. The loop body
    //      drains non-retire buckets at every tick; by the time the
    //      inspect update lands, the loop will have observed the
    //      retirement_request (which the snapshot reads BEFORE the
    //      short-circuit returns, because `inspect_state` is a sync
    //      update racing the workflow task) and short-circuited.
    let external_trigger = Trigger::External {
        kind: "test_signal".into(),
        payload: serde_json::json!({"hello": "world"}),
    };
    handle
        .signal(
            AgentWorkflow::external_signal,
            external_trigger.clone(),
            WorkflowSignalOptions::default(),
        )
        .await
        .context("signal AgentWorkflow::external_signal")?;
    eprintln!("signal_handlers: sent external_signal");

    let op = HumanOp::new(serde_json::json!({"action": "pause"}));
    handle
        .signal(
            AgentWorkflow::human_override,
            op.clone(),
            WorkflowSignalOptions::default(),
        )
        .await
        .context("signal AgentWorkflow::human_override")?;
    eprintln!("signal_handlers: sent human_override");

    let patch = MandatePatch::new(serde_json::json!({"model": "gpt-x"}));
    handle
        .signal(
            AgentWorkflow::mandate_update,
            patch.clone(),
            WorkflowSignalOptions::default(),
        )
        .await
        .context("signal AgentWorkflow::mandate_update")?;
    eprintln!("signal_handlers: sent mandate_update");

    handle
        .signal(
            AgentWorkflow::retire,
            "test asked".to_string(),
            WorkflowSignalOptions::default(),
        )
        .await
        .context("signal AgentWorkflow::retire")?;
    eprintln!("signal_handlers: sent retire");

    // ---- inspect_state: assert the retirement_request landed AND the
    //      three non-retire signals were observed via the cumulative
    //      counters. The pending_* bucket counts may race the loop's
    //      drain and so are NOT asserted exactly — the cumulative
    //      counters are the stable view of "did the signal land?",
    //      incremented by `drain_buckets` for every drained batch.
    //
    //      The update may race the workflow's post-retirement exit; in
    //      practice the update lands first because signals + updates
    //      serialize through the workflow task queue.
    let snap_after_retire: AgentSnapshot = handle
        .execute_update(
            AgentWorkflow::inspect_state,
            (),
            WorkflowExecuteUpdateOptions::default(),
        )
        .await
        .context("execute_update inspect_state (post-retire)")?;
    eprintln!("signal_handlers: post-retire snapshot = {snap_after_retire:?}");
    assert_eq!(
        snap_after_retire.retirement_request.as_deref(),
        Some("test asked"),
        "retire signal should record reason on retirement_request"
    );
    // Cumulative counters: each non-retire signal was observed at
    // least once. Comparing `>= 1` instead of `== 1` because the loop
    // may have run multiple ticks between signal arrival and inspect
    // (`INITIAL_NEXT_WAKE` is 1ms — plenty of room for several
    // iterations) and a re-armed empty bucket might bump the counter
    // by 0 each time but the first drain captures the signal.
    assert!(
        snap_after_retire.cumulative_triggers_observed >= 1,
        "external_signal should be observed at least once: {snap_after_retire:?}"
    );
    assert!(
        snap_after_retire.cumulative_human_ops_observed >= 1,
        "human_override should be observed at least once: {snap_after_retire:?}"
    );
    assert!(
        snap_after_retire.cumulative_mandate_patches_observed >= 1,
        "mandate_update should be observed at least once: {snap_after_retire:?}"
    );

    // ---- Workflow exits cleanly via the retirement arm. Matching on
    //      `Retired { reason }` proves the loop body went through
    //      `persist_retirement` and returned with the right reason.
    let result = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result")?;
    let coral_temporal::workflow::AgentResult::Retired { reason } = result;
    assert_eq!(
        reason, "test asked",
        "workflow should return the retire signal's reason verbatim"
    );
    eprintln!("signal_handlers: workflow {workflow_id} exited cleanly after retire");
    Ok(())
}

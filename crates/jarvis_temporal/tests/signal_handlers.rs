//! Stage 3.3 (JAR2-59) — `AgentWorkflow` signal/update integration test.
//!
//! Env-gated behind `TEMPORAL_LIVE_TEST=1`. When the env var is absent
//! the test no-ops cleanly so the default `cargo test` stays hermetic.
//! When set, the test:
//!
//! 1. Starts a Temporal worker registering `AgentWorkflow`.
//! 2. Starts an `AgentWorkflow` instance with `AgentInput::default()`
//!    under a unique workflow ID (epoch-ms suffixed).
//! 3. Lets the workflow continue-as-new (the post-CAN run waits up to
//!    `POST_CAN_RETIREMENT_WAIT` for a `retire` signal).
//! 4. Sends one signal of each type: `external_signal(Trigger)`,
//!    `human_override(HumanOp)`, `mandate_update(MandatePatch)`.
//! 5. Calls `inspect_state` and asserts the returned snapshot reflects
//!    all three signals (counts of 1 each, no retirement yet).
//! 6. Sends `retire(String)` and re-inspects, asserting the retirement
//!    reason landed.
//! 7. Awaits `get_result` — the workflow exits cleanly because the
//!    retire signal fires the `wait_condition` arm of the post-CAN
//!    `select!`.
//!
//! Each step is asserted independently so a failure points at the
//! specific signal arm that didn't land.
//!
//! ## Timing
//!
//! The post-CAN run has a `POST_CAN_RETIREMENT_WAIT` ceiling (10s in
//! `workflow.rs`). The test sends all signals + inspects + sends retire
//! within that window. Local Temporal Server completes the full flow in
//! well under 5s; we cap the entire test at 60s for stall detection.
//!
//! ## SDK constraints (see `scratch/temporal_rust_sdk_smoke.md` § 3)
//!
//! - `Worker` is not `Send` → worker stays on the test's main task; the
//!   driver runs on a `tokio::spawn`-ed task. Same shape as JAR2-58's
//!   `workflow_skeleton.rs`.
//! - `Worker::new` returns `Box<dyn Error>` (not `Send + Sync`) — wrapped
//!   via `anyhow::anyhow!("{e}")` inside `worker::build_worker`.
//! - Updates are sent via `handle.execute_update(...)`; signals via
//!   `handle.signal(...)`. Both round-trip the typed payload through
//!   Temporal's payload codec, so all signal/update types need
//!   `Serialize + Deserialize` (asserted hermetically in
//!   `workflow.rs` tests).

use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowExecuteUpdateOptions,
    WorkflowGetResultOptions, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use jarvis_node::trigger::{HumanOp, MandatePatch, Trigger};
use jarvis_temporal::worker::build_worker;
use jarvis_temporal::workflow::{agent_workflow_id, AgentInput, AgentSnapshot, AgentWorkflow};

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

async fn run_live_test() -> Result<()> {
    let suffix = run_suffix();
    let task_queue = format!("jarvis-agents-signal-test-{suffix}");

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
    let handle = client
        .start_workflow(
            AgentWorkflow::run,
            AgentInput::default(),
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow)")?;

    // ---- Send one signal of each non-retire type. The first-run
    //      body fires `continue_as_new` after a 1ms timer, so by the
    //      time the signals arrive the workflow is on its post-CAN run
    //      and waiting in the retirement `select!`. Temporal queues
    //      signals delivered during the CAN gap; we don't need to time
    //      this precisely.
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

    // ---- inspect_state: assert all three buckets reflect the signals
    //      and retirement has not yet been requested.
    let snap_before_retire: AgentSnapshot = handle
        .execute_update(
            AgentWorkflow::inspect_state,
            (),
            WorkflowExecuteUpdateOptions::default(),
        )
        .await
        .context("execute_update inspect_state (pre-retire)")?;
    eprintln!("signal_handlers: pre-retire snapshot = {snap_before_retire:?}");
    assert_eq!(
        snap_before_retire.pending_triggers_count, 1,
        "external_signal should push exactly 1 trigger onto pending_triggers"
    );
    assert_eq!(
        snap_before_retire.pending_human_ops_count, 1,
        "human_override should push exactly 1 op onto pending_human_ops"
    );
    assert_eq!(
        snap_before_retire.pending_mandate_patches_count, 1,
        "mandate_update should push exactly 1 patch onto pending_mandate_patches"
    );
    assert!(
        snap_before_retire.retirement_request.is_none(),
        "retirement_request should be None until the retire signal lands"
    );

    // ---- Send retire. The post-CAN body's `wait_condition` arm fires
    //      and the workflow exits. We re-inspect first to assert the
    //      reason was recorded; the `get_result` below proves the body
    //      observed the bucket and returned.
    handle
        .signal(
            AgentWorkflow::retire,
            "test asked".to_string(),
            WorkflowSignalOptions::default(),
        )
        .await
        .context("signal AgentWorkflow::retire")?;
    eprintln!("signal_handlers: sent retire");

    // Retire signal delivery and post-CAN exit race the next
    // `inspect_state`; in practice the update lands first because the
    // signal handler and update handler are serialized through the
    // workflow's task queue. If the workflow exits before this update
    // resolves we get an `execute_update` error — that's a real bug, so
    // bubble it up (don't swallow).
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

    // ---- Workflow exits cleanly via the retirement arm.
    let _result: jarvis_temporal::workflow::AgentResult = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result")?;
    eprintln!("signal_handlers: workflow {workflow_id} exited cleanly after retire");
    Ok(())
}

//! Stage 3.3 (JAR2-59) — `AgentWorkflow` signal/update integration test.
//!
//! Env-gated behind `TEMPORAL_LIVE_TEST=1`. When the env var is absent
//! the test no-ops cleanly so the default `cargo test` stays hermetic.
//! When set, the test:
//!
//! 1. Starts a Temporal worker registering `AgentWorkflow`.
//! 2. Starts an `AgentWorkflow` instance with `AgentInput::default()`
//!    under a unique workflow ID (epoch-ms suffixed).
//! 3. Sends a `retire` signal. The JAR2-60 loop body wakes on
//!    `retirement_request.is_some()` (or a per-tick timer) and
//!    short-circuits to the retirement path before draining any
//!    buckets, so subsequent inspect_state can still observe the
//!    retirement_request landing.
//! 4. Sends one signal of each non-retire type:
//!    `external_signal(Trigger)`, `human_override(HumanOp)`,
//!    `mandate_update(MandatePatch)`. These race the loop's drain —
//!    we don't assert bucket counts here because the JAR2-60 loop
//!    drains non-retire buckets at every tick (see JAR2-60 PR body
//!    "JAR2-59 conflict surfacing").
//! 5. Calls `inspect_state` and asserts the retirement_request landed.
//! 6. Awaits `get_result` — the workflow exits cleanly via the
//!    retirement short-circuit.
//!
//! ## JAR2-60 adaptation
//!
//! Before JAR2-60, the workflow body was a single `wait_condition + timer`
//! race; signal payloads stayed in their buckets indefinitely, and
//! inspect_state observing `pending_triggers_count: 1` was meaningful.
//! After JAR2-60, the loop drains those buckets at the top of every
//! tick. The test's "did the signal land?" intent is preserved by
//! asserting the cumulative counters [`AgentSnapshot::cumulative_triggers_observed`]
//! / `cumulative_human_ops_observed` / `cumulative_mandate_patches_observed`,
//! which `drain_buckets` increments by the drained batch size. Each
//! signal is observed by the loop at some point — whether the bucket
//! was already drained by the time inspect_state landed is irrelevant
//! to the cumulative view.
//!
//! ## Timing
//!
//! The JAR2-60 loop short-circuits on the first wake observing
//! `retirement_request.is_some()`. We cap the entire test at 60s for
//! stall detection.
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
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowExecuteUpdateOptions,
    WorkflowGetResultOptions, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use jarvis_node::decision::Decision;
use jarvis_node::storage::{AgentStorage, MemoryStorage};
use jarvis_node::tools::{EchoTool, ToolRegistry};
use jarvis_node::trigger::{HumanOp, MandatePatch, Trigger};
use jarvis_temporal::activities::set_decision_script;
use jarvis_temporal::worker::{build_worker, install_agent_storage, install_tool_registry};
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

/// JAR2-68: install a process-wide `AgentStorage` + `ToolRegistry` once
/// per process. Required because JAR2-66's `persist_retirement` and
/// JAR2-68's `append_decision_log` activity bodies reach for
/// `agent_storage()` — even on the retire-signal short-circuit path,
/// `persist_retirement` runs before the workflow returns. Pre-JAR2-66
/// the test didn't need this because the activity body was a no-op stub.
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
    // JAR2-68: install a long-Idle script so `decide_next_action` returns
    // without falling through to the (un-installed) live `Decide` impl.
    // The retire-signal short-circuit fires before `decide_next_action`
    // most of the time, but `INITIAL_NEXT_WAKE` (1ms) can let one tick
    // sneak in between worker start and the test's signals; that tick's
    // `decide_next_action` would otherwise panic on `decide_impl()`.
    // A single Idle{60s} is enough — the retire-signal fires within
    // tens of ms in practice and the second tick never runs.
    set_decision_script(vec![Decision::Idle {
        next_after: Duration::from_secs(60),
    }]);
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

    // ---- Send all four signals as quickly as possible. The JAR2-60
    //      loop body drains non-retire buckets at every tick; by the
    //      time the inspect update lands, the loop will have observed
    //      the retirement_request (which the snapshot reads BEFORE the
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
    //      counters are the JAR2-60-stable view of "did the signal
    //      land?" (incremented by `drain_buckets` for every drained
    //      batch).
    //
    //      The update may race the workflow's post-retirement exit; in
    //      practice the update lands first because signals + updates
    //      serialize through the workflow task queue. If the workflow
    //      exits before the update resolves the SDK returns a typed
    //      error — bubble it up.
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

    // ---- Workflow exits cleanly via the retirement arm. AgentResult is
    //      now a tagged enum; matching on `Retired { reason }` proves
    //      the loop body went through `persist_retirement` and returned
    //      with the right reason.
    let result = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result")?;
    let jarvis_temporal::workflow::AgentResult::Retired { reason } = result;
    assert_eq!(
        reason, "test asked",
        "workflow should return the retire signal's reason verbatim"
    );
    eprintln!("signal_handlers: workflow {workflow_id} exited cleanly after retire");
    Ok(())
}

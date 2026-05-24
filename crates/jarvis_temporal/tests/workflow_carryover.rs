//! Stage 3.11 (JAR2-67) — live integration test that the workflow's
//! typed [`Carryover`] survives a real `continue_as_new` boundary
//! against a running Temporal Server.
//!
//! Env-gated behind `TEMPORAL_LIVE_TEST=1` (same gate as the JAR2-60
//! `workflow_loop` tests). Skipped on default `cargo test` runs because
//! it needs `temporal server start-dev`.
//!
//! ## What the test pins
//!
//! 1. **CAN actually fires.** The SDK's
//!    [`temporalio_sdk::WorkflowContext::continue_as_new_suggested`] flag
//!    is server-driven (the `Workflow Activation` carries it on each
//!    task); the [`temporalio_sdk::ContinueAsNewOptions`] type has no
//!    history-threshold knob in SDK v0.4.0 (verified by inspecting the
//!    builder). So the test triggers CAN naturally by running enough
//!    `Decision::Idle` ticks for the server-side default suggested
//!    threshold (~4096 history events) to fire.
//!
//! 2. **Cumulative counter bridges the CAN boundary.** A `Trigger` is
//!    sent via the `external_signal` signal before the workflow starts
//!    consuming the script. The post-CAN run's snapshot
//!    (via the `inspect_state` update) must show
//!    `cumulative_triggers_observed >= 1`. Because the SDK's
//!    `get_result` follows runs, by the time we observe the workflow
//!    is on its second-or-later run, the counter must reflect every
//!    signal seen on every prior run.
//!
//! 3. **The workflow still completes via `Retire`** after CAN. The
//!    scripted decision sequence ends in `Decision::Retire`, and
//!    `get_result()` returns `AgentResult::Retired`.
//!
//! 4. **A real `WorkflowExecutionContinuedAsNew` event landed on the
//!    first run's history.** Captured via a fresh handle scoped to the
//!    original `run_id` we held onto from `start_workflow`.
//!
//! ## Why iteration count and not a CAN threshold knob
//!
//! `ContinueAsNewOptions` in temporalio-sdk 0.4.0 exposes
//! `workflow_type`, `task_queue`, timeouts, memo, headers,
//! search_attributes, retry_policy, versioning_intent — and nothing
//! else. The suggested-CAN threshold is owned by Temporal Server and
//! defaults to ~4096 events on `start-dev`. We pay this cost in test
//! time (~30s on a healthy laptop) rather than mocking the SDK flag.
//! Documented in the PR body too so JAR2-68 doesn't re-discover.

use std::env;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, NamespacedClient, WorkflowExecutionInfo,
    WorkflowFetchHistoryOptions, WorkflowGetResultOptions, WorkflowSignalOptions,
    WorkflowStartOptions,
};
use temporalio_common::protos::temporal::api::history::v1::history_event::Attributes;
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use jarvis_node::decision::Decision;
use jarvis_node::storage::{AgentStorage, MemoryStorage};
use jarvis_node::trigger::Trigger;
use jarvis_temporal::activities::set_decision_script;
use jarvis_temporal::worker::{build_worker, install_agent_storage};
use jarvis_temporal::workflow::{agent_workflow_id, AgentInput, AgentResult, AgentWorkflow};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// Number of `Decision::Idle` decisions to script before the final
/// `Decision::Retire`. Each iteration costs ~6-10 history events
/// (assemble_context start/scheduled/completed + decide_next_action
/// triple + timer events + maybe-workflow-task events); the server
/// default suggested-CAN threshold is ~4096 events. 600 ticks puts
/// us comfortably past the threshold while keeping the test under
/// 60s on a healthy local Temporal Server.
const IDLE_TICKS_TO_FORCE_CAN: usize = 600;

/// Per-loop idle. 1ms keeps the test from blocking on real wall-clock
/// time; the workflow's `next_wake` only needs to be non-zero so the
/// SDK's timer round-trip generates history events.
const IDLE_NEXT_AFTER: Duration = Duration::from_millis(1);

static SHARED_STORAGE: OnceLock<Arc<MemoryStorage>> = OnceLock::new();
static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_installed() -> Arc<MemoryStorage> {
    INIT.call_once(|| {
        let storage: Arc<MemoryStorage> = Arc::new(MemoryStorage::new());
        SHARED_STORAGE
            .set(Arc::clone(&storage))
            .expect("SHARED_STORAGE set exactly once");
        let dyn_storage: Arc<dyn AgentStorage> = storage;
        install_agent_storage(dyn_storage);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_continue_as_new_bridges_carryover() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping workflow_continue_as_new_bridges_carryover; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    run_live_can_test()
        .await
        .expect("live continue_as_new test");
}

async fn run_live_can_test() -> Result<()> {
    let suffix = run_suffix();
    let task_queue = format!("jarvis-agents-can-test-{suffix}");
    let agent_prefix = format!("graphs/g-can-test/agents/a-can-test-{suffix}");

    // Shared per-test storage; the workflow's `persist_retirement`
    // activity writes `<prefix>/retirement.json`. No FS planting
    // needed — the script never emits `EmitOutput`.
    let _storage = ensure_installed();

    // Script: many `Decision::Idle` to drive history past the
    // server's suggested-CAN threshold, then one `Decision::Retire`.
    // Using `set_decision_script` with `IDLE_TICKS_TO_FORCE_CAN + 1`
    // entries is safe even if CAN fires mid-script (the script is a
    // process-wide queue read by the `decide_next_action` activity;
    // the post-CAN run continues popping from the same queue).
    let mut script: Vec<Decision> = (0..IDLE_TICKS_TO_FORCE_CAN)
        .map(|_| Decision::Idle {
            next_after: IDLE_NEXT_AFTER,
        })
        .collect();
    script.push(Decision::Retire {
        reason: "carryover live test: scripted retire".into(),
    });
    set_decision_script(script);

    // Build the Temporal worker + client.
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
    let driver_client = client.clone();
    let driver = tokio::spawn(async move {
        let workflow_id = format!(
            "{}-can-{suffix}",
            agent_workflow_id("g-can-test", "a-can-test")
        );
        eprintln!("workflow_can: starting workflow_id={workflow_id} on {driver_task_queue}");
        struct ShutdownGuard<F: Fn()>(F);
        impl<F: Fn()> Drop for ShutdownGuard<F> {
            fn drop(&mut self) {
                (self.0)();
            }
        }
        let _guard = ShutdownGuard(shutdown);
        drive_can(
            driver_client,
            &driver_task_queue,
            &workflow_id,
            &agent_prefix,
        )
        .await
    });

    // Generous timeout — 600 idle ticks at 1ms wake + activity round-
    // trips runs ~25-40s on a healthy laptop. 120s gives 3x headroom
    // for CI variability.
    let worker_result = tokio::time::timeout(Duration::from_secs(120), worker.run())
        .await
        .map_err(|_| anyhow::anyhow!("worker.run() timed out (120s)"))?
        .map_err(|e| anyhow::anyhow!("worker.run() exited with error: {e}"));
    let driver_result = driver.await.context("driver task panicked")?;

    worker_result?;
    driver_result?;
    Ok(())
}

async fn drive_can(
    client: Client,
    task_queue: &str,
    workflow_id: &str,
    _agent_prefix: &str,
) -> Result<()> {
    // Start the workflow on its first run.
    let handle = client
        .start_workflow(
            AgentWorkflow::run,
            AgentInput::default(),
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow)")?;

    // Capture the first run's run_id so we can later fetch its
    // history to confirm a CAN event landed there. `get_result()`
    // follows runs by default, so post-completion the handle points
    // at the final post-CAN run — we'd lose visibility on the
    // boundary event without this capture.
    let first_run_id = handle
        .run_id()
        .ok_or_else(|| anyhow::anyhow!("start_workflow didn't return a run_id"))?
        .to_string();
    eprintln!("workflow_can: first_run_id={first_run_id}");

    // Pre-CAN signal: send one `external_signal`. The signal handler
    // increments `cumulative_triggers_observed` at receipt time.
    // After CAN, the counter must still report >= 1 in the snapshot
    // — that's the load-bearing bridging assertion.
    handle
        .signal(
            AgentWorkflow::external_signal,
            Trigger::External {
                kind: "carryover-test".into(),
                payload: serde_json::json!({"sent": "pre-CAN"}),
            },
            WorkflowSignalOptions::default(),
        )
        .await
        .context("external_signal pre-CAN")?;

    // Drive the workflow to completion. `get_result` follows the
    // continue-as-new chain by default (see SDK
    // `WorkflowGetResultOptions::follow_runs`), so we await the
    // post-CAN final run's `AgentResult::Retired`.
    let result: AgentResult = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result (post-CAN)")?;
    eprintln!("workflow_can: workflow returned {result:?}");
    let AgentResult::Retired { reason } = result;
    assert!(
        reason.contains("carryover live test"),
        "wrong retire reason: {reason:?}"
    );

    // Assert the first run actually emitted a CAN event. Build a
    // run-scoped handle by hand — the SDK's
    // `Client::get_workflow_handle` doesn't take a run_id (it's
    // a `WorkflowExecutionInfo { run_id: None, .. }` constructor),
    // so we go through the info builder directly.
    let first_run_info = WorkflowExecutionInfo {
        namespace: client.namespace(),
        workflow_id: workflow_id.to_string(),
        run_id: Some(first_run_id.clone()),
        first_execution_run_id: Some(first_run_id.clone()),
    };
    let first_run_handle = first_run_info.bind_untyped(client.clone());
    let first_history = first_run_handle
        .fetch_history(WorkflowFetchHistoryOptions::builder().build())
        .await
        .context("fetch_history for first run")?;
    let can_events = first_history
        .events()
        .iter()
        .filter(|ev| {
            matches!(
                ev.attributes,
                Some(Attributes::WorkflowExecutionContinuedAsNewEventAttributes(
                    _
                ))
            )
        })
        .count();
    eprintln!(
        "workflow_can: first run had {} history events, {can_events} CAN events",
        first_history.events().len()
    );
    assert_eq!(
        can_events, 1,
        "expected exactly 1 WorkflowExecutionContinuedAsNew on the first run, got {can_events}"
    );

    // ----------------------------------------------------------
    // Bridging-counter assertion. The final post-CAN run has
    // already completed (`Retired`); but `inspect_state` is a
    // **completed-workflow-safe update** in the SDK only if
    // the workflow's still open. Once retired the workflow is
    // closed and `start_workflow_update` would fail. Instead,
    // walk the full history chain by run-id, find the **last
    // WorkflowExecutionStarted event with carryover input**, and
    // assert the carryover's `cumulative_triggers_observed >= 1`.
    //
    // (The simpler approach — `inspect_state` mid-flight — is
    // race-prone: we don't know exactly when CAN fires, so we'd
    // have to poll. Reading the post-CAN run's start input is
    // deterministic and replay-stable.)
    let post_can_run_id = walk_to_post_can_run(&client, workflow_id, &first_run_id).await?;
    eprintln!("workflow_can: post-CAN run_id={post_can_run_id}");
    let counter =
        read_carryover_cumulative_triggers(&client, workflow_id, &post_can_run_id).await?;
    eprintln!("workflow_can: post-CAN carryover.cumulative_triggers_observed = {counter}");
    assert!(
        counter >= 1,
        "post-CAN carryover must reflect the pre-CAN trigger signal: got {counter}"
    );
    Ok(())
}

/// Follow the continue-as-new chain from `first_run_id` to the next
/// run. The chain is single-step today (the test's history budget
/// fires CAN exactly once); generalising to multi-CAN is straightforward
/// but unneeded.
async fn walk_to_post_can_run(
    client: &Client,
    workflow_id: &str,
    first_run_id: &str,
) -> Result<String> {
    let info = WorkflowExecutionInfo {
        namespace: client.namespace(),
        workflow_id: workflow_id.to_string(),
        run_id: Some(first_run_id.to_string()),
        first_execution_run_id: Some(first_run_id.to_string()),
    };
    let handle = info.bind_untyped(client.clone());
    let history = handle
        .fetch_history(WorkflowFetchHistoryOptions::builder().build())
        .await
        .context("fetch_history while walking to post-CAN run")?;
    for ev in history.events() {
        if let Some(Attributes::WorkflowExecutionContinuedAsNewEventAttributes(attrs)) =
            &ev.attributes
        {
            if !attrs.new_execution_run_id.is_empty() {
                return Ok(attrs.new_execution_run_id.clone());
            }
        }
    }
    Err(anyhow::anyhow!(
        "no WorkflowExecutionContinuedAsNew event found on first run history"
    ))
}

/// Read the `WorkflowExecutionStarted` event of a run and extract the
/// `cumulative_triggers_observed` field from the (typed) carryover
/// embedded in the run's start input.
///
/// The workflow input is `AgentInput`, encoded as a JSON payload on
/// the wire; the `Carryover` is nested under `carryover`. We decode
/// via `serde_json` directly off the payload bytes.
async fn read_carryover_cumulative_triggers(
    client: &Client,
    workflow_id: &str,
    run_id: &str,
) -> Result<u64> {
    let info = WorkflowExecutionInfo {
        namespace: client.namespace(),
        workflow_id: workflow_id.to_string(),
        run_id: Some(run_id.to_string()),
        first_execution_run_id: Some(run_id.to_string()),
    };
    let handle = info.bind_untyped(client.clone());
    let history = handle
        .fetch_history(WorkflowFetchHistoryOptions::builder().build())
        .await
        .context("fetch_history for post-CAN run start input")?;
    for ev in history.events() {
        if let Some(Attributes::WorkflowExecutionStartedEventAttributes(attrs)) = &ev.attributes {
            let input_payload = attrs
                .input
                .as_ref()
                .and_then(|i| i.payloads.first())
                .ok_or_else(|| anyhow::anyhow!("post-CAN start event missing input payload"))?;
            // Temporal's default JSON payload codec puts the JSON
            // bytes directly in `data`. The wire `AgentInput` shape
            // is `{ "cfg": ..., "fs_handle": ..., "parent_handle": ...,
            // "carryover": { ... } }`.
            let input_json: serde_json::Value = serde_json::from_slice(&input_payload.data)
                .context("decode AgentInput JSON from post-CAN start payload")?;
            let cum = input_json
                .get("carryover")
                .and_then(|c| c.get("cumulative_triggers_observed"))
                .and_then(|v| v.as_u64())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "post-CAN start input missing carryover.cumulative_triggers_observed: \
                         {input_json:#}"
                    )
                })?;
            return Ok(cum);
        }
    }
    Err(anyhow::anyhow!(
        "no WorkflowExecutionStarted event on post-CAN run"
    ))
}

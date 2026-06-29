//! Live integration test for the child to parent signal path.
//!
//! Env-gated behind `TEMPORAL_LIVE_TEST=1`. Spawns a parent that loops
//! idle capturing every `Trigger` from its `pending_triggers` bucket,
//! and a child that emits once (firing a `Trigger::ChildOutput` at the
//! parent via `ctx.external_workflow(...).signal(...)`) then retires on
//! its `step_cap=1` cap (firing `Trigger::ChildRetired`). Asserts on the
//! happy path and on the failure mode where the parent's workflow id does
//! not resolve.

use std::collections::VecDeque;
use std::env;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowExecuteUpdateOptions,
    WorkflowGetResultOptions, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use uuid::Uuid;

use coral_node::agent_ref::{AgentId, GraphId};
use coral_node::decision::{Decide, Decision, Session};
use coral_node::evidence::EvidenceRecord;
use coral_node::fs::AgentFs;
use coral_node::mandate::{Mandate, OutputId};
use coral_node::storage::{AgentStorage, MemoryStorage};
use coral_node::tools::ToolRegistry;
use coral_node::trigger::Trigger;
use coral_temporal::activities::set_decision_script;
use coral_temporal::worker::{
    build_worker, install_agent_storage, install_decide, install_structural_db_store,
    install_tool_registry,
};

mod common;
use coral_temporal::workflow::{
    AgentInput, AgentResult, AgentSnapshot, AgentWorkflow, FsHandle, ParentRef,
};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// Shared in-memory storage backend so parent and child see one view
/// for activity bodies and for the driver's evidence planting.
static SHARED_STORAGE: OnceLock<Arc<MemoryStorage>> = OnceLock::new();

static PARENT_OBSERVED_TRIGGERS: OnceLock<Arc<Mutex<Vec<Trigger>>>> = OnceLock::new();

static CHILD_SCRIPT: OnceLock<Mutex<VecDeque<Decision>>> = OnceLock::new();
static PARENT_SCRIPT: OnceLock<Mutex<VecDeque<Decision>>> = OnceLock::new();

const PARENT_MANDATE_TEXT: &str = "signal-parent";
const CHILD_MANDATE_TEXT: &str = "signal-child";

/// Serializes the live tests in this binary so they don't share
/// `PARENT_OBSERVED_TRIGGERS` or the per-role scripts across a parallel
/// test run.
static LIVE_TEST_GUARD: Mutex<()> = Mutex::new(());

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_installed() -> Arc<MemoryStorage> {
    INIT.call_once(|| {
        let storage: Arc<MemoryStorage> = Arc::new(MemoryStorage::new());
        SHARED_STORAGE
            .set(Arc::clone(&storage))
            .expect("SHARED_STORAGE set exactly once");
        let dyn_storage: Arc<dyn AgentStorage> = storage;
        install_agent_storage(dyn_storage);
        install_structural_db_store(Arc::new(common::NoopStructuralDb::new()));

        // The execute_tool activity body asserts a registry is installed
        // even when no tool will be dispatched.
        install_tool_registry(Arc::new(ToolRegistry::new()));

        PARENT_OBSERVED_TRIGGERS
            .set(Arc::new(Mutex::new(Vec::new())))
            .expect("PARENT_OBSERVED_TRIGGERS set exactly once");

        CHILD_SCRIPT
            .set(Mutex::new(VecDeque::new()))
            .expect("CHILD_SCRIPT set exactly once");
        PARENT_SCRIPT
            .set(Mutex::new(VecDeque::new()))
            .expect("PARENT_SCRIPT set exactly once");

        install_decide(Arc::new(RoutingDecide));
    });
    SHARED_STORAGE.get().cloned().expect("storage installed")
}

/// Routes decisions by `session.seed.mandate.text` and records every
/// trigger the parent observes. Falls back to a short `Idle` when a
/// script is empty so a misconfigured test loops politely rather than
/// panicking.
struct RoutingDecide;

#[async_trait]
impl Decide for RoutingDecide {
    async fn decide(&self, session: &Session) -> anyhow::Result<Decision> {
        let script = match session.seed.mandate.text.as_str() {
            PARENT_MANDATE_TEXT => {
                if !session.seed.triggers.is_empty() {
                    let log = PARENT_OBSERVED_TRIGGERS
                        .get()
                        .expect("PARENT_OBSERVED_TRIGGERS installed")
                        .clone();
                    let mut guard = log.lock().expect("trigger log mutex poisoned");
                    for t in &session.seed.triggers {
                        guard.push(t.clone());
                    }
                }
                PARENT_SCRIPT.get().expect("PARENT_SCRIPT installed")
            }
            CHILD_MANDATE_TEXT => CHILD_SCRIPT.get().expect("CHILD_SCRIPT installed"),
            other => panic!(
                "RoutingDecide saw unexpected mandate text: {other:?} \
                 (only PARENT_MANDATE_TEXT / CHILD_MANDATE_TEXT scripted)"
            ),
        };
        let popped = script.lock().expect("script mutex poisoned").pop_front();
        Ok(popped.unwrap_or(Decision::Idle {
            next_after: Duration::from_millis(50),
        }))
    }
}

/// Replace the contents of the per-role script slots so the tests in
/// this binary stay isolated.
fn install_role_scripts(parent: Vec<Decision>, child: Vec<Decision>) {
    {
        let mut p = PARENT_SCRIPT
            .get()
            .expect("PARENT_SCRIPT installed")
            .lock()
            .expect("PARENT_SCRIPT mutex poisoned");
        *p = parent.into();
    }
    {
        let mut c = CHILD_SCRIPT
            .get()
            .expect("CHILD_SCRIPT installed")
            .lock()
            .expect("CHILD_SCRIPT mutex poisoned");
        *c = child.into();
    }
    // Clear the DECISION_SCRIPT static so neither workflow pops a
    // stale entry from a previous test run.
    set_decision_script(Vec::new());
}

fn reset_parent_observed_triggers() {
    let log = PARENT_OBSERVED_TRIGGERS
        .get()
        .expect("PARENT_OBSERVED_TRIGGERS installed")
        .clone();
    let mut guard = log.lock().expect("trigger log mutex poisoned");
    guard.clear();
}

fn parent_observed_triggers_snapshot() -> Vec<Trigger> {
    let log = PARENT_OBSERVED_TRIGGERS
        .get()
        .expect("PARENT_OBSERVED_TRIGGERS installed")
        .clone();
    let guard = log.lock().expect("trigger log mutex poisoned");
    guard.clone()
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
    let client_options = ClientOptions::new(namespace).build();
    let client = Client::new(connection, client_options).context("building Temporal client")?;
    Ok(client)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::await_holding_lock)]
async fn child_emit_signals_parent_with_child_output_trigger() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping child_emit_signals_parent_with_child_output_trigger; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    let _guard = LIVE_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
    run_happy_path().await.expect("happy path");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::await_holding_lock)]
async fn child_continues_normally_when_parent_signal_fails() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping child_continues_normally_when_parent_signal_fails; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    let _guard = LIVE_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
    run_failure_path().await.expect("failure path");
}

async fn run_happy_path() -> Result<()> {
    let suffix = run_suffix();
    let task_queue = format!("coral-signal-happy-{suffix}");
    let parent_prefix = format!("graphs/g-signal/agents/parent-{suffix}");
    let child_prefix = format!("graphs/g-signal/agents/child-{suffix}");
    let parent_workflow_id = format!("graphs/g-signal/agents/parent-{suffix}");
    let child_workflow_id = format!("graphs/g-signal/agents/child-{suffix}");

    let storage = ensure_installed();
    reset_parent_observed_triggers();

    // Plant one evidence record under the CHILD's FS prefix so the
    // scripted WriteOutput resolves provenance. EvidenceId is content-
    // addressed; capture the returned id for the post-run assertion
    // on the trigger payload.
    let plant_mandate = Mandate::new("plant", Duration::from_millis(0), None);
    let plant_storage: Arc<dyn AgentStorage> = storage.clone();
    let plant_fs = AgentFs::new_with_storage(plant_storage, &child_prefix, &plant_mandate)
        .await
        .context("open planting AgentFs for child")?;
    let planted_id = plant_fs
        .record_evidence(
            EvidenceRecord::new(
                "echo",
                serde_json::json!({"k": "v"}),
                serde_json::json!({"hit": true}),
                Utc::now(),
            ),
            "echo",
        )
        .await
        .context("plant evidence for child WriteOutput")?;

    // Parent script: idle forever. The tick loop wakes on every signal
    // arrival (wake gate races `wait_condition` against the idle timer),
    // so an empty script plus the Idle fallback is enough for the
    // ChildOutput signal to land and be drained into a bundle that the
    // `RoutingDecide` records. A `retire` signal at the end ends the
    // parent cleanly.
    let parent_script: Vec<Decision> = Vec::new();
    let child_script = vec![Decision::WriteOutput {
        body: "child output".into(),
        citations: vec![planted_id.clone()],
    }];
    install_role_scripts(parent_script, child_script);

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
    let driver_parent_prefix = parent_prefix.clone();
    let driver_child_prefix = child_prefix.clone();
    let driver_parent_workflow_id = parent_workflow_id.clone();
    let driver_child_workflow_id = child_workflow_id.clone();
    let driver = tokio::spawn(async move {
        struct ShutdownGuard<F: Fn()>(F);
        impl<F: Fn()> Drop for ShutdownGuard<F> {
            fn drop(&mut self) {
                (self.0)();
            }
        }
        let _guard = ShutdownGuard(shutdown);
        drive_happy_path(
            client,
            &driver_task_queue,
            &driver_parent_workflow_id,
            &driver_child_workflow_id,
            &driver_parent_prefix,
            &driver_child_prefix,
        )
        .await
    });

    let worker_result = tokio::time::timeout(Duration::from_secs(90), worker.run())
        .await
        .map_err(|_| anyhow::anyhow!("worker.run() timed out (90s)"))?
        .map_err(|e| anyhow::anyhow!("worker.run() exited with error: {e}"));
    let driver_result = driver.await.context("driver task panicked")?;

    worker_result?;
    driver_result?;
    Ok(())
}

async fn drive_happy_path(
    client: Client,
    task_queue: &str,
    parent_workflow_id: &str,
    child_workflow_id: &str,
    parent_prefix: &str,
    child_prefix: &str,
) -> Result<()> {
    // Start the parent first so it's running and addressable when the
    // child fires the signal.
    let parent_input = AgentInput {
        cfg: Default::default(),
        fs_handle: FsHandle {
            prefix: parent_prefix.into(),
        },
        parent_handle: None,
        carryover: None,
        mandate: Mandate::new(PARENT_MANDATE_TEXT, Duration::from_millis(50), None),
        graph_id: GraphId::new(Uuid::nil()),
        agent_id: AgentId::new(Uuid::nil()),
        agent_name: "parent".into(),
    };
    let parent_handle = client
        .start_workflow(
            AgentWorkflow::run,
            parent_input,
            WorkflowStartOptions::new(task_queue, parent_workflow_id).build(),
        )
        .await
        .context("start_workflow(parent)")?;
    eprintln!("happy: parent started at {parent_workflow_id}");

    // Distinct UUID for the child's structural agent_id, recorded onto
    // `Trigger::ChildOutput.child_ref.agent_id` so the post-run
    // assertion can pin it.
    let child_agent_uuid =
        Uuid::parse_str("11111111-2222-3333-4444-555555555555").expect("hand-picked uuid is valid");
    let child_agent_id = AgentId::new(child_agent_uuid);
    let child_input = AgentInput {
        cfg: Default::default(),
        fs_handle: FsHandle {
            prefix: child_prefix.into(),
        },
        parent_handle: Some(ParentRef {
            workflow_id: parent_workflow_id.to_string(),
            ..ParentRef::default()
        }),
        carryover: None,
        mandate: Mandate::new(CHILD_MANDATE_TEXT, Duration::from_millis(50), Some(1)),
        graph_id: GraphId::new(Uuid::nil()),
        agent_id: child_agent_id,
        agent_name: "fda_scraper".into(),
    };
    let child_handle = client
        .start_workflow(
            AgentWorkflow::run,
            child_input,
            WorkflowStartOptions::new(task_queue, child_workflow_id).build(),
        )
        .await
        .context("start_workflow(child)")?;
    eprintln!("happy: child started at {child_workflow_id}");

    // Wait for the child to retire. It emits once (the `WriteOutput` arm
    // fires the ChildOutput signal at the parent), then the `step_cap=1`
    // cap retires it (firing the ChildRetired signal).
    let child_result: AgentResult = child_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("child get_result")?;
    let AgentResult::Retired { reason } = child_result;
    assert_eq!(
        reason, "step_cap (1) reached",
        "child workflow returned wrong retire reason: {reason:?}"
    );
    eprintln!("happy: child retired cleanly");

    // Poll the parent's `inspect_state` until cumulative_triggers_observed >= 1.
    // The signal command is recorded on the child's history when its
    // workflow task completes; the parent's signal-handler fires on its
    // next workflow task. On a healthy server this lands within
    // milliseconds, but we budget generously.
    let poll_start = std::time::Instant::now();
    let poll_budget = Duration::from_secs(30);
    let mut observed_at_parent: Option<AgentSnapshot> = None;
    let mut last_err: Option<anyhow::Error> = None;
    while poll_start.elapsed() < poll_budget {
        match parent_handle
            .execute_update(
                AgentWorkflow::inspect_state,
                (),
                WorkflowExecuteUpdateOptions::default(),
            )
            .await
        {
            Ok(snap) => {
                if snap.cumulative_triggers_observed >= 1 {
                    observed_at_parent = Some(snap);
                    break;
                }
            }
            Err(e) => {
                last_err = Some(anyhow::anyhow!("inspect_state error: {e}"));
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let observed_at_parent = observed_at_parent.ok_or_else(|| {
        last_err.unwrap_or_else(|| {
            anyhow::anyhow!(
                "parent's cumulative_triggers_observed stayed at 0 across 30s poll budget"
            )
        })
    })?;
    eprintln!(
        "happy: parent observed cumulative_triggers_observed={}",
        observed_at_parent.cumulative_triggers_observed
    );
    assert!(
        observed_at_parent.cumulative_triggers_observed >= 1,
        "parent did not observe the ChildOutput signal: {observed_at_parent:?}"
    );

    // Now retire the parent so the worker can drain. The parent's
    // `RoutingDecide` will see the queued trigger on the next tick (or
    // already saw it via the wake gate); either way the retire signal
    // short-circuits the tick before any further decision.
    parent_handle
        .signal(
            AgentWorkflow::retire,
            "happy: test asked".to_string(),
            WorkflowSignalOptions::default(),
        )
        .await
        .context("signal parent retire")?;
    let parent_result: AgentResult = parent_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("parent get_result")?;
    let AgentResult::Retired {
        reason: parent_reason,
    } = parent_result;
    assert!(
        parent_reason.contains("test asked"),
        "parent returned wrong retire reason: {parent_reason:?}"
    );

    // Inspect the trigger payload the parent's `RoutingDecide` recorded.
    // The parent may observe the trigger across one or more tick
    // bundles; assert at least one ChildOutput is in the captured set.
    let observed = parent_observed_triggers_snapshot();
    eprintln!(
        "happy: parent's RoutingDecide saw {} trigger(s)",
        observed.len()
    );
    let child_outputs: Vec<&Trigger> = observed
        .iter()
        .filter(|t| matches!(t, Trigger::ChildOutput { .. }))
        .collect();
    assert!(
        !child_outputs.is_empty(),
        "parent's RoutingDecide never saw a ChildOutput trigger; captured: {observed:?}"
    );
    let Trigger::ChildOutput {
        child_ref,
        agent_name,
        output_id,
    } = child_outputs[0]
    else {
        unreachable!("filter above guarantees ChildOutput");
    };
    assert_eq!(
        child_ref.workflow_id, child_workflow_id,
        "ChildOutput.child_ref.workflow_id mismatch"
    );
    assert_eq!(
        child_ref.agent_id, child_agent_id,
        "ChildOutput.child_ref.agent_id mismatch"
    );
    assert_eq!(agent_name, "fda_scraper", "ChildOutput.agent_name mismatch");
    // OutputId is the content-addressed hash of the body. Read the
    // child's single canonical output and assert the trigger's id
    // matches what `persist_output` minted from that body.
    let inspect_mandate = Mandate::new("inspect", Duration::from_millis(0), None);
    let inspect_storage: Arc<dyn AgentStorage> = SHARED_STORAGE
        .get()
        .expect("SHARED_STORAGE installed")
        .clone();
    let inspect_fs = AgentFs::new_with_storage(inspect_storage, child_prefix, &inspect_mandate)
        .await
        .context("open inspecting AgentFs over child")?;
    let body = inspect_fs
        .read_output()
        .await
        .context("read_output on child")?;
    assert_eq!(
        OutputId::new(&body),
        *output_id,
        "Trigger::ChildOutput.output_id must match the child's persisted output id"
    );
    // The body proves `persist_output` ran end-to-end (its provenance
    // check requires the planted evidence to resolve before writing).
    assert_eq!(body, "child output", "child's canonical output body");

    let _ = parent_prefix;

    Ok(())
}

async fn run_failure_path() -> Result<()> {
    let suffix = run_suffix();
    let task_queue = format!("coral-signal-fail-{suffix}");
    let child_prefix = format!("graphs/g-signal-fail/agents/child-{suffix}");
    let child_workflow_id = format!("graphs/g-signal-fail/agents/child-{suffix}");
    // Deliberately unused workflow id; no `start_workflow` ever runs
    // against it.
    let missing_parent_workflow_id =
        format!("graphs/g-signal-fail/agents/parent-DOES-NOT-EXIST-{suffix}");

    let storage = ensure_installed();
    reset_parent_observed_triggers();

    let plant_mandate = Mandate::new("plant", Duration::from_millis(0), None);
    let plant_storage: Arc<dyn AgentStorage> = storage.clone();
    let plant_fs = AgentFs::new_with_storage(plant_storage, &child_prefix, &plant_mandate)
        .await
        .context("open planting AgentFs for child (failure path)")?;
    let planted_id = plant_fs
        .record_evidence(
            EvidenceRecord::new(
                "echo",
                serde_json::json!({"k": "vv"}),
                serde_json::json!({"hit": true}),
                Utc::now(),
            ),
            "echo",
        )
        .await
        .context("plant evidence for child WriteOutput (failure path)")?;

    let child_script = vec![Decision::WriteOutput {
        body: "child (failure path) output".into(),
        citations: vec![planted_id.clone()],
    }];
    install_role_scripts(Vec::new(), child_script);

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
    let driver_child_prefix = child_prefix.clone();
    let driver_child_workflow_id = child_workflow_id.clone();
    let driver_missing_parent_workflow_id = missing_parent_workflow_id.clone();
    let driver = tokio::spawn(async move {
        struct ShutdownGuard<F: Fn()>(F);
        impl<F: Fn()> Drop for ShutdownGuard<F> {
            fn drop(&mut self) {
                (self.0)();
            }
        }
        let _guard = ShutdownGuard(shutdown);
        drive_failure_path(
            client,
            &driver_task_queue,
            &driver_child_workflow_id,
            &driver_child_prefix,
            &driver_missing_parent_workflow_id,
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

async fn drive_failure_path(
    client: Client,
    task_queue: &str,
    child_workflow_id: &str,
    child_prefix: &str,
    missing_parent_workflow_id: &str,
) -> Result<()> {
    let child_agent_uuid =
        Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").expect("hand-picked uuid is valid");
    let child_input = AgentInput {
        cfg: Default::default(),
        fs_handle: FsHandle {
            prefix: child_prefix.into(),
        },
        parent_handle: Some(ParentRef {
            workflow_id: missing_parent_workflow_id.to_string(),
            ..ParentRef::default()
        }),
        carryover: None,
        mandate: Mandate::new(CHILD_MANDATE_TEXT, Duration::from_millis(50), Some(1)),
        graph_id: GraphId::new(Uuid::nil()),
        agent_id: AgentId::new(child_agent_uuid),
        agent_name: "orphan_child".into(),
    };
    let child_handle = client
        .start_workflow(
            AgentWorkflow::run,
            child_input,
            WorkflowStartOptions::new(task_queue, child_workflow_id).build(),
        )
        .await
        .context("start_workflow(child failure path)")?;
    eprintln!(
        "fail: child started at {child_workflow_id}, parent_handle points at \
         non-existent {missing_parent_workflow_id}"
    );

    // Load-bearing assertion: despite the signal target being a
    // non-existent workflow, the child's workflow body does NOT error
    // out. It logs the failure and continues to the next tick (where the
    // `step_cap=1` cap retires it).
    let result: AgentResult = child_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("child get_result (failure path)")?;
    let AgentResult::Retired { reason } = result;
    assert_eq!(
        reason, "step_cap (1) reached",
        "child workflow did not complete normally despite signal-to-nonexistent-parent failure; \
         got: {reason:?}"
    );
    eprintln!("fail: child completed normally after signal failure");

    // Sanity: the child's output still landed on its own FS. Losing
    // the signal does not lose the data.
    let inspect_mandate = Mandate::new("inspect", Duration::from_millis(0), None);
    let inspect_storage: Arc<dyn AgentStorage> = SHARED_STORAGE
        .get()
        .expect("SHARED_STORAGE installed")
        .clone();
    let inspect_fs = AgentFs::new_with_storage(inspect_storage, child_prefix, &inspect_mandate)
        .await
        .context("open inspecting AgentFs over child (failure path)")?;
    let body = inspect_fs
        .read_output()
        .await
        .context("read_output on child (failure path)")?;
    assert_eq!(
        body, "child (failure path) output",
        "child must have persisted its output despite parent-unreachable"
    );

    Ok(())
}

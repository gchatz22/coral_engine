//! Stage 5.7 (JAR2-84) — `Decision::RetireChild` + `Decision::ReplaceChild`
//! live integration tests.
//!
//! Env-gated behind `TEMPORAL_LIVE_TEST=1`. Mirrors the JAR2-80
//! `spawn_child_live.rs` shape: a parent `MockDecide` script drives the
//! parent through `Idle → SpawnChild → <lifecycle op>`; the test then
//! signals the parent to retire and inspects:
//!
//! Test 1 — `retire_child_signals_child_and_drops_handle`: `RetireChild`
//! arm fires `AgentWorkflow::retire` at the child via the same SDK
//! two-step `external_workflow().signal()` chain JAR2-81 uses for
//! `ChildOutput` (reverse direction). The child exits cleanly with the
//! parent's reason; the child's own retirement path fires a
//! `Trigger::ChildRetired` back at the parent before exit (per the
//! JAR2-84 extension to the `retire()` helper).
//!
//! Test 2 — `replace_child_retires_old_and_spawns_fresh_with_new_mandate`:
//! `ReplaceChild` arm signals the old child's retire AND spawns a fresh
//! replacement via the same `register_child_in_structural_db` +
//! `ctx.child_workflow(..)` path as `SpawnChild`. The structural-DB
//! fake records exactly two `agents` rows + two `edges` rows (both the
//! original child and the replacement — the old edge stays per Stage
//! 5.7 decision "no `retired_at` column"); the old child workflow
//! exits.
//!
//! ## Why these live tests + not hermetic
//!
//! Per Stage 5 Project decision 11: there is no hermetic in-process
//! multi-workflow path in v1 — every multi-agent integration test is
//! `TEMPORAL_LIVE_TEST=1` gated. The hermetic shape only covers
//! workflow-state mutation invariants (see
//! `workflow::tests::retire_child_removes_handle_and_survives_carryover`
//! et al.); cross-workflow signaling + abandon semantics need a real
//! Temporal Server in the loop.

use std::collections::VecDeque;
use std::env;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_trait::async_trait;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowExecuteUpdateOptions,
    WorkflowGetResultOptions, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use uuid::Uuid;

use jarvis_node::agent_ref::{AgentId, GraphId};
use jarvis_node::decision::{ContextBundle, Decide, Decision};
use jarvis_node::mandate::Mandate;
use jarvis_node::storage::{AgentStorage, MemoryStorage};
use jarvis_node::tools::{EchoTool, ToolRegistry};
use jarvis_node::trigger::Trigger;
use jarvis_temporal::worker::{
    build_worker, install_agent_storage, install_decide, install_structural_db_store,
    install_tool_registry, StructuralDbStore,
};
use jarvis_temporal::workflow::{agent_workflow_id, AgentInput, AgentResult, AgentWorkflow};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// Shared backends installed exactly once for this binary's two live
/// tests (same `OnceLock` + `Once` pattern as `spawn_child_live.rs`).
static SHARED_STORAGE: OnceLock<Arc<MemoryStorage>> = OnceLock::new();
static SHARED_DB: OnceLock<Arc<MemoryStructuralDb>> = OnceLock::new();
static INIT: std::sync::Once = std::sync::Once::new();

/// Serializes the live tests in this binary so they don't share the
/// `MemoryStructuralDb` state across parallel runs (the assertions
/// count `agents` / `edges` rows by length, which would race if two
/// tests touched the DB concurrently).
static LIVE_TEST_GUARD: Mutex<()> = Mutex::new(());

/// Per-role decision script — keyed by the agent's mandate text so the
/// shared `Decide` impl can route the parent vs. the spawned children
/// to different scripts without leaking state across tests.
static PARENT_SCRIPT: OnceLock<Mutex<VecDeque<Decision>>> = OnceLock::new();

/// Every `Trigger` the parent's `decide_next_action` activity ever sees
/// in its [`ContextBundle`]. Populated by [`RoutingDecide::decide`] when
/// the routing key matches the parent. The post-retire assertions read
/// this snapshot to confirm the `ChildRetired` signal the child fires
/// (per the JAR2-84 extension to the `retire()` helper) actually
/// landed on the parent's `pending_triggers` and was drained into a
/// per-tick bundle.
static PARENT_OBSERVED_TRIGGERS: OnceLock<Arc<Mutex<Vec<Trigger>>>> = OnceLock::new();

const PARENT_MANDATE_TEXT: &str = "JAR2-84-parent";

#[derive(Clone, Debug, PartialEq, Eq)]
struct RecordedAgent {
    graph_id: GraphId,
    name: String,
    mandate_ref: Option<String>,
    allocated_id: AgentId,
}

#[derive(Debug)]
struct MemoryStructuralDb {
    next_id: std::sync::Mutex<u128>,
    agents: std::sync::Mutex<Vec<RecordedAgent>>,
    edges: std::sync::Mutex<Vec<(AgentId, AgentId)>>,
}

impl MemoryStructuralDb {
    fn new() -> Self {
        Self {
            next_id: std::sync::Mutex::new(1),
            agents: std::sync::Mutex::new(Vec::new()),
            edges: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn reset(&self) {
        *self.next_id.lock().unwrap() = 1;
        self.agents.lock().unwrap().clear();
        self.edges.lock().unwrap().clear();
    }
}

#[async_trait]
impl StructuralDbStore for MemoryStructuralDb {
    async fn add_agent(
        &self,
        graph_id: GraphId,
        name: &str,
        mandate_ref: Option<&str>,
    ) -> anyhow::Result<AgentId> {
        let mut next = self.next_id.lock().unwrap();
        let id = AgentId::new(Uuid::from_u128(*next));
        *next += 1;
        drop(next);
        self.agents.lock().unwrap().push(RecordedAgent {
            graph_id,
            name: name.to_string(),
            mandate_ref: mandate_ref.map(str::to_string),
            allocated_id: id,
        });
        Ok(id)
    }

    async fn add_edge(
        &self,
        parent_agent_id: AgentId,
        child_agent_id: AgentId,
    ) -> anyhow::Result<()> {
        self.edges
            .lock()
            .unwrap()
            .push((parent_agent_id, child_agent_id));
        Ok(())
    }
}

/// `Decide` impl shared across the parent + all children. Routes by
/// `bundle.mandate.text`: the parent gets the scripted decisions, every
/// other workflow (children spawned at runtime) falls back to a long
/// `Idle` so they stay alive in the test window without polling the
/// parent's script.
struct RoutingDecide;

#[async_trait]
impl Decide for RoutingDecide {
    async fn decide(&self, bundle: ContextBundle) -> anyhow::Result<Decision> {
        if bundle.mandate.text == PARENT_MANDATE_TEXT {
            // Record every trigger the parent observes this tick so
            // the post-RetireChild / post-ReplaceChild assertions can
            // verify a `Trigger::ChildRetired` actually landed.
            if !bundle.triggers.is_empty() {
                let log = PARENT_OBSERVED_TRIGGERS
                    .get()
                    .expect("PARENT_OBSERVED_TRIGGERS installed")
                    .clone();
                let mut guard = log.lock().expect("trigger log mutex poisoned");
                for t in &bundle.triggers {
                    guard.push(t.clone());
                }
            }
            let mut q = PARENT_SCRIPT
                .get()
                .expect("PARENT_SCRIPT installed")
                .lock()
                .expect("PARENT_SCRIPT mutex poisoned");
            return Ok(q.pop_front().unwrap_or(Decision::Idle {
                next_after: Duration::from_millis(50),
            }));
        }
        // Children stay alive until the parent's RetireChild signal
        // reaches them. `Abandon` is the close policy, so even if the
        // parent retires first, the child loops on Idle until killed.
        Ok(Decision::Idle {
            next_after: Duration::from_secs(60),
        })
    }
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

fn install_parent_script(script: Vec<Decision>) {
    let mut q = PARENT_SCRIPT
        .get()
        .expect("PARENT_SCRIPT installed")
        .lock()
        .expect("PARENT_SCRIPT mutex poisoned");
    *q = script.into();
}

fn ensure_installed() -> Arc<MemoryStructuralDb> {
    INIT.call_once(|| {
        let storage: Arc<MemoryStorage> = Arc::new(MemoryStorage::new());
        SHARED_STORAGE
            .set(Arc::clone(&storage))
            .expect("SHARED_STORAGE set exactly once");
        let dyn_storage: Arc<dyn AgentStorage> = storage;
        install_agent_storage(dyn_storage);

        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(EchoTool)).expect("register EchoTool");
        install_tool_registry(Arc::new(reg));

        let db = Arc::new(MemoryStructuralDb::new());
        SHARED_DB
            .set(Arc::clone(&db))
            .expect("SHARED_DB set exactly once");
        let dyn_db: Arc<dyn StructuralDbStore> = db;
        install_structural_db_store(dyn_db);

        PARENT_SCRIPT
            .set(Mutex::new(VecDeque::new()))
            .expect("PARENT_SCRIPT set exactly once");

        PARENT_OBSERVED_TRIGGERS
            .set(Arc::new(Mutex::new(Vec::new())))
            .expect("PARENT_OBSERVED_TRIGGERS set exactly once");

        install_decide(Arc::new(RoutingDecide));
    });
    SHARED_DB.get().cloned().expect("SHARED_DB installed")
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

/// Poll the structural-DB fake until it has at least `expected_agents`
/// recorded rows, or fail after 30s. Used to gate signal sending on
/// "the activity has actually run" rather than racing a fixed sleep.
async fn wait_for_agent_count(db: &MemoryStructuralDb, expected_agents: usize) -> Result<()> {
    let poll_deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if db.agents.lock().unwrap().len() >= expected_agents {
            return Ok(());
        }
        if std::time::Instant::now() >= poll_deadline {
            return Err(anyhow::anyhow!(
                "structural DB had {} agents after 30s; expected at least {}",
                db.agents.lock().unwrap().len(),
                expected_agents,
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// JAR2-84 live test: `Decision::RetireChild` fires the child's retire
/// signal and removes the child's handle from the parent's state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::await_holding_lock)]
async fn retire_child_signals_child_and_drops_handle() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping retire_child_signals_child_and_drops_handle; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    let _guard = LIVE_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
    run_retire_smoke()
        .await
        .expect("JAR2-84 retire_child live test");
}

async fn run_retire_smoke() -> Result<()> {
    let db = ensure_installed();
    db.reset();
    reset_parent_observed_triggers();

    let parent_graph_id = GraphId::new(Uuid::new_v4());
    let parent_agent_id = AgentId::new(Uuid::new_v4());
    let suffix = run_suffix();
    let task_queue = format!("jarvis-jar2-84-retire-{suffix}");

    // The parent's scripted decisions: Idle (drain first wake), Spawn,
    // Idle (let the child come up), then RetireChild. We DON'T script
    // a final Retire — the test driver signals it externally after
    // observing the child's retirement, matching `spawn_child_live.rs`'s
    // rationale.
    let child_mandate = Mandate::new(
        "JAR2-84 retire-child target",
        Duration::from_millis(500),
        Some(8),
    );

    // We can't know the child's allocated agent_id until the
    // `register_child_in_structural_db` activity runs — so the
    // RetireChild decision needs to be queued *after* the spawn has
    // landed. Strategy: script SpawnChild as the parent's first
    // scripted decision, poll the structural-DB fake until the child
    // is registered, then inject RetireChild as the next script entry.

    install_parent_script(vec![
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
        Decision::SpawnChild {
            agent_name: "doomed_fetcher".into(),
            mandate: child_mandate.clone(),
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
    let driver_db = db.clone();
    let driver = tokio::spawn(async move {
        struct ShutdownGuard<F: Fn()>(F);
        impl<F: Fn()> Drop for ShutdownGuard<F> {
            fn drop(&mut self) {
                (self.0)();
            }
        }
        let _guard = ShutdownGuard(shutdown);
        drive_retire(
            client,
            &driver_task_queue,
            parent_graph_id,
            parent_agent_id,
            &suffix,
            driver_db,
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

async fn drive_retire(
    client: Client,
    task_queue: &str,
    parent_graph_id: GraphId,
    parent_agent_id: AgentId,
    suffix: &str,
    db: Arc<MemoryStructuralDb>,
) -> Result<()> {
    let parent_workflow_id = format!(
        "{}-{suffix}",
        agent_workflow_id(&parent_graph_id.to_string(), &parent_agent_id.to_string()),
    );
    let mut input = AgentInput::new_for_test(parent_graph_id, parent_agent_id, "parent");
    input.fs_handle = jarvis_temporal::workflow::FsHandle {
        prefix: format!("graphs/{parent_graph_id}/agents/{parent_agent_id}-parent-retire"),
    };
    input.mandate = Mandate::new(PARENT_MANDATE_TEXT, Duration::from_millis(50), None);
    let parent_handle = client
        .start_workflow(
            AgentWorkflow::run,
            input,
            WorkflowStartOptions::new(task_queue, &parent_workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow) [parent]")?;

    // Wait for the parent's SpawnChild to land in the structural DB.
    wait_for_agent_count(&db, 1).await?;
    let child_agent_id = db.agents.lock().unwrap()[0].allocated_id;
    let child_workflow_id =
        agent_workflow_id(&parent_graph_id.to_string(), &child_agent_id.to_string());
    eprintln!(
        "JAR2-84 retire: child registered at workflow_id={child_workflow_id} agent_id={child_agent_id}"
    );

    // Now inject the RetireChild decision. The parent's loop will
    // pick it up on its next wake; the wake cadence is the prior
    // Idle's 50ms so it lands within that budget.
    install_parent_script(vec![Decision::RetireChild {
        child_ref: jarvis_node::agent_ref::AgentRef::new(child_workflow_id.clone(), child_agent_id),
        reason: "JAR2-84: scripted retire-child".into(),
    }]);

    // Wait for the child workflow to actually exit. The parent's
    // RetireChild arm fires the retire signal on the child; the child
    // observes its own `retire` handler on the next wake, runs
    // `persist_retirement`, then exits — at which point the child's
    // workflow result resolves.
    let child_result_handle =
        client.get_workflow_handle::<AgentWorkflow>(child_workflow_id.clone());
    let child_result = tokio::time::timeout(
        Duration::from_secs(60),
        child_result_handle.get_result(WorkflowGetResultOptions::default()),
    )
    .await
    .context("child workflow did not exit within 60s after RetireChild")?
    .context("child.get_result() failed")?;
    let AgentResult::Retired { reason } = child_result;
    assert!(
        reason.contains("scripted retire-child"),
        "child should retire with parent's scripted reason; got: {reason:?}"
    );
    eprintln!("JAR2-84 retire: child exited with reason={reason:?}");

    // The child's `retire()` helper fires a `Trigger::ChildRetired`
    // back at the parent before exit (per the JAR2-84 extension).
    // Poll the parent's `inspect_state` until that trigger has landed
    // on workflow state — this is the load-bearing acceptance
    // criterion. Mirrors JAR2-81's `child_emit_signals_parent_with_child_output_trigger`
    // gating pattern.
    let poll_start = std::time::Instant::now();
    let poll_budget = Duration::from_secs(30);
    let mut last_err: Option<anyhow::Error> = None;
    let mut observed = false;
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
                    observed = true;
                    break;
                }
            }
            Err(e) => {
                last_err = Some(anyhow::anyhow!("inspect_state error: {e}"));
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !observed {
        return Err(last_err.unwrap_or_else(|| {
            anyhow::anyhow!(
                "parent's cumulative_triggers_observed stayed at 0 across 30s poll budget; \
                 the child's ChildRetired signal never landed"
            )
        }));
    }

    // Now retire the parent so the worker can drain.
    parent_handle
        .signal(
            AgentWorkflow::retire,
            "JAR2-84 retire-child: parent retire".to_string(),
            WorkflowSignalOptions::default(),
        )
        .await
        .context("signal AgentWorkflow::retire [parent]")?;

    let parent_result: AgentResult = parent_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result [parent]")?;
    let AgentResult::Retired { reason: pr } = parent_result;
    assert!(
        pr.contains("parent retire"),
        "parent should retire via signal; got reason: {pr:?}"
    );

    // The structural DB should have exactly one agent (the original
    // doomed_fetcher child). RetireChild does NOT add a new row —
    // and per the Stage 5.7 decision the old edge stays without a
    // `retired_at` marker.
    let agents = db.agents.lock().unwrap().clone();
    let edges = db.edges.lock().unwrap().clone();
    assert_eq!(
        agents.len(),
        1,
        "RetireChild must not write a new agents row; got {agents:?}"
    );
    assert_eq!(agents[0].name, "doomed_fetcher");
    assert_eq!(
        edges.len(),
        1,
        "RetireChild must not write a new edges row; got {edges:?}"
    );
    assert_eq!(edges[0].0, parent_agent_id);
    assert_eq!(edges[0].1, child_agent_id);

    // Trigger payload assertion: the parent's `RoutingDecide` recorded
    // every drained trigger. There must be at least one
    // `Trigger::ChildRetired` carrying the matching child_ref +
    // agent_name + reason. Mirrors JAR2-81's payload assertion.
    let observed = parent_observed_triggers_snapshot();
    let child_retired: Vec<&Trigger> = observed
        .iter()
        .filter(|t| matches!(t, Trigger::ChildRetired { .. }))
        .collect();
    assert!(
        !child_retired.is_empty(),
        "parent's RoutingDecide never observed a ChildRetired trigger; captured: {observed:?}"
    );
    let Trigger::ChildRetired {
        child_ref: observed_ref,
        agent_name: observed_name,
        reason: observed_reason,
    } = child_retired[0]
    else {
        unreachable!("filter above guarantees ChildRetired");
    };
    assert_eq!(
        observed_ref.workflow_id, child_workflow_id,
        "ChildRetired.child_ref.workflow_id mismatch"
    );
    assert_eq!(
        observed_ref.agent_id, child_agent_id,
        "ChildRetired.child_ref.agent_id mismatch"
    );
    assert_eq!(
        observed_name, "doomed_fetcher",
        "ChildRetired.agent_name mismatch"
    );
    assert!(
        observed_reason.contains("scripted retire-child"),
        "ChildRetired.reason must echo the parent's RetireChild reason; got: {observed_reason:?}"
    );

    Ok(())
}

/// JAR2-84 live test: `Decision::ReplaceChild` retires the old child
/// and spawns a fresh replacement with the new mandate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::await_holding_lock)]
async fn replace_child_retires_old_and_spawns_fresh_with_new_mandate() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping replace_child_retires_old_and_spawns_fresh_with_new_mandate; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    let _guard = LIVE_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
    run_replace_smoke()
        .await
        .expect("JAR2-84 replace_child live test");
}

async fn run_replace_smoke() -> Result<()> {
    let db = ensure_installed();
    db.reset();
    reset_parent_observed_triggers();

    let parent_graph_id = GraphId::new(Uuid::new_v4());
    let parent_agent_id = AgentId::new(Uuid::new_v4());
    let suffix = run_suffix();
    let task_queue = format!("jarvis-jar2-84-replace-{suffix}");

    let old_mandate = Mandate::new(
        "JAR2-84 replace-child target",
        Duration::from_millis(500),
        Some(8),
    );

    install_parent_script(vec![
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
        Decision::SpawnChild {
            agent_name: "old_fetcher".into(),
            mandate: old_mandate.clone(),
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
    let driver_db = db.clone();
    let driver = tokio::spawn(async move {
        struct ShutdownGuard<F: Fn()>(F);
        impl<F: Fn()> Drop for ShutdownGuard<F> {
            fn drop(&mut self) {
                (self.0)();
            }
        }
        let _guard = ShutdownGuard(shutdown);
        drive_replace(
            client,
            &driver_task_queue,
            parent_graph_id,
            parent_agent_id,
            &suffix,
            driver_db,
        )
        .await
    });

    let worker_result = tokio::time::timeout(Duration::from_secs(120), worker.run())
        .await
        .map_err(|_| anyhow::anyhow!("worker.run() timed out (120s)"))?
        .map_err(|e| anyhow::anyhow!("worker.run() exited with error: {e}"));
    let driver_result = driver.await.context("driver task panicked")?;

    worker_result?;
    driver_result?;
    Ok(())
}

async fn drive_replace(
    client: Client,
    task_queue: &str,
    parent_graph_id: GraphId,
    parent_agent_id: AgentId,
    suffix: &str,
    db: Arc<MemoryStructuralDb>,
) -> Result<()> {
    let parent_workflow_id = format!(
        "{}-{suffix}",
        agent_workflow_id(&parent_graph_id.to_string(), &parent_agent_id.to_string()),
    );
    let mut input = AgentInput::new_for_test(parent_graph_id, parent_agent_id, "parent");
    input.fs_handle = jarvis_temporal::workflow::FsHandle {
        prefix: format!("graphs/{parent_graph_id}/agents/{parent_agent_id}-parent-replace"),
    };
    input.mandate = Mandate::new(PARENT_MANDATE_TEXT, Duration::from_millis(50), None);
    let parent_handle = client
        .start_workflow(
            AgentWorkflow::run,
            input,
            WorkflowStartOptions::new(task_queue, &parent_workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow) [parent]")?;

    wait_for_agent_count(&db, 1).await?;
    let old_child_agent_id = db.agents.lock().unwrap()[0].allocated_id;
    let old_child_workflow_id = agent_workflow_id(
        &parent_graph_id.to_string(),
        &old_child_agent_id.to_string(),
    );
    eprintln!(
        "JAR2-84 replace: old child registered at workflow_id={old_child_workflow_id} agent_id={old_child_agent_id}"
    );

    // Inject the ReplaceChild decision pointing at the live old child.
    let new_mandate = Mandate::new(
        "JAR2-84 replace-child: new mandate",
        Duration::from_millis(500),
        Some(8),
    );
    install_parent_script(vec![Decision::ReplaceChild {
        child_ref: jarvis_node::agent_ref::AgentRef::new(
            old_child_workflow_id.clone(),
            old_child_agent_id,
        ),
        new_mandate: new_mandate.clone(),
    }]);

    // Wait for the structural DB to have the replacement registered (2
    // agents total: the original old_fetcher + the new replacement).
    wait_for_agent_count(&db, 2).await?;
    let new_child_agent_id = db.agents.lock().unwrap()[1].allocated_id;
    let new_child_workflow_id = agent_workflow_id(
        &parent_graph_id.to_string(),
        &new_child_agent_id.to_string(),
    );
    eprintln!(
        "JAR2-84 replace: new child registered at workflow_id={new_child_workflow_id} agent_id={new_child_agent_id}"
    );

    // The old child should exit cleanly after the parent's retire
    // signal reaches it.
    let old_handle = client.get_workflow_handle::<AgentWorkflow>(old_child_workflow_id.clone());
    let old_result = tokio::time::timeout(
        Duration::from_secs(60),
        old_handle.get_result(WorkflowGetResultOptions::default()),
    )
    .await
    .context("old child did not exit within 60s after ReplaceChild")?
    .context("old child.get_result() failed")?;
    let AgentResult::Retired { reason: old_reason } = old_result;
    assert!(
        old_reason.contains("replacement-of-"),
        "old child should retire with the replacement-of marker; got: {old_reason:?}"
    );

    // Sanity: the replacement child is alive (its workflow handle is
    // valid; we don't poll for a specific state, just confirm the
    // workflow exists in Temporal). The handle constructor is
    // infallible; any structural-DB miss would have failed
    // `wait_for_agent_count(2)` already.
    let _new_handle = client.get_workflow_handle::<AgentWorkflow>(new_child_workflow_id.clone());

    // Wait for the parent to observe the old child's `ChildRetired`
    // signal before tearing it down. Same poll-on-cumulative-counter
    // shape as `drive_retire`.
    let poll_start = std::time::Instant::now();
    let poll_budget = Duration::from_secs(30);
    let mut last_err: Option<anyhow::Error> = None;
    let mut observed = false;
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
                    observed = true;
                    break;
                }
            }
            Err(e) => {
                last_err = Some(anyhow::anyhow!("inspect_state error: {e}"));
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !observed {
        return Err(last_err.unwrap_or_else(|| {
            anyhow::anyhow!(
                "parent's cumulative_triggers_observed stayed at 0 across 30s poll budget; \
                 the old child's ChildRetired signal never landed"
            )
        }));
    }

    // Now retire the parent.
    parent_handle
        .signal(
            AgentWorkflow::retire,
            "JAR2-84 replace-child: parent retire".to_string(),
            WorkflowSignalOptions::default(),
        )
        .await
        .context("signal AgentWorkflow::retire [parent]")?;
    let parent_result: AgentResult = parent_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result [parent]")?;
    let AgentResult::Retired { reason: pr } = parent_result;
    assert!(
        pr.contains("parent retire"),
        "parent should retire via signal; got reason: {pr:?}"
    );

    // Structural DB invariants per Stage 5.7:
    // - Two agents rows (old + replacement).
    // - Two edges rows (parent→old, parent→replacement). The old
    //   edge STAYS (no `retired_at` column, no deletion).
    let agents = db.agents.lock().unwrap().clone();
    let edges = db.edges.lock().unwrap().clone();
    assert_eq!(
        agents.len(),
        2,
        "ReplaceChild must write a new agents row (fresh agent_id); got {agents:?}"
    );
    assert_eq!(agents[0].name, "old_fetcher");
    assert!(
        agents[1].name.starts_with("replacement-of-"),
        "replacement name must use the deterministic marker; got: {:?}",
        agents[1].name,
    );
    assert_eq!(
        edges.len(),
        2,
        "ReplaceChild must write a new edges row AND leave the old in place; got {edges:?}"
    );
    assert_eq!(edges[0], (parent_agent_id, old_child_agent_id));
    assert_eq!(edges[1], (parent_agent_id, new_child_agent_id));

    // Trigger payload assertion: the parent must have observed the
    // old child's `ChildRetired` signal — same load-bearing
    // invariant as in `drive_retire`.
    let observed = parent_observed_triggers_snapshot();
    let child_retired: Vec<&Trigger> = observed
        .iter()
        .filter(|t| matches!(t, Trigger::ChildRetired { .. }))
        .collect();
    assert!(
        !child_retired.is_empty(),
        "parent's RoutingDecide never observed a ChildRetired trigger; captured: {observed:?}"
    );
    let Trigger::ChildRetired {
        child_ref: observed_ref,
        agent_name: observed_name,
        reason: observed_reason,
    } = child_retired[0]
    else {
        unreachable!("filter above guarantees ChildRetired");
    };
    assert_eq!(
        observed_ref.workflow_id, old_child_workflow_id,
        "ChildRetired.child_ref.workflow_id mismatch (must reference the OLD child, not the replacement)"
    );
    assert_eq!(
        observed_ref.agent_id, old_child_agent_id,
        "ChildRetired.child_ref.agent_id mismatch"
    );
    assert_eq!(
        observed_name, "old_fetcher",
        "ChildRetired.agent_name mismatch"
    );
    assert!(
        observed_reason.contains("replacement-of-"),
        "ChildRetired.reason must echo the parent's ReplaceChild reason; got: {observed_reason:?}"
    );

    Ok(())
}

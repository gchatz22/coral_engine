//! `Decision::SpawnChild` live integration test.
//!
//! Env-gated behind `TEMPORAL_LIVE_TEST=1`. Scripts a parent workflow
//! through `Idle → SpawnChild → Retire` and asserts via the parent's
//! Temporal history that a `StartChildWorkflowExecutionInitiated`
//! event lands with the expected child workflow id and
//! `ParentClosePolicy::Abandon`.
//!
//! Uses an in-memory `StructuralDbStore` fake installed via
//! `install_structural_db_store` — DB schema coverage lives in
//! `coral_graph::store::tests`.

use std::env;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_trait::async_trait;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowFetchHistoryOptions,
    WorkflowGetResultOptions, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::protos::temporal::api::enums::v1::ParentClosePolicy;
use temporalio_common::protos::temporal::api::history::v1::history_event::Attributes;
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use uuid::Uuid;

use coral_node::agent_ref::{AgentId, GraphId};
use coral_node::decision::{ContextBundle, Decide, Decision};
use coral_node::mandate::Mandate;
use coral_node::storage::{AgentStorage, MemoryStorage};
use coral_node::tools::{EchoTool, ToolRegistry};
use coral_temporal::activities::set_decision_script;
use coral_temporal::worker::{
    build_worker, install_agent_storage, install_decide, install_structural_db_store,
    install_tool_registry, StructuralDbStore,
};
use coral_temporal::workflow::{agent_workflow_id, AgentInput, AgentResult, AgentWorkflow};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// Shared in-memory backends. The install hooks panic on double-install,
/// so we share one set across this binary's tests (currently one).
static SHARED_STORAGE: OnceLock<Arc<MemoryStorage>> = OnceLock::new();
static SHARED_DB: OnceLock<Arc<MemoryStructuralDb>> = OnceLock::new();
static INIT: std::sync::Once = std::sync::Once::new();

/// One recorded `add_agent` call. Struct (rather than a tuple) keeps
/// clippy's `type_complexity` happy and the test assertions readable.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RecordedAgent {
    graph_id: GraphId,
    name: String,
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
}

#[async_trait]
impl StructuralDbStore for MemoryStructuralDb {
    async fn add_agent(&self, graph_id: GraphId, name: &str) -> anyhow::Result<AgentId> {
        let mut next = self.next_id.lock().unwrap();
        // Use a counter-driven UUID so the child's workflow id is
        // deterministic across test runs (UUID v4 random would still
        // work, but the deterministic form makes the eprintln'd id
        // easier to grep in CI logs).
        let id = AgentId::new(Uuid::from_u128(*next));
        *next += 1;
        drop(next);
        self.agents.lock().unwrap().push(RecordedAgent {
            graph_id,
            name: name.to_string(),
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

/// Fallback `Decide` impl installed for the child workflow. The
/// process-wide `DECISION_SCRIPT` is consumed by the parent's three
/// scripted ticks (Idle → SpawnChild → Retire); the child workflow
/// shares the task queue + worker (and therefore the `decide_next_action`
/// activity) and would race the parent for scripted decisions if no
/// fallback existed. Returning a long `Idle` keeps the child alive on
/// the daemon's side without polling for new decisions in the test's
/// observation window — and matches the `Abandon` semantics under test
/// (the child survives independently of parent retirement).
struct LongIdleDecide;

#[async_trait]
impl Decide for LongIdleDecide {
    async fn decide(&self, _ctx: ContextBundle) -> anyhow::Result<Decision> {
        Ok(Decision::Idle {
            next_after: Duration::from_secs(60),
        })
    }
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

        // Fallback `Decide` so the child workflow (which shares this
        // process-wide static via the `decide_next_action` activity)
        // has something to call after the parent drains the scripted
        // decisions. See `LongIdleDecide` doc for the rationale.
        let decide: Arc<dyn Decide> = Arc::new(LongIdleDecide);
        install_decide(decide);
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
        .context("connecting to Temporal Server")?;
    let client_options = ClientOptions::new(namespace).build();
    let client = Client::new(connection, client_options).context("building Temporal client")?;
    Ok(client)
}

/// Scripts `Idle → SpawnChild → Retire` on a parent workflow and
/// asserts the parent's history shows a
/// `StartChildWorkflowExecutionInitiated` event with the expected
/// workflow id + `ParentClosePolicy::Abandon`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// Same lock-across-await + script-injection rationale as
// `workflow_smoke.rs::workflow_smoke_lands_output_retirement_and_decision_log`.
#[allow(clippy::await_holding_lock)]
async fn spawn_child_dispatches_child_workflow_with_abandon_policy() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping spawn_child_dispatches_child_workflow_with_abandon_policy; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    run_smoke().await.expect("spawn_child live test");
}

async fn run_smoke() -> Result<()> {
    let db = ensure_installed();

    let parent_graph_id = GraphId::new(Uuid::new_v4());
    let parent_agent_id = AgentId::new(Uuid::new_v4());
    let suffix = run_suffix();
    let task_queue = format!("coral-agents-spawn-child-test-{suffix}");

    // Script: Idle (so the loop drains the first wake), SpawnChild
    // (the path under test), then a signalled retire (see comment below
    // on why this is not in the script).
    let child_mandate = Mandate::new(
        "child mandate (spawn_child live test)",
        Duration::from_millis(500),
        Some(2),
    );
    // The parent's two scripted decisions. The third tick is driven
    // by the `retire` signal the driver sends post-spawn, NOT another
    // scripted Retire — this avoids the shared-`DECISION_SCRIPT` race
    // where the child workflow (which spins up the moment SpawnChild
    // fires) might pop the parent's scripted Retire before the parent
    // reaches its third tick. The signal-driven retire short-circuits
    // the parent's loop without calling `decide_next_action`, so the
    // race is structurally impossible.
    set_decision_script(vec![
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
        Decision::SpawnChild {
            agent_name: "fetcher".into(),
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
        let parent_workflow_id = format!(
            "{}-{suffix}",
            agent_workflow_id(&parent_graph_id.to_string(), &parent_agent_id.to_string()),
        );
        eprintln!(
            "spawn_child_live: starting parent workflow_id={parent_workflow_id} on {driver_task_queue}"
        );
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
            &parent_workflow_id,
            parent_graph_id,
            parent_agent_id,
            driver_db,
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

async fn drive(
    client: Client,
    task_queue: &str,
    parent_workflow_id: &str,
    parent_graph_id: GraphId,
    parent_agent_id: AgentId,
    db: Arc<MemoryStructuralDb>,
) -> Result<()> {
    let mut input = AgentInput::new_for_test(parent_graph_id, parent_agent_id, "parent");
    // The mandate text is irrelevant to this test, but the per-agent
    // FS prefix needs to land somewhere the worker's MemoryStorage
    // can hold (default `FsHandle` is fine since MemoryStorage is
    // unscoped).
    input.fs_handle = coral_temporal::workflow::FsHandle {
        prefix: format!("graphs/{parent_graph_id}/agents/{parent_agent_id}-parent"),
    };
    let handle = client
        .start_workflow(
            AgentWorkflow::run,
            input,
            WorkflowStartOptions::new(task_queue, parent_workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow) [parent]")?;

    // Poll the structural-DB fake until SpawnChild has actually fired
    // (the activity body wrote one agent row), then signal Retire.
    // The poll-then-signal sequence is load-bearing: if we signal
    // before the parent reaches its SpawnChild tick, the retirement
    // short-circuit at the top of the next iteration catches the
    // pending retire and the parent retires WITHOUT spawning the
    // child — defeating the test's premise. (A naive `sleep(2s)` racy
    // because the Temporal worker may take a variable time to start
    // executing the workflow's first task.) Cap at 30s — sub-second
    // is the expected success path on a healthy local server.
    let poll_deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if !db.agents.lock().unwrap().is_empty() {
            break;
        }
        if std::time::Instant::now() >= poll_deadline {
            return Err(anyhow::anyhow!(
                "structural DB still empty after 30s — SpawnChild activity never fired"
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    handle
        .signal(
            AgentWorkflow::retire,
            "spawn_child live test: parent retire".to_string(),
            WorkflowSignalOptions::default(),
        )
        .await
        .context("signal AgentWorkflow::retire [parent]")?;

    let result: AgentResult = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result [parent]")?;
    let AgentResult::Retired { reason } = result;
    assert!(
        reason.contains("parent retire"),
        "parent should retire via signal; got reason: {reason:?}"
    );

    // The structural DB fake should have recorded exactly one agent
    // (the child) + one edge (parent → child). The child's allocated
    // AgentId determines the expected child workflow id.
    let agents = db.agents.lock().unwrap().clone();
    let edges = db.edges.lock().unwrap().clone();
    assert_eq!(
        agents.len(),
        1,
        "register_child_in_structural_db should write exactly one agent row; got {agents:?}"
    );
    assert_eq!(agents[0].name, "fetcher", "agent name mismatch");
    let child_agent_id = agents[0].allocated_id;
    assert_eq!(
        edges.len(),
        1,
        "register_child_in_structural_db should write exactly one edge; got {edges:?}"
    );
    assert_eq!(edges[0].0, parent_agent_id, "edge parent mismatch");
    assert_eq!(edges[0].1, child_agent_id, "edge child mismatch");

    let expected_child_workflow_id =
        agent_workflow_id(&parent_graph_id.to_string(), &child_agent_id.to_string());
    eprintln!("spawn_child_live: expected child workflow_id={expected_child_workflow_id}");

    // Parent history: find the StartChildWorkflowExecutionInitiated
    // event and assert its workflow_id + parent_close_policy.
    let history = handle
        .fetch_history(WorkflowFetchHistoryOptions::builder().build())
        .await
        .context("fetch_history [parent]")?;
    let mut found = false;
    for ev in history.events() {
        if let Some(Attributes::StartChildWorkflowExecutionInitiatedEventAttributes(a)) =
            &ev.attributes
        {
            if a.workflow_id == expected_child_workflow_id {
                assert_eq!(
                    a.parent_close_policy,
                    ParentClosePolicy::Abandon as i32,
                    "child workflow must be started with ParentClosePolicy::Abandon; \
                     got policy={}",
                    a.parent_close_policy,
                );
                found = true;
                break;
            }
        }
    }
    assert!(
        found,
        "no StartChildWorkflowExecutionInitiated event found for \
         workflow_id={expected_child_workflow_id} in parent's history"
    );
    Ok(())
}

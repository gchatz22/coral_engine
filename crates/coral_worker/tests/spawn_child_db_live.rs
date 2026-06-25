//! `Decision::SpawnChild` against a real Postgres-backed structural DB.
//!
//! This is the production-wiring counterpart to
//! `coral_temporal/tests/spawn_child_live.rs`: instead of an in-memory
//! `StructuralDbStore` fake, it installs the real `GraphStore` the worker
//! daemon installs at boot, drives a parent through
//! `Idle → SpawnChild → Retire`, and asserts the child's `agents` row +
//! parent→child `edges` row actually land in Postgres (read back via
//! `GraphStore::list_children` / `list_edges_in_graph`).
//!
//! Double-gated: it runs only when `TEMPORAL_LIVE_TEST=1` AND `DATABASE_URL`
//! are both set (a local Temporal Server + docker-compose Postgres). It
//! migrates the target DB itself; the worker daemon does not.
//!
//! One test per binary: the process-wide install hooks panic on
//! double-install, and this test installs a `GraphStore` bound to its own
//! per-run graph.

use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use coral_graph::{GraphStore, MIGRATOR};
use coral_node::agent_ref::{AgentId, GraphId};
use coral_node::decision::{Decide, Decision, Session};
use coral_node::mandate::Mandate;
use coral_node::storage::{AgentStorage, MemoryStorage};
use coral_node::tools::{EchoTool, ToolRegistry};
use coral_temporal::activities::set_decision_script;
use coral_temporal::worker::{
    build_worker, install_agent_storage, install_decide, install_structural_db_store,
    install_tool_registry, StructuralDbStore,
};
use coral_temporal::workflow::{
    agent_workflow_id, AgentInput, AgentResult, AgentWorkflow, FsHandle,
};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// Fallback `Decide` for the child workflow, which shares this worker's
/// `decide_step` activity and would otherwise race the parent for the
/// scripted decisions. A long `Idle` keeps the child alive without polling
/// — the child outlives parent retirement. It also catches the parent once
/// its scripted decisions drain: the empty-script `decide_step` falls back
/// here and returns `Idle`, ending the parent's cycle without a panic.
struct LongIdleDecide;

#[async_trait]
impl Decide for LongIdleDecide {
    async fn decide(&self, _session: &Session) -> anyhow::Result<Decision> {
        Ok(Decision::Idle {
            next_after: Duration::from_secs(60),
        })
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// Same lock-across-await + script-injection rationale as the fake-backed
// `spawn_child_live` test.
#[allow(clippy::await_holding_lock)]
async fn spawn_child_writes_agent_and_edge_rows_to_postgres() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping spawn_child_writes_agent_and_edge_rows_to_postgres; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    let Some(database_url) = env::var("DATABASE_URL").ok().filter(|s| !s.is_empty()) else {
        eprintln!(
            "skipping spawn_child_writes_agent_and_edge_rows_to_postgres; \
             set DATABASE_URL to a docker-compose Postgres to run"
        );
        return;
    };
    run_smoke(&database_url)
        .await
        .expect("spawn_child DB live test");
}

async fn run_smoke(database_url: &str) -> Result<()> {
    // ---- Real structural DB: migrate, seed the parent graph + agent ----
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(database_url)
        .await
        .context("connecting to structural DB (DATABASE_URL)")?;
    MIGRATOR
        .run(&pool)
        .await
        .context("applying structural-DB migrations")?;
    let store = GraphStore::new(pool);

    let suffix = run_suffix();
    let graph = store
        .create_graph(
            &format!("spawn-child-worker-{suffix}"),
            serde_json::json!({}),
        )
        .await
        .context("seeding parent graph")?;
    let parent = store
        .add_agent(graph.id, "parent")
        .await
        .context("seeding parent agent")?;
    let parent_graph_id = GraphId::new(graph.id);
    let parent_agent_id = AgentId::new(parent.id);

    // Install the real GraphStore as the worker's structural-DB store —
    // the exact production wiring under test. The store clone above stays
    // local for read-back assertions (`PgPool` is `Arc`-shaped, so the
    // clone shares the connection pool).
    install_agent_storage(Arc::new(MemoryStorage::new()) as Arc<dyn AgentStorage>);
    let mut reg = ToolRegistry::new();
    reg.register(Arc::new(EchoTool)).expect("register EchoTool");
    install_tool_registry(Arc::new(reg));
    install_decide(Arc::new(LongIdleDecide) as Arc<dyn Decide>);
    install_structural_db_store(Arc::new(store.clone()) as Arc<dyn StructuralDbStore>);

    let task_queue = format!("coral-agents-spawn-child-db-{suffix}");

    let child_mandate = Mandate::new(
        "child mandate (spawn_child DB live test)",
        Duration::from_millis(500),
        Some(2),
    );
    // One scripted decision for the parent's first cycle: SpawnChild runs
    // as the cycle's step 0, then the empty-script `decide_step` falls back
    // to `LongIdleDecide` (Idle), ending the cycle. Only the parent exists
    // when the script is installed — the child is spawned by this decision —
    // so the parent wins this single FIFO entry without racing the child.
    // Retirement is driven by the post-spawn `retire` signal, not a scripted
    // decision, to avoid the shared-script race once the child is alive.
    set_decision_script(vec![Decision::SpawnChild {
        agent_name: "fetcher".into(),
        mandate: child_mandate,
    }]);

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
    let driver_store = store.clone();
    let driver = tokio::spawn(async move {
        let parent_workflow_id = format!(
            "{}-{suffix}",
            agent_workflow_id(&parent_graph_id.to_string(), &parent_agent_id.to_string()),
        );
        eprintln!(
            "spawn_child_db_live: starting parent workflow_id={parent_workflow_id} on {driver_task_queue}"
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
            driver_store,
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
    store: GraphStore,
) -> Result<()> {
    let mut input = AgentInput::new_for_test(parent_graph_id, parent_agent_id, "parent");
    input.fs_handle = FsHandle {
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

    // Poll the real DB until the child row lands (SpawnChild fired), then
    // signal Retire. Polling before signaling is load-bearing: signaling
    // too early lets the parent's retirement short-circuit win and the
    // child is never spawned. Cap at 30s; sub-second is the healthy path.
    let parent_uuid = parent_agent_id.into_uuid();
    let poll_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let children = store
            .list_children(parent_uuid)
            .await
            .context("polling list_children")?;
        if !children.is_empty() {
            break;
        }
        if Instant::now() >= poll_deadline {
            return Err(anyhow::anyhow!(
                "no child row after 30s — SpawnChild activity never wrote to Postgres"
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    handle
        .signal(
            AgentWorkflow::retire,
            "spawn_child DB live test: parent retire".to_string(),
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

    // The structural DB should hold exactly one child agent row + one
    // parent→child edge row.
    let children = store
        .list_children(parent_uuid)
        .await
        .context("list_children after retire")?;
    assert_eq!(
        children.len(),
        1,
        "register_child_in_structural_db should write exactly one agent row; got {children:?}"
    );
    assert_eq!(children[0].name, "fetcher", "child agent name mismatch");
    let child_agent_id = AgentId::new(children[0].id);

    let edges = store
        .list_edges_in_graph(parent_graph_id.into_uuid())
        .await
        .context("list_edges_in_graph after retire")?;
    assert_eq!(
        edges.len(),
        1,
        "register_child_in_structural_db should write exactly one edge; got {edges:?}"
    );
    assert_eq!(
        AgentId::new(edges[0].parent_agent_id),
        parent_agent_id,
        "edge parent mismatch"
    );
    assert_eq!(
        AgentId::new(edges[0].child_agent_id),
        child_agent_id,
        "edge child mismatch"
    );

    eprintln!("spawn_child_db_live: child agent {child_agent_id} + edge landed in Postgres");
    Ok(())
}

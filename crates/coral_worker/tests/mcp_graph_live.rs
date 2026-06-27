//! End-to-end MCP-on-the-workflow-path smoke.
//!
//! Proves the full Option-B path the feature exists for: a `graph.yaml`
//! with a `kind: mcp` tool, applied to the structural DB, runs on the
//! worker — which builds the agent's per-graph registry by reading that
//! tool row and spawning the MCP server — dispatches a real tool call,
//! and emits an Output whose evidence traces back to that call.
//!
//! This is the regression anchor for the feature and the end-to-end proof
//! the hermetic MCP-3 tests deferred: `execute_tool` resolving the
//! per-graph registry by `graph_id`, through the real DB-backed provider.
//!
//! Triple-gated. It runs only when all of `TEMPORAL_LIVE_TEST=1` (a local
//! Temporal Server is up), `DATABASE_URL` (docker-compose Postgres; the
//! test migrates it), and `CORAL_SMOKE_MCP=1` (Node on PATH; the worker
//! spawns the server) are set. Otherwise it returns early so the default
//! `cargo test` stays hermetic and offline.
//!
//! Run it:
//! ```bash
//! TEMPORAL_LIVE_TEST=1 CORAL_SMOKE_MCP=1 \
//!   DATABASE_URL=postgres://coral:coral@localhost:5432/coral_structural \
//!   cargo test -p coral_worker --test mcp_graph_live -- --nocapture
//! ```

use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use coral_graph::yaml::{parse_and_validate, ToolKind};
use coral_graph::{GraphStore, MIGRATOR};
use coral_node::agent_ref::GraphId;
use coral_node::decision::{ClaimSeed, Decision, ToolCall};
use coral_node::evidence::EvidenceId;
use coral_node::fs::AgentFs;
use coral_node::mandate::Mandate;
use coral_node::mcp::McpClient;
use coral_node::storage::{AgentStorage, MemoryStorage};
use coral_temporal::activities::set_decision_script;
use coral_temporal::worker::{
    build_worker, install_agent_storage, install_structural_db_store,
    install_tool_registry_provider, StructuralDbStore,
};
use coral_temporal::workflow::{
    agent_workflow_id, AgentInput, AgentResult, AgentWorkflow, FsHandle,
};
use coral_worker::tool_provider::DbToolRegistryProvider;
use sqlx::postgres::PgPoolOptions;

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// `get-sum` is the server-advertised tool name; the model (and this
/// scripted run) call it by that name, not by the graph.yaml tool `id`.
const TOOL_NAME: &str = "get-sum";

fn run_suffix() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().to_string())
        .unwrap_or_else(|_| "no-suffix".into())
}

fn example_graph_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root above crates/coral_worker")
        .join("examples")
        .join("smoke_mcp_temporal")
        .join("graph.yaml")
}

async fn build_client() -> Result<Client> {
    let address = env::var("TEMPORAL_ADDRESS").unwrap_or_else(|_| DEFAULT_ADDRESS.into());
    let namespace = env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| DEFAULT_NAMESPACE.into());
    let url = Url::parse(&address).context("parsing TEMPORAL_ADDRESS")?;
    let connection = Connection::connect(ConnectionOptions::new(url).build())
        .await
        .context("connecting to Temporal Server")?;
    Client::new(connection, ClientOptions::new(namespace).build())
        .context("building Temporal client")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// Lock held across await: the scripted decision queue + process-wide
// installs are global, same rationale as the other live smokes.
#[allow(clippy::await_holding_lock)]
async fn mcp_graph_dispatches_tool_and_emits_cited_output() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping mcp_graph_dispatches_tool_and_emits_cited_output; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    if env::var("CORAL_SMOKE_MCP").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping mcp_graph_dispatches_tool_and_emits_cited_output; \
             set CORAL_SMOKE_MCP=1 (Node on PATH) to run"
        );
        return;
    }
    let Some(database_url) = env::var("DATABASE_URL").ok().filter(|s| !s.is_empty()) else {
        eprintln!(
            "skipping mcp_graph_dispatches_tool_and_emits_cited_output; \
             set DATABASE_URL to a docker-compose Postgres to run"
        );
        return;
    };
    run_smoke(&database_url)
        .await
        .expect("mcp graph live smoke");
}

async fn run_smoke(database_url: &str) -> Result<()> {
    let suffix = run_suffix();

    // ---- Parse the example graph, make its name unique for this run ----
    let yaml_text = std::fs::read_to_string(example_graph_path())
        .context("reading examples/smoke_mcp_temporal/graph.yaml")?;
    let mut graph_yaml = parse_and_validate(&yaml_text).context("validating example graph")?;
    graph_yaml.metadata.name = format!("smoke-mcp-temporal-{suffix}");

    // Pull the MCP server spawn command straight from the graph so the
    // probe below spawns the *same* server the worker will — identical
    // (command, args) ⇒ identical content-addressed evidence id.
    let (command, args) = graph_yaml
        .tools
        .iter()
        .find_map(|t| match &t.kind {
            ToolKind::Mcp { command, args, .. } => Some((command.clone(), args.clone())),
            _ => None,
        })
        .ok_or_else(|| anyhow!("example graph has no kind: mcp tool"))?;

    // ---- Apply to the real structural DB ----
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(database_url)
        .await
        .context("connecting to structural DB (DATABASE_URL)")?;
    MIGRATOR
        .run(&pool)
        .await
        .context("applying structural-DB migrations")?;
    let store = Arc::new(GraphStore::new(pool));
    let applied = store
        .create_from_yaml(&graph_yaml)
        .await
        .context("create_from_yaml(example mcp graph)")?;
    let graph_id = applied.graph_id;
    let root = applied
        .agents
        .iter()
        .find(|a| a.operator_id == "root")
        .ok_or_else(|| anyhow!("applied graph has no `root` agent"))?;
    let root_agent_id = root.db_agent_id;

    // ---- Compute the evidence id the agent's get-sum call will produce ----
    // Probe the same server out-of-band: `McpTool::call` returns
    // `McpClient::call_tool` verbatim, and `EvidenceId` is content-addressed
    // on (tool, args, result), so this id equals the one `execute_tool`
    // records — letting the scripted EmitOutput cite it ahead of time.
    let tool_args = serde_json::json!({"a": 2, "b": 3});
    let args_refs: Vec<&str> = args.iter().map(String::as_str).collect();
    let probe = tokio::time::timeout(
        Duration::from_secs(120),
        McpClient::connect_stdio(&command, &args_refs),
    )
    .await
    .map_err(|_| anyhow!("MCP probe handshake timed out — is npm/network available?"))?
    .context("probe: connect_stdio to MCP server")?;
    let probe_result = probe
        .call_tool(TOOL_NAME, tool_args.clone())
        .await
        .context("probe: call get-sum")?;
    probe.shutdown().await.context("probe: shutdown")?;
    let evidence_id = EvidenceId::new(TOOL_NAME, &tool_args, &probe_result);

    // ---- Install the production wiring under test ----
    let storage = Arc::new(MemoryStorage::new());
    install_agent_storage(storage.clone() as Arc<dyn AgentStorage>);
    install_structural_db_store(store.clone() as Arc<dyn StructuralDbStore>);
    // The real DB-backed provider: builds the per-graph registry by reading
    // this graph's MCP tool row and spawning the server.
    install_tool_registry_provider(Arc::new(DbToolRegistryProvider::new(store.clone())));

    // Scripted single cycle: CallTools(get-sum) → EmitOutput citing the
    // evidence id → Idle (the sole terminal). No `Decide` is installed, so
    // the script must be exactly consumed: the trailing Idle ends the cycle
    // and the step_cap (1 cycle) retires the workflow before any further
    // `decide_step` would fall back to a missing `Decide`. No LLM; the
    // mandate text is decorative on this path.
    set_decision_script(vec![
        Decision::CallTools {
            calls: vec![ToolCall::new(
                TOOL_NAME,
                tool_args.clone(),
                ClaimSeed::new("mcp-smoke-seed"),
            )],
        },
        Decision::EmitOutput {
            content: "mcp_graph_live: get-sum via server-everything".into(),
            evidence: vec![evidence_id.clone()],
        },
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
    ]);

    let task_queue = format!("coral-agents-mcp-graph-{suffix}");
    let agent_prefix = format!("graphs/{graph_id}/agents/{root_agent_id}");

    let telemetry_options = TelemetryOptions::builder().build();
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(telemetry_options)
            .build()
            .map_err(|e| anyhow!("RuntimeOptions build failed: {e}"))?,
    )?;
    let client = build_client().await?;
    let mut worker = build_worker(&runtime, client.clone(), &task_queue)?;
    let shutdown = worker.shutdown_handle();

    let driver_task_queue = task_queue.clone();
    let driver_prefix = agent_prefix.clone();
    let driver_evidence = evidence_id.clone();
    let driver = tokio::spawn(async move {
        let workflow_id = format!(
            "{}-{suffix}",
            agent_workflow_id(&graph_id.to_string(), &root_agent_id.to_string()),
        );
        eprintln!("mcp_graph_live: starting workflow_id={workflow_id} on {driver_task_queue}");
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
            &workflow_id,
            graph_id,
            root_agent_id,
            &driver_prefix,
            driver_evidence,
            storage,
        )
        .await
    });

    let worker_result = tokio::time::timeout(Duration::from_secs(120), worker.run())
        .await
        .map_err(|_| anyhow!("worker.run() timed out (120s)"))?
        .map_err(|e| anyhow!("worker.run() exited with error: {e}"));
    let driver_result = driver.await.context("driver task panicked")?;

    worker_result?;
    driver_result?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn drive(
    client: Client,
    task_queue: &str,
    workflow_id: &str,
    graph_id: GraphId,
    root_agent_id: coral_node::agent_ref::AgentId,
    agent_prefix: &str,
    evidence_id: EvidenceId,
    storage: Arc<MemoryStorage>,
) -> Result<()> {
    let mut input = AgentInput::new_for_test(graph_id, root_agent_id, "root");
    input.fs_handle = FsHandle {
        prefix: agent_prefix.to_string(),
    };
    // Dispatch is scoped per agent. The fixture assigns the MCP def `everything`
    // to `root` (`tools: [everything]`), which advertises `get-sum`; grant that
    // def so the scripted call is allowed. The real `DbToolRegistryProvider`
    // records the advertised-name -> def-id ownership at registry build.
    input.mandate.tools = vec!["everything".to_string()];
    // One cycle runs the 3 scripted steps (CallTools → EmitOutput → Idle),
    // then the cap stops the workflow (agents never self-terminate).
    input.mandate.step_cap = Some(1);
    let handle = client
        .start_workflow(
            AgentWorkflow::run,
            input,
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow)")?;

    let result: AgentResult = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result")?;
    let AgentResult::Retired { reason } = result;
    assert_eq!(
        reason, "step_cap (1) reached",
        "workflow returned wrong retire reason: {reason:?}"
    );

    // The emitted Output must exist and cite the MCP call's evidence id.
    // `persist_output` enforces provenance (the cited id must resolve to a
    // record on disk), so a present, correctly-cited output proves the
    // get-sum call was dispatched through the per-graph MCP registry and
    // its evidence persisted.
    let inspect_mandate = Mandate::new("inspect", Duration::from_millis(0), None);
    let inspect_storage: Arc<dyn AgentStorage> = storage.clone();
    let fs = AgentFs::new_with_storage(inspect_storage, agent_prefix, &inspect_mandate)
        .await
        .context("open inspecting AgentFs")?;
    let outs = fs
        .list_recent_outputs(8)
        .await
        .context("list_recent_outputs")?;
    assert_eq!(
        outs.len(),
        1,
        "expected exactly one output after EmitOutput; got {}: {outs:?}",
        outs.len()
    );
    assert!(
        outs[0].evidence.contains(&evidence_id),
        "output must cite the MCP call's evidence id {evidence_id:?}; got {:?}",
        outs[0].evidence
    );

    // The evidence record itself must be on disk (provenance resolves).
    let key = format!("{agent_prefix}/evidence/{evidence_id}.json");
    assert!(
        storage
            .get(&key)
            .await
            .context("MemoryStorage::get evidence record")?
            .is_some(),
        "evidence record absent at {key} — the MCP tool call did not persist"
    );
    eprintln!("mcp_graph_live: output cites MCP evidence id {evidence_id}");
    Ok(())
}

//! `jarvis apply` — thin Temporal client with multi-agent YAML support.
//!
//! Operator-facing CLI: reads a `graph.yaml`, validates it, writes the
//! structural DB (CREATE-only), starts every `AgentWorkflow` in the
//! graph (parents-before-children DFS) on the long-lived worker
//! daemon's canonical task queue, signals the seed triggers, prints
//! the workflow IDs + a Temporal CLI describe hint per agent, and
//! exits. **Does not host a Temporal worker.** Execution happens on
//! the separately-deployed daemon at
//! `crates/jarvis_worker/src/bin/worker.rs`.
//!
//! Workflow IDs are the UUID-shaped flat form
//! `graphs/<graph_uuid>/agents/<agent_uuid>` — cross-agent FS reads
//! look agents up by `agent_id`, so the workflow id format must derive
//! from the same UUID.
//!
//! # Scope
//!
//! - **CREATE-only.** Re-applying a graph with the same `metadata.name`
//!   errors out.
//! - **`kind: builtin` tools only.**
//! - **Signaled, not stored.** `seed.triggers` are consumed at apply
//!   time and signaled to the targeted agent; not persisted.
//! - **`policy:` is pass-through.** Stored verbatim into
//!   `graphs.metadata`; not enforced.
//! - **Hierarchical YAML only.** Flat `parent:`-ref form not supported.
//!
//! # Env vars
//!
//! - `DATABASE_URL` — sqlx Postgres URL. Required.
//! - `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` — Temporal client config,
//!   defaults `http://localhost:7233` / `default`.
//! - `TEMPORAL_TASK_QUEUE` — task queue to dispatch onto. Defaults to
//!   [`DEFAULT_TASK_QUEUE`] (`jarvis-agents`).

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{anyhow, Context, Result};
use sqlx::postgres::PgPoolOptions;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowSignalOptions,
    WorkflowStartOptions,
};
use temporalio_sdk_core::Url;
use tracing::info;

use jarvis_graph::yaml::{
    build_workflow_starts, is_multi_agent, parse_and_validate, yaml_seed_triggers, GraphYaml,
};
use jarvis_graph::{GraphStore, GraphStoreError, MIGRATOR};
use jarvis_temporal::worker::DEFAULT_TASK_QUEUE;
use jarvis_temporal::workflow::AgentWorkflow;

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

const USAGE: &str = "\
jarvis-apply — bring a graph into existence from a graph.yaml.

Thin Temporal client: writes the structural DB, dispatches every
AgentWorkflow in the graph (parents-first DFS) onto the worker
daemon's task queue, signals seed triggers, prints the workflow IDs,
and exits. Workflow execution happens on the long-lived worker daemon
(run `cargo run -p jarvis_worker --bin worker` in a separate
terminal, or via the `worker` compose service — see top-level
README's Dev Environment section).

USAGE:
    jarvis-apply <graph.yaml>

ARGS:
    <graph.yaml>  Operator-authored graph definition. See
                  `examples/graph.schema.json` for the JSON schema +
                  `scratch/graph_yaml_schema.md` §§ 2-3 for the v1
                  single-agent and multi-agent shapes.

SCOPE:
    - Hierarchical multi-agent supported. `agents:` is a forest;
      each agent may nest `children:`. Flat `parent:`-ref form is not
      supported.
    - CREATE-only. Re-applying a graph with the same `metadata.name`
      errors out cleanly.
    - `kind: builtin` tools only. `kind: mcp` is rejected.
    - `seed.triggers:` are signaled to the targeted workflow at apply
      time, not persisted anywhere. Any agent in the tree can be a
      seed target (not just the root).
    - `policy:` is pass-through into `graphs.metadata`; not enforced.

ENV:
    DATABASE_URL                           Postgres URL for the structural DB.
                                           Required. Dev stack URL lives in
                                           `.env.example`
                                           (postgres://jarvis:jarvis@localhost:5432/jarvis_structural).
    TEMPORAL_ADDRESS / TEMPORAL_NAMESPACE  Temporal Server connection (defaults
                                           localhost:7233 / default).
    TEMPORAL_TASK_QUEUE                    Task queue the worker daemon listens on
                                           (default `jarvis-agents`). Override only
                                           when the daemon is also running under a
                                           non-default queue.
";

#[tokio::main]
async fn main() -> ExitCode {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,temporalio=warn")),
        )
        .try_init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("jarvis-apply: {e:#}");
            ExitCode::from(1)
        }
    }
}

async fn run() -> Result<()> {
    let args = parse_args(env::args().skip(1).collect())?;
    let graph = load_graph(&args.graph_yaml)?;
    let agent_count: usize = count_agents(&graph);
    let shape = if is_multi_agent(&graph) {
        "multi-agent"
    } else {
        "single-agent"
    };
    info!(
        graph_yaml = %args.graph_yaml.display(),
        name = graph.metadata.name.as_str(),
        agent_count,
        shape,
        "parsed + validated graph.yaml",
    );

    // ---- Structural DB write ------------------------------------------
    // Done *before* the Temporal client connect so a collision (CREATE-
    // only) fails fast without a wasted Temporal round-trip. The walk
    // is DFS parents-first across the agent forest; tools + agent_tools
    // attach in the same transaction.
    let database_url = env::var("DATABASE_URL")
        .map_err(|_| anyhow!("DATABASE_URL must be set (see jarvis-apply --help)"))?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .with_context(|| format!("connecting to structural DB at DATABASE_URL ({database_url})"))?;
    MIGRATOR
        .run(&pool)
        .await
        .context("applying structural-DB migrations (jarvis_graph::MIGRATOR)")?;
    let store = GraphStore::new(pool);
    let applied = match store.create_from_yaml(&graph).await {
        Ok(a) => a,
        Err(GraphStoreError::GraphAlreadyExists { name }) => {
            return Err(anyhow!(
                "graph {name:?} already exists in the structural DB; \
                 `jarvis apply` is CREATE-only. Drop the existing graph \
                 or use a different `metadata.name`."
            ));
        }
        Err(e) => return Err(e).context("create_from_yaml"),
    };
    info!(
        graph_id = %applied.graph_id,
        graph_name = applied.graph_name.as_str(),
        agent_count = applied.agents.len(),
        "wrote structural-DB rows for graph (graph + agents + edges + tools)",
    );

    // ---- Build per-agent AgentInputs against allocated UUIDs ----------
    // `build_workflow_starts` consumes the validated YAML + the
    // allocated `AppliedGraph` and produces DFS parents-first
    // (workflow_id, AgentInput) pairs. Each child's
    // `parent_handle.workflow_id` is set to the parent's workflow id
    // already issued earlier in this vector. Every
    // `AgentInput.agent_id` is the real DB UUID.
    let task_queue = env::var("TEMPORAL_TASK_QUEUE").unwrap_or_else(|_| DEFAULT_TASK_QUEUE.into());
    let starts = build_workflow_starts(&graph, &applied);
    let triggers = yaml_seed_triggers(&graph, &applied)
        .context("yaml_seed_triggers (post-validate runtime guard)")?;

    let client = build_client().await?;
    info!(
        agent_count = starts.len(),
        task_queue, "dispatching AgentWorkflows (parents-first)"
    );

    // Start workflows in DFS parents-first order. Each child references
    // its parent's `workflow_id` via `parent_handle` — `start_workflow`
    // does not require the parent's workflow to have produced a first
    // activation yet; only that the id has been issued (which happens
    // synchronously on the server when `start_workflow` returns).
    for start in &starts {
        client
            .start_workflow(
                AgentWorkflow::run,
                start.input.clone(),
                WorkflowStartOptions::new(&task_queue, &start.workflow_id).build(),
            )
            .await
            .with_context(|| {
                format!(
                    "start_workflow(AgentWorkflow) for workflow_id={} — is the worker daemon running?",
                    start.workflow_id
                )
            })?;
    }

    // Signal each seed trigger against its resolved workflow id. Order
    // is preserved from the YAML. We use the bare client (not a handle
    // returned from `start_workflow`) so we can target any agent —
    // `seed.triggers[].agent` may name any node in the tree, not just
    // a root.
    for (i, resolved) in triggers.into_iter().enumerate() {
        let handle = client.get_workflow_handle::<AgentWorkflow>(&resolved.workflow_id);
        handle
            .signal(
                AgentWorkflow::external_signal,
                resolved.trigger,
                WorkflowSignalOptions::default(),
            )
            .await
            .with_context(|| {
                format!(
                    "signaling seed trigger #{i} against workflow {}",
                    resolved.workflow_id
                )
            })?;
    }

    // Operator-facing output. One line per started workflow + one
    // `temporal workflow describe` hint per agent. Operators usually
    // care most about the root; we print roots first (DFS parents-first
    // ordering means root entries are at the top of the list naturally).
    println!("jarvis-apply: graph_id={}", applied.graph_id);
    println!(
        "jarvis-apply: graph_name={} agent_count={}",
        applied.graph_name,
        starts.len()
    );
    for start in &starts {
        // Recover the operator id for the line so the operator can
        // grep their YAML.
        let operator_id = applied
            .agents
            .iter()
            .find(|a| {
                applied
                    .id_map
                    .get(&a.operator_id)
                    .map(|w| w.workflow_id == start.workflow_id)
                    .unwrap_or(false)
            })
            .map(|a| a.operator_id.as_str())
            .unwrap_or("?");
        println!(
            "jarvis-apply: started agent {operator_id} workflow_id={}",
            start.workflow_id
        );
    }
    if let Some(first) = starts.first() {
        println!(
            "jarvis-apply: temporal workflow describe --workflow-id {}",
            first.workflow_id
        );
    }
    Ok(())
}

/// Count every agent in the YAML forest (top-level + all nested
/// children). Used for the info-log line; purely informational.
fn count_agents(graph: &GraphYaml) -> usize {
    fn rec(agent: &jarvis_graph::yaml::Agent, n: &mut usize) {
        *n += 1;
        for c in &agent.children {
            rec(c, n);
        }
    }
    let mut n = 0;
    for root in &graph.agents {
        rec(root, &mut n);
    }
    n
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

#[derive(Debug, PartialEq)]
struct Args {
    graph_yaml: PathBuf,
}

fn parse_args(argv: Vec<String>) -> Result<Args> {
    if argv.iter().any(|a| a == "-h" || a == "--help") {
        print!("{USAGE}");
        std::process::exit(0);
    }
    if argv.len() != 1 {
        return Err(anyhow!(
            "expected 1 positional arg (<graph.yaml>), got {}\n\n{USAGE}",
            argv.len()
        ));
    }
    Ok(Args {
        graph_yaml: PathBuf::from(&argv[0]),
    })
}

/// Read + parse + validate the YAML file. Source-located errors propagate
/// verbatim via the `Display` impls on `GraphYamlError` (which prepend
/// `line:col` where the underlying serde / validator path can supply
/// one). Returning the typed value avoids re-parsing in the caller.
fn load_graph(path: &Path) -> Result<GraphYaml> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("reading graph.yaml from {}", path.display()))?;
    parse_and_validate(&text).with_context(|| format!("parsing graph.yaml at {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_args_single_positional() {
        let p = parse_args(v(&["graph.yaml"])).unwrap();
        assert_eq!(p.graph_yaml, PathBuf::from("graph.yaml"));
    }

    #[test]
    fn parse_args_rejects_wrong_arity() {
        let err = parse_args(v(&[])).unwrap_err();
        assert!(format!("{err:#}").contains("1 positional"), "got: {err}");
        let err = parse_args(v(&["a", "b"])).unwrap_err();
        assert!(format!("{err:#}").contains("1 positional"), "got: {err}");
    }

    #[test]
    fn usage_documents_v1_scope_narrowings() {
        // `--help` must list the scope narrowings; pin those lines so
        // the renderer doesn't silently drop them.
        assert!(
            USAGE.contains("Hierarchical multi-agent supported"),
            "USAGE: {USAGE}",
        );
        assert!(USAGE.contains("CREATE-only"), "USAGE: {USAGE}");
        assert!(USAGE.contains("`kind: builtin`"), "USAGE: {USAGE}");
        assert!(
            USAGE.contains("signaled to the targeted workflow"),
            "USAGE: {USAGE}"
        );
    }

    #[test]
    fn usage_documents_thin_client_shape() {
        // The CLI is a thin Temporal client; the worker daemon executes
        // the workflow. The help text must say so loudly enough that an
        // operator who skips the README doesn't try to run the CLI
        // without a daemon and get a confusing connect error.
        assert!(USAGE.contains("Thin Temporal client"), "USAGE: {USAGE}");
        assert!(USAGE.contains("worker daemon"), "USAGE: {USAGE}");
        assert!(USAGE.contains("TEMPORAL_TASK_QUEUE"), "USAGE: {USAGE}");
    }

    #[test]
    fn load_graph_returns_parsed_value_on_happy_path() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("graph.yaml");
        fs::write(
            &path,
            "apiVersion: jarvis.engine/v1alpha1\n\
             kind: Graph\n\
             metadata:\n  name: smoke\n\
             tools:\n  - id: echo\n    kind: builtin\n    builtin: echo\n\
             agents:\n  - id: root\n    mandate:\n      text: x\n      idle_period: 1s\n    tools: [echo]\n\
             seed:\n  triggers:\n    - agent: root\n      at: start\n      external:\n        kind: kickoff\n        payload: {}\n",
        )
        .unwrap();
        let g = load_graph(&path).unwrap();
        assert_eq!(g.metadata.name, "smoke");
    }

    #[test]
    fn load_graph_surfaces_validator_errors_with_path_context() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("graph.yaml");
        fs::write(
            &path,
            "apiVersion: jarvis.engine/v1alpha1\n\
             kind: Graph\n\
             metadata:\n  name: smoke\n\
             tools:\n  - id: echo\n    kind: builtin\n    builtin: echo\n\
             agents:\n  - id: root\n    mandate:\n      text: x\n      idle_period: 1s\n    tools: [missing]\n\
             seed:\n  triggers:\n    - agent: root\n      at: start\n      external:\n        kind: kickoff\n        payload: {}\n",
        )
        .unwrap();
        let err = load_graph(&path).unwrap_err();
        let msg = format!("{err:#}");
        // Path context from anyhow's `with_context`.
        assert!(msg.contains("graph.yaml"), "got: {msg}");
        // Underlying validator error names the offending tool id.
        assert!(msg.contains("missing"), "got: {msg}");
    }
}

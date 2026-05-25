//! Stage 4.2 cleanup (JAR2-76) — `jarvis apply` thin Temporal client.
//!
//! Operator-facing CLI: reads a `graph.yaml`, validates it, writes the
//! structural DB (CREATE-only — see <JAR2-71>), starts an `AgentWorkflow`
//! on the long-lived worker daemon's canonical task queue, signals the
//! seed triggers, prints the workflow ID + a Temporal CLI describe hint,
//! and exits. **Does not host a Temporal worker.** Execution happens on
//! the separately-deployed daemon at
//! `crates/jarvis_temporal/src/bin/worker.rs`.
//!
//! See `scratch/temporal_staged_plan.md` § 2.6 (operator CLIs are thin
//! Temporal clients). JAR2-73 first shipped this binary in smoke-style
//! (inline worker + `get_result` block) as a pragmatic v1 expedient when
//! no worker daemon was deployed; JAR2-75 made the daemon the canonical
//! dev-loop target, and this ticket (JAR2-76) finishes the cleanup by
//! switching the CLI to the thin-client shape.
//!
//! # What it does
//!
//! 1. Parse `<graph.yaml>` from argv.
//! 2. Read the YAML, run `jarvis_graph::yaml::parse_and_validate` —
//!    source-located errors (`line:col: message`) print to stderr.
//! 3. Connect to the structural DB via `DATABASE_URL`, run migrations
//!    idempotently, and call `GraphStore::create_from_yaml`. The
//!    `metadata.name` collision is a typed
//!    `GraphStoreError::GraphAlreadyExists`; print a clean error and
//!    exit non-zero.
//! 4. Convert the validated YAML into an `AgentInput` via
//!    `jarvis_graph::yaml::into_agent_input` (hermetic, no DB / Temporal).
//! 5. Connect a Temporal client.
//! 6. `start_workflow(AgentWorkflow, ..., task_queue)` against
//!    [`DEFAULT_TASK_QUEUE`] (`jarvis-agents`, overrideable via
//!    `TEMPORAL_TASK_QUEUE` so a fleet-shard / personal-queue setup
//!    stays consistent with the daemon's same override). The workflow
//!    ID is `agent_workflow_id(metadata.name, agents[0].id)`.
//! 7. Signal each `seed.triggers` entry (in YAML order) via
//!    `handle.signal(AgentWorkflow::external_signal, ...)`.
//! 8. Print the workflow ID + a `temporal workflow describe` hint to
//!    stdout. **Exit.** The agent retires asynchronously on the daemon.
//!
//! # FS root ownership
//!
//! The CLI does **not** take a `<fs_root>` argument and does **not** stamp
//! a per-invocation subdir. The worker daemon owns the FS root via
//! `AGENT_FS_ROOT` (`./agent-fs` by default; see the worker binary's
//! module doc). Operators inspecting on-disk artifacts read them under
//! the daemon's root. Single source of truth — the CLI has no business
//! knowing where the daemon stores state.
//!
//! # v1 scope narrowings (locked in by <JAR2-71>)
//!
//! - **Single-agent only.** The validator rejects zero / more-than-one
//!   `agents:` entries.
//! - **CREATE-only.** `jarvis apply` against a graph with the same
//!   `metadata.name` as an existing row errors out. No edit / prune /
//!   warn-and-leave today. Stage 5 owns reconciliation.
//! - **`kind: builtin` tools only.** MCP-in-worker is <JAR2-63>'s
//!   flagged follow-up.
//! - **Signaled, not stored.** `seed.triggers` are consumed at apply
//!   time and signaled to the workflow; not persisted anywhere.
//!
//! # Env vars
//!
//! - `DATABASE_URL` — sqlx Postgres URL. Required; no default (the dev-
//!   stack URL lives in `.env.example`).
//! - `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` — Temporal client config,
//!   defaults `http://localhost:7233` / `default`. Matches the worker
//!   binary's env-var names verbatim.
//! - `TEMPORAL_TASK_QUEUE` — task queue to dispatch onto. Defaults to
//!   [`DEFAULT_TASK_QUEUE`] (`jarvis-agents`). Override only when also
//!   running the daemon under a non-default queue (fleet-shard etc.).

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

use jarvis_graph::yaml::{into_agent_input, parse_and_validate, yaml_seed_triggers, GraphYaml};
use jarvis_graph::{GraphStore, GraphStoreError, MIGRATOR};
use jarvis_temporal::worker::DEFAULT_TASK_QUEUE;
use jarvis_temporal::workflow::{agent_workflow_id, AgentWorkflow};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

const USAGE: &str = "\
jarvis-apply — bring a graph into existence from a graph.yaml.

Thin Temporal client: writes the structural DB, dispatches an
AgentWorkflow onto the worker daemon's task queue, signals seed
triggers, prints the workflow ID, and exits. Workflow execution
happens on the long-lived worker daemon (run `cargo run -p
jarvis_temporal --bin worker` in a separate terminal, or via the
`worker` compose service — see top-level README's Dev Environment
section).

USAGE:
    jarvis-apply <graph.yaml>

ARGS:
    <graph.yaml>  Operator-authored graph definition. See
                  `examples/graph.schema.json` for the JSON schema +
                  `scratch/graph_yaml_schema.md` § 2 for the v1 shape.

V1 SCOPE NARROWINGS (locked in by JAR2-71):
    - Single-agent only. `agents:` must contain exactly one entry; multi-
      agent topology is Stage 5.
    - CREATE-only. Re-applying a graph with the same `metadata.name`
      errors out cleanly; reconciliation (edit / prune / warn-and-leave)
      is Stage 5+.
    - `kind: builtin` tools only. `kind: mcp` is rejected with a
      pointer at JAR2-63's MCP-in-worker follow-up.
    - `seed.triggers:` are signaled to the workflow at apply time, not
      stored anywhere.

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
    info!(
        graph_yaml = %args.graph_yaml.display(),
        name = graph.metadata.name.as_str(),
        agent = graph.agents[0].id.as_str(),
        "parsed + validated graph.yaml",
    );

    // ---- Structural DB write ------------------------------------------
    // Done *before* the Temporal client connect so a collision (CREATE-
    // only) fails fast without a wasted Temporal round-trip.
    let database_url = env::var("DATABASE_URL")
        .map_err(|_| anyhow!("DATABASE_URL must be set (see jarvis-apply --help)"))?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .with_context(|| format!("connecting to structural DB at DATABASE_URL ({database_url})"))?;
    // Apply migrations idempotently — match the worker boot pattern so
    // operators on a fresh DB get the schema applied without a separate
    // `sqlx migrate run` step.
    MIGRATOR
        .run(&pool)
        .await
        .context("applying structural-DB migrations (jarvis_graph::MIGRATOR)")?;
    let store = GraphStore::new(pool);
    let graph_row = match store.create_from_yaml(&graph).await {
        Ok(g) => g,
        Err(GraphStoreError::GraphAlreadyExists { name }) => {
            return Err(anyhow!(
                "graph {name:?} already exists in the structural DB; \
                 v1 of `jarvis apply` is CREATE-only (reconciliation is \
                 deferred to Stage 5 — see JAR2-71). Drop the existing \
                 graph or use a different `metadata.name`."
            ));
        }
        Err(e) => return Err(e).context("create_from_yaml"),
    };
    info!(
        graph_id = %graph_row.id,
        graph_name = graph_row.name.as_str(),
        "wrote structural-DB rows for graph (graph + agent + tools)",
    );

    // ---- Dispatch the workflow onto the daemon ------------------------
    // Task queue is the worker-daemon's canonical queue (overrideable
    // via `TEMPORAL_TASK_QUEUE` to keep the CLI and daemon's override
    // in sync). The daemon owns FS rooting via `AGENT_FS_ROOT`; the
    // `AgentInput.fs_handle.prefix` `into_agent_input` returns is the
    // empty string, deferring all subdir stamping to the daemon's
    // `LocalStorage` + the workflow's per-prefix `AgentFs`.
    let task_queue = env::var("TEMPORAL_TASK_QUEUE").unwrap_or_else(|_| DEFAULT_TASK_QUEUE.into());
    let workflow_id = agent_workflow_id(&graph.metadata.name, &graph.agents[0].id);
    let input = into_agent_input(&graph);
    let triggers = yaml_seed_triggers(&graph);

    let client = build_client().await?;
    info!(workflow_id, task_queue, "dispatching AgentWorkflow");
    let handle = client
        .start_workflow(
            AgentWorkflow::run,
            input,
            WorkflowStartOptions::new(&task_queue, &workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow) — is the worker daemon running?")?;

    // Signal each seed trigger in the YAML's declared order. Sending
    // immediately after `start_workflow` returns is sufficient: the
    // server round-trip for the first workflow task is much longer
    // than the signal-delivery latency, so the first tick observes
    // these on its initial wake.
    for (i, trigger) in triggers.into_iter().enumerate() {
        handle
            .signal(
                AgentWorkflow::external_signal,
                trigger,
                WorkflowSignalOptions::default(),
            )
            .await
            .with_context(|| format!("signaling seed trigger #{i}"))?;
    }

    // Operator-facing output. The workflow ID is the load-bearing
    // identifier the operator follows the run through: Temporal Web UI,
    // `temporal workflow describe`, `temporal workflow show`, etc. The
    // hint on the next line is intentionally a runnable command.
    println!("jarvis-apply: workflow_id={workflow_id}");
    println!("jarvis-apply: temporal workflow describe --workflow-id {workflow_id}");
    Ok(())
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
        // The ticket calls out: `--help` must list the v1 scope narrowings
        // (single-agent only, CREATE-only, `kind: builtin` tools only).
        // Pin those lines so the renderer doesn't silently drop them.
        assert!(USAGE.contains("Single-agent only"), "USAGE: {USAGE}");
        assert!(USAGE.contains("CREATE-only"), "USAGE: {USAGE}");
        assert!(USAGE.contains("`kind: builtin`"), "USAGE: {USAGE}");
        assert!(USAGE.contains("signaled to the workflow"), "USAGE: {USAGE}");
    }

    #[test]
    fn usage_documents_thin_client_shape() {
        // JAR2-76: the CLI is a thin Temporal client; the worker daemon
        // executes the workflow. The help text must say so loudly enough
        // that an operator who skips the README doesn't try to run the
        // CLI without a daemon and get a confusing connect error.
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

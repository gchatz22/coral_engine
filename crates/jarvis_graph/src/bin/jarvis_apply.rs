//! Stage 4.2 (JAR2-73) — `jarvis apply` binary.
//!
//! Operator-facing CLI: reads a `graph.yaml`, validates it, writes the
//! structural DB (CREATE-only — see <JAR2-71>), starts an `AgentWorkflow`
//! per the single agent the YAML declares, signals the seed triggers,
//! waits for retirement, and prints the produced FS-tree artifacts.
//!
//! # What it does
//!
//! 1. Parse `<graph.yaml> <fs_root>` from argv.
//! 2. Read the YAML, run `jarvis_graph::yaml::parse_and_validate` —
//!    source-located errors (`line:col: message`) print to stderr.
//! 3. Connect to the structural DB via `DATABASE_URL` and run
//!    `GraphStore::create_from_yaml`. The `metadata.name` collision is a
//!    typed `GraphStoreError::GraphAlreadyExists`; print a clean error
//!    and exit non-zero.
//! 4. Convert the validated YAML into an `AgentInput` via
//!    `jarvis_graph::yaml::into_agent_input` (hermetic, no DB / Temporal).
//! 5. Stamp a per-invocation FS subdir under `<fs_root>` and root
//!    `LocalStorage` at it (mirrors `jarvis_run_workflow.rs` lines
//!    ~165-200). Install the worker-shared `AgentStorage`, vendor-driven
//!    `Decide`, and bootstrap `ToolRegistry` (just `EchoTool`, matching
//!    today's worker; see <JAR2-63> for the MCP follow-up).
//! 6. Boot a worker on a randomized task queue + drive the workflow on
//!    a spawned task. The workflow ID is
//!    `agent_workflow_id(metadata.name, agents[0].id)` per <JAR2-71>'s
//!    decision matrix.
//! 7. Signal each `seed.triggers` entry (in YAML order) via
//!    `handle.signal(AgentWorkflow::external_signal, ...)`.
//! 8. Wait for `get_result` and print the FS tree under the per-
//!    invocation subdir.
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
//! # Reused from `jarvis_run_workflow.rs` (JAR2-68)
//!
//! - Temporal client construction (`build_client`).
//! - Worker boot via `build_worker` + the spawned-driver shape (worker
//!   on main task, driver on a spawned task, `ShutdownGuard` on drop).
//! - `build_decide_from_env` for vendor selection (reused, not
//!   duplicated).
//! - The FS-subdir stamping (`resolve_fs_root`, `subdir_name`,
//!   `run_suffix`) + FS-tree printing (`print_tree`, `print_tree_entry`).
//!   These are deliberately duplicated for v1; a shared helper crate
//!   becomes worth it when a third caller emerges. Filed as a PR-body
//!   follow-up.
//!
//! # Diverged from `jarvis_run_workflow.rs`
//!
//! - **Three positional args → two.** Triggers are inline in the YAML
//!   (`seed.triggers:`), not a separate `triggers.jsonl` file.
//! - **No JSON config.** The `Mandate` is derived from the YAML's
//!   `agents[0].mandate.{text, idle_period, max_ticks}` via
//!   `into_agent_input`.
//! - **DB writes.** Adds a `GraphStore::create_from_yaml` call between
//!   parse + workflow start.
//!
//! # Env vars
//!
//! - `DATABASE_URL` — sqlx Postgres URL. Required; no default (the dev-
//!   stack URL lives in `.env.example`).
//! - `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` — Temporal client config,
//!   defaults `http://localhost:7233` / `default`. Matches the worker
//!   binary's env-var names verbatim.
//! - `JARVIS_MODEL_VENDOR` + API-key envs (`ANTHROPIC_API_KEY`,
//!   `COHERE_API_KEY`) — handled by `build_decide_from_env`; same
//!   precedence as the worker binary.
//!
//! `AGENT_FS_ROOT` is intentionally **not** an env var here: the ticket
//! flagged it as either-or, but `jarvis_run_workflow.rs` takes
//! `<fs_root>` positional, and the smallest-correct-diff move is to
//! match that surface so the binaries' call shapes are consistent.

use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use jarvis_node::storage::LocalStorage;
use jarvis_node::tools::{EchoTool, ToolRegistry};
use sqlx::postgres::PgPoolOptions;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use tracing::info;

use jarvis_graph::yaml::{into_agent_input, parse_and_validate, yaml_seed_triggers, GraphYaml};
use jarvis_graph::{GraphStore, GraphStoreError, MIGRATOR};
use jarvis_temporal::worker::{
    build_decide_from_env, build_worker, install_agent_storage, install_decide,
    install_tool_registry,
};
use jarvis_temporal::workflow::{
    agent_workflow_id, AgentInput, AgentResult, AgentWorkflow, FsHandle,
};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

const USAGE: &str = "\
jarvis-apply — bring a graph into existence from a graph.yaml.

USAGE:
    jarvis-apply <graph.yaml> <fs_root>

ARGS:
    <graph.yaml>  Operator-authored graph definition. See
                  `examples/graph.schema.json` for the JSON schema +
                  `scratch/graph_yaml_schema.md` § 2 for the v1 shape.
    <fs_root>     Parent directory for the per-agent FS. The binary stamps a
                  fresh timestamped subdirectory inside it for this run
                  (`<YYYY-MM-DDTHH-MM-SS-sssZ>`); the resolved absolute path
                  prints on the first stdout line (`jarvis-apply: fs_root=...`).

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
    JARVIS_MODEL_VENDOR                    Optional explicit vendor selector
                                           (`anthropic` | `cohere`). Defaults to
                                           whichever vendor's API key is set.
    ANTHROPIC_API_KEY / ANTHROPIC_MODEL    Anthropic adapter (when selected).
    COHERE_API_KEY / COHERE_MODEL          Cohere adapter (when selected).
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
    // Done *before* the FS subdir stamp / worker boot so a collision
    // (CREATE-only) fails fast without leaving an orphan FS subdir or
    // having to tear down a partially-booted worker.
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

    // ---- FS stamp + worker-shared installs -----------------------------
    let now = Utc::now();
    let resolved_fs_root = resolve_fs_root(&args.fs_root, now)
        .with_context(|| format!("resolving fs_root under parent {}", args.fs_root.display()))?;
    // Load-bearing first line of stdout. Mirrors `jarvis-run-workflow`'s
    // shape so JAR2-74's integration test can grep the resolved path
    // off the binary's stdout without parsing TYPE-specific headers.
    println!("jarvis-apply: fs_root={}", resolved_fs_root.display());

    let storage = Arc::new(
        LocalStorage::new(resolved_fs_root.clone())
            .with_context(|| format!("opening LocalStorage at {}", resolved_fs_root.display()))?,
    );
    install_agent_storage(storage);
    info!(
        fs_root = %resolved_fs_root.display(),
        "installed AgentStorage backend",
    );

    let (vendor_tag, decide) = build_decide_from_env()?;
    install_decide(decide);
    info!(vendor = vendor_tag, "installed Decide backend");

    // Bootstrap tool registry: `EchoTool` only — same shape as today's
    // worker binary. JAR2-63's MCP-in-worker follow-up will broaden this
    // here and in `bin/worker.rs` together (the structural-DB tool rows
    // we wrote above already preserve operator-authored tool ids, so
    // the worker side becomes a registry-only change later).
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(EchoTool))
        .context("registering EchoTool")?;
    install_tool_registry(Arc::new(registry));
    info!("installed ToolRegistry with tools: echo");

    // ---- Build worker + Temporal client --------------------------------
    let suffix = run_suffix();
    let task_queue = format!("jarvis-apply-{suffix}");
    // Workflow ID derives directly from the validated YAML — operator
    // can `temporal workflow describe graphs/<name>/agents/<id>` and
    // correlate to the YAML by eye (per JAR2-71's decision matrix).
    let workflow_id = agent_workflow_id(&graph.metadata.name, &graph.agents[0].id);

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

    // ---- Drive the workflow on a spawned task --------------------------
    // `Worker::new` returns a non-`Send` `Worker`; the worker runs on
    // the main task while the driver (which talks to the server, not
    // the in-process worker) runs on a spawned task. Same shape as
    // `jarvis_run_workflow::run` (JAR2-68).
    let driver_task_queue = task_queue.clone();
    let driver_workflow_id = workflow_id.clone();
    let driver_input = into_agent_input(&graph);
    let driver_triggers = yaml_seed_triggers(&graph);
    let driver = tokio::spawn(async move {
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
            &driver_workflow_id,
            driver_input,
            driver_triggers,
        )
        .await
    });

    let worker_result = worker
        .run()
        .await
        .map_err(|e| anyhow!("worker.run() exited with error: {e}"));
    let driver_result = driver.await.context("driver task panicked")?;
    worker_result?;
    driver_result?;

    // ---- Print artifacts ----------------------------------------------
    println!("jarvis-apply: fs tree at {}:", resolved_fs_root.display());
    let mut out = io::stdout().lock();
    print_tree(&mut out, &resolved_fs_root)?;
    Ok(())
}

/// Drive a single `AgentWorkflow` start-to-retirement cycle. Mirror of
/// `jarvis_run_workflow::drive` minus the `TriggerLine` envelope shape —
/// `seed.triggers` come pre-decoded as `Trigger::External` values from
/// `yaml_seed_triggers`.
async fn drive(
    client: Client,
    task_queue: &str,
    workflow_id: &str,
    mut input: AgentInput,
    triggers: Vec<jarvis_node::trigger::Trigger>,
) -> Result<()> {
    // `into_agent_input` builds the AgentInput with `fs_handle.prefix =
    // ""` (empty), which matches `jarvis_run_workflow`'s pattern of
    // rooting LocalStorage directly at the per-invocation FS subdir.
    // The reaffirmation here is for the reader: confirm that's the
    // shape we want before we start the workflow.
    input.fs_handle = FsHandle {
        prefix: String::new(),
    };

    info!(workflow_id, task_queue, "starting AgentWorkflow");
    let handle = client
        .start_workflow(
            AgentWorkflow::run,
            input,
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow)")?;

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

    let result: AgentResult = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result")?;
    let AgentResult::Retired { reason } = result;
    println!(
        "jarvis-apply: workflow returned Retired {{ reason: {reason:?} }} (workflow_id={workflow_id})"
    );
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
    fs_root: PathBuf,
}

fn parse_args(argv: Vec<String>) -> Result<Args> {
    if argv.iter().any(|a| a == "-h" || a == "--help") {
        print!("{USAGE}");
        std::process::exit(0);
    }
    if argv.len() != 2 {
        return Err(anyhow!(
            "expected 2 positional args (<graph.yaml> <fs_root>), got {}\n\n{USAGE}",
            argv.len()
        ));
    }
    Ok(Args {
        graph_yaml: PathBuf::from(&argv[0]),
        fs_root: PathBuf::from(&argv[1]),
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

fn run_suffix() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().to_string())
        .unwrap_or_else(|_| "no-suffix".into())
}

/// Format a per-invocation subdirectory name. Duplicate of
/// `jarvis_run_workflow::subdir_name` — extraction to a shared helper
/// crate is a follow-up when a third caller emerges (filed in the PR
/// body).
fn subdir_name(now: DateTime<Utc>) -> String {
    now.format("%Y-%m-%dT%H-%M-%S-%3fZ").to_string()
}

/// Duplicate of `jarvis_run_workflow::resolve_fs_root`. Creates the
/// parent if missing, stamps a fresh timestamped subdir, retries with a
/// ULID suffix on a same-millisecond collision. Documented duplication
/// rather than extraction (see binary header).
fn resolve_fs_root(parent: &Path, now: DateTime<Utc>) -> Result<PathBuf> {
    fs::create_dir_all(parent)
        .with_context(|| format!("creating fs_root parent at {}", parent.display()))?;
    let base = subdir_name(now);
    let mut candidate = parent.join(&base);
    match fs::create_dir(&candidate) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            let suffix = ulid::Ulid::new();
            candidate = parent.join(format!("{base}-{suffix}"));
            fs::create_dir(&candidate).with_context(|| {
                format!(
                    "creating fs_root subdir at {} (post-collision)",
                    candidate.display()
                )
            })?;
        }
        Err(e) => {
            return Err(e)
                .with_context(|| format!("creating fs_root subdir at {}", candidate.display()));
        }
    }
    fs::canonicalize(&candidate)
        .with_context(|| format!("canonicalizing fs_root subdir {}", candidate.display()))
}

/// Duplicate of `jarvis_run_workflow::print_tree`. See binary header
/// for the duplicate-rather-than-extract decision.
fn print_tree(out: &mut impl Write, root: &Path) -> Result<()> {
    if !root.exists() {
        writeln!(out, "(missing)")?;
        return Ok(());
    }
    writeln!(out, "{}", root.display())?;
    let mut entries: Vec<_> = fs::read_dir(root)
        .with_context(|| format!("reading {}", root.display()))?
        .collect::<io::Result<Vec<_>>>()
        .with_context(|| format!("listing {}", root.display()))?;
    entries.sort_by_key(|e| e.file_name());
    let n = entries.len();
    for (i, entry) in entries.into_iter().enumerate() {
        let last = i + 1 == n;
        print_tree_entry(out, &entry.path(), "", last)?;
    }
    Ok(())
}

fn print_tree_entry(out: &mut impl Write, path: &Path, prefix: &str, last: bool) -> Result<()> {
    let connector = if last { "└── " } else { "├── " };
    let name = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string());
    writeln!(out, "{prefix}{connector}{name}")?;
    if path.is_dir() {
        let new_prefix = format!("{prefix}{}", if last { "    " } else { "│   " });
        let mut entries: Vec<_> = fs::read_dir(path)
            .with_context(|| format!("reading {}", path.display()))?
            .collect::<io::Result<Vec<_>>>()
            .with_context(|| format!("listing {}", path.display()))?;
        entries.sort_by_key(|e| e.file_name());
        let n = entries.len();
        for (i, entry) in entries.into_iter().enumerate() {
            print_tree_entry(out, &entry.path(), &new_prefix, i + 1 == n)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_args_two_positionals() {
        let p = parse_args(v(&["graph.yaml", "/tmp/fs"])).unwrap();
        assert_eq!(p.graph_yaml, PathBuf::from("graph.yaml"));
        assert_eq!(p.fs_root, PathBuf::from("/tmp/fs"));
    }

    #[test]
    fn parse_args_rejects_wrong_arity() {
        let err = parse_args(v(&["graph.yaml"])).unwrap_err();
        assert!(format!("{err:#}").contains("2 positional"), "got: {err}");
        let err = parse_args(v(&["a", "b", "c"])).unwrap_err();
        assert!(format!("{err:#}").contains("2 positional"), "got: {err}");
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
    fn subdir_name_uses_dashes_and_ms() {
        use chrono::{TimeZone, Timelike};
        let dt = Utc
            .with_ymd_and_hms(2026, 5, 25, 4, 30, 7)
            .unwrap()
            .with_nanosecond(123_000_000)
            .unwrap();
        let s = subdir_name(dt);
        assert_eq!(s, "2026-05-25T04-30-07-123Z");
    }

    #[test]
    fn resolve_fs_root_creates_missing_parent() {
        let tmp = tempfile::TempDir::new().unwrap();
        let parent = tmp.path().join("nested").join("does-not-exist");
        let now = Utc::now();
        let resolved = resolve_fs_root(&parent, now).unwrap();
        assert!(parent.exists());
        assert!(resolved.exists());
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

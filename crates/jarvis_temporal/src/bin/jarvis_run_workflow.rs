//! Stage 3.12 (JAR2-68) — live-vendor smoke binary for the
//! `AgentWorkflow` host. Mirror of `jarvis_node/bin/node_run_llm.rs`
//! against the Temporal-hosted runtime.
//!
//! # What it does
//!
//! 1. Loads a [`Mandate`] from a `config.json` (same shape as
//!    `examples/smoke_llm_mcp/config.json`).
//! 2. Stamps a fresh timestamped subdirectory under the supplied
//!    `<fs_root>` parent (per-invocation FS), prints the absolute path
//!    on the first stdout line, opens a `LocalStorage` rooted there.
//! 3. Installs the worker-shared [`AgentStorage`] / [`Decide`] /
//!    [`ToolRegistry`] (reusing `worker::build_decide_from_env` and
//!    `worker::install_*` — same plumbing as the long-running
//!    `worker` binary).
//! 4. Boots a Temporal worker on its own task queue.
//! 5. Drives an `AgentWorkflow` via the Temporal client with the URL-
//!    shaped workflow ID
//!    [`agent_workflow_id`](jarvis_temporal::workflow::agent_workflow_id)
//!    and `AgentInput { mandate, fs_handle: <prefix>, .. }`.
//! 6. Waits for the workflow to return `Retired`, then prints the
//!    durable artifacts the workflow body produced under the FS root:
//!    `outputs/`, `retirement.json`, `decisions/`, `evidence/`,
//!    `mandate.json`.
//!
//! `node-run-llm` stays alive as the in-process reference. This binary
//! is the **parallel verification** path per JAR2-68 — same fixture
//! shape (`<config.json> <fs_root>`), same artifact contract.
//!
//! # MCP-server wiring intentionally absent
//!
//! Today's worker only registers the bootstrap `EchoTool` (JAR2-63's
//! flagged follow-up). Threading MCP server spawn configs through env
//! vars + enabling the `mcp` feature on `jarvis_temporal` is queued for
//! stage 4+; this smoke uses the echo-only
//! `examples/smoke_llm_temporal/config.json` fixture and is documented
//! as such. The `node-run-llm` binary remains the path for MCP-tool
//! smokes against the workflow runtime.
//!
//! # Usage
//!
//! ```text
//! ANTHROPIC_API_KEY=sk-... \
//! cargo run --features "llm-anthropic" --bin jarvis-run-workflow -- \
//!     examples/smoke_llm_temporal/config.json \
//!     /tmp/jarvis-smoke-temporal-fs
//! ```
//!
//! `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` are read the same way the
//! `worker` binary reads them. The driver's task queue is randomized
//! per run so repeated invocations don't share state.
//!
//! # Feature gating
//!
//! Requires at least one of `llm-anthropic` / `llm-cohere`. A zero-
//! vendor build still compiles (mirrors the `worker` binary's shape);
//! `build_decide_from_env` errors at boot with a "rebuild with
//! --features" hint.

use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use jarvis_node::mandate::Mandate;
use jarvis_node::storage::LocalStorage;
use jarvis_node::tools::{EchoTool, ToolRegistry};
use jarvis_node::trigger::Trigger;
use serde::Deserialize;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use tracing::info;

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
jarvis-run-workflow — boot the AgentWorkflow against a real Temporal Server and a real Decide.

USAGE:
    jarvis-run-workflow <config.json> <fs_root> <triggers.jsonl>

ARGS:
    <config.json>     JSON-serialized `Mandate` (text, idle_period ms, max_ticks).
                      Same shape as `examples/smoke_llm_mcp/config.json`.
    <fs_root>         Parent directory for the per-agent FS. The binary stamps a
                      fresh timestamped subdirectory inside it for this run
                      (`<YYYY-MM-DDTHH-MM-SS-sssZ>`); the resolved absolute path
                      prints on the first stdout line (`jarvis-run-workflow: fs_root=...`).
    <triggers.jsonl>  One JSON object per line. Either a bare `Trigger` or an
                      envelope: `{\"delay_ms\": <u64>, \"trigger\": <Trigger>}`.
                      Lines starting with `#` (or blank) are ignored. Mirrors
                      `node-run-llm`'s second positional; required because
                      the workflow's first tick has no triggers otherwise and
                      `decide_next_action` would send the LLM an empty prompt.
                      A bootstrap fixture lives at
                      `examples/smoke_llm_temporal/triggers.jsonl`.

ENV:
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
            eprintln!("jarvis-run-workflow: {e:#}");
            ExitCode::from(1)
        }
    }
}

async fn run() -> Result<()> {
    let args = parse_args(env::args().skip(1).collect())?;
    let mandate = load_mandate(&args.config)?;
    let triggers = load_triggers(&args.triggers)?;
    info!(
        triggers_path = %args.triggers.display(),
        count = triggers.len(),
        "loaded initial triggers",
    );

    let now = Utc::now();
    let resolved_fs_root = resolve_fs_root(&args.fs_root, now)
        .with_context(|| format!("resolving fs_root under parent {}", args.fs_root.display()))?;
    // Load-bearing first line of stdout. CI assertions and integration
    // tests parse the absolute path by prefix.
    println!(
        "jarvis-run-workflow: fs_root={}",
        resolved_fs_root.display()
    );

    // ---- Install the worker-shared OnceLocks --------------------------
    // Storage rooted at the per-invocation FS subdir.
    let storage = Arc::new(
        LocalStorage::new(resolved_fs_root.clone())
            .with_context(|| format!("opening LocalStorage at {}", resolved_fs_root.display()))?,
    );
    install_agent_storage(storage);
    info!(
        fs_root = %resolved_fs_root.display(),
        "installed AgentStorage backend",
    );

    // Vendor-driven Decide impl (reuses `worker::build_decide_from_env`
    // so the selection precedence + feature gating live in the library,
    // not duplicated here).
    let (vendor_tag, decide) = build_decide_from_env()?;
    install_decide(decide);
    info!(vendor = vendor_tag, "installed Decide backend");

    // Bootstrap ToolRegistry — `EchoTool` only. MCP-server wiring is
    // JAR2-63's flagged follow-up; the `smoke_llm_temporal` fixture is
    // echo-only on purpose (see module doc).
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(EchoTool))
        .context("registering EchoTool")?;
    install_tool_registry(Arc::new(registry));
    info!("installed ToolRegistry with tools: echo");

    // ---- Build worker + client ----------------------------------------
    let suffix = run_suffix();
    let task_queue = format!("jarvis-run-workflow-{suffix}");
    let graph_id = "smoke";
    let agent_id = format!("a-{suffix}");
    // FS prefix is empty: `LocalStorage` is already rooted at the
    // per-invocation subdir, so `<prefix>/outputs/...` resolves to
    // `<fs_root>/outputs/...` on disk.
    let prefix = String::new();
    let workflow_id = agent_workflow_id(graph_id, &agent_id);

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

    // ---- Drive the workflow from a spawned task -----------------------
    // `Worker::new` returns a non-`Send` `Worker`; the worker runs on
    // the main task while the driver (which talks to the server, not
    // the in-process worker) runs on a spawned task. Same shape as
    // `tests/workflow_loop.rs` and `bin/temporal_smoke.rs`.
    let driver_task_queue = task_queue.clone();
    let driver_workflow_id = workflow_id.clone();
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
            prefix,
            mandate,
            triggers,
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
    println!(
        "jarvis-run-workflow: fs tree at {}:",
        resolved_fs_root.display()
    );
    let mut out = io::stdout().lock();
    print_tree(&mut out, &resolved_fs_root)?;
    Ok(())
}

async fn drive(
    client: Client,
    task_queue: &str,
    workflow_id: &str,
    prefix: String,
    mandate: Mandate,
    triggers: Vec<TriggerLine>,
) -> Result<()> {
    info!(workflow_id, task_queue, "starting AgentWorkflow");
    let input = AgentInput {
        fs_handle: FsHandle { prefix },
        mandate,
        ..AgentInput::default()
    };
    let handle = client
        .start_workflow(
            AgentWorkflow::run,
            input,
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow)")?;

    // Feed the initial triggers via the existing `external_signal` handler.
    // The workflow's `wait_condition(triggers_pending) || timer(next_wake)`
    // race means the first signal must land before the workflow's first
    // tick observes an empty queue; sending immediately after
    // `start_workflow` returns is sufficient in practice (the server
    // round-trip for the first workflow task is much longer than the
    // signal-delivery latency). Honors `delay_ms` for envelopes that
    // request a sleep before the signal lands.
    for (i, line) in triggers.into_iter().enumerate() {
        let (delay, trigger) = line.into_parts();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        handle
            .signal(
                AgentWorkflow::external_signal,
                trigger,
                WorkflowSignalOptions::default(),
            )
            .await
            .with_context(|| format!("signaling initial trigger #{i}"))?;
    }

    let result: AgentResult = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentWorkflow.get_result")?;
    let AgentResult::Retired { reason } = result;
    println!(
        "jarvis-run-workflow: workflow returned Retired {{ reason: {reason:?} }} (workflow_id={workflow_id})"
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
    config: PathBuf,
    fs_root: PathBuf,
    triggers: PathBuf,
}

fn parse_args(argv: Vec<String>) -> Result<Args> {
    if argv.iter().any(|a| a == "-h" || a == "--help") {
        print!("{USAGE}");
        std::process::exit(0);
    }
    if argv.len() != 3 {
        return Err(anyhow!(
            "expected 3 positional args (<config.json> <fs_root> <triggers.jsonl>), got {}\n\n{USAGE}",
            argv.len()
        ));
    }
    Ok(Args {
        config: PathBuf::from(&argv[0]),
        fs_root: PathBuf::from(&argv[1]),
        triggers: PathBuf::from(&argv[2]),
    })
}

fn load_mandate(path: &Path) -> Result<Mandate> {
    let bytes =
        fs::read(path).with_context(|| format!("reading mandate from {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing mandate JSON in {}", path.display()))
}

/// One line of `triggers.jsonl`. Either a bare `Trigger` value, or an
/// `{delay_ms, trigger}` envelope. Direct mirror of
/// `node_run_llm::TriggerLine` (deliberately inlined rather than shared
/// to keep the smallest correct diff — there are only two callers
/// today).
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TriggerLine {
    Envelope {
        #[serde(default)]
        delay_ms: u64,
        trigger: Trigger,
    },
    Bare(Trigger),
}

impl TriggerLine {
    fn into_parts(self) -> (Duration, Trigger) {
        match self {
            TriggerLine::Envelope { delay_ms, trigger } => {
                (Duration::from_millis(delay_ms), trigger)
            }
            TriggerLine::Bare(t) => (Duration::ZERO, t),
        }
    }
}

fn load_triggers(path: &Path) -> Result<Vec<TriggerLine>> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("reading triggers from {}", path.display()))?;
    let mut out = Vec::new();
    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parsed: TriggerLine = serde_json::from_str(line).with_context(|| {
            format!(
                "parsing trigger JSON on line {} of {}",
                i + 1,
                path.display()
            )
        })?;
        out.push(parsed);
    }
    if out.is_empty() {
        return Err(anyhow!(
            "triggers file {} contained no triggers; at least one is required \
             (workflow's first tick would otherwise drain an empty queue and send \
             the LLM an empty prompt)",
            path.display()
        ));
    }
    Ok(out)
}

fn run_suffix() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().to_string())
        .unwrap_or_else(|_| "no-suffix".into())
}

/// Format a per-invocation subdirectory name. Mirror of
/// `node_run_llm::subdir_name` so the JAR2-68 binary's stamped paths
/// look identical to `node-run-llm`'s (filename-safe ISO-8601 with
/// millisecond precision).
fn subdir_name(now: DateTime<Utc>) -> String {
    now.format("%Y-%m-%dT%H-%M-%S-%3fZ").to_string()
}

/// Mirror of `node_run_llm::resolve_fs_root`. Creates the parent if
/// missing, stamps a fresh timestamped subdir, retries with a ULID
/// suffix on the astronomically unlikely same-millisecond collision.
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

// Tied to a hand-rolled `Args` rather than `clap`; mirror of `node-run-llm`
// (which avoided `clap` for the same reason). Unit-test the parser
// against synthetic argv vectors here.
#[cfg(test)]
mod tests {
    use super::*;

    fn v(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_args_three_positionals() {
        let p = parse_args(v(&["config.json", "/tmp/fs", "triggers.jsonl"])).unwrap();
        assert_eq!(p.config, PathBuf::from("config.json"));
        assert_eq!(p.fs_root, PathBuf::from("/tmp/fs"));
        assert_eq!(p.triggers, PathBuf::from("triggers.jsonl"));
    }

    #[test]
    fn parse_args_rejects_wrong_arity() {
        let err = parse_args(v(&["config.json", "/tmp/fs"])).unwrap_err();
        assert!(format!("{err:#}").contains("3 positional"), "got: {err}");
    }

    #[test]
    fn load_triggers_parses_envelope_and_bare_and_skips_comments() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("triggers.jsonl");
        fs::write(
            &path,
            "# leading comment\n\
             {\"delay_ms\": 50, \"trigger\": {\"type\": \"external\", \"kind\": \"kickoff\", \"payload\": {}}}\n\
             \n\
             {\"type\": \"scheduled_wake\"}\n",
        )
        .unwrap();
        let lines = load_triggers(&path).unwrap();
        assert_eq!(lines.len(), 2);
        let (d0, t0) = lines.into_iter().next().unwrap().into_parts();
        assert_eq!(d0, Duration::from_millis(50));
        assert!(matches!(t0, Trigger::External { .. }));
    }

    #[test]
    fn load_triggers_rejects_empty_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("empty.jsonl");
        fs::write(&path, "# only a comment\n\n").unwrap();
        let err = load_triggers(&path).unwrap_err();
        assert!(format!("{err:#}").contains("contained no triggers"));
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
}

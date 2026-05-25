//! Stage 3.2 (JAR2-58) — Jarvis Temporal worker binary.
//! Stage 3.6 (JAR2-62) — env-driven [`LlmDecide`] vendor selection
//! installed on the worker-shared `decide_impl` `OnceLock`.
//! Stage 0 follow-up (JAR2-75) — header expanded to frame this binary as
//! the canonical long-lived daemon that operator CLIs dispatch to.
//!
//! Connects to a Temporal Server, builds a worker via
//! [`jarvis_temporal::worker::build_worker`] (registers
//! [`jarvis_temporal::workflow::AgentWorkflow`] +
//! [`jarvis_temporal::activities::AgentActivities`]), and runs until
//! SIGINT (Ctrl-C).
//!
//! ## Role: canonical dev-loop daemon
//!
//! This is the **long-lived worker daemon** that operator CLIs target
//! per `scratch/temporal_staged_plan.md` § 2.6. The canonical task queue
//! is [`jarvis_temporal::worker::DEFAULT_TASK_QUEUE`] (`jarvis-agents`),
//! exported from the library module so future thin-client CLIs
//! (`jarvis apply`, `jarvis signal`, `jarvis inspect`, `jarvis retire`)
//! import it from one place. Operators dispatch workflows via the
//! Temporal CLI (`temporal workflow start --task-queue jarvis-agents
//! ...`) or those thin-client CLIs; the daemon picks them up.
//!
//! The recommended dev loop is:
//!
//! ```text
//! docker compose up -d                              # backing services
//! cargo run -p jarvis_temporal --bin worker          # this binary
//! ```
//!
//! JAR2-76 finished the thin-client refactor: `jarvis apply` dispatches
//! against this daemon's queue (the legacy `jarvis-run-workflow` smoke
//! binary was deleted in the same ticket — its sole consumer
//! `jarvis_apply_smoke.rs` now spins a worker fixture inline).
//!
//! ## SDK constraints (per `scratch/temporal_rust_sdk_smoke.md`)
//!
//! - **`Worker` is NOT `Send`** (§ 3.1). The worker runs on the main
//!   `tokio` task; SIGINT handling uses a separately-spawned task that
//!   calls the worker's `shutdown_handle()` — a `Fn()`-shaped closure
//!   that asks `worker.run()` to return.
//! - **`Worker::new` returns `Box<dyn Error>` (not `Send + Sync`)** (§ 3.5)
//!   — the `?` chain wraps via `anyhow::anyhow!("{e}")`. Handled in
//!   `jarvis_temporal::worker::build_worker`.
//!
//! ## Configuration
//!
//! - `TEMPORAL_ADDRESS` — gRPC URL, default `http://localhost:7233`.
//! - `TEMPORAL_NAMESPACE` — default `default`.
//! - `TEMPORAL_TASK_QUEUE` — default
//!   [`jarvis_temporal::worker::DEFAULT_TASK_QUEUE`]. Workflow starts
//!   must use the same task queue or workers will not pick them up.
//! - `AGENT_FS_ROOT` — per-agent FS root, default `./agent-fs`. Resolved
//!   on boot into a `LocalStorage` backend installed via
//!   [`jarvis_temporal::worker::install_agent_storage`]. Stage 3.5+
//!   activity bodies reach for it via
//!   [`jarvis_temporal::worker::agent_storage`].
//! - `JARVIS_MODEL_VENDOR` — optional explicit vendor selector,
//!   `"anthropic"` or `"cohere"`. Defaults to whichever vendor's API
//!   key is set (preferring `anthropic` when both are present, mirroring
//!   the `node-run-llm` precedence). Panics at boot if neither key is
//!   set, since no `Decide` impl can be installed and the
//!   `decide_next_action` activity would panic at the first tick
//!   instead.
//! - `ANTHROPIC_API_KEY` / `ANTHROPIC_MODEL` — used by the Anthropic
//!   vendor adapter when selected (see
//!   `jarvis_node::model_client::anthropic`).
//! - `COHERE_API_KEY` / `COHERE_MODEL` — used by the Cohere vendor
//!   adapter when selected.
//!
//! ## Feature gating
//!
//! The library half (`worker.rs`, `activities.rs`) is feature-agnostic.
//! Vendor-specific imports (`AnthropicClient`, `CohereClient`) live
//! behind `#[cfg(feature = "...")]` guards in this binary so a build
//! with neither feature still compiles, but errors at boot when it
//! can't find a vendor to install. Mirrors
//! `jarvis_node/src/bin/node_run_llm.rs`'s feature-gating shape.
//!
//! ## Tool registry (JAR2-63)
//!
//! On boot the worker builds a [`ToolRegistry`] with the bootstrap
//! [`EchoTool`] registered under its default name (`"echo"`) and
//! installs it via
//! [`jarvis_temporal::worker::install_tool_registry`]. The `execute_tool`
//! activity body fetches it via
//! [`jarvis_temporal::worker::tool_registry`] per invocation.
//!
//! **MCP server wiring is a follow-up.** Mirroring `node_run_mcp.rs`'s
//! `ToolRegistry::register_mcp_server_with_policy` requires the `mcp`
//! cargo feature on `jarvis_node`, which `jarvis_temporal` does not yet
//! enable (kept off for the bootstrap to match the rest of the test
//! surface). Threading MCP server spawn configs through env vars and
//! enabling the feature lands in the JAR2-68 close-the-project smoke;
//! today's worker only serves the `echo` tool, which is enough for the
//! JAR2-60 + JAR2-63 live tests.

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use jarvis_node::storage::LocalStorage;
use jarvis_node::tools::{EchoTool, ToolRegistry};
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use tracing::info;

use jarvis_temporal::worker::{
    build_decide_from_env, build_worker, install_agent_storage, install_decide,
    install_tool_registry, DEFAULT_TASK_QUEUE,
};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";
const DEFAULT_FS_ROOT: &str = "./agent-fs";

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

#[tokio::main]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,temporalio=warn")),
        )
        .try_init();

    let task_queue = env::var("TEMPORAL_TASK_QUEUE").unwrap_or_else(|_| DEFAULT_TASK_QUEUE.into());

    // Install the process-wide AgentStorage before the worker starts
    // polling. JAR2-61..66 activity bodies will reach for it via
    // jarvis_temporal::worker::agent_storage().
    let fs_root = env::var("AGENT_FS_ROOT").unwrap_or_else(|_| DEFAULT_FS_ROOT.into());
    let fs_root_path = PathBuf::from(&fs_root);
    let storage = Arc::new(
        LocalStorage::new(fs_root_path.clone())
            .with_context(|| format!("opening LocalStorage at {fs_root}"))?,
    );
    install_agent_storage(storage);
    info!(fs_root = fs_root.as_str(), "installed AgentStorage backend");

    // JAR2-62 — install the process-wide `Decide` impl from env-driven
    // vendor selection before the worker starts polling, so the first
    // `decide_next_action` activity has something to call. Panics at
    // boot if neither vendor's API key is set (see
    // `jarvis_temporal::worker::build_decide_from_env`).
    let (vendor_tag, decide) = build_decide_from_env()?;
    install_decide(decide);
    info!(vendor = vendor_tag, "installed Decide backend");

    // JAR2-63: install the process-wide ToolRegistry the `execute_tool`
    // activity dispatches through. Bootstrap registers only EchoTool;
    // MCP server wiring lands later in the stack (see module doc).
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(EchoTool))
        .context("registering EchoTool in worker boot ToolRegistry")?;
    install_tool_registry(Arc::new(registry));
    info!("installed ToolRegistry with tools: echo");

    // JAR2-80 (stage 5.3) follow-up — the `register_child_in_structural_db`
    // activity body reaches for `worker::structural_db_store()`. Wiring
    // a real `GraphStore` (from `jarvis_graph`) here would require a
    // `jarvis_graph` dep on this binary, which is structurally fine
    // (binaries are leaves), but `jarvis_graph` -> `jarvis_temporal`
    // already exists so adding the reverse edge needs the
    // worker-binary-in-jarvis_graph relocation the staged plan reserves
    // for the Stage 6 operator-surface cleanup. Until then, daemons
    // that drive multi-agent workflows (`Decision::SpawnChild`) will
    // panic on the first spawn; single-agent + signal / retire paths
    // (everything Stage 3 + 4 ships) are unaffected.

    let telemetry_options = TelemetryOptions::builder().build();
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(telemetry_options)
            .build()
            .map_err(|e| anyhow::anyhow!("RuntimeOptions build failed: {e}"))?,
    )?;

    let client = build_client().await?;
    let mut worker = build_worker(&runtime, client, &task_queue)?;
    let shutdown = worker.shutdown_handle();

    // SIGINT handler runs on a spawned task (tokio::signal is Send-safe).
    // The worker stays on the main task because `Worker` is not `Send`.
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("received Ctrl-C; initiating worker shutdown");
        shutdown();
    });

    info!(
        task_queue = task_queue.as_str(),
        vendor = vendor_tag,
        "jarvis worker starting; registered: AgentWorkflow + AgentActivities"
    );
    worker
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("worker.run() exited with error: {e}"))?;
    info!("jarvis worker exited cleanly");
    Ok(())
}

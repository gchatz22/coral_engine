//! Long-lived Jarvis Temporal worker daemon. Connects to a Temporal
//! Server, registers [`jarvis_temporal::workflow::AgentWorkflow`] and
//! [`jarvis_temporal::activities::AgentActivities`] against the
//! canonical task queue, and runs until SIGINT.
//!
//! Configuration env vars: `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`,
//! `TEMPORAL_TASK_QUEUE`, `AGENT_FS_ROOT`, `JARVIS_MODEL_VENDOR`,
//! `ANTHROPIC_API_KEY` / `ANTHROPIC_MODEL`, `COHERE_API_KEY` /
//! `COHERE_MODEL`. The library half is feature-agnostic; vendor-specific
//! constructors are `#[cfg]`-gated here. A build with no vendor still
//! compiles but errors at boot when no `Decide` impl can be installed.

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

    let fs_root = env::var("AGENT_FS_ROOT").unwrap_or_else(|_| DEFAULT_FS_ROOT.into());
    let fs_root_path = PathBuf::from(&fs_root);
    let storage = Arc::new(
        LocalStorage::new(fs_root_path.clone())
            .with_context(|| format!("opening LocalStorage at {fs_root}"))?,
    );
    install_agent_storage(storage);
    info!(fs_root = fs_root.as_str(), "installed AgentStorage backend");

    let (vendor_tag, decide) = build_decide_from_env()?;
    install_decide(decide);
    info!(vendor = vendor_tag, "installed Decide backend");

    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(EchoTool))
        .context("registering EchoTool in worker boot ToolRegistry")?;
    install_tool_registry(Arc::new(registry));
    info!("installed ToolRegistry with tools: echo");

    // `register_child_in_structural_db` reaches for
    // `worker::structural_db_store()`; wiring a real `GraphStore` here
    // requires resolving the `jarvis_graph` <-> `jarvis_temporal`
    // dependency direction, so multi-agent workflows
    // (`Decision::SpawnChild`) still panic on first spawn from this
    // daemon. Single-agent + signal / retire paths are unaffected.

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

    // SIGINT handler runs on a spawned task because `Worker` is not `Send`
    // and must stay pinned to the main task.
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

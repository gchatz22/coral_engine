//! Stage 3.2 (JAR2-58) — Jarvis Temporal worker binary.
//!
//! Connects to a Temporal Server, builds a worker via
//! [`jarvis_temporal::worker::build_worker`] (registers
//! [`jarvis_temporal::workflow::AgentWorkflow`] + a noop activity set),
//! and runs until SIGINT (Ctrl-C). Stage 3.4–3.10 fill in the real
//! activity set; today the noop is enough to prove the registration
//! pipeline.
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

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use jarvis_node::storage::LocalStorage;
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use tracing::info;

use jarvis_temporal::worker::{build_worker, install_agent_storage, DEFAULT_TASK_QUEUE};

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
        "jarvis worker starting; registered: AgentWorkflow + NoopActivities"
    );
    worker
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("worker.run() exited with error: {e}"))?;
    info!("jarvis worker exited cleanly");
    Ok(())
}

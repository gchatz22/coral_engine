//! Stage 3.2 (JAR2-58) — Jarvis Temporal worker binary.
//! Stage 3.6 (JAR2-62) — env-driven [`LlmDecide`] vendor selection
//! installed on the worker-shared `decide_impl` `OnceLock`.
//!
//! Connects to a Temporal Server, builds a worker via
//! [`jarvis_temporal::worker::build_worker`] (registers
//! [`jarvis_temporal::workflow::AgentWorkflow`] +
//! [`jarvis_temporal::activities::AgentActivities`]), and runs until
//! SIGINT (Ctrl-C).
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

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use jarvis_node::decide_llm::LlmDecide;
use jarvis_node::decision::Decide;
#[cfg(feature = "llm-anthropic")]
use jarvis_node::model_client::anthropic::AnthropicClient;
#[cfg(feature = "llm-cohere")]
use jarvis_node::model_client::cohere::CohereClient;
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use jarvis_node::model_client::CompleteOptions;
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use jarvis_node::model_client::ModelClient;
use jarvis_node::storage::LocalStorage;
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use tracing::info;

use jarvis_temporal::worker::{
    build_worker, install_agent_storage, install_decide, DEFAULT_TASK_QUEUE,
};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";
const DEFAULT_FS_ROOT: &str = "./agent-fs";

// These three env-var constants are only consumed inside `resolve_vendor`,
// which is itself gated on at-least-one vendor feature. Gate the consts
// the same way so a feature-less build doesn't fire `dead_code`.
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
const VENDOR_ENV: &str = "JARVIS_MODEL_VENDOR";
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
const ANTHROPIC_API_KEY_ENV: &str = "ANTHROPIC_API_KEY";
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
const COHERE_API_KEY_ENV: &str = "COHERE_API_KEY";

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

    // JAR2-62: install the process-wide `Decide` impl from env-driven
    // vendor selection before the worker starts polling, so the first
    // `decide_next_action` activity has something to call. Panics at
    // boot if neither vendor's API key is set (see `build_decide`).
    let (vendor_tag, decide) = build_decide()?;
    install_decide(decide);
    info!(vendor = vendor_tag, "installed Decide backend");

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

/// Pick the LLM vendor and build the [`Decide`] implementation the
/// `decide_next_action` activity body will call.
///
/// **Selection precedence:**
/// 1. `JARVIS_MODEL_VENDOR` env var, if set to `"anthropic"` or
///    `"cohere"`. Unknown values bubble as an error.
/// 2. Otherwise: whichever vendor's API key is set in the environment.
///    If both are set, prefer `anthropic` (matches `node-run-llm`'s
///    documented vendor order in `bin/node_run_llm.rs::USAGE`).
/// 3. If neither key is set, bail. The activity body would panic at
///    the first `decide_next_action` call anyway; an early-and-loud
///    failure is friendlier.
///
/// **Feature gating.** A vendor selected at runtime must be compiled
/// in (`--features llm-anthropic` / `--features llm-cohere`); the
/// not-built variants return an error pointing at the missing
/// feature. The non-feature-gated body is itself
/// `#[cfg]`-gated on at-least-one vendor, with a "no vendors built"
/// stub for the zero-feature build (still compiles, errors at runtime).
///
/// Returns the vendor tag (`"anthropic"` / `"cohere"`) alongside the
/// trait object so the caller can log it.
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
fn build_decide() -> Result<(&'static str, Arc<dyn Decide>)> {
    let vendor = resolve_vendor()?;
    let model_client: Arc<dyn ModelClient> = match vendor {
        "anthropic" => build_anthropic_client()?,
        "cohere" => build_cohere_client()?,
        other => return Err(anyhow!("internal: resolve_vendor returned `{other}`")),
    };
    let options = CompleteOptions::default();
    let decide: Arc<dyn Decide> = Arc::new(LlmDecide::new(model_client, options));
    Ok((vendor, decide))
}

/// Resolve the vendor selector to one of the compiled-in adapter
/// names. Pulled out of `build_decide` so the selection precedence is
/// readable in one place.
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
fn resolve_vendor() -> Result<&'static str> {
    if let Ok(v) = env::var(VENDOR_ENV) {
        return match v.as_str() {
            "anthropic" => Ok("anthropic"),
            "cohere" => Ok("cohere"),
            other => Err(anyhow!(
                "{VENDOR_ENV}=`{other}` is not a known vendor (expected `anthropic` or `cohere`)"
            )),
        };
    }
    let have_anthropic = env::var(ANTHROPIC_API_KEY_ENV)
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    let have_cohere = env::var(COHERE_API_KEY_ENV)
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    match (have_anthropic, have_cohere) {
        (true, _) => Ok("anthropic"),
        (false, true) => Ok("cohere"),
        (false, false) => Err(anyhow!(
            "no vendor selected: set {VENDOR_ENV}, {ANTHROPIC_API_KEY_ENV}, or {COHERE_API_KEY_ENV}"
        )),
    }
}

/// Zero-vendor stub. The worker binary still compiles in a feature-less
/// build (the workspace `cargo build` does this), but boots only when
/// a vendor is compiled in.
#[cfg(not(any(feature = "llm-anthropic", feature = "llm-cohere")))]
fn build_decide() -> Result<(&'static str, Arc<dyn Decide>)> {
    Err(anyhow!(
        "no LLM vendor compiled in; rebuild with --features llm-anthropic and/or --features llm-cohere"
    ))
}

#[cfg(feature = "llm-anthropic")]
fn build_anthropic_client() -> Result<Arc<dyn ModelClient>> {
    Ok(Arc::new(AnthropicClient::new()))
}

#[cfg(all(
    any(feature = "llm-anthropic", feature = "llm-cohere"),
    not(feature = "llm-anthropic")
))]
fn build_anthropic_client() -> Result<Arc<dyn ModelClient>> {
    Err(anyhow!(
        "vendor `anthropic` requested but not compiled in; rebuild with --features llm-anthropic"
    ))
}

#[cfg(feature = "llm-cohere")]
fn build_cohere_client() -> Result<Arc<dyn ModelClient>> {
    Ok(Arc::new(CohereClient::new()))
}

#[cfg(all(
    any(feature = "llm-anthropic", feature = "llm-cohere"),
    not(feature = "llm-cohere")
))]
fn build_cohere_client() -> Result<Arc<dyn ModelClient>> {
    Err(anyhow!(
        "vendor `cohere` requested but not compiled in; rebuild with --features llm-cohere"
    ))
}

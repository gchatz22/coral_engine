//! Workflow-driven smoke. Proves the full per-tick loop runs end-to-end
//! against a real Temporal Server and lands the three durable artifacts:
//!
//! - `<prefix>/outputs/<ulid>.json` — a provenance-grounded `EmitOutput`.
//! - `<prefix>/retirement.json` — the retirement marker.
//! - `<prefix>/decisions/<tick>.jsonl` — one-line JSONL entry per tick.
//!
//! Env-gated behind `TEMPORAL_LIVE_TEST=1`. Uses a scripted `Decide` and
//! a planted evidence id so `EmitOutput`'s content-addressed provenance
//! check resolves against an on-disk record.

use std::env;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use coral_node::agent_ref::{AgentId, GraphId};
use coral_node::decision::{ClaimSeed, Decision, ToolCall};
use coral_node::evidence::EvidenceRecord;
use coral_node::fs::AgentFs;
use coral_node::mandate::Mandate;
use coral_node::storage::{AgentStorage, MemoryStorage};
use coral_node::tools::{Tool, ToolRegistry};
use coral_temporal::activities::{set_decision_script, DecisionLogEntry};
use coral_temporal::worker::{build_worker, install_agent_storage, install_tool_registry};
use coral_temporal::workflow::{agent_workflow_id, AgentInput, AgentResult, AgentWorkflow};
use uuid::Uuid;

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// Process-wide in-memory storage backend so the activity bodies'
/// `agent_storage()` lookup and the test driver's post-run assertions
/// read the same bytes. The worker-shared install hooks panic on
/// double-install.
static SHARED_STORAGE: OnceLock<Arc<MemoryStorage>> = OnceLock::new();
static INIT: std::sync::Once = std::sync::Once::new();

/// Produces a deterministic `EvidenceRecord` we can plant against and
/// assert on in the EmitOutput arm.
struct EchoLike {
    name: String,
}

#[async_trait]
impl Tool for EchoLike {
    fn name(&self) -> &str {
        &self.name
    }
    async fn call(&self, args: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        Ok(serde_json::json!({"echoed": args, "from": self.name}))
    }
}

const TOOL_NAME: &str = "echo";

/// Install the worker-shared storage + tool registry exactly once. The
/// installs panic on double-install; `INIT` keeps a multi-test binary
/// from hitting that.
fn ensure_installed() -> Arc<MemoryStorage> {
    INIT.call_once(|| {
        let storage: Arc<MemoryStorage> = Arc::new(MemoryStorage::new());
        SHARED_STORAGE
            .set(Arc::clone(&storage))
            .expect("SHARED_STORAGE set exactly once");
        let dyn_storage: Arc<dyn AgentStorage> = storage;
        install_agent_storage(dyn_storage);

        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(EchoLike {
            name: TOOL_NAME.into(),
        }))
        .expect("register echo tool");
        install_tool_registry(Arc::new(reg));
    });
    SHARED_STORAGE.get().cloned().expect("storage installed")
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
        .context("connecting to Temporal Server (is `temporal server start-dev` running?)")?;
    let client_options = ClientOptions::new(namespace).build();
    let client = Client::new(connection, client_options).context("building Temporal client")?;
    Ok(client)
}

/// Scripts a four-tick run (Idle → CallTools → EmitOutput → Retire),
/// drives it via Temporal, and asserts the three durable artifacts land.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// Lock is held across `await` because the scripted decision queue + the
// installed storage / registry are process-wide.
#[allow(clippy::await_holding_lock)]
async fn workflow_smoke_lands_output_retirement_and_decision_log() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping workflow_smoke_lands_output_retirement_and_decision_log; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    run_smoke().await.expect("workflow smoke");
}

async fn run_smoke() -> Result<()> {
    let suffix = run_suffix();
    let task_queue = format!("coral-agents-smoke-{suffix}");
    let agent_prefix = format!("graphs/g-smoke/agents/a-smoke-{suffix}");
    let storage = ensure_installed();

    // Plant one `EchoLike`-shaped `EvidenceRecord` under the workflow's
    // FS prefix so the scripted `EmitOutput` cites a real, on-disk
    // evidence id. EvidenceId is content-addressed on
    // (tool, args, result), so we compute it by building the same
    // record the planting `AgentFs` writes — `record_evidence` returns
    // the id.
    let plant_mandate = Mandate::new("plant", Duration::from_millis(0), None);
    let plant_storage: Arc<dyn AgentStorage> = storage.clone();
    let plant_fs = AgentFs::new_with_storage(plant_storage, &agent_prefix, &plant_mandate)
        .await
        .context("open planting AgentFs")?;
    let planted_id = plant_fs
        .record_evidence(EvidenceRecord::new(
            TOOL_NAME,
            serde_json::json!({"k": "v"}),
            serde_json::json!({"hit": true}),
            Utc::now(),
        ))
        .await
        .context("plant evidence for EmitOutput")?;

    // Scripted sequence: Idle (tick 0) → CallTools(echo) (tick 1) →
    // EmitOutput citing the planted id (tick 2) → Retire (tick 3).
    // Total: four ticks, four decision-log files.
    set_decision_script(vec![
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
        Decision::CallTools {
            calls: vec![ToolCall::new(
                TOOL_NAME,
                serde_json::json!({"q": "smoke"}),
                ClaimSeed::new("smoke-seed"),
            )],
        },
        Decision::EmitOutput {
            content: "workflow_smoke: echo result observed".into(),
            evidence: vec![planted_id.clone()],
        },
        Decision::Retire {
            reason: "workflow_smoke: scripted retire".into(),
        },
    ]);

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
    let driver_storage = storage.clone();
    let driver_prefix = agent_prefix.clone();
    let driver_planted_id = planted_id.clone();
    let driver = tokio::spawn(async move {
        let workflow_id = format!("{}-{suffix}", agent_workflow_id("g-smoke", "a-smoke"));
        eprintln!("workflow_smoke: starting workflow_id={workflow_id} on {driver_task_queue}");
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
            &driver_prefix,
            driver_storage,
            driver_planted_id,
        )
        .await
    });

    // 60-second budget catches stalls; the smoke completes in <2s on a
    // healthy local server.
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
    workflow_id: &str,
    agent_prefix: &str,
    storage: Arc<MemoryStorage>,
    planted_id: coral_node::evidence::EvidenceId,
) -> Result<()> {
    // Override `fs_handle` so the per-run prefix namespaces storage writes.
    let mut input = AgentInput::new_for_test(
        GraphId::new(Uuid::new_v4()),
        AgentId::new(Uuid::new_v4()),
        "workflow-smoke-test",
    );
    input.fs_handle = coral_temporal::workflow::FsHandle {
        prefix: agent_prefix.into(),
    };
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
    assert!(
        reason.contains("scripted retire"),
        "workflow returned wrong retire reason: {reason:?}"
    );

    // ---- Artifact 1: `<prefix>/retirement.json` ---------------------------
    let key = format!("{agent_prefix}/retirement.json");
    let bytes = storage
        .get(&key)
        .await
        .context("MemoryStorage::get on retirement.json")?
        .ok_or_else(|| anyhow::anyhow!("retirement.json absent at key {key}"))?;
    let v: serde_json::Value =
        serde_json::from_slice(&bytes).context("retirement.json is not JSON")?;
    let reason_on_disk = v
        .get("reason")
        .and_then(|x| x.as_str())
        .ok_or_else(|| anyhow::anyhow!("retirement.json missing reason"))?;
    assert!(
        reason_on_disk.contains("scripted retire"),
        "retirement.json carries wrong reason: {reason_on_disk:?}"
    );

    // ---- Artifact 2: `<prefix>/outputs/<ulid>.json` -----------------------
    // Open a fresh `AgentFs` view over the same storage so the
    // tail-index is exercised. The single scripted `EmitOutput` must
    // land exactly one output.
    let inspect_mandate = Mandate::new("inspect", Duration::from_millis(0), None);
    let inspect_storage: Arc<dyn AgentStorage> = storage.clone();
    let inspect_fs = AgentFs::new_with_storage(inspect_storage, agent_prefix, &inspect_mandate)
        .await
        .context("open inspecting AgentFs")?;
    let outs = inspect_fs
        .list_recent_outputs(8)
        .await
        .context("list_recent_outputs")?;
    assert_eq!(
        outs.len(),
        1,
        "expected exactly one output on disk after EmitOutput; got {}: {outs:?}",
        outs.len()
    );
    let on_disk = &outs[0];
    assert_eq!(
        on_disk.content, "workflow_smoke: echo result observed",
        "output content must match scripted EmitOutput"
    );
    assert!(
        on_disk.evidence.contains(&planted_id),
        "output must cite the planted evidence id; got {:?}",
        on_disk.evidence
    );
    eprintln!(
        "workflow_smoke: output landed at outputs/{}.json with {} evidence id(s)",
        on_disk.id,
        on_disk.evidence.len()
    );

    // ---- Artifact 3: `<prefix>/decisions/<tick>.jsonl` --------------------
    // One entry per tick. The scripted sequence produces four decisions
    // (Idle, CallTools, EmitOutput, Retire); the workflow body bumps
    // `tick` only on non-retire arms, so the four ticks land at
    // `decisions/{0,1,2,3}.jsonl`.
    let page = storage
        .list(&format!("{agent_prefix}/decisions/"), None, usize::MAX)
        .await
        .context("listing decisions/")?;
    let mut decision_keys: Vec<&String> =
        page.keys.iter().filter(|k| k.ends_with(".jsonl")).collect();
    decision_keys.sort();
    eprintln!("workflow_smoke: decision-log keys: {decision_keys:?}");
    assert_eq!(
        decision_keys.len(),
        4,
        "expected 4 decision-log entries (Idle, CallTools, EmitOutput, Retire); got {decision_keys:?}"
    );

    // Each file is a single JSONL line that deserializes back to a
    // typed `DecisionLogEntry`. Pin the per-tick decision_summary
    // contract: the four entries match the four scripted decisions.
    let mut summaries: Vec<String> = Vec::with_capacity(4);
    for k in &decision_keys {
        let bytes = storage
            .get(k)
            .await
            .with_context(|| format!("storage.get({k})"))?
            .unwrap_or_else(|| panic!("decision log {k} absent"));
        let line = std::str::from_utf8(bytes.as_ref())
            .with_context(|| format!("{k} body utf-8"))?
            .trim();
        let entry: DecisionLogEntry = serde_json::from_str(line)
            .with_context(|| format!("{k} is not a DecisionLogEntry: {line}"))?;
        summaries.push(entry.decision_summary);
    }
    // First decision was `Idle`, last was `Retire`. The middle two
    // (`CallTools` / `EmitOutput`) are pinned for the same reason —
    // the artifact stream must reflect every scripted tick. Pin a loose
    // contains-check on each so the formatter is free to evolve without
    // breaking this assertion's intent.
    assert!(
        summaries[0].starts_with("Idle"),
        "tick 0 summary: {:?}",
        summaries[0]
    );
    assert!(
        summaries[1].starts_with("CallTools"),
        "tick 1 summary: {:?}",
        summaries[1]
    );
    assert!(
        summaries[2].starts_with("EmitOutput"),
        "tick 2 summary: {:?}",
        summaries[2]
    );
    assert!(
        summaries[3].starts_with("Retire"),
        "tick 3 summary: {:?}",
        summaries[3]
    );

    Ok(())
}

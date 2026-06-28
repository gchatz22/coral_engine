//! graph.yaml-driven smoke for the thin-client `coral-apply` library
//! surface. Drives an `AgentWorkflow` through `parse_and_validate` →
//! `into_agent_input` + `yaml_seed_triggers` → `start_workflow` +
//! `external_signal`, with the worker hosted inline as a test fixture
//! (production runs it out-of-process on the daemon). Asserts the
//! three durable artifacts the workflow body produces land on disk:
//! an output, a retirement marker, and per-tick decision-log entries.
//!
//! The hermetic test cannot rely on a daemon being up, so it builds a
//! worker inline on a per-run randomized task queue and calls
//! `get_result` directly to synchronize the FS-end-state assertions.
//! Inline rather than a helper module — single test today.
//!
//! Structural-DB writes are covered by the `GraphStore::create_from_yaml`
//! unit tests; this test does not spin up Postgres. The binary's stdout
//! contract is exercised in `coral_apply.rs::tests`. `Decide` is
//! scripted: EvidenceId is content-addressed on `(tool, args, result)`,
//! so a real-LLM citation against a synthetic id would fail the
//! workflow's provenance check; we plant a real `EchoLike`-shaped
//! evidence record under the workflow's FS prefix, then script
//! WriteOutput to cite that planted id.
//!
//! `TEMPORAL_LIVE_TEST=1` opts in — same env var `workflow_smoke.rs`
//! uses, so a single CI gate covers both. Without it the test prints a
//! one-line skip and returns Ok.

use std::env;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use coral_graph::yaml::{
    into_agent_input, parse_and_validate, yaml_seed_triggers, AppliedGraph, Cadence, ResolvedAgent,
    ResolvedAgentWorkflow,
};
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

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// Path to the YAML fixture this test exercises. Resolved relative to
/// the workspace root via `CARGO_MANIFEST_DIR` (which points at
/// `crates/coral_graph` at test time).
const GRAPH_YAML_REL: &str = "../../examples/smoke_llm_temporal/graph.yaml";

/// Shared in-memory storage backend. Process-wide because the activity
/// bodies' `agent_storage()` lookup and this test's post-run assertions
/// must read the same bytes. `OnceLock` mirrors `workflow_smoke.rs`'s
/// pattern; the worker-shared install hooks panic on double-install,
/// so all tests in this binary share one storage.
static SHARED_STORAGE: OnceLock<Arc<MemoryStorage>> = OnceLock::new();
static INIT: std::sync::Once = std::sync::Once::new();

/// Test double — same shape as `workflow_smoke.rs::EchoLike`. Produces
/// a deterministic `EvidenceRecord` we can plant against. Duplicated
/// inline per "smallest correct diff"; promoting to the test surface
/// is deferred until a third caller emerges.
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

/// Install the worker-shared storage + tool registry once per process.
/// Matches `workflow_smoke.rs::ensure_installed` verbatim — separate
/// test binary means a separate process means a separate set of
/// `OnceLock`s, so this can't conflict with that one.
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
        // Dispatch is scoped per agent. The fixture YAML defines the tool
        // with def id `echo` and assigns it to `root` (`tools: [echo]`), so
        // map the advertised name to that def id for the call to be allowed.
        reg.record_owner(TOOL_NAME, "echo");
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

fn load_graph_yaml() -> Result<String> {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let path = manifest_dir.join(GRAPH_YAML_REL);
    std::fs::read_to_string(&path)
        .with_context(|| format!("reading graph.yaml fixture from {}", path.display()))
}

/// Live test: drives a single cycle (CallTools → WriteOutput → Idle)
/// where the workflow input and seed triggers come from
/// `examples/smoke_llm_temporal/graph.yaml`. Asserts the three durable
/// artifacts land on disk.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// Lock-across-await: the scripted decision queue + installed storage /
// registry are process-wide. Same rationale as `workflow_smoke.rs`.
#[allow(clippy::await_holding_lock)]
async fn coral_apply_smoke_lands_output_retirement_and_decision_log() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping coral_apply_smoke_lands_output_retirement_and_decision_log; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    run_smoke().await.expect("coral-apply smoke");
}

async fn run_smoke() -> Result<()> {
    // ---- Load + parse the YAML fixture ------------------------------------
    let yaml_text = load_graph_yaml()?;
    let graph = parse_and_validate(&yaml_text).context("parse_and_validate")?;

    // Pin the fixture contents so a YAML edit that drifts the mandate
    // shape fails the test, not the workflow.
    assert_eq!(graph.metadata.name, "smoke-llm-temporal");
    assert_eq!(graph.agents.len(), 1);
    assert_eq!(graph.agents[0].id, "root");
    assert_eq!(graph.agents[0].tools, vec!["echo".to_string()]);
    assert_eq!(
        graph.agents[0].mandate.idle_period,
        Some(Cadence::Every(Duration::from_secs(1)))
    );
    assert_eq!(graph.seed.triggers.len(), 1);
    assert_eq!(graph.seed.triggers[0].external.kind, "kickoff");

    // Pin the mandate-derivation invariants: a YAML edit that drifts
    // the mandate translation should bite here. `into_agent_input`
    // requires the `(graph_id, agent_id)` allocated by
    // `GraphStore::create_from_yaml`; this hermetic test doesn't run
    // against Postgres, so we synthesize fresh UUIDs here. Production
    // `coral apply` gets these from the DB.
    let synth_graph_id = GraphId::new(uuid::Uuid::new_v4());
    let synth_agent_id = AgentId::new(uuid::Uuid::new_v4());
    let agent_input = into_agent_input(&graph, synth_graph_id, synth_agent_id);
    assert_eq!(
        agent_input.mandate.idle_period,
        Some(Duration::from_secs(1))
    );
    // `step_cap` is the harness-only runaway backstop; YAML never authors it.
    assert!(agent_input.mandate.step_cap.is_none());
    assert!(
        agent_input.mandate.text.contains("call the `echo` tool"),
        "mandate.text drifted from the smoke fixture's intent: {:?}",
        agent_input.mandate.text
    );

    // ---- Per-test setup ---------------------------------------------------
    let suffix = run_suffix();
    let task_queue = format!("coral-apply-smoke-{suffix}");
    // `into_agent_input` returns the production-shape prefix
    // `graphs/<metadata.name>/agents/<agents[0].id>`. `MemoryStorage`
    // here is process-wide via `OnceLock`, so any second test in this
    // binary would collide on that prefix; the timestamp suffix sits
    // on top of the prod-shape prefix for per-test isolation while
    // still exercising the production derivation.
    let agent_prefix = format!("{}-{suffix}", agent_input.fs_handle.prefix);
    let storage = ensure_installed();

    // Plant one `EchoLike`-shaped evidence record under the workflow's
    // FS prefix so the scripted WriteOutput cites a real, on-disk
    // evidence id. EvidenceId is content-addressed on
    // (tool, args, result), so we compute it by writing the same record
    // the planting AgentFs writes — `record_evidence` returns the id.
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
        .context("plant evidence for WriteOutput")?;

    // One scripted cycle: CallTools(echo) → WriteOutput citing the planted
    // id → Idle (the sole terminal step ends the cycle). The `step_cap`
    // backstop then retires at the top of the next cycle (agents never
    // self-terminate). Proves the YAML-derived AgentInput drives the
    // expected agent-loop end-state.
    set_decision_script(vec![
        Decision::CallTools {
            calls: vec![ToolCall::new(
                TOOL_NAME,
                serde_json::json!({"q": "smoke"}),
                ClaimSeed::new("smoke-seed"),
            )],
        },
        Decision::WriteOutput {
            body: "coral_apply_smoke: echo result observed".into(),
            citations: vec![planted_id.clone()],
        },
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
    ]);

    // ---- Boot worker + drive the workflow ---------------------------------
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

    // Build the AgentInput from the YAML — `into_agent_input` is the
    // public conversion the binary calls. Override the FS prefix for
    // per-test namespacing.
    let mut input = agent_input;
    input.fs_handle = coral_temporal::workflow::FsHandle {
        prefix: agent_prefix.clone(),
    };
    // Cap the run at one cycle so the loop terminates on the runaway backstop
    // right after the scripted cycle ends on `Idle`. `step_cap` counts cycles;
    // without this the script would exhaust into the (uninstalled) decide impl
    // and panic. The agent never self-terminates; `step_cap` is harness-only.
    input.mandate.step_cap = Some(1);
    // `yaml_seed_triggers` takes an `AppliedGraph` so it can resolve
    // each `seed.triggers[].agent` to a concrete workflow_id (any node
    // in the tree, not just the root). Synthesize a minimal
    // AppliedGraph matching the synthetic UUIDs above; the test only
    // uses the resolved `trigger` field (not `workflow_id`).
    let mut synth_id_map = std::collections::HashMap::new();
    synth_id_map.insert(
        graph.agents[0].id.clone(),
        ResolvedAgentWorkflow {
            db_agent_id: synth_agent_id,
            workflow_id: format!(
                "graphs/{}/agents/{}",
                synth_graph_id.into_uuid(),
                synth_agent_id.into_uuid(),
            ),
        },
    );
    let synth_applied = AppliedGraph {
        graph_id: synth_graph_id,
        graph_name: graph.metadata.name.clone(),
        agents: vec![ResolvedAgent {
            operator_id: graph.agents[0].id.clone(),
            db_agent_id: synth_agent_id,
            parent_db_agent_id: None,
        }],
        id_map: synth_id_map,
    };
    let triggers: Vec<coral_node::trigger::Trigger> = yaml_seed_triggers(&graph, &synth_applied)
        .expect("seed triggers resolve")
        .into_iter()
        .map(|r| r.trigger)
        .collect();

    let driver_task_queue = task_queue.clone();
    let driver_prefix = agent_prefix.clone();
    let driver_storage = storage.clone();
    let driver_planted_id = planted_id.clone();
    let driver_graph_name = graph.metadata.name.clone();
    let driver_agent_id = graph.agents[0].id.clone();
    let driver = tokio::spawn(async move {
        // URL-shaped workflow ID derived from the YAML.
        let workflow_id = format!(
            "{}-{suffix}",
            agent_workflow_id(&driver_graph_name, &driver_agent_id),
        );
        eprintln!("coral_apply_smoke: starting workflow_id={workflow_id} on {driver_task_queue}");
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
            input,
            triggers,
            &driver_prefix,
            driver_storage,
            driver_planted_id,
        )
        .await
    });

    // 60-second budget mirrors `workflow_smoke.rs` — the smoke completes
    // in <2s on a healthy local server.
    let worker_result = tokio::time::timeout(Duration::from_secs(60), worker.run())
        .await
        .map_err(|_| anyhow::anyhow!("worker.run() timed out (60s)"))?
        .map_err(|e| anyhow::anyhow!("worker.run() exited with error: {e}"));
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
    input: AgentInput,
    triggers: Vec<coral_node::trigger::Trigger>,
    agent_prefix: &str,
    storage: Arc<MemoryStorage>,
    _planted_id: coral_node::evidence::EvidenceId,
) -> Result<()> {
    let handle = client
        .start_workflow(
            AgentWorkflow::run,
            input,
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow)")?;

    // Same signal pattern the binary uses post-`start_workflow`:
    // each YAML seed trigger → one `external_signal` in declared order.
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
    assert_eq!(
        reason, "step_cap (1) reached",
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
    assert_eq!(
        reason_on_disk, "step_cap (1) reached",
        "retirement.json carries wrong reason: {reason_on_disk:?}"
    );

    // ---- Artifact 2: `<prefix>/outputs/output.md` -------------------------
    // The agent keeps a single canonical Output, overwritten each write.
    // `read_output` returns its body; citations live in the DB reference
    // graph, not the file, so only the body is checked here.
    let inspect_mandate = Mandate::new("inspect", Duration::from_millis(0), None);
    let inspect_storage: Arc<dyn AgentStorage> = storage.clone();
    let inspect_fs = AgentFs::new_with_storage(inspect_storage, agent_prefix, &inspect_mandate)
        .await
        .context("open inspecting AgentFs")?;
    let body = inspect_fs.read_output().await.context("read_output")?;
    assert_eq!(
        body, "coral_apply_smoke: echo result observed",
        "output body must match scripted WriteOutput"
    );
    eprintln!("coral_apply_smoke: output landed at outputs/output.md");

    // ---- Artifact 3: `<prefix>/decisions/<tick>-<step>.jsonl` -------------
    // One entry per logged decision (one file per step within the cycle).
    // The scripted cycle produces three decisions (CallTools, WriteOutput,
    // Idle) at `decisions/0-{0,1,2}.jsonl`. The `step_cap` backstop retires
    // at the top of the next cycle without logging a decision, so it adds no
    // fourth entry.
    let page = storage
        .list(&format!("{agent_prefix}/decisions/"), None, usize::MAX)
        .await
        .context("listing decisions/")?;
    let mut decision_keys: Vec<&String> =
        page.keys.iter().filter(|k| k.ends_with(".jsonl")).collect();
    decision_keys.sort();
    eprintln!("coral_apply_smoke: decision-log keys: {decision_keys:?}");
    assert_eq!(
        decision_keys.len(),
        3,
        "expected 3 decision-log entries (CallTools, WriteOutput, Idle); got {decision_keys:?}"
    );

    let mut summaries: Vec<String> = Vec::with_capacity(3);
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
    assert!(
        summaries[0].starts_with("CallTools"),
        "step 0 summary: {:?}",
        summaries[0]
    );
    assert!(
        summaries[1].starts_with("WriteOutput"),
        "step 1 summary: {:?}",
        summaries[1]
    );
    assert!(
        summaries[2].starts_with("Idle"),
        "step 2 summary: {:?}",
        summaries[2]
    );

    Ok(())
}

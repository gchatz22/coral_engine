//! Stage 4.3 (JAR2-74) — graph.yaml-driven smoke, refreshed in JAR2-76
//! after the `jarvis-apply` thin-client refactor.
//!
//! Drives an `AgentWorkflow` through the library surface the thin-client
//! `jarvis-apply` CLI dispatches into (`parse_and_validate` →
//! `into_agent_input` + `yaml_seed_triggers` → `start_workflow` +
//! `external_signal`), with the worker hosted **inline as a test
//! fixture** rather than out-of-process on the daemon. Asserts the
//! three durable artifacts the workflow body produces land on disk:
//! an output, a retirement marker, and per-tick decision-log entries.
//!
//! ## What this test proves
//!
//! `jarvis apply graph.yaml` produces the expected workflow end-state
//! (output + retirement + decision log) when dispatched onto a worker
//! that's listening for the same task queue. The production CLI is the
//! thin-client refactored in JAR2-76 — this test exercises the same
//! library surface (`yaml::{parse_and_validate, into_agent_input,
//! yaml_seed_triggers}` then `client.start_workflow` then
//! `handle.signal(AgentWorkflow::external_signal, ...)`) with a worker
//! spun up inline via `build_worker` for the lifetime of the test.
//!
//! ## Test-fixture shape (JAR2-76)
//!
//! Pre-JAR2-76 the binary itself hosted the worker on a randomized task
//! queue and blocked on `get_result`; after JAR2-76 the binary returns
//! once seed signals are sent and execution runs on the worker daemon.
//! For hermeticity the test cannot rely on a daemon being up, so it
//! builds a worker inline (`build_worker(...)` + `tokio::spawn`) on a
//! per-run randomized task queue and calls `get_result` directly to
//! synchronize the FS-end-state assertions. Inline rather than a
//! helper module — single test today; promoting is a follow-up when a
//! second caller appears.
//!
//! ## What this test deliberately does NOT exercise
//!
//! - **Structural-DB writes.** The binary's `GraphStore::create_from_yaml`
//!   call is exercised by JAR2-73's unit tests against an ephemeral
//!   `#[sqlx::test]` DB. Adding it here would force the CI
//!   `temporal-workflow-smoke` job to spin up Postgres for end-state
//!   value already covered upstream. The parent's acceptance bar is
//!   workflow end-state, not DB rows.
//! - **The binary's stdout contract.** "Prints `workflow_id=...`" is
//!   unit-tested in `jarvis_apply.rs::tests`; this test goes through
//!   the same call sites a level deeper.
//! - **Real LLM Decide.** Hermetic — the `decide_next_action` activity
//!   body checks the scripted `DECISION_SCRIPT` *before* reaching for
//!   the installed `Decide` (`activities.rs::decide_next_action`).
//!
//! ## Why a scripted Decide
//!
//! Same rationale as `crates/jarvis_temporal/tests/workflow_smoke.rs`
//! (the test this one mirrors): EvidenceId is content-addressed on
//! `(tool, args, result)`, so a real-LLM citation against a synthetic
//! id would fail the workflow's provenance check. We plant a real
//! `EchoLike`-shaped evidence record under the workflow's FS prefix,
//! then script EmitOutput to cite that planted id.
//!
//! ## Env-gated
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

use jarvis_graph::yaml::{
    into_agent_input, parse_and_validate, yaml_seed_triggers, AppliedGraph, ResolvedAgent,
    ResolvedAgentWorkflow,
};
use jarvis_node::agent_ref::{AgentId, GraphId};
use jarvis_node::decision::{ClaimSeed, Decision, ToolCall};
use jarvis_node::evidence::EvidenceRecord;
use jarvis_node::fs::AgentFs;
use jarvis_node::mandate::Mandate;
use jarvis_node::storage::{AgentStorage, MemoryStorage};
use jarvis_node::tools::{Tool, ToolRegistry};
use jarvis_temporal::activities::{set_decision_script, DecisionLogEntry};
use jarvis_temporal::worker::{build_worker, install_agent_storage, install_tool_registry};
use jarvis_temporal::workflow::{agent_workflow_id, AgentInput, AgentResult, AgentWorkflow};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// Path to the YAML fixture this test exercises. Resolved relative to
/// the workspace root via `CARGO_MANIFEST_DIR` (which points at
/// `crates/jarvis_graph` at test time).
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
/// `OnceLock`s, so this can't conflict with the JAR2-68 smoke.
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

fn load_graph_yaml() -> Result<String> {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let path = manifest_dir.join(GRAPH_YAML_REL);
    std::fs::read_to_string(&path)
        .with_context(|| format!("reading graph.yaml fixture from {}", path.display()))
}

/// JAR2-74 live test: drives a four-tick run (Idle → CallTools →
/// EmitOutput → Retire) where the workflow input and seed triggers
/// come from `examples/smoke_llm_temporal/graph.yaml`. Asserts the
/// three node-run-llm-shaped artifacts land — same artifact contract
/// JAR2-68 pins.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// Lock-across-await: the scripted decision queue + installed storage /
// registry are process-wide. Same rationale as `workflow_smoke.rs`.
#[allow(clippy::await_holding_lock)]
async fn jarvis_apply_smoke_lands_output_retirement_and_decision_log() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping jarvis_apply_smoke_lands_output_retirement_and_decision_log; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    run_smoke().await.expect("jarvis-apply smoke");
}

async fn run_smoke() -> Result<()> {
    // ---- Load + parse the YAML fixture ------------------------------------
    let yaml_text = load_graph_yaml()?;
    let graph = parse_and_validate(&yaml_text).context("parse_and_validate")?;

    // The YAML's content is the test's promise: the fixture encodes the
    // same Mandate the legacy JAR2-68 fixture pair does. Pinning these
    // assertions here means a future YAML edit that drifts away from
    // the legacy `config.json` shape fails the test, not the workflow.
    assert_eq!(graph.metadata.name, "smoke-llm-temporal");
    assert_eq!(graph.agents.len(), 1);
    assert_eq!(graph.agents[0].id, "root");
    assert_eq!(graph.agents[0].tools, vec!["echo".to_string()]);
    assert_eq!(
        graph.agents[0].mandate.idle_period,
        Some(Duration::from_secs(1))
    );
    assert_eq!(graph.agents[0].mandate.max_ticks, Some(8));
    assert_eq!(graph.seed.triggers.len(), 1);
    assert_eq!(graph.seed.triggers[0].external.kind, "kickoff");

    // Pin the mandate-derivation invariants in code now that the legacy
    // `config.json` / `triggers.jsonl` fixtures (the JAR2-68 reference
    // shapes) were deleted in JAR2-76 along with `jarvis_run_workflow`.
    // The YAML is the canonical fixture going forward; these assertions
    // bite if a future YAML edit drifts the mandate translation.
    //
    // JAR2-85 + JAR2-89: `into_agent_input` now requires the
    // `(graph_id, agent_id)` allocated by `GraphStore::create_from_yaml`.
    // The hermetic smoke fixture doesn't run against Postgres (the
    // structural-DB write is exercised by JAR2-73's unit tests instead
    // — see the module-doc note "What this test deliberately does NOT
    // exercise"); we synthesize fresh UUIDs here so the workflow body
    // still gets a real identity triple. Production `jarvis apply`
    // gets these from the DB.
    let synth_graph_id = GraphId::new(uuid::Uuid::new_v4());
    let synth_agent_id = AgentId::new(uuid::Uuid::new_v4());
    let agent_input = into_agent_input(&graph, synth_graph_id, synth_agent_id);
    assert_eq!(agent_input.mandate.idle_period, Duration::from_secs(1));
    assert_eq!(agent_input.mandate.max_ticks, Some(8));
    assert!(
        agent_input.mandate.text.contains("call the `echo` tool"),
        "mandate.text drifted from the smoke fixture's intent: {:?}",
        agent_input.mandate.text
    );

    // ---- Per-test setup ---------------------------------------------------
    let suffix = run_suffix();
    let task_queue = format!("jarvis-apply-smoke-{suffix}");
    // Post-JAR2-76: `into_agent_input` returns the production-shape
    // prefix `graphs/<metadata.name>/agents/<agents[0].id>` so the
    // daemon's shared `LocalStorage` namespaces artifacts per agent.
    // `MemoryStorage` here is process-wide via `OnceLock`, so any second
    // test in this binary would still collide on that one prefix — we
    // append a timestamp suffix to keep the per-test namespace hermetic.
    // The suffix sits on top of the prod-shape prefix rather than
    // replacing it, so this test exercises the same prefix derivation
    // production uses (only with the extra suffix layer for isolation).
    let agent_prefix = format!("{}-{suffix}", agent_input.fs_handle.prefix);
    let storage = ensure_installed();

    // Plant one `EchoLike`-shaped evidence record under the workflow's
    // FS prefix so the scripted EmitOutput cites a real, on-disk
    // evidence id. EvidenceId is content-addressed on
    // (tool, args, result), so we compute it by writing the same record
    // the planting AgentFs writes — `record_evidence` returns the id.
    // Mirrors `workflow_smoke.rs` lines 168-182.
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

    // Scripted sequence: Idle → CallTools(echo) → EmitOutput citing the
    // planted id → Retire. Same shape as JAR2-68's smoke — proves the
    // YAML-derived AgentInput drives the same agent-loop end-state.
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
            content: "jarvis_apply_smoke: echo result observed".into(),
            evidence: vec![planted_id.clone()],
        },
        Decision::Retire {
            reason: "jarvis_apply_smoke: scripted retire".into(),
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
    // public conversion JAR2-73 calls. Override the FS prefix for the
    // per-test namespacing (same shape as `workflow_smoke.rs`).
    let mut input = agent_input;
    input.fs_handle = jarvis_temporal::workflow::FsHandle {
        prefix: agent_prefix.clone(),
    };
    // JAR2-85: `yaml_seed_triggers` now takes an `AppliedGraph` so it
    // can resolve each `seed.triggers[].agent` to a concrete
    // workflow_id (multi-agent supports seeds targeting any node in
    // the tree, not just the root). Synthesize a minimal AppliedGraph
    // matching the synthetic UUIDs above; the test only uses the
    // resolved `trigger` field (not `workflow_id`), so the synthesis
    // is a thin shell.
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
    let triggers: Vec<jarvis_node::trigger::Trigger> = yaml_seed_triggers(&graph, &synth_applied)
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
        // Same URL-shaped workflow ID JAR2-73 derives from the YAML.
        let workflow_id = format!(
            "{}-{suffix}",
            agent_workflow_id(&driver_graph_name, &driver_agent_id),
        );
        eprintln!("jarvis_apply_smoke: starting workflow_id={workflow_id} on {driver_task_queue}");
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
    triggers: Vec<jarvis_node::trigger::Trigger>,
    agent_prefix: &str,
    storage: Arc<MemoryStorage>,
    planted_id: jarvis_node::evidence::EvidenceId,
) -> Result<()> {
    let handle = client
        .start_workflow(
            AgentWorkflow::run,
            input,
            WorkflowStartOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .context("start_workflow(AgentWorkflow)")?;

    // Same signal pattern JAR2-73's binary uses post-`start_workflow`:
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

    // ---- Artifact 2: `<prefix>/outputs/<sha>.json` ------------------------
    // OutputId is content-addressed (JAR2-70 #1) — `sha256(content, evidence)`
    // — so given the same scripted EmitOutput, the filename on disk is
    // determined.
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
        on_disk.content, "jarvis_apply_smoke: echo result observed",
        "output content must match scripted EmitOutput"
    );
    assert!(
        on_disk.evidence.contains(&planted_id),
        "output must cite the planted evidence id; got {:?}",
        on_disk.evidence
    );
    eprintln!(
        "jarvis_apply_smoke: output landed at outputs/{}.json with {} evidence id(s)",
        on_disk.id,
        on_disk.evidence.len()
    );

    // ---- Artifact 3: `<prefix>/decisions/<tick>.jsonl` --------------------
    // Plan § 8 decision 6: one entry per tick. The scripted sequence
    // produces four decisions (Idle, CallTools, EmitOutput, Retire); the
    // workflow body bumps `tick` only on non-retire arms, so the four
    // ticks land at `decisions/{0,1,2,3}.jsonl`.
    let page = storage
        .list(&format!("{agent_prefix}/decisions/"), None, usize::MAX)
        .await
        .context("listing decisions/")?;
    let mut decision_keys: Vec<&String> =
        page.keys.iter().filter(|k| k.ends_with(".jsonl")).collect();
    decision_keys.sort();
    eprintln!("jarvis_apply_smoke: decision-log keys: {decision_keys:?}");
    assert_eq!(
        decision_keys.len(),
        4,
        "expected 4 decision-log entries (Idle, CallTools, EmitOutput, Retire); got {decision_keys:?}"
    );

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

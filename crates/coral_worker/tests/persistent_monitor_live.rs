//! End-to-end persistent-monitor smoke on the workflow path.
//!
//! Proves the continuous loop's **runtime contract** end-to-end against a
//! real Temporal Server + Postgres: a reduced graph of three `persistent`
//! agents (one parent + two children) cycles, the parent re-reconciles
//! newer child outputs, and every agent stops only via the `max_ticks`
//! guardrail — never a model `Retire` (which CM-2 demotes to `Idle` for
//! persistent agents).
//!
//! A deterministic [`CyclingDecide`] drives the loop and the children cite
//! planted evidence, so this needs **no model key and no Node** — only the
//! `TEMPORAL_LIVE_TEST=1` + `DATABASE_URL` gates. That keeps the three
//! assertions (≥2 distinct outputs each, ≥1 re-reconciliation, max_ticks
//! stop) deterministic: they are properties of the loop machinery
//! (CM-2/CM-3/CM-4), not of model behaviour.
//!
//! What this does NOT cover: the persistent **prompt** clauses (CM-3/CM-4
//! text, exercised only by a real model — already snapshot-covered) and the
//! open "can a small model actually drive this loop" question. That
//! loop-viability run is documented in `examples/persistent_monitor/README.md`
//! for a manual run with a vendor key.
//!
//! Run it:
//! ```bash
//! TEMPORAL_LIVE_TEST=1 \
//!   DATABASE_URL=postgres://coral:coral@localhost:5432/coral_structural \
//!   cargo test -p coral_worker --test persistent_monitor_live -- --nocapture
//! ```

use std::collections::BTreeSet;
use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use coral_graph::yaml::{build_workflow_starts, parse_and_validate, yaml_seed_triggers};
use coral_graph::{GraphStore, MIGRATOR};
use coral_node::agent_ref::GraphId;
use coral_node::decision::{ContextBundle, Decide, Decision, ReconcileSource};
use coral_node::evidence::{EvidenceId, EvidenceRecord};
use coral_node::fs::AgentFs;
use coral_node::mandate::Mandate;
use coral_node::storage::{AgentStorage, MemoryStorage};
use coral_node::tools::ToolRegistry;
use coral_node::trigger::Trigger;
use coral_temporal::worker::{
    build_worker, install_agent_storage, install_decide, install_structural_db_store,
    install_tool_registry, StructuralDbStore,
};
use coral_temporal::workflow::{AgentResult, AgentWorkflow};
use sqlx::postgres::PgPoolOptions;

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// Substring that identifies the parent's mandate (the graph keys nothing
/// else the bundle carries to a `Decide`).
const PARENT_MARKER: &str = "coordinate two researchers";

/// Evidence id the children cite, planted identically on each child's FS so
/// the same content-addressed id resolves under either prefix. Set before
/// the worker starts.
static CHILD_EVIDENCE: OnceLock<EvidenceId> = OnceLock::new();

/// Serializes the single live test against the process-wide installs.
static LIVE_GUARD: Mutex<()> = Mutex::new(());

fn run_suffix() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().to_string())
        .unwrap_or_else(|_| "no-suffix".into())
}

fn example_graph_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root above crates/coral_worker")
        .join("examples")
        .join("persistent_monitor")
        .join("graph.yaml")
}

/// Deterministic loop driver, routed by mandate text.
///
/// - **Children** emit a fresh, distinct output every wake (`finding N`,
///   `N` derived from their own recent-outputs window) citing the planted
///   evidence — so each child accumulates ≥2 distinct outputs before its
///   `max_ticks` stop. Never idles, never retires.
/// - **Parent** reconciles whatever `ChildOutput`s the tick carries, then —
///   once it has reconciled more than it has reported — emits a refreshed
///   consolidated report citing a synthetic reconcile record. Distinct child
///   outputs arriving over time make it re-reconcile newer ones. Never
///   retires; only `max_ticks` stops it.
struct CyclingDecide;

#[async_trait]
impl Decide for CyclingDecide {
    async fn decide(&self, bundle: ContextBundle) -> Result<Decision> {
        let idle = Decision::Idle {
            next_after: bundle.mandate.idle_period,
        };
        if bundle.mandate.text.contains(PARENT_MARKER) {
            // Parent.
            let sources: Vec<ReconcileSource> = bundle
                .triggers
                .iter()
                .filter_map(|t| match t {
                    Trigger::ChildOutput {
                        child_ref,
                        output_id,
                        ..
                    } => Some(ReconcileSource {
                        child_ref: child_ref.clone(),
                        output_id: output_id.clone(),
                    }),
                    _ => None,
                })
                .collect();

            let reconcile_records: Vec<&EvidenceId> = bundle
                .recent_evidence
                .iter()
                .filter(|e| e.tool == "reconcile")
                .map(|e| &e.id)
                .collect();
            let n_reports = bundle.recent_outputs.len();

            // Report whenever we've reconciled more than we've published —
            // this is what turns folded child outputs into refreshed parent
            // reports and keeps reconcile/emit alternating.
            if reconcile_records.len() > n_reports {
                let cite = (*reconcile_records.last().expect("non-empty by the > check")).clone();
                return Ok(Decision::EmitOutput {
                    content: format!("consolidated report {}", n_reports + 1),
                    evidence: vec![cite],
                });
            }
            if !sources.is_empty() {
                return Ok(Decision::ReconcileChildren {
                    sources,
                    conflict: None,
                });
            }
            Ok(idle)
        } else {
            // Child: emit a fresh distinct finding citing the planted id.
            let n = bundle.recent_outputs.len() + 1;
            let ev = CHILD_EVIDENCE
                .get()
                .expect("CHILD_EVIDENCE planted before worker start")
                .clone();
            Ok(Decision::EmitOutput {
                content: format!("finding {n}"),
                evidence: vec![ev],
            })
        }
    }
}

async fn build_client() -> Result<Client> {
    let address = env::var("TEMPORAL_ADDRESS").unwrap_or_else(|_| DEFAULT_ADDRESS.into());
    let namespace = env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| DEFAULT_NAMESPACE.into());
    let url = Url::parse(&address).context("parsing TEMPORAL_ADDRESS")?;
    let connection = Connection::connect(ConnectionOptions::new(url).build())
        .await
        .context("connecting to Temporal Server")?;
    Client::new(connection, ClientOptions::new(namespace).build())
        .context("building Temporal client")
}

/// Hermetic (no live deps): the example graph parses and validates as a
/// reduced all-persistent monitor — the apply-time gate `coral apply` runs
/// before touching the DB. The always-on guard that the fixture stays
/// applyable and clears the CM-4 degenerate-combo validation.
#[test]
fn example_graph_parses_and_validates_as_all_persistent_monitor() {
    let yaml = std::fs::read_to_string(example_graph_path())
        .expect("read examples/persistent_monitor/graph.yaml");
    let graph = parse_and_validate(&yaml).expect("example graph validates");

    // One parent + two children, every one persistent. A persistent parent
    // with all-one-shot children is rejected by CM-4's validator; this
    // fixture's children are persistent, so it must clear that gate.
    assert_eq!(graph.agents.len(), 1, "single root");
    let analyst = &graph.agents[0];
    assert!(analyst.mandate.persistent, "analyst persistent");
    assert_eq!(analyst.children.len(), 2, "two researchers");
    assert!(
        analyst.children.iter().all(|c| c.mandate.persistent),
        "both researchers persistent (not the degenerate combo CM-4 rejects)"
    );
    // Seeds kick off all three so they begin cycling immediately.
    assert_eq!(graph.seed.triggers.len(), 3, "three seed kickoffs");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::await_holding_lock)]
async fn persistent_monitor_cycles_reconciles_and_stops_via_max_ticks() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping persistent_monitor_cycles_reconciles_and_stops_via_max_ticks; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    let Some(database_url) = env::var("DATABASE_URL").ok().filter(|s| !s.is_empty()) else {
        eprintln!(
            "skipping persistent_monitor_cycles_reconciles_and_stops_via_max_ticks; \
             set DATABASE_URL to a docker-compose Postgres to run"
        );
        return;
    };
    let _guard = LIVE_GUARD.lock().unwrap_or_else(|p| p.into_inner());
    run_smoke(&database_url)
        .await
        .expect("persistent monitor smoke");
}

async fn run_smoke(database_url: &str) -> Result<()> {
    let suffix = run_suffix();

    // ---- Parse + apply the example graph to the real structural DB ----
    let yaml_text =
        std::fs::read_to_string(example_graph_path()).context("read example graph.yaml")?;
    let mut graph_yaml = parse_and_validate(&yaml_text).context("validate example graph")?;
    graph_yaml.metadata.name = format!("persistent-monitor-{suffix}");

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(database_url)
        .await
        .context("connecting to structural DB (DATABASE_URL)")?;
    MIGRATOR.run(&pool).await.context("migrate structural DB")?;
    let store = Arc::new(GraphStore::new(pool));
    let applied = store
        .create_from_yaml(&graph_yaml)
        .await
        .context("create_from_yaml(persistent monitor)")?;
    let graph_id = applied.graph_id;

    // ---- Install runtime wiring: in-memory FS, deterministic Decide ----
    let storage = Arc::new(MemoryStorage::new());
    install_agent_storage(storage.clone() as Arc<dyn AgentStorage>);
    install_decide(Arc::new(CyclingDecide) as Arc<dyn Decide>);
    install_structural_db_store(store.clone() as Arc<dyn StructuralDbStore>);
    // The CyclingDecide never dispatches a tool, so an empty registry is
    // enough — no MCP server is spawned (no Node dependency).
    install_tool_registry(Arc::new(ToolRegistry::new()));

    // ---- Plant the evidence the children cite (identical ⇒ same id) ----
    let plant_mandate = Mandate::new("plant", Duration::from_millis(0), None);
    let mut planted: Option<EvidenceId> = None;
    for operator_id in ["researcher-alpha", "researcher-beta"] {
        let agent = applied
            .agents
            .iter()
            .find(|a| a.operator_id == operator_id)
            .ok_or_else(|| anyhow!("missing {operator_id} in applied graph"))?;
        let prefix = format!("graphs/{graph_id}/agents/{}/", agent.db_agent_id);
        let fs = AgentFs::new_with_storage(
            storage.clone() as Arc<dyn AgentStorage>,
            &prefix,
            &plant_mandate,
        )
        .await
        .with_context(|| format!("open child FS for {operator_id}"))?;
        let id = fs
            .record_evidence(EvidenceRecord::new(
                "echo",
                serde_json::json!({"seed": "persistent-monitor"}),
                serde_json::json!({"ok": true}),
                chrono::Utc::now(),
            ))
            .await
            .context("plant child evidence")?;
        match &planted {
            Some(prev) => assert_eq!(prev, &id, "identical evidence ⇒ identical id"),
            None => planted = Some(id),
        }
    }
    CHILD_EVIDENCE
        .set(planted.expect("planted at least one"))
        .map_err(|_| anyhow!("CHILD_EVIDENCE set twice"))?;

    // ---- Host the worker; start + seed all three workflows ----
    let task_queue = format!("coral-persistent-monitor-{suffix}");
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(TelemetryOptions::builder().build())
            .build()
            .map_err(|e| anyhow!("RuntimeOptions build failed: {e}"))?,
    )?;
    let client = build_client().await?;
    let mut worker = build_worker(&runtime, client.clone(), &task_queue)?;
    let shutdown = worker.shutdown_handle();

    let starts = build_workflow_starts(&graph_yaml, &applied);
    let seeds = yaml_seed_triggers(&graph_yaml, &applied).context("resolve seed triggers")?;

    let driver_storage = storage.clone();
    let driver_tq = task_queue.clone();
    let driver = tokio::spawn(async move {
        struct ShutdownGuard<F: Fn()>(F);
        impl<F: Fn()> Drop for ShutdownGuard<F> {
            fn drop(&mut self) {
                (self.0)();
            }
        }
        let _guard = ShutdownGuard(shutdown);
        drive(client, &driver_tq, graph_id, starts, seeds, driver_storage).await
    });

    // 180s budget: three agents at sub-second cadence finish in a few
    // seconds; the ceiling only catches a stall.
    let worker_result = tokio::time::timeout(Duration::from_secs(180), worker.run())
        .await
        .map_err(|_| anyhow!("worker.run() timed out (180s)"))?
        .map_err(|e| anyhow!("worker.run() exited with error: {e}"));
    let driver_result = driver.await.context("driver task panicked")?;
    worker_result?;
    driver_result
}

async fn drive(
    client: Client,
    task_queue: &str,
    graph_id: GraphId,
    starts: Vec<coral_graph::yaml::WorkflowStart>,
    seeds: Vec<coral_graph::yaml::ResolvedSeedTrigger>,
    storage: Arc<MemoryStorage>,
) -> Result<()> {
    // Start parents-first, then signal each seed kickoff.
    for start in &starts {
        client
            .start_workflow(
                AgentWorkflow::run,
                start.input.clone(),
                WorkflowStartOptions::new(task_queue, &start.workflow_id).build(),
            )
            .await
            .with_context(|| format!("start_workflow {}", start.workflow_id))?;
    }
    for seed in &seeds {
        client
            .get_workflow_handle::<AgentWorkflow>(&seed.workflow_id)
            .signal(
                AgentWorkflow::external_signal,
                seed.trigger.clone(),
                WorkflowSignalOptions::default(),
            )
            .await
            .with_context(|| format!("signal seed {}", seed.workflow_id))?;
    }

    // Wait for every agent to stop. Each is persistent, so the only stop is
    // the max_ticks guardrail — assert the reason verbatim (proves the stop
    // contract: no model Retire ended a persistent agent).
    let max_ticks_by_agent = |name: &str| -> u64 {
        if name == "analyst" {
            8
        } else {
            4
        }
    };
    for start in &starts {
        let agent_name = start.input.agent_name.clone();
        let result: AgentResult = client
            .get_workflow_handle::<AgentWorkflow>(&start.workflow_id)
            .get_result(WorkflowGetResultOptions::default())
            .await
            .with_context(|| format!("get_result for {agent_name}"))?;
        let AgentResult::Retired { reason } = result;
        let expected = format!("max_ticks ({}) reached", max_ticks_by_agent(&agent_name));
        assert_eq!(
            reason, expected,
            "{agent_name} must stop via the guardrail, not a model Retire"
        );
    }

    // ---- Assertion 1: each agent emitted ≥2 distinct outputs ----
    for start in &starts {
        let agent_id = start.input.agent_id;
        let fs =
            AgentFs::open_for_agent(storage.clone() as Arc<dyn AgentStorage>, graph_id, agent_id);
        let outs = fs
            .list_recent_outputs(16)
            .await
            .with_context(|| format!("list outputs for {}", start.input.agent_name))?;
        let distinct: BTreeSet<&str> = outs.iter().map(|o| o.content.as_str()).collect();
        assert!(
            distinct.len() >= 2,
            "{} must emit ≥2 distinct outputs (emit→idle→refresh repeats); got {:?}",
            start.input.agent_name,
            outs.iter().map(|o| &o.content).collect::<Vec<_>>()
        );
    }

    // ---- Assertion 2: the parent re-reconciled a newer child output ----
    // Count distinct `source_output_id`s across the parent's synthetic
    // reconcile records: ≥2 means it folded a newer output at least once.
    let parent = starts
        .iter()
        .find(|s| s.input.agent_name == "analyst")
        .ok_or_else(|| anyhow!("no analyst start"))?;
    let parent_fs = AgentFs::open_for_agent(
        storage.clone() as Arc<dyn AgentStorage>,
        graph_id,
        parent.input.agent_id,
    );
    let evidence = parent_fs
        .list_recent_evidence(32)
        .await
        .context("list parent evidence")?;
    let reconciled_outputs: BTreeSet<String> = evidence
        .iter()
        .filter(|e| e.tool == "reconcile")
        .filter_map(|e| e.args.get("source_output_id").map(|v| v.to_string()))
        .collect();
    assert!(
        reconciled_outputs.len() >= 2,
        "parent must re-reconcile ≥1 newer child output (≥2 distinct source outputs folded); got {reconciled_outputs:?}"
    );

    eprintln!(
        "persistent_monitor: all 3 agents stopped via max_ticks; parent folded {} distinct child outputs",
        reconciled_outputs.len()
    );
    Ok(())
}

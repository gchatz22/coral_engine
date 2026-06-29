//! End-to-end multi-agent integration test: parent + 2 children +
//! scripted disagreement, byte-checkable end-state. Env-gated behind
//! `TEMPORAL_LIVE_TEST=1`; there is no hermetic in-process multi-agent
//! path.
//!
//! ## Topology
//!
//! ```text
//! root  (parent, MockDecide-scripted)
//! ├── child-a  (emits "claim-A says X")
//! └── child-b  (emits "claim-B says NOT-X")
//! ```
//!
//! Mirrors the `examples/smoke_multi_agent/graph.yaml` fixture. The
//! fixture's YAML is parsed at test startup as a schema/topology
//! regression-guard; the actual workflow dispatch uses
//! `client.start_workflow` directly because production `coral apply`
//! always wires `LlmDecide` through the worker daemon and there is no
//! production-supported seam to swap in `MockDecide`.
//!
//! ## End-state assertions (load-bearing)
//!
//! 1. Each child's canonical `outputs/output.md` body equals the scripted
//!    content.
//! 2. Parent's `evidence/` contains exactly 2 synthetic `EvidenceRecord`s
//!    (`tool == "reconcile"`), one per child output, with `args`
//!    referencing the right `(child_agent_id, child_workflow_id,
//!    source_output_id)` triple.
//! 3. Parent's canonical `outputs/output.md` body is the reconciled output.
//!    Citations live in the DB reference graph, not the file, so the
//!    synthetic-id provenance is pinned via the reconcile evidence records
//!    and the cross-FS walk rather than a per-output citation list.
//! 4. Parent's `conflicts/` contains exactly 1 `ConflictRecord` with
//!    `kind == HeldOpen`, `alternatives.len() == 2`, and `resolution ==
//!    None`. Each `ConflictAlternative.source_child` +
//!    `source_output_id` resolves correctly.
//! 5. **Cross-FS provenance trail:** each parent reconcile evidence
//!    record's `args.source_output_id` resolves to the child's canonical
//!    `outputs/output.md` via cross-agent `AgentFs::open_for_agent` — the
//!    cross-read body's content-addressed `OutputId` matches the recorded
//!    `source_output_id`, no string-shape guessing.

use std::collections::VecDeque;
use std::env;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_trait::async_trait;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use uuid::Uuid;

use coral_graph::yaml::parse_and_validate;
use coral_node::agent_ref::{AgentId, AgentRef, GraphId};
use coral_node::conflict::ConflictKind;
use coral_node::decision::{
    ConflictAlternative, ConflictRecordIntent, Decide, Decision, ReconcileSource, Session,
};
use coral_node::evidence::EvidenceRecord;
use coral_node::fs::AgentFs;
use coral_node::mandate::{Mandate, OutputId};
use coral_node::storage::{AgentStorage, BlobSha, MemoryStorage};
use coral_node::tools::ToolRegistry;
use coral_node::trigger::Trigger;
use coral_temporal::activities::set_decision_script;
use coral_temporal::worker::{
    build_worker, install_agent_storage, install_decide, install_structural_db_store,
    install_tool_registry, StructuralDbStore,
};
use coral_temporal::workflow::{AgentInput, AgentResult, AgentWorkflow, FsHandle, ParentRef};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// Mandate-text discriminators the `RoutingDecide` matches on. Mirror
/// the strings encoded in `examples/smoke_multi_agent/graph.yaml`; if
/// either side drifts, the test's `parse_and_validate` startup check
/// fires before any workflow runs.
const PARENT_MANDATE_TEXT: &str = "multi-agent-parent";
const CHILD_A_MANDATE_TEXT: &str = "multi-agent-child-a";
const CHILD_B_MANDATE_TEXT: &str = "multi-agent-child-b";

/// Path (relative to this crate's manifest dir) to the multi-agent
/// fixture this test pins.
const GRAPH_YAML_REL: &str = "../../examples/smoke_multi_agent/graph.yaml";

/// Scripted child-A output content. The literal string surfaces in the
/// FS-end-state assertion and (verbatim) as the alternative's `claim`
/// in the conflict record.
const CHILD_A_CONTENT: &str = "claim-A says X";
const CHILD_B_CONTENT: &str = "claim-B says NOT-X";
const PARENT_OUTPUT_CONTENT: &str = "reconciled: held open";

/// Shared in-memory storage backend: parent + child workflows all run
/// their activities against this. The test driver also opens views
/// over it to inspect what landed on disk post-run.
static SHARED_STORAGE: OnceLock<Arc<MemoryStorage>> = OnceLock::new();

/// Captured triggers the parent's `Decide` observed. The
/// parent-script synthesizer (`synthesize_parent_decision`) reads this
/// to discover child OutputIds dynamically — they're content-addressed
/// so they can't be hardcoded.
static PARENT_OBSERVED_TRIGGERS: OnceLock<Arc<Mutex<Vec<Trigger>>>> = OnceLock::new();

/// Per-role decision queues. The parent script uses two sentinels
/// (`reconcile_placeholder`, `emit_with_synthetic_placeholder`) to
/// indicate "synthesize this decision at decide time from observed
/// state"; the child scripts are straightforward FIFOs.
static PARENT_SCRIPT: OnceLock<Mutex<VecDeque<Decision>>> = OnceLock::new();
static CHILD_A_SCRIPT: OnceLock<Mutex<VecDeque<Decision>>> = OnceLock::new();
static CHILD_B_SCRIPT: OnceLock<Mutex<VecDeque<Decision>>> = OnceLock::new();

static INIT: std::sync::Once = std::sync::Once::new();

/// No-op `StructuralDbStore`. The reconcile + `WriteOutput` cycles drive
/// the `persist_output` activity, which writes the file index / citation
/// edges; this test asserts the FS-side provenance trail (reconcile
/// evidence records, output bodies) on `MemoryStorage`, not the DB rows,
/// so those writes are dropped. The structural-DB surface is covered by
/// the `GraphStore` unit tests. No `SpawnChild` is scripted (children are
/// seeded workflows), so the spawn-path methods are unreachable.
struct NoopStructuralDb;

#[async_trait]
impl StructuralDbStore for NoopStructuralDb {
    async fn add_agent(&self, _graph_id: GraphId, _name: &str) -> anyhow::Result<AgentId> {
        Ok(AgentId::new(Uuid::new_v4()))
    }

    async fn add_edge(
        &self,
        _parent_agent_id: AgentId,
        _child_agent_id: AgentId,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn list_tool_def_ids_for_graph(&self, _graph_id: GraphId) -> anyhow::Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn set_file_version(
        &self,
        _agent_id: AgentId,
        _filepath: &str,
        _blob_sha: &BlobSha,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn add_citation(
        &self,
        _citing_agent_id: AgentId,
        _citing_filepath: &str,
        _citing_blob_sha: &BlobSha,
        _cited_agent_id: AgentId,
        _cited_filepath: &str,
        _cited_blob_sha: &BlobSha,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

/// One-shot install of shared storage + tool registry +
/// `RoutingDecide`. Subsequent calls are no-ops (the install hooks
/// panic on double-install; `Once` guards against that).
fn ensure_installed() -> Arc<MemoryStorage> {
    INIT.call_once(|| {
        let storage: Arc<MemoryStorage> = Arc::new(MemoryStorage::new());
        SHARED_STORAGE
            .set(Arc::clone(&storage))
            .expect("SHARED_STORAGE set exactly once");
        let dyn_storage: Arc<dyn AgentStorage> = storage;
        install_agent_storage(dyn_storage);

        // Empty registry — this test does not exercise tool dispatch.
        // The `execute_tool` activity body still demands the OnceLock
        // be installed, so we install an empty one.
        install_tool_registry(Arc::new(ToolRegistry::new()));

        PARENT_OBSERVED_TRIGGERS
            .set(Arc::new(Mutex::new(Vec::new())))
            .expect("PARENT_OBSERVED_TRIGGERS set exactly once");
        PARENT_SCRIPT
            .set(Mutex::new(VecDeque::new()))
            .expect("PARENT_SCRIPT set exactly once");
        CHILD_A_SCRIPT
            .set(Mutex::new(VecDeque::new()))
            .expect("CHILD_A_SCRIPT set exactly once");
        CHILD_B_SCRIPT
            .set(Mutex::new(VecDeque::new()))
            .expect("CHILD_B_SCRIPT set exactly once");

        install_decide(Arc::new(RoutingDecide));

        install_structural_db_store(Arc::new(NoopStructuralDb));
    });
    SHARED_STORAGE.get().cloned().expect("storage installed")
}

/// `Decide` that routes per `session.seed.mandate.text`:
///
/// - Parent (`PARENT_MANDATE_TEXT`): records every trigger the seed
///   carries (once per cycle); synthesizes a `Decision::ReconcileChildren`
///   from the first observed `ChildOutput` triggers (one per child) when
///   the script pops the `reconcile_placeholder` sentinel; synthesizes a
///   `Decision::WriteOutput` citing the synthetic evidence paths recovered
///   from the prior `ReconcileChildren` step's observation in this cycle's
///   session when the script pops the `emit_with_synthetic_placeholder`
///   sentinel.
/// - Children A / B: pop from their own per-role script FIFO. Empty
///   script defaults to a short idle so a misconfigured test loops
///   politely rather than panicking the activity.
struct RoutingDecide;

#[async_trait]
impl Decide for RoutingDecide {
    async fn decide(&self, session: &Session) -> anyhow::Result<Decision> {
        match session.seed.mandate.text.as_str() {
            PARENT_MANDATE_TEXT => decide_parent(session),
            CHILD_A_MANDATE_TEXT => decide_child(CHILD_A_SCRIPT.get().expect("CHILD_A_SCRIPT")),
            CHILD_B_MANDATE_TEXT => decide_child(CHILD_B_SCRIPT.get().expect("CHILD_B_SCRIPT")),
            other => panic!(
                "RoutingDecide saw unexpected mandate text: {other:?} \
                 (test scripts only PARENT / CHILD_A / CHILD_B mandate text)"
            ),
        }
    }
}

/// Parent-side decide body. Records observed triggers (once per cycle,
/// since the seed's triggers are constant across the cycle's steps), then
/// either returns the next scripted decision verbatim or synthesizes one
/// of the placeholder sentinels.
fn decide_parent(session: &Session) -> anyhow::Result<Decision> {
    if session.is_empty() && !session.seed.triggers.is_empty() {
        let log = PARENT_OBSERVED_TRIGGERS
            .get()
            .expect("PARENT_OBSERVED_TRIGGERS installed")
            .clone();
        let mut guard = log.lock().expect("trigger log mutex poisoned");
        for t in &session.seed.triggers {
            guard.push(t.clone());
        }
    }
    let popped = {
        let mut q = PARENT_SCRIPT
            .get()
            .expect("PARENT_SCRIPT installed")
            .lock()
            .expect("PARENT_SCRIPT mutex poisoned");
        q.pop_front()
    };
    match popped {
        Some(d) if is_reconcile_placeholder(&d) => synthesize_reconcile_or_wait(),
        Some(d) if is_emit_placeholder(&d) => synthesize_emit_or_wait(session),
        Some(d) => Ok(d),
        None => Ok(Decision::Idle {
            next_after: Duration::from_millis(50),
        }),
    }
}

/// Child-side decide body. Pops from the per-role script; defaults to
/// short idle when empty.
fn decide_child(script: &Mutex<VecDeque<Decision>>) -> anyhow::Result<Decision> {
    let popped = script
        .lock()
        .expect("child script mutex poisoned")
        .pop_front();
    Ok(popped.unwrap_or(Decision::Idle {
        next_after: Duration::from_millis(50),
    }))
}

// ---------------------------------------------------------------------------
// Placeholder sentinels for the parent script
//
// `Decision` is the contract enum — adding test-only variants is wrong.
// We encode "synthesize at decide time" by overloading
// `Decision::Idle { next_after }` with sentinel `Duration` values that
// no production script would emit (u64::MAX, u64::MAX - 1).
// ---------------------------------------------------------------------------

fn reconcile_placeholder() -> Decision {
    Decision::Idle {
        next_after: Duration::from_secs(u64::MAX),
    }
}

fn is_reconcile_placeholder(d: &Decision) -> bool {
    matches!(d, Decision::Idle { next_after } if *next_after == Duration::from_secs(u64::MAX))
}

fn emit_with_synthetic_placeholder() -> Decision {
    Decision::Idle {
        next_after: Duration::from_secs(u64::MAX - 1),
    }
}

fn is_emit_placeholder(d: &Decision) -> bool {
    matches!(
        d,
        Decision::Idle { next_after } if *next_after == Duration::from_secs(u64::MAX - 1)
    )
}

/// Synthesize `Decision::ReconcileChildren` from observed
/// `ChildOutput` triggers, ONCE both children have signaled. If
/// either is missing, push the placeholder back onto the parent
/// script and idle briefly so the wake gate races the next signal.
///
/// The reconcile decision's `conflict` carries both children's claims
/// verbatim (`CHILD_A_CONTENT` / `CHILD_B_CONTENT`) so the
/// `ConflictAlternative`s on disk pin the disagreement shape.
/// `resolution: None` → `HeldOpen`.
fn synthesize_reconcile_or_wait() -> anyhow::Result<Decision> {
    let observed = PARENT_OBSERVED_TRIGGERS
        .get()
        .expect("PARENT_OBSERVED_TRIGGERS installed")
        .lock()
        .expect("observed triggers mutex poisoned")
        .clone();
    let child_outputs: Vec<(AgentRef, String, OutputId)> = observed
        .iter()
        .filter_map(|t| match t {
            Trigger::ChildOutput {
                child_ref,
                agent_name,
                output_id,
            } => Some((child_ref.clone(), agent_name.clone(), output_id.clone())),
            _ => None,
        })
        .collect();

    // Need BOTH children's ChildOutput trigger. Match on agent_name
    // (the operator-authored id from the YAML, threaded through
    // `AgentInput.agent_name` on each child) — agent_id UUIDs are
    // freshly minted per run so we can't hardcode them.
    let child_a = child_outputs.iter().find(|(_, name, _)| name == "child-a");
    let child_b = child_outputs.iter().find(|(_, name, _)| name == "child-b");
    let (Some(a), Some(b)) = (child_a, child_b) else {
        // At least one child still hasn't emitted. Put the
        // placeholder back and idle so the wake gate races the
        // pending signal.
        let mut q = PARENT_SCRIPT
            .get()
            .expect("PARENT_SCRIPT installed")
            .lock()
            .expect("PARENT_SCRIPT mutex poisoned");
        q.push_front(reconcile_placeholder());
        return Ok(Decision::Idle {
            next_after: Duration::from_millis(50),
        });
    };
    let (a_ref, _a_name, a_oid) = a.clone();
    let (b_ref, _b_name, b_oid) = b.clone();

    let sources = vec![
        ReconcileSource {
            child_ref: a_ref.clone(),
            output_id: a_oid.clone(),
        },
        ReconcileSource {
            child_ref: b_ref.clone(),
            output_id: b_oid.clone(),
        },
    ];
    let conflict = Some(ConflictRecordIntent {
        alternatives: vec![
            ConflictAlternative {
                source_child: a_ref,
                source_output_id: a_oid,
                claim: CHILD_A_CONTENT.into(),
            },
            ConflictAlternative {
                source_child: b_ref,
                source_output_id: b_oid,
                claim: CHILD_B_CONTENT.into(),
            },
        ],
        resolution: None,
    });
    Ok(Decision::ReconcileChildren { sources, conflict })
}

/// Synthesize `Decision::WriteOutput { body, citations }` from the synthetic
/// evidence the reconcile step produced earlier in this same cycle. The
/// reconcile observation names each minted `evidence/` path directly, so the
/// parent recovers them straight from that step's observation — no `List` of
/// `evidence/` needed. If the reconcile step hasn't run yet in this cycle (or
/// fewer than 2 paths are present), push the placeholder back and idle so a
/// later cycle retries.
fn synthesize_emit_or_wait(session: &Session) -> anyhow::Result<Decision> {
    let observation = session
        .steps
        .iter()
        .rev()
        .find_map(|s| match &s.action {
            Decision::ReconcileChildren { .. } => Some(s.observation.content.clone()),
            _ => None,
        })
        .unwrap_or_default();
    let synthetic_ids: Vec<String> = observation
        .split_whitespace()
        .map(|t| t.trim_end_matches([',', '.']))
        .filter(|t| t.starts_with("evidence/") && t.ends_with(".json"))
        .map(|t| t.to_string())
        .collect();
    // Need exactly 2 — one per source. Tolerate "not yet" (0 or 1) by
    // putting the placeholder back; intolerable > 2 panics rather
    // than silently emitting against a stale view.
    match synthetic_ids.len() {
        0 | 1 => {
            let mut q = PARENT_SCRIPT
                .get()
                .expect("PARENT_SCRIPT installed")
                .lock()
                .expect("PARENT_SCRIPT mutex poisoned");
            q.push_front(emit_with_synthetic_placeholder());
            Ok(Decision::Idle {
                next_after: Duration::from_millis(50),
            })
        }
        2 => Ok(Decision::WriteOutput {
            body: PARENT_OUTPUT_CONTENT.into(),
            citations: synthetic_ids,
        }),
        n => panic!(
            "synthesize_emit_or_wait: unexpected synthetic-evidence count {n} \
             (expected 2 — one per source); the reconcile activity wrote more \
             records than the test scripted)"
        ),
    }
}

/// Replace the contents of all three per-role script slots.
fn install_role_scripts(parent: Vec<Decision>, child_a: Vec<Decision>, child_b: Vec<Decision>) {
    {
        let mut p = PARENT_SCRIPT
            .get()
            .expect("PARENT_SCRIPT installed")
            .lock()
            .expect("PARENT_SCRIPT mutex poisoned");
        *p = parent.into();
    }
    {
        let mut a = CHILD_A_SCRIPT
            .get()
            .expect("CHILD_A_SCRIPT installed")
            .lock()
            .expect("CHILD_A_SCRIPT mutex poisoned");
        *a = child_a.into();
    }
    {
        let mut b = CHILD_B_SCRIPT
            .get()
            .expect("CHILD_B_SCRIPT installed")
            .lock()
            .expect("CHILD_B_SCRIPT mutex poisoned");
        *b = child_b.into();
    }
    // Belt-and-braces: clear the DECISION_SCRIPT static so the
    // activity's script-first guardrail doesn't pop a stale decision
    // from a previous test binary's run.
    set_decision_script(Vec::new());
}

fn reset_observed_triggers() {
    PARENT_OBSERVED_TRIGGERS
        .get()
        .expect("PARENT_OBSERVED_TRIGGERS installed")
        .lock()
        .expect("observed triggers mutex poisoned")
        .clear();
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

fn build_runtime() -> Result<CoreRuntime> {
    let telemetry_options = TelemetryOptions::builder().build();
    let rt = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(telemetry_options)
            .build()
            .map_err(|e| anyhow::anyhow!("RuntimeOptions build failed: {e}"))?,
    )?;
    Ok(rt)
}

fn load_graph_yaml() -> Result<String> {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let path = manifest_dir.join(GRAPH_YAML_REL);
    std::fs::read_to_string(&path)
        .with_context(|| format!("reading multi-agent fixture from {}", path.display()))
}

/// End-to-end live test. See module doc for the assertion list.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::await_holding_lock)]
async fn parent_two_children_disagreement_reconciles_with_held_open_conflict() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping parent_two_children_disagreement_reconciles_with_held_open_conflict; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    run_end_to_end().await.expect("end-to-end smoke");
}

async fn run_end_to_end() -> Result<()> {
    // ---- 1. YAML schema/topology smoke-check --------------------------
    //
    // Parse the fixture so a future YAML edit that drifts mandate text
    // or topology shape fails the test, not the workflow.
    let yaml_text = load_graph_yaml()?;
    let graph = parse_and_validate(&yaml_text).context("parse_and_validate multi-agent fixture")?;
    assert_eq!(graph.metadata.name, "smoke-multi-agent");
    assert_eq!(graph.agents.len(), 1, "single root forest");
    let root = &graph.agents[0];
    assert_eq!(root.id, "root");
    assert_eq!(root.children.len(), 2, "exactly 2 children");
    assert_eq!(root.mandate.text, PARENT_MANDATE_TEXT);
    let child_yaml_a = root
        .children
        .iter()
        .find(|c| c.id == "child-a")
        .expect("child-a in YAML");
    let child_yaml_b = root
        .children
        .iter()
        .find(|c| c.id == "child-b")
        .expect("child-b in YAML");
    assert_eq!(child_yaml_a.mandate.text, CHILD_A_MANDATE_TEXT);
    assert_eq!(child_yaml_b.mandate.text, CHILD_B_MANDATE_TEXT);

    // ---- 2. Per-run setup --------------------------------------------
    let suffix = run_suffix();
    let task_queue = format!("coral-multi-agent-{suffix}");

    // Unique graph_id per run so reruns don't collide. Per-agent
    // workflow ids derive from `(graph_id, agent_id)` (flat scheme).
    // `MemoryStorage` is process-wide so the per-graph UUID is what
    // isolates this test from any other test in the same binary.
    let graph_id = GraphId::new(Uuid::new_v4());
    let parent_agent_id = AgentId::new(Uuid::new_v4());
    let child_a_agent_id = AgentId::new(Uuid::new_v4());
    let child_b_agent_id = AgentId::new(Uuid::new_v4());

    let parent_prefix = format!("graphs/{graph_id}/agents/{parent_agent_id}");
    let child_a_prefix = format!("graphs/{graph_id}/agents/{child_a_agent_id}");
    let child_b_prefix = format!("graphs/{graph_id}/agents/{child_b_agent_id}");
    let parent_workflow_id = parent_prefix.clone();
    let child_a_workflow_id = child_a_prefix.clone();
    let child_b_workflow_id = child_b_prefix.clone();

    // `ensure_installed` is called for its side effects (installs the
    // shared storage / Decide / tool registry into the worker's
    // OnceLock slots). The returned `Arc<MemoryStorage>` is used
    // here to plant the per-child evidence below; the driver task
    // reads the same `SHARED_STORAGE` slot for its inspect-only view.
    let storage = ensure_installed();
    reset_observed_triggers();

    // Plant one evidence record under each child's FS prefix so the
    // scripted WriteOutput resolves provenance. `AgentFs::persist_output`
    // rejects empty `evidence` (`FsError::EmptyEvidence`) — children
    // therefore need to cite *something*. The synthetic-evidence
    // pattern is the parent's mechanism for cross-agent provenance and
    // is orthogonal to whatever each child's own output cites.
    //
    // Distinct planted records per child (different `args` payload)
    // so the two `EvidenceId`s are distinct on disk — keeps the
    // FS-end-state assertions sharp.
    let plant_mandate = Mandate::new("plant", Duration::from_millis(0), None);
    let plant_storage_a: Arc<dyn AgentStorage> = storage.clone();
    let plant_fs_a = AgentFs::new_with_storage(plant_storage_a, &child_a_prefix, &plant_mandate)
        .await
        .expect("open planting AgentFs for child-a");
    let planted_a_id = plant_fs_a
        .record_evidence(
            EvidenceRecord::new(
                "echo",
                serde_json::json!({"child": "a"}),
                serde_json::json!({"hit": true}),
                chrono::Utc::now(),
            ),
            "echo child a",
        )
        .await
        .expect("plant evidence for child-a WriteOutput");
    let plant_storage_b: Arc<dyn AgentStorage> = storage.clone();
    let plant_fs_b = AgentFs::new_with_storage(plant_storage_b, &child_b_prefix, &plant_mandate)
        .await
        .expect("open planting AgentFs for child-b");
    let planted_b_id = plant_fs_b
        .record_evidence(
            EvidenceRecord::new(
                "echo",
                serde_json::json!({"child": "b"}),
                serde_json::json!({"hit": true}),
                chrono::Utc::now(),
            ),
            "echo child b",
        )
        .await
        .expect("plant evidence for child-b WriteOutput");

    // ---- 3. Scripts ---------------------------------------------------
    //
    // Children: each runs an Idle cycle, then a WriteOutput cycle (the
    // WriteOutput signals the parent), then the `step_cap=2` cap stops each
    // (agents never self-terminate). Each WriteOutput cites its planted
    // evidence id (one per child) so `persist_output`'s provenance contract
    // is satisfied.
    //
    // Parent script (one cycle, three repertoire steps + a terminal idle):
    // reconcile (via sentinel; pushes itself back + ends the cycle with Idle
    // until both ChildOutput signals land) → List the parent's evidence/ so
    // the synthetic ids minted by reconcile become observable → emit with
    // synthetic (via sentinel; recovers the 2 ids from the List observation;
    // pushes itself back if fewer than 2 surfaced) → then idle (no script
    // left) until its generous `step_cap` cap stops it.
    let child_a_script = vec![
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
        Decision::WriteOutput {
            body: CHILD_A_CONTENT.into(),
            citations: vec![planted_a_id.clone()],
        },
    ];
    let child_b_script = vec![
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
        Decision::WriteOutput {
            body: CHILD_B_CONTENT.into(),
            citations: vec![planted_b_id.clone()],
        },
    ];
    let parent_script = vec![reconcile_placeholder(), emit_with_synthetic_placeholder()];
    install_role_scripts(parent_script, child_a_script, child_b_script);

    // ---- 4. Worker + driver ------------------------------------------
    let runtime = build_runtime()?;
    let client = build_client().await?;
    let mut worker = build_worker(&runtime, client.clone(), &task_queue)?;
    let shutdown = worker.shutdown_handle();

    let driver = tokio::spawn({
        let task_queue = task_queue.clone();
        let parent_prefix = parent_prefix.clone();
        let child_a_prefix = child_a_prefix.clone();
        let child_b_prefix = child_b_prefix.clone();
        let parent_workflow_id = parent_workflow_id.clone();
        let child_a_workflow_id = child_a_workflow_id.clone();
        let child_b_workflow_id = child_b_workflow_id.clone();
        let storage_arc: Arc<MemoryStorage> = SHARED_STORAGE
            .get()
            .expect("SHARED_STORAGE installed")
            .clone();
        async move {
            struct ShutdownGuard<F: Fn()>(F);
            impl<F: Fn()> Drop for ShutdownGuard<F> {
                fn drop(&mut self) {
                    (self.0)();
                }
            }
            let _g = ShutdownGuard(shutdown);
            drive(
                client,
                &task_queue,
                graph_id,
                parent_agent_id,
                child_a_agent_id,
                child_b_agent_id,
                &parent_workflow_id,
                &child_a_workflow_id,
                &child_b_workflow_id,
                &parent_prefix,
                &child_a_prefix,
                &child_b_prefix,
                storage_arc,
            )
            .await
            // `_` on storage_arc here would silence the unused-must-use
            // warning the spawn closure picks up if drive() doesn't
            // consume the storage_arc — but drive() does consume it,
            // so this is just the closure's natural return shape.
        }
    });

    // 180s budget — the multi-agent topology takes ~2 extra wake-gate
    // round-trips over the single-agent smoke. Plenty of headroom.
    let worker_result = tokio::time::timeout(Duration::from_secs(180), worker.run())
        .await
        .map_err(|_| anyhow::anyhow!("worker.run() timed out (180s)"))?
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
    graph_id: GraphId,
    parent_agent_id: AgentId,
    child_a_agent_id: AgentId,
    child_b_agent_id: AgentId,
    parent_workflow_id: &str,
    child_a_workflow_id: &str,
    child_b_workflow_id: &str,
    parent_prefix: &str,
    child_a_prefix: &str,
    child_b_prefix: &str,
    storage: Arc<MemoryStorage>,
) -> Result<()> {
    // Start parent first so it is addressable when each child fires
    // its ChildOutput signal.
    let parent_input = AgentInput {
        cfg: Default::default(),
        fs_handle: FsHandle {
            prefix: parent_prefix.into(),
        },
        parent_handle: None,
        carryover: None,
        // Mandate idle_period sets the wake-gate cadence between
        // signal arrivals. 50ms balances "quick reaction to
        // ChildOutput" against "don't hammer Temporal".
        mandate: Mandate::new(PARENT_MANDATE_TEXT, Duration::from_millis(50), Some(30)),
        graph_id,
        agent_id: parent_agent_id,
        agent_name: "root".into(),
    };
    let parent_handle = client
        .start_workflow(
            AgentWorkflow::run,
            parent_input,
            WorkflowStartOptions::new(task_queue, parent_workflow_id).build(),
        )
        .await
        .context("start_workflow(parent)")?;
    eprintln!("parent started at {parent_workflow_id}");

    let child_a_input = AgentInput {
        cfg: Default::default(),
        fs_handle: FsHandle {
            prefix: child_a_prefix.into(),
        },
        parent_handle: Some(ParentRef {
            workflow_id: parent_workflow_id.to_string(),
            ..ParentRef::default()
        }),
        carryover: None,
        mandate: Mandate::new(CHILD_A_MANDATE_TEXT, Duration::from_millis(50), Some(2)),
        graph_id,
        agent_id: child_a_agent_id,
        agent_name: "child-a".into(),
    };
    let child_a_handle = client
        .start_workflow(
            AgentWorkflow::run,
            child_a_input,
            WorkflowStartOptions::new(task_queue, child_a_workflow_id).build(),
        )
        .await
        .context("start_workflow(child-a)")?;
    eprintln!("child-a started at {child_a_workflow_id}");

    let child_b_input = AgentInput {
        cfg: Default::default(),
        fs_handle: FsHandle {
            prefix: child_b_prefix.into(),
        },
        parent_handle: Some(ParentRef {
            workflow_id: parent_workflow_id.to_string(),
            ..ParentRef::default()
        }),
        carryover: None,
        mandate: Mandate::new(CHILD_B_MANDATE_TEXT, Duration::from_millis(50), Some(2)),
        graph_id,
        agent_id: child_b_agent_id,
        agent_name: "child-b".into(),
    };
    let child_b_handle = client
        .start_workflow(
            AgentWorkflow::run,
            child_b_input,
            WorkflowStartOptions::new(task_queue, child_b_workflow_id).build(),
        )
        .await
        .context("start_workflow(child-b)")?;
    eprintln!("child-b started at {child_b_workflow_id}");

    // Wait for each child to retire (WriteOutput, then the step_cap cap).
    // Each signals the parent on its WriteOutput cycle before the cap stops
    // it. The cap-driven retirement also fires `Trigger::ChildRetired` at
    // the parent — orthogonal to the reconcile assertion but exercises the
    // lifecycle-signal path as a side effect.
    let _child_a_result: AgentResult = child_a_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("child-a get_result")?;
    eprintln!("child-a retired cleanly");
    let _child_b_result: AgentResult = child_b_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("child-b get_result")?;
    eprintln!("child-b retired cleanly");

    // Parent loops on the reconcile_placeholder (one idle cycle per wait)
    // until both ChildOutput triggers land, then in a single cycle
    // synthesizes the reconcile decision, lists its evidence/ directory to
    // observe the synthetic ids, emits the reconciled output citing them,
    // and finally retires when the step_cap cap is reached.
    let retire_timer = tokio::time::timeout(
        Duration::from_secs(120),
        parent_handle.get_result(WorkflowGetResultOptions::default()),
    )
    .await;
    let parent_result: AgentResult = match retire_timer {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => return Err(anyhow::anyhow!("parent get_result: {e}")),
        Err(_) => {
            // Belt-and-braces: signal retire so the loop terminates
            // for inspection (a stuck parent loop probably means the
            // synthesize fast-paths didn't observe their inputs;
            // surface the FS assertions below anyway).
            parent_handle
                .signal(
                    AgentWorkflow::retire,
                    "test asked".to_string(),
                    WorkflowSignalOptions::default(),
                )
                .await
                .context("signal parent retire (timeout fallback)")?;
            parent_handle
                .get_result(WorkflowGetResultOptions::default())
                .await
                .context("parent get_result after signal retire")?
        }
    };
    let AgentResult::Retired { reason } = parent_result;
    eprintln!("parent retired ({reason})");

    // ---- 5. FS-end-state assertions ----------------------------------
    //
    // The whole point of this test. Open inspect-only AgentFs views
    // over each agent's FS root and pin every artifact. The parent's
    // `agent_id` is not needed here — the parent prefix string is
    // sufficient for opening its FS, and we identify the children by
    // their agent_ids inside the cross-FS provenance walk.
    let _ = parent_agent_id;
    assert_end_state(
        storage,
        graph_id,
        child_a_agent_id,
        child_b_agent_id,
        parent_prefix,
        child_a_prefix,
        child_b_prefix,
        child_a_workflow_id,
        child_b_workflow_id,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn assert_end_state(
    storage: Arc<MemoryStorage>,
    graph_id: GraphId,
    child_a_agent_id: AgentId,
    child_b_agent_id: AgentId,
    parent_prefix: &str,
    child_a_prefix: &str,
    child_b_prefix: &str,
    child_a_workflow_id: &str,
    child_b_workflow_id: &str,
) -> Result<()> {
    let inspect_mandate = Mandate::new("inspect", Duration::from_millis(0), None);
    let inspect_storage: Arc<dyn AgentStorage> = storage.clone();

    // --- Children' outputs ---------------------------------------------
    //
    // Each child keeps a single canonical Output (`outputs/output.md`,
    // overwritten each write); `read_output` returns its body. The
    // content-addressed `OutputId` is recomputed from the scripted body —
    // it equals the id the child's `ChildOutput` trigger carried, so the
    // downstream provenance cross-checks pin the same value.
    let child_a_view =
        AgentFs::new_with_storage(inspect_storage.clone(), child_a_prefix, &inspect_mandate)
            .await
            .context("open child-a AgentFs")?;
    let child_a_body = child_a_view
        .read_output()
        .await
        .context("read_output(child-a)")?;
    assert_eq!(
        child_a_body, CHILD_A_CONTENT,
        "child-a output body drifted from scripted WriteOutput"
    );
    let child_a_output_id = OutputId::new(CHILD_A_CONTENT);

    let child_b_view =
        AgentFs::new_with_storage(inspect_storage.clone(), child_b_prefix, &inspect_mandate)
            .await
            .context("open child-b AgentFs")?;
    let child_b_body = child_b_view
        .read_output()
        .await
        .context("read_output(child-b)")?;
    assert_eq!(
        child_b_body, CHILD_B_CONTENT,
        "child-b output body drifted from scripted WriteOutput"
    );
    let child_b_output_id = OutputId::new(CHILD_B_CONTENT);

    // --- Parent evidence -----------------------------------------------
    //
    // Exactly 2 synthetic records, `tool == "reconcile"`, each carrying
    // the right `(child_agent_id, child_workflow_id, source_output_id)`
    // triple.
    let parent_view =
        AgentFs::new_with_storage(inspect_storage.clone(), parent_prefix, &inspect_mandate)
            .await
            .context("open parent AgentFs")?;
    let parent_evidence = parent_view
        .list_recent_evidence(16)
        .await
        .context("list_recent_evidence(parent)")?;
    let reconcile_records: Vec<&EvidenceRecord> = parent_evidence
        .iter()
        .filter(|e| e.tool == "reconcile")
        .collect();
    assert_eq!(
        reconcile_records.len(),
        2,
        "parent should have exactly 2 synthetic reconcile evidence records; \
         total evidence on disk: {} ({:?})",
        parent_evidence.len(),
        parent_evidence.iter().map(|e| &e.tool).collect::<Vec<_>>(),
    );

    // Each reconcile record must point at one of the two children's
    // emitted outputs. Pair them up by source_output_id; the per-record
    // args triple is asserted shape-wise below.
    let assert_record_matches = |rec: &EvidenceRecord,
                                 expected_child_workflow_id: &str,
                                 expected_child_agent_id: AgentId,
                                 expected_output_id: &OutputId|
     -> Result<()> {
        let args = rec
            .args
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("reconcile evidence args is not a JSON object"))?;
        let wfid = args
            .get("child_workflow_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("reconcile evidence args missing child_workflow_id"))?;
        assert_eq!(wfid, expected_child_workflow_id);
        let aid = args
            .get("child_agent_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("reconcile evidence args missing child_agent_id"))?;
        assert_eq!(aid, expected_child_agent_id.to_string());
        let source_oid: OutputId =
            serde_json::from_value(args.get("source_output_id").cloned().ok_or_else(|| {
                anyhow::anyhow!("reconcile evidence args missing source_output_id")
            })?)
            .context("source_output_id deserialize")?;
        assert_eq!(&source_oid, expected_output_id);
        Ok(())
    };

    let rec_for_a = reconcile_records
        .iter()
        .find(|r| {
            r.args
                .as_object()
                .and_then(|o| o.get("source_output_id"))
                .and_then(|v| serde_json::from_value::<OutputId>(v.clone()).ok())
                .map(|o| o == child_a_output_id)
                .unwrap_or(false)
        })
        .copied()
        .expect("expected one reconcile record pointing at child-a's output");
    let rec_for_b = reconcile_records
        .iter()
        .find(|r| {
            r.args
                .as_object()
                .and_then(|o| o.get("source_output_id"))
                .and_then(|v| serde_json::from_value::<OutputId>(v.clone()).ok())
                .map(|o| o == child_b_output_id)
                .unwrap_or(false)
        })
        .copied()
        .expect("expected one reconcile record pointing at child-b's output");
    assert_record_matches(
        rec_for_a,
        child_a_workflow_id,
        child_a_agent_id,
        &child_a_output_id,
    )?;
    assert_record_matches(
        rec_for_b,
        child_b_workflow_id,
        child_b_agent_id,
        &child_b_output_id,
    )?;

    // --- Parent output -------------------------------------------------
    //
    // The parent keeps a single canonical reconciled Output. Citations live
    // in the DB reference graph, not the file, so the synthetic-id
    // provenance is checked via the reconcile evidence records on disk
    // (`rec_for_a`/`rec_for_b`, asserted above) and the cross-FS walk below.
    let parent_body = parent_view
        .read_output()
        .await
        .context("read_output(parent)")?;
    assert_eq!(parent_body, PARENT_OUTPUT_CONTENT);

    // --- Parent conflicts ----------------------------------------------
    //
    // Exactly 1 ConflictRecord, kind = HeldOpen, 2 alternatives, the
    // claims match the scripted child contents.
    let conflicts = parent_view
        .list_conflicts()
        .await
        .context("list_conflicts(parent)")?;
    assert_eq!(
        conflicts.len(),
        1,
        "parent should have exactly one conflict record; got {}",
        conflicts.len()
    );
    let conflict = &conflicts[0];
    assert_eq!(conflict.kind, ConflictKind::HeldOpen);
    assert!(conflict.resolution.is_none());
    assert_eq!(
        conflict.alternatives.len(),
        2,
        "conflict record must carry exactly two alternatives"
    );
    let alt_a = conflict
        .alternatives
        .iter()
        .find(|a| a.source_output_id == child_a_output_id)
        .expect("conflict alternatives must include child-a's output");
    let alt_b = conflict
        .alternatives
        .iter()
        .find(|a| a.source_output_id == child_b_output_id)
        .expect("conflict alternatives must include child-b's output");
    assert_eq!(alt_a.claim, CHILD_A_CONTENT);
    assert_eq!(alt_b.claim, CHILD_B_CONTENT);
    assert_eq!(alt_a.source_child.workflow_id, child_a_workflow_id);
    assert_eq!(alt_a.source_child.agent_id, child_a_agent_id);
    assert_eq!(alt_b.source_child.workflow_id, child_b_workflow_id);
    assert_eq!(alt_b.source_child.agent_id, child_b_agent_id);

    // --- Cross-FS provenance trail (load-bearing) ---------------------
    //
    // parent's reconcile evidence records → each record's
    // args.source_output_id + child_agent_id → cross-agent open of the
    // child's FS → read_output() returns the child's current Output body
    // → byte-equal to what we already asserted on the child side, and the
    // content-addressed OutputId matches the recorded source_output_id.
    for rec in [rec_for_a, rec_for_b] {
        assert_eq!(rec.tool, "reconcile");
        let args = rec
            .args
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("reconcile evidence args is not a JSON object"))?;
        let source_oid: OutputId = serde_json::from_value(
            args.get("source_output_id")
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("reconcile evidence missing source_output_id"))?,
        )?;
        let source_child_aid_str = args
            .get("child_agent_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("reconcile evidence missing child_agent_id"))?;
        let source_child_aid: AgentId = source_child_aid_str
            .parse()
            .context("reconcile evidence child_agent_id parses as AgentId")?;

        // Cross-agent FS read — the operation the synthetic-evidence
        // pattern is designed to make a normal evidence trail support.
        let child_fs = AgentFs::open_for_agent(storage.clone(), graph_id, source_child_aid);
        let resolved_body = child_fs
            .read_output()
            .await
            .with_context(|| format!("cross-agent read_output() on child {source_child_aid}"))?;
        // The body we cross-read must match the child's own current Output
        // byte-for-byte, and its content-addressed OutputId must equal the
        // source_output_id the reconcile record recorded.
        let (expected_body, expected_id) = if source_child_aid == child_a_agent_id {
            (CHILD_A_CONTENT, &child_a_output_id)
        } else if source_child_aid == child_b_agent_id {
            (CHILD_B_CONTENT, &child_b_output_id)
        } else {
            return Err(anyhow::anyhow!(
                "reconcile evidence pointed at a child this test never started: {source_child_aid}"
            ));
        };
        assert_eq!(resolved_body, expected_body);
        assert_eq!(&OutputId::new(&resolved_body), expected_id);
        assert_eq!(&source_oid, expected_id);

        // The child's `evidence/` directory contains exactly one
        // record — the per-child planted "echo" record cited by the
        // scripted WriteOutput. (Children don't call any tools at
        // runtime; the planted record is the test-harness analogue
        // of what a real `execute_tool` activity would have written.)
        // The point of the assertion is to confirm the cross-agent
        // FS open resolves the directory cleanly + that the cited
        // evidence id is on disk under the child's prefix.
        let child_inspect = AgentFs::new_with_storage(
            inspect_storage.clone(),
            &format!("graphs/{graph_id}/agents/{source_child_aid}"),
            &inspect_mandate,
        )
        .await
        .context("open child FS for evidence/ inspection")?;
        let child_evs = child_inspect
            .list_recent_evidence(16)
            .await
            .context("list_recent_evidence(child)")?;
        assert_eq!(
            child_evs.len(),
            1,
            "child {source_child_aid} should have exactly one planted evidence record; got {} records",
            child_evs.len()
        );
        assert_eq!(child_evs[0].tool, "echo");
    }

    Ok(())
}

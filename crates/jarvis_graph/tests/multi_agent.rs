//! Stage 5.9 (JAR2-86) — end-to-end multi-agent integration test:
//! parent + 2 children + scripted disagreement, byte-checkable end-state.
//!
//! Env-gated behind `TEMPORAL_LIVE_TEST=1` (Stage 5 Project decision 11
//! — no hermetic in-process multi-agent path; `AgentCore::dispatch` stays
//! single-agent forever).
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
//! `client.start_workflow` directly (per the test's implementer's-choice
//! "Pattern A" — see PR body for the rationale).
//!
//! ## End-state assertions (load-bearing)
//!
//! 1. Each child's `outputs/<output_id>.json` contains the scripted
//!    content.
//! 2. Parent's `evidence/` contains exactly 2 synthetic `EvidenceRecord`s
//!    (`tool == "reconcile"`), one per child output, with `args`
//!    referencing the right `(child_agent_id, child_workflow_id,
//!    source_output_id)` triple.
//! 3. Parent's `outputs/` contains exactly 1 reconciled output citing
//!    both synthetic evidence ids in `evidence`.
//! 4. Parent's `conflicts/` contains exactly 1 `ConflictRecord` with
//!    `kind == HeldOpen`, `alternatives.len() == 2`, and `resolution ==
//!    None`. Each `ConflictAlternative.source_child` +
//!    `source_output_id` resolves correctly.
//! 5. **Cross-FS provenance trail (load-bearing per JAR2-86 acceptance):**
//!    starting from the parent's output, the cited evidence record's
//!    `args.source_output_id` resolves to the child's
//!    `outputs/<id>.json` via cross-agent `AgentFs::open_for_agent` —
//!    no ambiguity, no string-shape guessing.
//!
//! ## Implementer's-choice — Pattern A
//!
//! The JAR2-86 ticket-acceptance reads "runs `jarvis apply
//! graph.yaml` end-to-end via the post-JAR2-76 thin-client path."
//! Production `jarvis apply` always wires `LlmDecide` (real LLM)
//! through the worker daemon; there is no production-supported seam
//! to swap in `MockDecide` from a YAML field or CLI flag. The two
//! options were:
//!
//! - **Pattern A (chosen)**: bypass `jarvis apply`, construct the
//!   multi-agent topology directly via `client.start_workflow` per
//!   agent with `MockDecide` installed at the worker — the shape every
//!   existing multi-agent live test uses (`spawn_child_live.rs`,
//!   `child_parent_signal.rs`, `reconcile_children_live.rs`,
//!   `lifecycle_ops_live.rs`).
//! - **Pattern B**: extend the worker / `jarvis apply` with a
//!   `--decide=mock` flag so MockDecide scripts load off disk. More
//!   test-realistic; more invasive production-code surface.
//!
//! Pattern A wins on "smallest correct diff"; Pattern B is queued as a
//! follow-up. The fixture YAML is still parsed at test startup so
//! schema + topology shape regress here too.

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

use jarvis_graph::yaml::parse_and_validate;
use jarvis_node::agent_ref::{AgentId, AgentRef, GraphId};
use jarvis_node::conflict::ConflictKind;
use jarvis_node::decision::{
    ConflictAlternative, ConflictRecordIntent, ContextBundle, Decide, Decision, ReconcileSource,
};
use jarvis_node::evidence::{EvidenceId, EvidenceRecord};
use jarvis_node::fs::AgentFs;
use jarvis_node::mandate::{Mandate, OutputId};
use jarvis_node::storage::{AgentStorage, MemoryStorage};
use jarvis_node::tools::ToolRegistry;
use jarvis_node::trigger::Trigger;
use jarvis_temporal::activities::set_decision_script;
use jarvis_temporal::worker::{
    build_worker, install_agent_storage, install_decide, install_tool_registry,
};
use jarvis_temporal::workflow::{AgentInput, AgentResult, AgentWorkflow, FsHandle, ParentRef};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

/// Mandate-text discriminators the `RoutingDecide` matches on. Mirror
/// the strings encoded in `examples/smoke_multi_agent/graph.yaml`; if
/// either side drifts, the test's `parse_and_validate` startup check
/// fires before any workflow runs.
const PARENT_MANDATE_TEXT: &str = "JAR2-86-parent";
const CHILD_A_MANDATE_TEXT: &str = "JAR2-86-child-a";
const CHILD_B_MANDATE_TEXT: &str = "JAR2-86-child-b";

/// Path (relative to this crate's manifest dir) to the multi-agent
/// fixture this test pins. Same shape as the JAR2-74 single-agent
/// smoke's `GRAPH_YAML_REL`.
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

        // Empty registry — JAR2-86 does not exercise tool dispatch. The
        // `execute_tool` activity body still demands the OnceLock be
        // installed, so we install an empty one.
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
    });
    SHARED_STORAGE.get().cloned().expect("storage installed")
}

/// `Decide` that routes per `bundle.mandate.text`:
///
/// - Parent (`PARENT_MANDATE_TEXT`): records every trigger the bundle
///   carries; synthesizes a `Decision::ReconcileChildren` from the
///   first observed `ChildOutput` triggers (one per child) when the
///   script pops the `reconcile_placeholder` sentinel; synthesizes a
///   `Decision::EmitOutput` citing the synthetic evidence ids from
///   `bundle.recent_evidence` (filtered to `tool == "reconcile"`)
///   when the script pops the `emit_with_synthetic_placeholder`
///   sentinel.
/// - Children A / B: pop from their own per-role script FIFO. Empty
///   script defaults to a short idle so a misconfigured test loops
///   politely rather than panicking the activity.
struct RoutingDecide;

#[async_trait]
impl Decide for RoutingDecide {
    async fn decide(&self, bundle: ContextBundle) -> anyhow::Result<Decision> {
        match bundle.mandate.text.as_str() {
            PARENT_MANDATE_TEXT => decide_parent(&bundle),
            CHILD_A_MANDATE_TEXT => decide_child(CHILD_A_SCRIPT.get().expect("CHILD_A_SCRIPT")),
            CHILD_B_MANDATE_TEXT => decide_child(CHILD_B_SCRIPT.get().expect("CHILD_B_SCRIPT")),
            other => panic!(
                "RoutingDecide saw unexpected mandate text: {other:?} \
                 (JAR2-86 multi-agent test scripts only PARENT / CHILD_A / CHILD_B \
                  mandate text)"
            ),
        }
    }
}

/// Parent-side decide body. Records observed triggers, then either
/// returns the next scripted decision verbatim or synthesizes one of
/// the placeholder sentinels.
fn decide_parent(bundle: &ContextBundle) -> anyhow::Result<Decision> {
    if !bundle.triggers.is_empty() {
        let log = PARENT_OBSERVED_TRIGGERS
            .get()
            .expect("PARENT_OBSERVED_TRIGGERS installed")
            .clone();
        let mut guard = log.lock().expect("trigger log mutex poisoned");
        for t in &bundle.triggers {
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
        Some(d) if is_emit_placeholder(&d) => synthesize_emit_or_wait(bundle),
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
/// `resolution: None` → `HeldOpen` per Stage 5 Project decision 14.
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

/// Synthesize `Decision::EmitOutput { content, evidence }` from the
/// synthetic evidence records the reconcile activity just wrote into
/// the parent's `evidence/` directory. The records surface in this
/// tick's `bundle.recent_evidence` (no workflow-state slot — Stage 5
/// Project decision 3). If they're missing, push the placeholder back
/// and idle so the next tick sees the freshly-written records.
fn synthesize_emit_or_wait(bundle: &ContextBundle) -> anyhow::Result<Decision> {
    let synthetic_ids: Vec<EvidenceId> = bundle
        .recent_evidence
        .iter()
        .filter(|e| e.tool == "reconcile")
        .map(|e| e.id.clone())
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
        2 => Ok(Decision::EmitOutput {
            content: PARENT_OUTPUT_CONTENT.into(),
            evidence: synthetic_ids,
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

/// JAR2-86 end-to-end live test. See module doc for the assertion list.
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
    run_end_to_end().await.expect("JAR2-86 end-to-end smoke");
}

async fn run_end_to_end() -> Result<()> {
    // ---- 1. YAML schema/topology smoke-check --------------------------
    //
    // Parse the fixture so a future YAML edit that drifts mandate text
    // or topology shape fails the test, not the workflow. We don't
    // dispatch through `jarvis apply` (see Pattern A rationale in
    // module doc); the schema-level pin still belongs here.
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
    let task_queue = format!("jarvis-jar2-86-{suffix}");

    // Unique graph_id per run so reruns don't collide. Per-agent
    // workflow ids derive from `(graph_id, agent_id)` per Stage 5
    // Project decision 6's flat scheme. `MemoryStorage` is
    // process-wide so the per-graph UUID is what isolates this test
    // from any other test in the same binary.
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
    // scripted EmitOutput resolves provenance. `AgentFs::persist_output`
    // rejects empty `evidence` (`FsError::EmptyEvidence`) — children
    // therefore need to cite *something*. The synthetic-evidence
    // pattern (Stage 5 Project decision 3) is the parent's mechanism
    // for cross-agent provenance and is orthogonal to whatever each
    // child's own output cites.
    //
    // Distinct planted records per child (different `args` payload)
    // so the two `EvidenceId`s are distinct on disk — keeps the
    // FS-end-state assertions sharp (child-a's evidence/ stays
    // distinguishable from child-b's).
    let plant_mandate = Mandate::new("plant", Duration::from_millis(0), None);
    let plant_storage_a: Arc<dyn AgentStorage> = storage.clone();
    let plant_fs_a = AgentFs::new_with_storage(plant_storage_a, &child_a_prefix, &plant_mandate)
        .await
        .expect("open planting AgentFs for child-a");
    let planted_a_id = plant_fs_a
        .record_evidence(EvidenceRecord::new(
            "echo",
            serde_json::json!({"child": "a"}),
            serde_json::json!({"hit": true}),
            chrono::Utc::now(),
        ))
        .await
        .expect("plant evidence for child-a EmitOutput");
    let plant_storage_b: Arc<dyn AgentStorage> = storage.clone();
    let plant_fs_b = AgentFs::new_with_storage(plant_storage_b, &child_b_prefix, &plant_mandate)
        .await
        .expect("open planting AgentFs for child-b");
    let planted_b_id = plant_fs_b
        .record_evidence(EvidenceRecord::new(
            "echo",
            serde_json::json!({"child": "b"}),
            serde_json::json!({"hit": true}),
            chrono::Utc::now(),
        ))
        .await
        .expect("plant evidence for child-b EmitOutput");

    // ---- 3. Scripts ---------------------------------------------------
    //
    // Children: plain Idle → EmitOutput → Retire. Each EmitOutput
    // cites its planted evidence id (one per child) so
    // `persist_output`'s provenance contract is satisfied.
    //
    // Parent: idle (so children get scheduling room) → reconcile (via
    // sentinel; pushes itself back until both ChildOutput signals
    // land) → emit with synthetic (via sentinel; pushes itself back
    // until 2 reconcile-evidence records appear in recent_evidence) →
    // retire.
    let child_a_script = vec![
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
        Decision::EmitOutput {
            content: CHILD_A_CONTENT.into(),
            evidence: vec![planted_a_id.clone()],
        },
        Decision::Retire {
            reason: "JAR2-86 child-a: scripted retire".into(),
        },
    ];
    let child_b_script = vec![
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
        Decision::EmitOutput {
            content: CHILD_B_CONTENT.into(),
            evidence: vec![planted_b_id.clone()],
        },
        Decision::Retire {
            reason: "JAR2-86 child-b: scripted retire".into(),
        },
    ];
    let parent_script = vec![
        reconcile_placeholder(),
        emit_with_synthetic_placeholder(),
        Decision::Retire {
            reason: "JAR2-86 parent: smoke complete".into(),
        },
    ];
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
    // its ChildOutput signal (mirrors the JAR2-81 / JAR2-82 ordering).
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
        mandate: Mandate::new(PARENT_MANDATE_TEXT, Duration::from_millis(50), None),
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
    eprintln!("JAR2-86: parent started at {parent_workflow_id}");

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
        mandate: Mandate::new(CHILD_A_MANDATE_TEXT, Duration::from_millis(50), None),
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
    eprintln!("JAR2-86: child-a started at {child_a_workflow_id}");

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
        mandate: Mandate::new(CHILD_B_MANDATE_TEXT, Duration::from_millis(50), None),
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
    eprintln!("JAR2-86: child-b started at {child_b_workflow_id}");

    // Wait for each child to retire (EmitOutput → Retire). Each
    // signals the parent on its EmitOutput tick (JAR2-81 path) before
    // retiring. The retirement also fires `Trigger::ChildRetired` at
    // the parent (JAR2-84 path) — orthogonal to the reconcile assertion
    // but exercises the lifecycle-signal path as a side effect.
    let _child_a_result: AgentResult = child_a_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("child-a get_result")?;
    eprintln!("JAR2-86: child-a retired cleanly");
    let _child_b_result: AgentResult = child_b_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("child-b get_result")?;
    eprintln!("JAR2-86: child-b retired cleanly");

    // Parent loops on the reconcile_placeholder until both
    // ChildOutput triggers land, then synthesizes the reconcile
    // decision, then loops on the emit-with-synthetic placeholder
    // until the 2 synthetic evidence records surface in
    // recent_evidence, then emits the reconciled output, then
    // retires.
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
                    "JAR2-86: test asked".to_string(),
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
    eprintln!("JAR2-86: parent retired ({reason})");

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
    // Each child's `outputs/<id>.json` carries the scripted content.
    let child_a_view =
        AgentFs::new_with_storage(inspect_storage.clone(), child_a_prefix, &inspect_mandate)
            .await
            .context("open child-a AgentFs")?;
    let a_outs = child_a_view
        .list_recent_outputs(8)
        .await
        .context("list_recent_outputs(child-a)")?;
    assert_eq!(
        a_outs.len(),
        1,
        "child-a should have exactly one output on disk; got {}",
        a_outs.len()
    );
    let child_a_output = &a_outs[0];
    assert_eq!(
        child_a_output.content, CHILD_A_CONTENT,
        "child-a output content drifted from scripted EmitOutput"
    );

    let child_b_view =
        AgentFs::new_with_storage(inspect_storage.clone(), child_b_prefix, &inspect_mandate)
            .await
            .context("open child-b AgentFs")?;
    let b_outs = child_b_view
        .list_recent_outputs(8)
        .await
        .context("list_recent_outputs(child-b)")?;
    assert_eq!(
        b_outs.len(),
        1,
        "child-b should have exactly one output on disk; got {}",
        b_outs.len()
    );
    let child_b_output = &b_outs[0];
    assert_eq!(
        child_b_output.content, CHILD_B_CONTENT,
        "child-b output content drifted from scripted EmitOutput"
    );

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
                .map(|o| o == child_a_output.id)
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
                .map(|o| o == child_b_output.id)
                .unwrap_or(false)
        })
        .copied()
        .expect("expected one reconcile record pointing at child-b's output");
    assert_record_matches(
        rec_for_a,
        child_a_workflow_id,
        child_a_agent_id,
        &child_a_output.id,
    )?;
    assert_record_matches(
        rec_for_b,
        child_b_workflow_id,
        child_b_agent_id,
        &child_b_output.id,
    )?;

    // --- Parent output -------------------------------------------------
    //
    // Exactly 1 reconciled output, citing both synthetic evidence ids.
    let parent_outs = parent_view
        .list_recent_outputs(8)
        .await
        .context("list_recent_outputs(parent)")?;
    assert_eq!(
        parent_outs.len(),
        1,
        "parent should have exactly one output (the reconciled one); got {}",
        parent_outs.len()
    );
    let parent_output = &parent_outs[0];
    assert_eq!(parent_output.content, PARENT_OUTPUT_CONTENT);
    assert_eq!(
        parent_output.evidence.len(),
        2,
        "parent output must cite both synthetic evidence ids"
    );
    assert!(
        parent_output.evidence.contains(&rec_for_a.id),
        "parent output evidence missing child-a's synthetic id"
    );
    assert!(
        parent_output.evidence.contains(&rec_for_b.id),
        "parent output evidence missing child-b's synthetic id"
    );

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
        .find(|a| a.source_output_id == child_a_output.id)
        .expect("conflict alternatives must include child-a's output");
    let alt_b = conflict
        .alternatives
        .iter()
        .find(|a| a.source_output_id == child_b_output.id)
        .expect("conflict alternatives must include child-b's output");
    assert_eq!(alt_a.claim, CHILD_A_CONTENT);
    assert_eq!(alt_b.claim, CHILD_B_CONTENT);
    assert_eq!(alt_a.source_child.workflow_id, child_a_workflow_id);
    assert_eq!(alt_a.source_child.agent_id, child_a_agent_id);
    assert_eq!(alt_b.source_child.workflow_id, child_b_workflow_id);
    assert_eq!(alt_b.source_child.agent_id, child_b_agent_id);

    // --- Cross-FS provenance trail (load-bearing per ticket) -----------
    //
    // parent output → its evidence ids → resolve each via the
    // parent's own evidence/<id>.json → that record's
    // args.source_output_id → cross-agent open of the child's FS
    // → read_output(source_output_id) returns the child's output
    // → byte-equal to what we already asserted on the child side.
    for synth_ev in &parent_output.evidence {
        let rec = parent_evidence
            .iter()
            .find(|e| &e.id == synth_ev)
            .ok_or_else(|| {
                anyhow::anyhow!("synthetic evidence id missing from parent evidence/")
            })?;
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
        let resolved = child_fs.read_output(&source_oid).await.with_context(|| {
            format!("cross-agent read_output({source_oid:?}) on child {source_child_aid}")
        })?;
        // The output we cross-read must match the child's own
        // list_recent_outputs view byte-for-byte (modulo created_at,
        // which the test doesn't compare — OutputId is
        // content-addressed over (content, evidence), so id+content+
        // evidence equality is the load-bearing pin).
        let expected = if source_child_aid == child_a_agent_id {
            child_a_output
        } else if source_child_aid == child_b_agent_id {
            child_b_output
        } else {
            return Err(anyhow::anyhow!(
                "reconcile evidence pointed at a child this test never started: {source_child_aid}"
            ));
        };
        assert_eq!(resolved.id, expected.id);
        assert_eq!(resolved.content, expected.content);
        assert_eq!(resolved.evidence, expected.evidence);

        // The child's `evidence/` directory contains exactly one
        // record — the per-child planted "echo" record cited by the
        // scripted EmitOutput. (Children don't call any tools at
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

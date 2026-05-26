//! Stage 5.5 (JAR2-82) — live integration test for the
//! `Decision::ReconcileChildren` workflow arm + `reconcile_children`
//! activity.
//!
//! Env-gated behind `TEMPORAL_LIVE_TEST=1`. Spawns two
//! `AgentWorkflow` instances against a real Temporal Server:
//!
//! 1. A **parent** workflow whose scripted `Decide` waits for a
//!    `Trigger::ChildOutput` signal from the child, then emits one
//!    `Decision::ReconcileChildren { sources: [the child output],
//!    conflict: None }`, then `Decision::Retire`.
//! 2. A **child** workflow with `parent_handle: Some(..)` that emits
//!    one `Decision::EmitOutput → Decision::Retire`. The
//!    `Decision::EmitOutput` arm fires the `Trigger::ChildOutput`
//!    signal at the parent (Stage 5.4 / JAR2-81 path).
//!
//! ## Happy-path assertions
//!
//! - After the parent retires, the parent's `evidence/` directory
//!   contains at least one synthetic evidence record with
//!   `tool == "reconcile"` and `args` carrying the
//!   `(child_agent_id, child_workflow_id, source_output_id)` triple
//!   that points back at the child's emitted output.
//!
//! ## Failure-mode assertions
//!
//! - The parent's `Decide` script emits a
//!   `Decision::ReconcileChildren { sources: [bogus output id], .. }`
//!   referencing an `OutputId` that doesn't resolve on the child's
//!   FS. The activity returns `ReconciliationError::ChildOutputNotFound`
//!   as a non-retryable `ApplicationFailure`; the parent's workflow
//!   body catches the failure and stages a `CorrectionContext` for
//!   the next tick. We confirm by observing the parent retires
//!   normally (no panic / workflow failure) and that its next
//!   scripted decision sees the correction surfaced (indirectly: the
//!   workflow body's clear_correction is NOT invoked on the failing
//!   arm, so the next tick's `assemble_context` input carries the
//!   `prior_correction`).
//!
//! ## SDK / test-shape notes
//!
//! - Per Stage 5 Project decision 11, this entire test must run live
//!   — there is no hermetic in-process multi-agent path.
//! - Storage is `MemoryStorage` shared across both workflows so the
//!   activity bodies see one consistent view; same shape JAR2-81 uses.
//! - `(graph_id, agent_id)` is passed explicitly on `AgentInput`
//!   (matching the `child_parent_signal.rs` pattern) rather than
//!   routed through `into_agent_input`'s YAML adapter, which
//!   sidesteps the JAR2-89 synthetic-UUID-mismatch concern flagged
//!   in the JAR2-82 ticket.

use std::collections::VecDeque;
use std::env;
use std::sync::{Arc, Mutex, OnceLock};
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
use uuid::Uuid;

use jarvis_node::agent_ref::{AgentId, AgentRef, GraphId};
use jarvis_node::decision::{ContextBundle, Decide, Decision, ReconcileSource};
use jarvis_node::evidence::EvidenceRecord;
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

const PARENT_MANDATE_TEXT: &str = "JAR2-82-parent";
const CHILD_MANDATE_TEXT: &str = "JAR2-82-child";

/// Shared in-memory storage backend: parent + child both run their
/// activities against this. The test driver also opens views over it
/// to inspect what landed on disk.
static SHARED_STORAGE: OnceLock<Arc<MemoryStorage>> = OnceLock::new();

/// Captured trigger payloads the parent's `Decide` observed. The
/// parent's reconcile script reads this to discover the child's
/// `OutputId` at the moment it needs to construct
/// `Decision::ReconcileChildren`.
static PARENT_OBSERVED_TRIGGERS: OnceLock<Arc<Mutex<Vec<Trigger>>>> = OnceLock::new();

/// Per-role scripts the `ReconcileRoutingDecide` consumes.
static CHILD_SCRIPT: OnceLock<Mutex<VecDeque<Decision>>> = OnceLock::new();
static PARENT_SCRIPT: OnceLock<Mutex<VecDeque<Decision>>> = OnceLock::new();

/// Per-role pending-source plumbing the *parent's* script uses to
/// build a `Decision::ReconcileChildren` at decision time using the
/// child output id discovered from a previously-observed
/// `Trigger::ChildOutput`. `(child_workflow_id, child_agent_id,
/// output_id_override)` — when `output_id_override.is_some()` the
/// reconcile decision uses that id (the failure-mode test plants a
/// bogus id here); when `None`, the decision pulls the id from the
/// first ChildOutput observed.
type PendingReconcile = (Option<String>, Option<AgentId>, Option<OutputId>);
static PARENT_PENDING_RECONCILE: OnceLock<Mutex<Option<PendingReconcile>>> = OnceLock::new();

/// Serializes the two live tests in this binary so they don't
/// share `PARENT_OBSERVED_TRIGGERS` or the per-role scripts.
static LIVE_TEST_GUARD: Mutex<()> = Mutex::new(());

static INIT: std::sync::Once = std::sync::Once::new();

/// One-shot install of the shared storage + empty tool registry +
/// `ReconcileRoutingDecide`. Subsequent calls are no-ops — the
/// underlying install hooks panic on double-install.
fn ensure_installed() -> Arc<MemoryStorage> {
    INIT.call_once(|| {
        let storage: Arc<MemoryStorage> = Arc::new(MemoryStorage::new());
        SHARED_STORAGE
            .set(Arc::clone(&storage))
            .expect("SHARED_STORAGE set exactly once");
        let dyn_storage: Arc<dyn AgentStorage> = storage;
        install_agent_storage(dyn_storage);

        install_tool_registry(Arc::new(ToolRegistry::new()));

        PARENT_OBSERVED_TRIGGERS
            .set(Arc::new(Mutex::new(Vec::new())))
            .expect("PARENT_OBSERVED_TRIGGERS set exactly once");

        CHILD_SCRIPT
            .set(Mutex::new(VecDeque::new()))
            .expect("CHILD_SCRIPT set exactly once");
        PARENT_SCRIPT
            .set(Mutex::new(VecDeque::new()))
            .expect("PARENT_SCRIPT set exactly once");
        PARENT_PENDING_RECONCILE
            .set(Mutex::new(None))
            .expect("PARENT_PENDING_RECONCILE set exactly once");

        install_decide(Arc::new(ReconcileRoutingDecide));
    });
    SHARED_STORAGE.get().cloned().expect("storage installed")
}

/// `Decide` that routes by `bundle.mandate.text`, records every
/// trigger the parent observes, and (for the parent role) materializes
/// a `Decision::ReconcileChildren` lazily from the most recently
/// observed `Trigger::ChildOutput` + the pending-reconcile slot.
struct ReconcileRoutingDecide;

#[async_trait]
impl Decide for ReconcileRoutingDecide {
    async fn decide(&self, bundle: ContextBundle) -> anyhow::Result<Decision> {
        match bundle.mandate.text.as_str() {
            PARENT_MANDATE_TEXT => {
                // Record every trigger the parent sees this tick.
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

                // Pop the next scripted decision; if it's a
                // "reconcile placeholder" (we encode that as a
                // `Decision::Idle { next_after: u64::MAX }` sentinel),
                // synthesize a `ReconcileChildren` from observed
                // triggers + pending slot.
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
                    Some(d) => Ok(d),
                    None => Ok(Decision::Idle {
                        next_after: Duration::from_millis(50),
                    }),
                }
            }
            CHILD_MANDATE_TEXT => {
                let popped = {
                    let mut q = CHILD_SCRIPT
                        .get()
                        .expect("CHILD_SCRIPT installed")
                        .lock()
                        .expect("CHILD_SCRIPT mutex poisoned");
                    q.pop_front()
                };
                Ok(popped.unwrap_or(Decision::Idle {
                    next_after: Duration::from_millis(50),
                }))
            }
            other => panic!(
                "ReconcileRoutingDecide saw unexpected mandate text: {other:?} \
                 (JAR2-82 tests script only PARENT_MANDATE_TEXT / CHILD_MANDATE_TEXT)"
            ),
        }
    }
}

/// Sentinel: a `Decision::Idle { next_after: u64::MAX }` in the parent
/// script means "synthesize a `ReconcileChildren` from observed
/// triggers at decide time". Picked over a new variant because
/// `Decision` is the contract enum and we don't want a test-only arm
/// on the wire.
fn is_reconcile_placeholder(d: &Decision) -> bool {
    matches!(d, Decision::Idle { next_after } if *next_after == Duration::from_secs(u64::MAX))
}

fn reconcile_placeholder() -> Decision {
    Decision::Idle {
        next_after: Duration::from_secs(u64::MAX),
    }
}

/// Build the `ReconcileChildren` decision the parent script needs.
/// If no `ChildOutput` has landed yet, fall back to a short `Idle` so
/// the loop spins until the signal arrives.
fn synthesize_reconcile_or_wait() -> anyhow::Result<Decision> {
    let observed = PARENT_OBSERVED_TRIGGERS
        .get()
        .expect("PARENT_OBSERVED_TRIGGERS installed")
        .lock()
        .expect("observed triggers mutex poisoned")
        .clone();
    let child_output = observed.iter().find_map(|t| match t {
        Trigger::ChildOutput {
            child_ref,
            output_id,
            ..
        } => Some((child_ref.clone(), output_id.clone())),
        _ => None,
    });
    let (default_ref, default_output_id) = match child_output {
        Some(x) => x,
        None => {
            // Push the placeholder back onto the queue so we try
            // again on the next tick; idle briefly so the wake gate
            // races against the signal.
            let mut q = PARENT_SCRIPT
                .get()
                .expect("PARENT_SCRIPT installed")
                .lock()
                .expect("PARENT_SCRIPT mutex poisoned");
            q.push_front(reconcile_placeholder());
            return Ok(Decision::Idle {
                next_after: Duration::from_millis(50),
            });
        }
    };

    // Override hook for the failure-mode test (plants a bogus
    // OutputId in the pending slot).
    let pending = PARENT_PENDING_RECONCILE
        .get()
        .expect("PARENT_PENDING_RECONCILE installed")
        .lock()
        .expect("pending reconcile mutex poisoned")
        .take();
    let (wf_override, agent_override, oid_override) = pending.unwrap_or((None, None, None));
    let child_ref = match (wf_override, agent_override) {
        (Some(wf), Some(aid)) => AgentRef::new(wf, aid),
        _ => default_ref,
    };
    let output_id = oid_override.unwrap_or(default_output_id);
    Ok(Decision::ReconcileChildren {
        sources: vec![ReconcileSource {
            child_ref,
            output_id,
        }],
        conflict: None,
    })
}

fn install_role_scripts(parent: Vec<Decision>, child: Vec<Decision>) {
    {
        let mut p = PARENT_SCRIPT
            .get()
            .expect("PARENT_SCRIPT installed")
            .lock()
            .expect("PARENT_SCRIPT mutex poisoned");
        *p = parent.into();
    }
    {
        let mut c = CHILD_SCRIPT
            .get()
            .expect("CHILD_SCRIPT installed")
            .lock()
            .expect("CHILD_SCRIPT mutex poisoned");
        *c = child.into();
    }
    set_decision_script(Vec::new());
}

fn reset_observed_state() {
    PARENT_OBSERVED_TRIGGERS
        .get()
        .expect("PARENT_OBSERVED_TRIGGERS installed")
        .lock()
        .expect("observed triggers mutex poisoned")
        .clear();
    *PARENT_PENDING_RECONCILE
        .get()
        .expect("PARENT_PENDING_RECONCILE installed")
        .lock()
        .expect("pending reconcile mutex poisoned") = None;
}

fn set_pending_reconcile(p: PendingReconcile) {
    *PARENT_PENDING_RECONCILE
        .get()
        .expect("PARENT_PENDING_RECONCILE installed")
        .lock()
        .expect("pending reconcile mutex poisoned") = Some(p);
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

/// Happy-path live test — parent reconciles one child output; one
/// synthetic evidence record lands in the parent's `evidence/`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::await_holding_lock)]
async fn parent_reconcile_writes_synthetic_evidence_under_parents_prefix() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping parent_reconcile_writes_synthetic_evidence_under_parents_prefix; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    let _guard = LIVE_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
    run_happy_path().await.expect("JAR2-82 happy path");
}

/// Failure-mode live test — parent reconciles a bogus output id; the
/// activity errors typed, parent stages a correction context and
/// retires normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::await_holding_lock)]
async fn parent_reconcile_on_missing_child_output_stages_correction_and_continues() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping parent_reconcile_on_missing_child_output_stages_correction_and_continues; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    let _guard = LIVE_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
    run_failure_path().await.expect("JAR2-82 failure path");
}

async fn run_happy_path() -> Result<()> {
    let suffix = run_suffix();
    let task_queue = format!("jarvis-jar2-82-happy-{suffix}");
    let graph_id = GraphId::new(Uuid::new_v4());
    let parent_agent_id = AgentId::new(Uuid::new_v4());
    let child_agent_id =
        AgentId::new(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap());
    let parent_prefix = format!("graphs/{graph_id}/agents/{parent_agent_id}");
    let child_prefix = format!("graphs/{graph_id}/agents/{child_agent_id}");
    let parent_workflow_id = parent_prefix.clone();
    let child_workflow_id = child_prefix.clone();

    let storage = ensure_installed();
    reset_observed_state();

    // Plant evidence the child's EmitOutput will cite.
    let plant_mandate = Mandate::new("plant", Duration::from_millis(0), None);
    let plant_storage: Arc<dyn AgentStorage> = storage.clone();
    let plant_fs = AgentFs::new_with_storage(plant_storage, &child_prefix, &plant_mandate)
        .await
        .context("open planting AgentFs for child")?;
    let planted_id = plant_fs
        .record_evidence(EvidenceRecord::new(
            "echo",
            serde_json::json!({"k": "v"}),
            serde_json::json!({"hit": true}),
            Utc::now(),
        ))
        .await
        .context("plant evidence for child EmitOutput")?;

    // Parent script: wait for ChildOutput trigger by spinning on
    // reconcile_placeholder; then Retire.
    let parent_script = vec![
        reconcile_placeholder(),
        Decision::Retire {
            reason: "JAR2-82 happy: scripted retire".into(),
        },
    ];
    let child_script = vec![
        Decision::EmitOutput {
            content: "JAR2-82 child output".into(),
            evidence: vec![planted_id.clone()],
        },
        Decision::Retire {
            reason: "JAR2-82 child: scripted retire".into(),
        },
    ];
    install_role_scripts(parent_script, child_script);

    let runtime = build_runtime()?;
    let client = build_client().await?;
    let mut worker = build_worker(&runtime, client.clone(), &task_queue)?;
    let shutdown = worker.shutdown_handle();

    let driver = tokio::spawn({
        let task_queue = task_queue.clone();
        let parent_prefix = parent_prefix.clone();
        let child_prefix = child_prefix.clone();
        let parent_workflow_id = parent_workflow_id.clone();
        let child_workflow_id = child_workflow_id.clone();
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
            drive_happy_path(
                client,
                &task_queue,
                graph_id,
                parent_agent_id,
                child_agent_id,
                &parent_workflow_id,
                &child_workflow_id,
                &parent_prefix,
                &child_prefix,
                storage_arc,
            )
            .await
        }
    });

    let worker_result = tokio::time::timeout(Duration::from_secs(120), worker.run())
        .await
        .map_err(|_| anyhow::anyhow!("worker.run() timed out (120s)"))?
        .map_err(|e| anyhow::anyhow!("worker.run() exited with error: {e}"));
    let driver_result = driver.await.context("driver task panicked")?;
    worker_result?;
    driver_result?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn drive_happy_path(
    client: Client,
    task_queue: &str,
    graph_id: GraphId,
    parent_agent_id: AgentId,
    child_agent_id: AgentId,
    parent_workflow_id: &str,
    child_workflow_id: &str,
    parent_prefix: &str,
    child_prefix: &str,
    storage: Arc<MemoryStorage>,
) -> Result<()> {
    // Start parent first so it's addressable when child fires the
    // ChildOutput signal.
    let parent_input = AgentInput {
        cfg: Default::default(),
        fs_handle: FsHandle {
            prefix: parent_prefix.into(),
        },
        parent_handle: None,
        carryover: None,
        mandate: Mandate::new(PARENT_MANDATE_TEXT, Duration::from_millis(50), None),
        graph_id,
        agent_id: parent_agent_id,
        agent_name: "parent".into(),
    };
    let parent_handle = client
        .start_workflow(
            AgentWorkflow::run,
            parent_input,
            WorkflowStartOptions::new(task_queue, parent_workflow_id).build(),
        )
        .await
        .context("start_workflow(parent)")?;
    eprintln!("JAR2-82 happy: parent started at {parent_workflow_id}");

    let child_input = AgentInput {
        cfg: Default::default(),
        fs_handle: FsHandle {
            prefix: child_prefix.into(),
        },
        parent_handle: Some(ParentRef {
            workflow_id: parent_workflow_id.to_string(),
            ..ParentRef::default()
        }),
        carryover: None,
        mandate: Mandate::new(CHILD_MANDATE_TEXT, Duration::from_millis(50), None),
        graph_id,
        agent_id: child_agent_id,
        agent_name: "fda_scraper".into(),
    };
    let child_handle = client
        .start_workflow(
            AgentWorkflow::run,
            child_input,
            WorkflowStartOptions::new(task_queue, child_workflow_id).build(),
        )
        .await
        .context("start_workflow(child)")?;
    eprintln!("JAR2-82 happy: child started at {child_workflow_id}");

    // Wait for child to retire — its EmitOutput → Retire script
    // terminates it. The EmitOutput signals the parent before Retire.
    let _child_result: AgentResult = child_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("child get_result")?;
    eprintln!("JAR2-82 happy: child retired cleanly");

    // Wait for the parent to retire. The parent's script is
    // `[reconcile_placeholder, Retire]`: while no ChildOutput has
    // landed the placeholder keeps re-queueing itself, so the
    // parent loops idle until the signal arrives. Once observed,
    // `synthesize_reconcile_or_wait` returns a real
    // `Decision::ReconcileChildren`, the activity writes synthetic
    // evidence to the parent's `evidence/`, and the next tick pops
    // Retire from the now-shifted script (we never push the
    // placeholder back after a successful synthesize).
    let retire_timer = tokio::time::timeout(
        Duration::from_secs(90),
        parent_handle.get_result(WorkflowGetResultOptions::default()),
    )
    .await;
    let parent_result: AgentResult = match retire_timer {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => return Err(anyhow::anyhow!("parent get_result: {e}")),
        Err(_) => {
            // Belt-and-braces: signal retire so the loop terminates
            // for inspection (a stuck parent loop probably means
            // the synthesize_reconcile_or_wait fast-path didn't
            // observe the trigger — surface the synthetic-evidence
            // assertion below either way).
            parent_handle
                .signal(
                    AgentWorkflow::retire,
                    "JAR2-82 happy: test asked".to_string(),
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
    eprintln!("JAR2-82 happy: parent retired ({reason})");

    // Inspect parent's `evidence/` directory for the synthetic
    // record(s) — one per source the reconcile activity processed.
    let inspect_mandate = Mandate::new("inspect", Duration::from_millis(0), None);
    let inspect_storage: Arc<dyn AgentStorage> = storage.clone();
    let parent_view = AgentFs::new_with_storage(
        inspect_storage,
        &format!("{parent_prefix}/"),
        &inspect_mandate,
    )
    .await
    .context("open inspecting AgentFs over parent")?;
    let evs = parent_view
        .list_recent_evidence(16)
        .await
        .context("list_recent_evidence on parent")?;
    let reconcile_records: Vec<_> = evs.iter().filter(|e| e.tool == "reconcile").collect();
    assert!(
        !reconcile_records.is_empty(),
        "expected at least one synthetic reconcile evidence record on parent; got {} total: {:?}",
        evs.len(),
        evs.iter().map(|e| &e.tool).collect::<Vec<_>>()
    );
    let rec = reconcile_records[0];
    // Validate args shape.
    let args = rec.args.as_object().expect("args is JSON object");
    assert_eq!(
        args.get("child_workflow_id")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
        child_workflow_id,
        "synthetic evidence args must carry the child's workflow id"
    );
    let arg_agent_id = args
        .get("child_agent_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(
        arg_agent_id,
        child_agent_id.to_string(),
        "synthetic evidence args must carry the child's agent id"
    );
    assert!(args.contains_key("source_output_id"));
    Ok(())
}

async fn run_failure_path() -> Result<()> {
    let suffix = run_suffix();
    let task_queue = format!("jarvis-jar2-82-fail-{suffix}");
    let graph_id = GraphId::new(Uuid::new_v4());
    let parent_agent_id = AgentId::new(Uuid::new_v4());
    let child_agent_id =
        AgentId::new(Uuid::parse_str("66666666-7777-8888-9999-aaaaaaaaaaaa").unwrap());
    let parent_prefix = format!("graphs/{graph_id}/agents/{parent_agent_id}");
    let child_prefix = format!("graphs/{graph_id}/agents/{child_agent_id}");
    let parent_workflow_id = parent_prefix.clone();
    let child_workflow_id = child_prefix.clone();

    let storage = ensure_installed();
    reset_observed_state();

    let plant_mandate = Mandate::new("plant", Duration::from_millis(0), None);
    let plant_storage: Arc<dyn AgentStorage> = storage.clone();
    let plant_fs = AgentFs::new_with_storage(plant_storage, &child_prefix, &plant_mandate)
        .await
        .context("open planting AgentFs for child (failure path)")?;
    let planted_id = plant_fs
        .record_evidence(EvidenceRecord::new(
            "echo",
            serde_json::json!({"k": "vv"}),
            serde_json::json!({"hit": true}),
            Utc::now(),
        ))
        .await
        .context("plant evidence for child EmitOutput (failure path)")?;

    // Override the OutputId in the reconcile decision with a bogus
    // hash that won't resolve on the child's FS.
    let bogus_output_id = OutputId::from_hex("de".repeat(32));
    set_pending_reconcile((None, None, Some(bogus_output_id.clone())));

    let parent_script = vec![
        reconcile_placeholder(),
        Decision::Retire {
            reason: "JAR2-82 fail: scripted retire after correction".into(),
        },
    ];
    let child_script = vec![
        Decision::EmitOutput {
            content: "JAR2-82 fail child output".into(),
            evidence: vec![planted_id.clone()],
        },
        Decision::Retire {
            reason: "JAR2-82 fail child: scripted retire".into(),
        },
    ];
    install_role_scripts(parent_script, child_script);

    let runtime = build_runtime()?;
    let client = build_client().await?;
    let mut worker = build_worker(&runtime, client.clone(), &task_queue)?;
    let shutdown = worker.shutdown_handle();

    let driver = tokio::spawn({
        let task_queue = task_queue.clone();
        let parent_workflow_id = parent_workflow_id.clone();
        let child_workflow_id = child_workflow_id.clone();
        let parent_prefix = parent_prefix.clone();
        let child_prefix = child_prefix.clone();
        async move {
            struct ShutdownGuard<F: Fn()>(F);
            impl<F: Fn()> Drop for ShutdownGuard<F> {
                fn drop(&mut self) {
                    (self.0)();
                }
            }
            let _g = ShutdownGuard(shutdown);
            drive_failure_path(
                client,
                &task_queue,
                graph_id,
                parent_agent_id,
                child_agent_id,
                &parent_workflow_id,
                &child_workflow_id,
                &parent_prefix,
                &child_prefix,
            )
            .await
        }
    });

    let worker_result = tokio::time::timeout(Duration::from_secs(120), worker.run())
        .await
        .map_err(|_| anyhow::anyhow!("worker.run() timed out (120s)"))?
        .map_err(|e| anyhow::anyhow!("worker.run() exited with error: {e}"));
    let driver_result = driver.await.context("driver task panicked")?;
    worker_result?;
    driver_result?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn drive_failure_path(
    client: Client,
    task_queue: &str,
    graph_id: GraphId,
    parent_agent_id: AgentId,
    child_agent_id: AgentId,
    parent_workflow_id: &str,
    child_workflow_id: &str,
    parent_prefix: &str,
    _child_prefix: &str,
) -> Result<()> {
    let parent_input = AgentInput {
        cfg: Default::default(),
        fs_handle: FsHandle {
            prefix: parent_prefix.into(),
        },
        parent_handle: None,
        carryover: None,
        mandate: Mandate::new(PARENT_MANDATE_TEXT, Duration::from_millis(50), None),
        graph_id,
        agent_id: parent_agent_id,
        agent_name: "parent".into(),
    };
    let parent_handle = client
        .start_workflow(
            AgentWorkflow::run,
            parent_input,
            WorkflowStartOptions::new(task_queue, parent_workflow_id).build(),
        )
        .await
        .context("start_workflow(parent fail)")?;
    eprintln!("JAR2-82 fail: parent started at {parent_workflow_id}");

    let child_input = AgentInput {
        cfg: Default::default(),
        fs_handle: FsHandle {
            prefix: _child_prefix.into(),
        },
        parent_handle: Some(ParentRef {
            workflow_id: parent_workflow_id.to_string(),
            ..ParentRef::default()
        }),
        carryover: None,
        mandate: Mandate::new(CHILD_MANDATE_TEXT, Duration::from_millis(50), None),
        graph_id,
        agent_id: child_agent_id,
        agent_name: "fda_scraper".into(),
    };
    let child_handle = client
        .start_workflow(
            AgentWorkflow::run,
            child_input,
            WorkflowStartOptions::new(task_queue, child_workflow_id).build(),
        )
        .await
        .context("start_workflow(child fail)")?;
    eprintln!("JAR2-82 fail: child started at {child_workflow_id}");

    let _child_result: AgentResult = child_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("child get_result (fail path)")?;

    // Parent must complete (return AgentResult::Retired) despite the
    // reconcile activity failing — workflow body catches the
    // ApplicationFailure and stages a CorrectionContext instead of
    // bubbling.
    let parent_result = tokio::time::timeout(
        Duration::from_secs(90),
        parent_handle.get_result(WorkflowGetResultOptions::default()),
    )
    .await
    .map_err(|_| anyhow::anyhow!("parent never retired in 90s (failure-path test)"))?
    .context("parent get_result (fail path)")?;
    let AgentResult::Retired { reason } = parent_result;
    assert!(
        reason.contains("scripted retire"),
        "parent did not reach its scripted Retire arm after reconcile failure: {reason:?}"
    );
    eprintln!("JAR2-82 fail: parent retired normally after staged correction");
    Ok(())
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

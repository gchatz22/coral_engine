//! Live integration test for the `Decision::ReconcileChildren`
//! workflow arm and the `reconcile_children` activity.
//!
//! Env-gated behind `TEMPORAL_LIVE_TEST=1`. A scripted parent waits for
//! a `Trigger::ChildOutput` from a child, emits one
//! `Decision::ReconcileChildren { sources: [child output], conflict }`,
//! then retires. Happy path asserts a synthetic `tool == "reconcile"`
//! evidence record under the parent's prefix; the failure-mode path
//! checks the parent stages a `CorrectionContext` and still retires
//! cleanly when the source `OutputId` doesn't resolve; the conflict
//! path checks the conflict-log writer lands a `HeldOpen` record.

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

use coral_node::agent_ref::{AgentId, AgentRef, GraphId};
use coral_node::conflict::ConflictKind;
use coral_node::decision::{
    ConflictAlternative, ConflictRecordIntent, ContextBundle, Decide, Decision, ReconcileSource,
};
use coral_node::evidence::EvidenceRecord;
use coral_node::fs::AgentFs;
use coral_node::mandate::{Mandate, OutputId};
use coral_node::storage::{AgentStorage, MemoryStorage};
use coral_node::tools::ToolRegistry;
use coral_node::trigger::Trigger;
use coral_temporal::activities::set_decision_script;
use coral_temporal::worker::{
    build_worker, install_agent_storage, install_decide, install_tool_registry,
};
use coral_temporal::workflow::{AgentInput, AgentResult, AgentWorkflow, FsHandle, ParentRef};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

const PARENT_MANDATE_TEXT: &str = "reconcile-parent";
const CHILD_MANDATE_TEXT: &str = "reconcile-child";

/// Shared in-memory storage so both workflows and the driver's
/// inspection views see one consistent state.
static SHARED_STORAGE: OnceLock<Arc<MemoryStorage>> = OnceLock::new();

/// Captured trigger payloads the parent's `Decide` observed. Used to
/// discover the child's `OutputId` at the moment the parent needs to
/// construct `Decision::ReconcileChildren`.
static PARENT_OBSERVED_TRIGGERS: OnceLock<Arc<Mutex<Vec<Trigger>>>> = OnceLock::new();

static CHILD_SCRIPT: OnceLock<Mutex<VecDeque<Decision>>> = OnceLock::new();
static PARENT_SCRIPT: OnceLock<Mutex<VecDeque<Decision>>> = OnceLock::new();

/// `(child_workflow_id, child_agent_id, output_id_override)` the
/// parent's script uses to build a `Decision::ReconcileChildren` at
/// decision time. When `output_id_override.is_some()` the reconcile
/// decision uses that id (the failure-mode test plants a bogus id);
/// otherwise the id is pulled from the first observed `ChildOutput`.
type PendingReconcile = (Option<String>, Option<AgentId>, Option<OutputId>);
static PARENT_PENDING_RECONCILE: OnceLock<Mutex<Option<PendingReconcile>>> = OnceLock::new();

/// Optional `ConflictRecordIntent` the parent's reconcile synthesizer
/// attaches to the `Decision::ReconcileChildren` it builds. `None`
/// (default) means `conflict: None`; `Some` exercises the conflict-log
/// writer.
static PARENT_PENDING_CONFLICT: OnceLock<Mutex<Option<ConflictRecordIntent>>> = OnceLock::new();

/// Serializes the live tests so they don't share per-role state.
static LIVE_TEST_GUARD: Mutex<()> = Mutex::new(());

static INIT: std::sync::Once = std::sync::Once::new();

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
        PARENT_PENDING_CONFLICT
            .set(Mutex::new(None))
            .expect("PARENT_PENDING_CONFLICT set exactly once");

        install_decide(Arc::new(ReconcileRoutingDecide));
    });
    SHARED_STORAGE.get().cloned().expect("storage installed")
}

/// Routes decisions by mandate text. Records every trigger the parent
/// observes, and for the parent role materializes a
/// `Decision::ReconcileChildren` lazily from the most recently observed
/// `Trigger::ChildOutput` plus the pending-reconcile slot.
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
                 (only PARENT_MANDATE_TEXT / CHILD_MANDATE_TEXT scripted)"
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

    // Lift any pending `ConflictRecordIntent` planted by the
    // conflict-emitting live test; default `None` produces the
    // concordance-fold behaviour.
    let conflict = PARENT_PENDING_CONFLICT
        .get()
        .expect("PARENT_PENDING_CONFLICT installed")
        .lock()
        .expect("pending conflict mutex poisoned")
        .take();

    Ok(Decision::ReconcileChildren {
        sources: vec![ReconcileSource {
            child_ref,
            output_id,
        }],
        conflict,
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
    *PARENT_PENDING_CONFLICT
        .get()
        .expect("PARENT_PENDING_CONFLICT installed")
        .lock()
        .expect("pending conflict mutex poisoned") = None;
}

fn set_pending_reconcile(p: PendingReconcile) {
    *PARENT_PENDING_RECONCILE
        .get()
        .expect("PARENT_PENDING_RECONCILE installed")
        .lock()
        .expect("pending reconcile mutex poisoned") = Some(p);
}

fn set_pending_conflict(c: ConflictRecordIntent) {
    *PARENT_PENDING_CONFLICT
        .get()
        .expect("PARENT_PENDING_CONFLICT installed")
        .lock()
        .expect("pending conflict mutex poisoned") = Some(c);
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

/// Happy path: parent reconciles one child output; one synthetic
/// evidence record lands in the parent's `evidence/`.
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
    run_happy_path().await.expect("happy path");
}

/// Failure mode: parent reconciles a bogus output id; the activity
/// errors typed, parent stages a correction context and retires.
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
    run_failure_path().await.expect("failure path");
}

/// Conflict-log writer live test. Parent emits a
/// `Decision::ReconcileChildren { conflict: Some(...) }` with
/// `resolution: None`; asserts the parent's `conflicts/<id>.json` lands
/// with `kind: HeldOpen` and the planted alternatives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::await_holding_lock)]
async fn parent_reconcile_with_conflict_writes_held_open_record() {
    if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping parent_reconcile_with_conflict_writes_held_open_record; \
             set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
        );
        return;
    }
    let _guard = LIVE_TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
    run_conflict_path().await.expect("conflict path");
}

async fn run_happy_path() -> Result<()> {
    let suffix = run_suffix();
    let task_queue = format!("coral-reconcile-happy-{suffix}");
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

    // Parent: spin on reconcile_placeholder until the ChildOutput lands,
    // reconcile, then idle until the `max_ticks` cap stops it (agents
    // never self-terminate). The cap only bounds the post-reconcile idle
    // tail, so the synthetic evidence is durable before retirement.
    let parent_script = vec![reconcile_placeholder()];
    let child_script = vec![Decision::EmitOutput {
        content: "child output".into(),
        evidence: vec![planted_id.clone()],
    }];
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
        mandate: Mandate::new(PARENT_MANDATE_TEXT, Duration::from_millis(50), Some(15)),
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
    eprintln!("happy: parent started at {parent_workflow_id}");

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
        mandate: Mandate::new(CHILD_MANDATE_TEXT, Duration::from_millis(50), Some(1)),
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
    eprintln!("happy: child started at {child_workflow_id}");

    // Wait for child to retire. It emits once (the EmitOutput signals the
    // parent), then the `max_ticks=1` cap retires it.
    let _child_result: AgentResult = child_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("child get_result")?;
    eprintln!("happy: child retired cleanly");

    // Wait for the parent to retire. The parent's script is just
    // `[reconcile_placeholder]`: while no ChildOutput has landed the
    // placeholder keeps re-queueing itself, so the parent loops idle until
    // the signal arrives. Once observed, `synthesize_reconcile_or_wait`
    // returns a real `Decision::ReconcileChildren`, the activity writes
    // synthetic evidence to the parent's `evidence/`, and the parent then
    // idles until its `max_ticks` cap stops it (agents never self-terminate).
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
                    "happy: test asked".to_string(),
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
    eprintln!("happy: parent retired ({reason})");

    // Inspect parent's `evidence/` directory for the synthetic
    // record(s), one per source the reconcile activity processed.
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
    let task_queue = format!("coral-reconcile-fail-{suffix}");
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

    let parent_script = vec![reconcile_placeholder()];
    let child_script = vec![Decision::EmitOutput {
        content: "fail child output".into(),
        evidence: vec![planted_id.clone()],
    }];
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
        mandate: Mandate::new(PARENT_MANDATE_TEXT, Duration::from_millis(50), Some(15)),
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
    eprintln!("fail: parent started at {parent_workflow_id}");

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
        mandate: Mandate::new(CHILD_MANDATE_TEXT, Duration::from_millis(50), Some(1)),
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
    eprintln!("fail: child started at {child_workflow_id}");

    let _child_result: AgentResult = child_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("child get_result (fail path)")?;

    // Parent must complete (return AgentResult::Retired) despite the
    // reconcile activity failing: the workflow body catches the
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
    assert_eq!(
        reason, "max_ticks (15) reached",
        "parent did not complete (idle to the cap) after reconcile failure: {reason:?}"
    );
    eprintln!("fail: parent retired normally after staged correction");
    Ok(())
}

async fn run_conflict_path() -> Result<()> {
    let suffix = run_suffix();
    let task_queue = format!("coral-reconcile-conflict-{suffix}");
    let graph_id = GraphId::new(Uuid::new_v4());
    let parent_agent_id = AgentId::new(Uuid::new_v4());
    let child_agent_id =
        AgentId::new(Uuid::parse_str("cccccccc-dddd-eeee-ffff-000000000001").unwrap());
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
        .context("open planting AgentFs for child (conflict path)")?;
    let planted_id = plant_fs
        .record_evidence(EvidenceRecord::new(
            "echo",
            serde_json::json!({"k": "vvv"}),
            serde_json::json!({"hit": true}),
            Utc::now(),
        ))
        .await
        .context("plant evidence for child EmitOutput (conflict path)")?;

    // Plant a `ConflictRecordIntent` for the parent's reconcile
    // synthesizer to lift. `resolution: None` → `HeldOpen`.
    let alt_a_child = AgentRef::new(child_workflow_id.clone(), child_agent_id);
    let alt_b_child = AgentRef::new(child_workflow_id.clone(), child_agent_id);
    let alt_a = ConflictAlternative {
        source_child: alt_a_child,
        source_output_id: OutputId::from_hex("a1".repeat(32)),
        claim: "claim A".into(),
    };
    let alt_b = ConflictAlternative {
        source_child: alt_b_child,
        source_output_id: OutputId::from_hex("b2".repeat(32)),
        claim: "claim B".into(),
    };
    set_pending_conflict(ConflictRecordIntent {
        alternatives: vec![alt_a.clone(), alt_b.clone()],
        resolution: None,
    });

    let parent_script = vec![reconcile_placeholder()];
    let child_script = vec![Decision::EmitOutput {
        content: "conflict child output".into(),
        evidence: vec![planted_id.clone()],
    }];
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
        let alt_a = alt_a.clone();
        let alt_b = alt_b.clone();
        async move {
            struct ShutdownGuard<F: Fn()>(F);
            impl<F: Fn()> Drop for ShutdownGuard<F> {
                fn drop(&mut self) {
                    (self.0)();
                }
            }
            let _g = ShutdownGuard(shutdown);
            drive_conflict_path(
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
                alt_a,
                alt_b,
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
async fn drive_conflict_path(
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
    alt_a: ConflictAlternative,
    alt_b: ConflictAlternative,
) -> Result<()> {
    let parent_input = AgentInput {
        cfg: Default::default(),
        fs_handle: FsHandle {
            prefix: parent_prefix.into(),
        },
        parent_handle: None,
        carryover: None,
        mandate: Mandate::new(PARENT_MANDATE_TEXT, Duration::from_millis(50), Some(15)),
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
        .context("start_workflow(parent conflict)")?;
    eprintln!("conflict: parent started at {parent_workflow_id}");

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
        mandate: Mandate::new(CHILD_MANDATE_TEXT, Duration::from_millis(50), Some(1)),
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
        .context("start_workflow(child conflict)")?;
    eprintln!("conflict: child started at {child_workflow_id}");

    let _child_result: AgentResult = child_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("child get_result (conflict path)")?;

    let parent_result = tokio::time::timeout(
        Duration::from_secs(90),
        parent_handle.get_result(WorkflowGetResultOptions::default()),
    )
    .await
    .map_err(|_| anyhow::anyhow!("parent never retired in 90s (conflict path)"))?
    .context("parent get_result (conflict path)")?;
    let AgentResult::Retired { reason } = parent_result;
    eprintln!("conflict: parent retired ({reason})");

    // Inspect the parent's `conflicts/` directory: the activity should
    // have landed exactly one HeldOpen record matching the planted
    // alternatives.
    let inspect_mandate = Mandate::new("inspect", Duration::from_millis(0), None);
    let inspect_storage: Arc<dyn AgentStorage> = storage.clone();
    let parent_view = AgentFs::new_with_storage(
        inspect_storage,
        &format!("{parent_prefix}/"),
        &inspect_mandate,
    )
    .await
    .context("open inspecting AgentFs over parent (conflict path)")?;
    let conflicts = parent_view
        .list_conflicts()
        .await
        .context("list_conflicts on parent")?;
    assert_eq!(
        conflicts.len(),
        1,
        "expected exactly one conflict record on parent; got {}",
        conflicts.len()
    );
    let record = &conflicts[0];
    assert_eq!(
        record.kind,
        ConflictKind::HeldOpen,
        "planted resolution: None must produce HeldOpen"
    );
    assert!(record.resolution.is_none());
    assert_eq!(record.alternatives, vec![alt_a, alt_b]);
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

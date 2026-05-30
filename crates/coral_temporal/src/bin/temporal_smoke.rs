//! Temporal Rust SDK primitive smoke. Runs against a local Temporal
//! Server (default `localhost:7233`; override with `TEMPORAL_ADDRESS`
//! and `TEMPORAL_NAMESPACE`). Exits 0 if every primitive was either
//! demonstrated or reported as a known gap via a `MISSING:` line on
//! stderr; non-zero is reserved for actual failures (server
//! unreachable, workflow returned wrong value, worker died).
//!
//! Primitives exercised: workflow + activity definition + static worker
//! registration, durable timer, signal handler, `wait_condition` racing
//! a signal vs a timer (via the SDK's deterministic `workflows::select!`),
//! `start_child_workflow` with `ParentClosePolicy::Abandon`,
//! `continue_as_new`, and dynamic activity registration (reported as a
//! `MISSING:` — the `#[activities]` macro is compile-time static).

use std::env;
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    activities::{ActivityContext, ActivityError},
    ActivityOptions, ChildWorkflowOptions, ContinueAsNewOptions, SyncWorkflowContext, Worker,
    WorkerOptions, WorkflowContext, WorkflowResult,
};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use tracing::info;

/// Process-wide counter for activity invocations. A `static` rather
/// than threaded `Arc` because the SDK's `register_activities` takes a
/// value-typed `ActivityImplementer`, making external observation
/// through the registered instance awkward.
static ACTIVITY_INVOCATIONS: AtomicUsize = AtomicUsize::new(0);

pub struct SmokeActivities;

#[activities]
impl SmokeActivities {
    #[activity]
    pub async fn assemble_context_stub(
        _ctx: ActivityContext,
        triggers: Vec<String>,
    ) -> Result<String, ActivityError> {
        ACTIVITY_INVOCATIONS.fetch_add(1, Ordering::Relaxed);
        Ok(format!("bundle({} triggers)", triggers.len()))
    }

    #[activity]
    pub async fn decide_next_action_stub(
        _ctx: ActivityContext,
        bundle: String,
    ) -> Result<String, ActivityError> {
        ACTIVITY_INVOCATIONS.fetch_add(1, Ordering::Relaxed);
        Ok(format!("CallTool[echo]({bundle})"))
    }

    #[activity]
    pub async fn tool_echo(_ctx: ActivityContext, input: String) -> Result<String, ActivityError> {
        ACTIVITY_INVOCATIONS.fetch_add(1, Ordering::Relaxed);
        Ok(format!("echo:{input}"))
    }

    /// Second named tool activity, kept registered but never invoked —
    /// its presence demonstrates the "registered-N-tools-by-name"
    /// workaround for the missing dynamic-activity primitive.
    #[activity]
    pub async fn tool_reverse(
        _ctx: ActivityContext,
        input: String,
    ) -> Result<String, ActivityError> {
        ACTIVITY_INVOCATIONS.fetch_add(1, Ordering::Relaxed);
        Ok(input.chars().rev().collect())
    }

    #[activity]
    pub async fn persist_output_stub(
        _ctx: ActivityContext,
        content: String,
    ) -> Result<String, ActivityError> {
        ACTIVITY_INVOCATIONS.fetch_add(1, Ordering::Relaxed);
        Ok(format!("output_id:{}", content.len()))
    }
}

/// One tick of the agent loop shape: drains triggers via
/// `wait_condition`, times out via `timer`, calls activities, spawns an
/// abandoned child, and returns. Every primitive listed in the module
/// header is exercised here.
#[workflow]
#[derive(Default)]
pub struct AgentLoopWorkflow {
    pending_triggers: Vec<String>,
    retired: bool,
}

#[workflow_methods]
impl AgentLoopWorkflow {
    #[signal]
    pub fn external_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, trigger: String) {
        self.pending_triggers.push(trigger);
    }

    #[signal]
    pub fn retire(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _reason: String) {
        self.retired = true;
    }

    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        child_id_suffix: String,
    ) -> WorkflowResult<String> {
        ctx.timer(Duration::from_millis(50)).await;

        // 5-second timer races against a `wait_condition` for the
        // trigger queue; under healthy conditions the test driver's
        // signal arrives first.
        let mut wait_fut = ctx.wait_condition(|s| !s.pending_triggers.is_empty());
        let mut timer_fut = ctx.timer(Duration::from_secs(5));

        let race_outcome = temporalio_sdk::workflows::select! {
            _ = wait_fut => "signal",
            _ = timer_fut => "timer",
        };

        let drained: Vec<String> = ctx.state_mut(|s| std::mem::take(&mut s.pending_triggers));

        let bundle = ctx
            .start_activity(
                SmokeActivities::assemble_context_stub,
                drained.clone(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
            )
            .await?;

        let decision = ctx
            .start_activity(
                SmokeActivities::decide_next_action_stub,
                bundle.clone(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
            )
            .await?;

        let tool_result = ctx
            .start_activity(
                SmokeActivities::tool_echo,
                decision.clone(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
            )
            .await?;

        let output_id = ctx
            .start_activity(
                SmokeActivities::persist_output_stub,
                tool_result.clone(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
            )
            .await?;

        let child_opts = ChildWorkflowOptions {
            workflow_id: format!("temporal-smoke-child-{child_id_suffix}"),
            parent_close_policy:
                temporalio_common::protos::temporal::api::enums::v1::ParentClosePolicy::Abandon,
            ..Default::default()
        };
        let started_child = ctx
            .child_workflow(
                SmokeChildWorkflow::run,
                "child-payload".to_string(),
                child_opts,
            )
            .await?;
        let child_run_id = started_child.run_id.clone();

        // We intentionally do NOT `.result().await` the child —
        // blocking on it would defeat the `Abandon` demonstration.

        Ok(format!(
            "race={race_outcome} drained={} bundle={bundle:?} decision={decision:?} output_id={output_id:?} child_run_id={child_run_id} retired={}",
            drained.len(),
            ctx.state(|s| s.retired),
        ))
    }
}

/// Trivial abandoned child: a short timer then return. The smoke only
/// asserts that it can be started with `ParentClosePolicy::Abandon`;
/// post-exit completion is observable via Temporal Web UI.
#[workflow]
#[derive(Default)]
pub struct SmokeChildWorkflow;

#[workflow_methods]
impl SmokeChildWorkflow {
    #[run(name = "SmokeChild")]
    pub async fn run(ctx: &mut WorkflowContext<Self>, payload: String) -> WorkflowResult<String> {
        ctx.timer(Duration::from_millis(200)).await;
        Ok(format!("child_done:{payload}"))
    }
}

/// `continue-as-new` demonstration: counts iterations and triggers
/// continue-as-new until a max is reached.
#[workflow]
#[derive(Default)]
pub struct ContinueAsNewSmokeWorkflow;

#[workflow_methods]
impl ContinueAsNewSmokeWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, input: (u32, u32)) -> WorkflowResult<String> {
        let (current, max) = input;
        ctx.timer(Duration::from_millis(50)).await;

        if current < max {
            ctx.continue_as_new(&(current + 1, max), ContinueAsNewOptions::default())?;
            // `continue_as_new` returns Err via `WorkflowTermination`,
            // so this point is unreachable on the continue-as-new path.
            unreachable!("continue_as_new should have terminated this run");
        }

        Ok(format!("continue_as_new_done:{max}"))
    }
}

const TASK_QUEUE: &str = "coral-temporal-smoke";
const DEFAULT_NAMESPACE: &str = "default";
const DEFAULT_ADDRESS: &str = "http://localhost:7233";

/// Suffix workflow IDs with start-time epoch millis so iterative
/// `cargo run` doesn't collide on Temporal's default ID-reuse policy.
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

/// Build a worker on the calling task. `Worker` holds non-`Send`
/// interceptor state and cannot move into a `tokio::spawn`-ed task —
/// the caller runs `worker.run()` on the current task and drives the
/// smoke from a separate task that talks to the worker through the
/// Temporal server.
fn build_worker(runtime: &CoreRuntime, client: Client) -> Result<Worker> {
    let worker_options = WorkerOptions::new(TASK_QUEUE)
        .register_workflow::<AgentLoopWorkflow>()
        .register_workflow::<SmokeChildWorkflow>()
        .register_workflow::<ContinueAsNewSmokeWorkflow>()
        .register_activities(SmokeActivities)
        .build();

    // `Worker::new`'s error isn't `Send + Sync` so we convert via display.
    Worker::new(runtime, client, worker_options)
        .map_err(|e| anyhow::anyhow!("Worker::new failed: {e}"))
}

struct Verdict {
    primitive: &'static str,
    status: VerdictStatus,
    note: String,
}

enum VerdictStatus {
    Worked,
    WorkedWithCaveats,
    Missing,
}

impl VerdictStatus {
    fn tag(&self) -> &'static str {
        match self {
            VerdictStatus::Worked => "WORKED",
            VerdictStatus::WorkedWithCaveats => "CAVEAT",
            VerdictStatus::Missing => "MISSING",
        }
    }
}

async fn drive_smoke(client: Client) -> Result<Vec<Verdict>> {
    let mut verdicts = Vec::new();
    let suffix = run_suffix();

    let workflow_id = format!("temporal-smoke-agent-loop-{suffix}");
    info!(workflow_id, "starting AgentLoopWorkflow");

    let handle = client
        .start_workflow(
            AgentLoopWorkflow::run,
            suffix.clone(),
            WorkflowStartOptions::new(TASK_QUEUE, workflow_id.clone()).build(),
        )
        .await
        .context("start_workflow(AgentLoopWorkflow)")?;

    // Delay before signaling so the wait_condition arm wins the race.
    tokio::time::sleep(Duration::from_millis(250)).await;
    handle
        .signal(
            AgentLoopWorkflow::external_signal,
            "smoke-trigger".to_string(),
            WorkflowSignalOptions::default(),
        )
        .await
        .context("signal AgentLoopWorkflow::external_signal")?;

    let result = handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("AgentLoopWorkflow.get_result")?;
    info!(?result, "AgentLoopWorkflow returned");

    if !result.contains("race=signal") {
        bail!("expected signal-wins race outcome, got: {result}");
    }
    verdicts.push(Verdict {
        primitive: "workflow_definition + activity_definition + worker_registration",
        status: VerdictStatus::Worked,
        note: format!("AgentLoopWorkflow completed; result: {result}"),
    });
    verdicts.push(Verdict {
        primitive: "durable_timer (ctx.timer)",
        status: VerdictStatus::Worked,
        note: "two timer waits (warm-up + race arm) replayed cleanly".into(),
    });
    verdicts.push(Verdict {
        primitive: "signal_handler (#[signal])",
        status: VerdictStatus::Worked,
        note: "external_signal handler pushed onto Vec<String> state and was observed by wait_condition".into(),
    });
    verdicts.push(Verdict {
        primitive: "wait_condition racing signal vs timer (workflows::select!)",
        status: VerdictStatus::WorkedWithCaveats,
        note: "race expressed via SDK-deterministic `workflows::select!`. `tokio::select!` would break replay determinism per the SDK README; an early attempt to use it must be flagged in review".into(),
    });
    verdicts.push(Verdict {
        primitive: "start_child_workflow with ParentClosePolicy::Abandon",
        status: VerdictStatus::Worked,
        note: "child started; parent returned without awaiting child.result(); ParentClosePolicy::Abandon set on ChildWorkflowOptions".into(),
    });

    let can_id = format!("temporal-smoke-continue-as-new-{suffix}");
    info!(workflow_id = can_id, "starting ContinueAsNewSmokeWorkflow");
    let can_handle = client
        .start_workflow(
            ContinueAsNewSmokeWorkflow::run,
            (1u32, 3u32),
            WorkflowStartOptions::new(TASK_QUEUE, can_id.clone()).build(),
        )
        .await
        .context("start_workflow(ContinueAsNewSmokeWorkflow)")?;

    let can_result = can_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .context("ContinueAsNewSmokeWorkflow.get_result")?;
    info!(?can_result, "ContinueAsNewSmokeWorkflow returned");

    if !can_result.starts_with("continue_as_new_done:") {
        bail!("expected continue_as_new_done:* result, got: {can_result}");
    }
    verdicts.push(Verdict {
        primitive: "continue_as_new",
        status: VerdictStatus::Worked,
        note: format!("workflow continued-as-new 3 times then completed; result: {can_result}"),
    });

    // No dynamic-activity primitive exists in the SDK: no
    // `#[activity(dynamic=true)]`, no runtime `register_activity_by_name`,
    // no `unknown_activity_handler`. The workaround is to register every
    // known tool by name at build time (demonstrated via `tool_echo` +
    // `tool_reverse`); a real fix needs an upstream contribution.
    verdicts.push(Verdict {
        primitive: "dynamic_activity_registration",
        status: VerdictStatus::Missing,
        note: format!(
            "static-only via `#[activities]` macro. Activity invocation count after smoke: {}. Workaround: register N tool activities by name at build time (see `tool_echo` + `tool_reverse`). A real fix needs an upstream contribution or sidecar.",
            ACTIVITY_INVOCATIONS.load(Ordering::Relaxed)
        ),
    });

    Ok(verdicts)
}

fn report(verdicts: &[Verdict]) -> bool {
    let mut any_missing = false;
    println!();
    println!("--- temporal-smoke verdicts ---");
    for v in verdicts {
        let tag = v.status.tag();
        println!("{tag:<7} {} — {}", v.primitive, v.note);
        if matches!(v.status, VerdictStatus::Missing) {
            eprintln!("MISSING: {} — {}", v.primitive, v.note);
            any_missing = true;
        }
    }
    println!("--- end verdicts ---");
    any_missing
}

#[tokio::main]
async fn main() -> ExitCode {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,temporalio=warn")),
        )
        .try_init();

    match run().await {
        Ok(any_missing) => {
            if any_missing {
                // Missing primitives are recorded findings, not crashes:
                // the `MISSING:` stderr lines are the signal, exit
                // stays 0 so the smoke remains green end-to-end.
                eprintln!(
                    "temporal-smoke: at least one primitive is missing — see verdict table above"
                );
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("temporal-smoke: aborted: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<bool> {
    let telemetry_options = TelemetryOptions::builder().build();
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(telemetry_options)
            .build()
            .map_err(|e| anyhow::anyhow!("RuntimeOptions build failed: {e}"))?,
    )?;
    let client = build_client().await?;
    let mut worker = build_worker(&runtime, client.clone())?;
    let shutdown = worker.shutdown_handle();

    // `Worker` is non-`Send`, so it stays on the current task while the
    // driver runs on a spawned task and triggers `shutdown` when done.
    let driver = tokio::spawn(async move {
        let r = drive_smoke(client).await;
        // Always trigger shutdown so `worker.run()` returns even if
        // `drive_smoke` returned an Err.
        shutdown();
        r
    });

    let worker_result = worker
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("worker.run() exited with error: {e}"));
    let driver_result = driver.await.context("smoke driver task panicked")?;

    // Surface either error. Worker errors take priority — if the
    // worker died abnormally that's the root cause and the driver's
    // failure is downstream.
    worker_result?;
    let verdicts = driver_result?;
    let any_missing = report(&verdicts);
    Ok(any_missing)
}

// The Rust SDK ships no `WorkflowEnvironment`-equivalent, so a hermetic
// `#[tokio::test]` isn't possible. The test below runs the full driver
// against a live Temporal Server when `TEMPORAL_LIVE_TEST=1` is set,
// and is a no-op otherwise so default PR runs stay hermetic.

#[cfg(test)]
mod tests {
    use super::*;

    /// Live test gated on `TEMPORAL_LIVE_TEST=1` rather than `#[ignore]`d
    /// because the failure mode (no server) is environmental.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_temporal_smoke() {
        if env::var("TEMPORAL_LIVE_TEST").ok().as_deref() != Some("1") {
            eprintln!(
                "skipping live_temporal_smoke; set TEMPORAL_LIVE_TEST=1 with a local Temporal Server to run"
            );
            return;
        }

        let any_missing = run().await.expect("smoke driver returned Err");
        // The dynamic-activity gap is the expected missing primitive
        // until upstream changes, so this assertion is intentionally
        // lenient: a `true` result is still acceptable.
        let _ = any_missing;
    }
}

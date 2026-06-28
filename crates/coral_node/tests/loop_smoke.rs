//! Integration tests for the `Agent` run loop.
//!
//! Exercises the public surface only (`Agent::new`, `Agent::signal`,
//! `Agent::run`) plus the FS root the agent writes to: signal/deadline
//! wakeups; the `Read`/`List`/`Search`/`WriteOutput`/`RewriteFs`/`CallTool`
//! repertoire steps; the inner ReAct cycle (multiple steps, terminated by
//! `Idle`); `step_cap` (now counted in *cycles*); in-cycle failure
//! adaptation; health budget exhaustion + recovery across cycles.
//!
//! Cycle model: each `Agent::run` outer iteration is one *cycle*. The model
//! drives an inner loop of steps via `MockDecide` until it returns `Idle`
//! (the sole terminal). So a MockDecide script is a sequence of *steps*,
//! and a multi-step cycle ends at the next `Idle`. `step_cap` bounds cycles.
//!
//! Time-sensitive tests use `#[tokio::test(flavor = "current_thread",
//! start_paused = true)]` so the runtime auto-advances when the only
//! pending task is a sleep, making the deadline-arm tests deterministic.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;

use coral_node::agent::{Agent, RetireReason};
use coral_node::decision::{ClaimSeed, Decide, Decision, FsOp, MockDecide, Session, ToolCall};
use coral_node::evidence::{EvidenceId, EvidenceRecord};
use coral_node::fs::AgentFs;
use coral_node::health::{HealthTracker, RetryBudget};
use coral_node::mandate::Mandate;
use coral_node::tools::{EchoTool, Tool, ToolRegistry};
use coral_node::trigger::Trigger;

async fn fresh_fs(idle_period: Duration) -> (TempDir, AgentFs, Mandate) {
    let tmp = TempDir::new().expect("tempdir");
    let mandate = Mandate::new("loop smoke", idle_period, None);
    let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
        .await
        .expect("open fs");
    (tmp, fs, mandate)
}

/// Build a `HealthTracker` rooted at `root` with the default budget.
/// Tests that need to drive exhaustion override the budget directly via
/// `fresh_health_with`.
fn fresh_health(root: &Path) -> HealthTracker {
    HealthTracker::open(root, RetryBudget::default(), Utc::now()).expect("open health")
}

fn fresh_health_with(root: &Path, budget: RetryBudget) -> HealthTracker {
    HealthTracker::open(root, budget, Utc::now()).expect("open health")
}

fn registry_with_echo() -> ToolRegistry {
    let mut r = ToolRegistry::new();
    r.register(Arc::new(EchoTool)).expect("register echo");
    r
}

/// A short idle so paused-time auto-advance drives the deadline arm, and a
/// terminal `Idle` step the model returns to end a cycle.
fn idle() -> Decision {
    Decision::Idle {
        next_after: Duration::from_millis(50),
    }
}

/// Read `dir` and return the files that are *agent record files* —
/// excluding the tail-index sidecar (`_tail.json`). Returns an empty
/// Vec for a missing dir to match `AgentFs::open`'s lazy
/// directory-creation behavior. Used by assertions that count
/// "how many outputs/evidence files were written"; the tail file is
/// an index artefact, not a record.
fn agent_record_files(dir: &Path) -> Vec<PathBuf> {
    if !dir.exists() {
        return Vec::new();
    }
    std::fs::read_dir(dir)
        .expect("read agent record dir")
        .map(|e| e.expect("dirent").path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n != "_tail.json")
                .unwrap_or(true)
        })
        .collect()
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn loop_wakes_on_injected_signal_and_runs() {
    // Idle period is huge so the test relies on the signal, not the
    // scheduled wake, to drive the first cycle.
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_secs(3600)).await;
    mandate.step_cap = Some(1);
    let script = vec![Decision::Idle {
        next_after: Duration::from_secs(3600),
    }];
    let agent = Agent::new(
        mandate,
        fs,
        MockDecide::new(script),
        registry_with_echo(),
        fresh_health(tmp.path()),
    );
    let sink = agent.signal();

    let handle = tokio::spawn(agent.run());

    // Push an external trigger; the loop's `wait_nonempty` arm should fire
    // long before the 1-hour deadline.
    sink.send(Trigger::External {
        kind: "test".into(),
        payload: json!({"hello": "world"}),
    })
    .expect("send");

    // Bound the wait to 5s of (paused) virtual time so a regression that
    // misses the signal arm fails loudly instead of hanging.
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "step_cap (1) reached");
    assert!(tmp.path().join("retirement.json").is_file());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn loop_wakes_on_deadline_when_no_signal_arrives() {
    // Short idle period so the deadline-arm fires with paused-time auto-
    // advance. The cycle idles immediately; if it ran, the deadline fired,
    // we drained a `ScheduledWake`, and decide returned. The `step_cap`
    // cap stops the loop after that single deadline-driven cycle.
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_millis(50)).await;
    mandate.step_cap = Some(1);
    let script = vec![idle()];
    let agent = Agent::new(
        mandate,
        fs,
        MockDecide::new(script),
        registry_with_echo(),
        fresh_health(tmp.path()),
    );

    let handle = tokio::spawn(agent.run());

    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "step_cap (1) reached");
    assert!(tmp.path().join("retirement.json").is_file());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn write_output_with_valid_evidence_writes_canonical_output() {
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_millis(50)).await;
    mandate.step_cap = Some(1);
    // Pre-seed an evidence record on disk so the WriteOutput's citation id
    // resolves. Computing the id here keeps the test independent of how
    // the agent would have produced it.
    let rec = EvidenceRecord::new(
        "echo",
        json!({"msg": "hi"}),
        json!({"echoed": {"msg": "hi"}}),
        chrono::Utc::now(),
    );
    let ev_id: EvidenceId = fs.record_evidence(rec).await.expect("seed evidence");

    // One cycle: write the output, then idle (the terminal step).
    let script = vec![
        Decision::WriteOutput {
            body: "the answer".into(),
            citations: vec![ev_id.clone()],
        },
        idle(),
    ];
    let agent = Agent::new(
        mandate,
        fs,
        MockDecide::new(script),
        registry_with_echo(),
        fresh_health(tmp.path()),
    );

    let handle = tokio::spawn(agent.run());
    let _ = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");

    // The single canonical Output landed at the stable path with the body.
    let output_path = tmp.path().join("outputs").join("output.md");
    assert!(output_path.is_file(), "expected canonical output file");
    let body = std::fs::read_to_string(&output_path).expect("read output");
    assert_eq!(body, "the answer");
}

/// A `never`-cadence agent self-wakes only its *first* cycle: it runs once
/// with no inbound trigger, then blocks on the trigger queue with no
/// self-wake timer armed. A recurring agent would instead fire a second
/// deadline-driven cycle. This pins the invariant that an all-`never` graph
/// still produces (leaves fire once) but never spins on a self-clock.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn never_cadence_fires_first_cycle_then_waits_for_triggers() {
    let tmp = TempDir::new().expect("tempdir");
    // `never` cadence (no recurring self-wake); a generous step_cap that a
    // correct `never` node never reaches because it blocks first.
    let mandate = Mandate::new_never("never leaf", Some(3));
    let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
        .await
        .expect("open fs");
    let ev_id: EvidenceId = fs
        .record_evidence(EvidenceRecord::new(
            "echo",
            json!({"msg": "hi"}),
            json!({"echoed": {"msg": "hi"}}),
            chrono::Utc::now(),
        ))
        .await
        .expect("seed evidence");
    // Two scripted cycles, each a write + idle. A correct `never` node
    // reaches only the first (it blocks before a second self-wake). A
    // self-wake regression would run the second cycle and overwrite the
    // canonical Output with "finding 2".
    let script = vec![
        Decision::WriteOutput {
            body: "finding 1".into(),
            citations: vec![ev_id.clone()],
        },
        idle(),
        Decision::WriteOutput {
            body: "finding 2".into(),
            citations: vec![ev_id],
        },
        idle(),
    ];
    let agent = Agent::new(
        mandate,
        fs,
        MockDecide::new(script),
        registry_with_echo(),
        fresh_health(tmp.path()),
    );

    // No trigger is ever pushed. The first cycle still fires (bootstrap
    // wake); the second must not (no self-wake timer for `never`), so the
    // run blocks and the timeout trips rather than retiring on step_cap.
    let result = timeout(Duration::from_secs(60), agent.run()).await;
    assert!(
        result.is_err(),
        "never node must not self-wake past its first cycle (expected block, got {result:?})"
    );

    // The canonical Output holds the first cycle's body; the second cycle
    // was unreached, so it never overwrote it with "finding 2".
    let body = std::fs::read_to_string(tmp.path().join("outputs").join("output.md"))
        .expect("read canonical output");
    assert_eq!(
        body, "finding 1",
        "never node must emit exactly its first-cycle output"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn rewrite_fs_writes_file_under_notes() {
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_millis(50)).await;
    mandate.step_cap = Some(1);
    let script = vec![
        Decision::RewriteFs {
            ops: vec![FsOp::WriteFile {
                path: "notes/scratch.md".into(),
                content: "hello from the loop".into(),
            }],
        },
        idle(),
    ];
    let agent = Agent::new(
        mandate,
        fs,
        MockDecide::new(script),
        registry_with_echo(),
        fresh_health(tmp.path()),
    );

    let handle = tokio::spawn(agent.run());
    let _ = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");

    let written = tmp.path().join("notes").join("scratch.md");
    assert_eq!(
        std::fs::read_to_string(&written).expect("read note"),
        "hello from the loop"
    );
}

/// One cycle, multiple steps: the model calls echo, then writes an output
/// citing the evidence echo produced, then idles. Exercises that `CallTools`
/// and `WriteOutput` compose inside a single cycle (what used to be two ticks
/// is now two steps of one cycle).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn call_tool_records_evidence_and_write_output_consumes_it_in_one_cycle() {
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_millis(50)).await;
    mandate.step_cap = Some(1);
    // Compute the id we expect echo to produce so we can reference it in
    // the WriteOutput step.
    let args = json!({"msg": "hi"});
    let result = json!({"echoed": {"msg": "hi"}});
    let expected_ev = EvidenceId::new("echo", &args, &result);

    let script = vec![
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "echo",
                args.clone(),
                ClaimSeed::new("seed-1"),
            )],
        },
        Decision::WriteOutput {
            body: "echoed".into(),
            citations: vec![expected_ev.clone()],
        },
        idle(),
    ];
    let agent = Agent::new(
        mandate,
        fs,
        MockDecide::new(script),
        registry_with_echo(),
        fresh_health(tmp.path()),
    );

    let handle = tokio::spawn(agent.run());
    let _ = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");

    // Evidence file written by the CallTool step.
    let ev_path = tmp
        .path()
        .join("evidence")
        .join(format!("{}.json", expected_ev));
    assert!(ev_path.is_file(), "expected evidence file at {ev_path:?}");

    // Canonical Output written by the WriteOutput step.
    let body = std::fs::read_to_string(tmp.path().join("outputs").join("output.md"))
        .expect("read canonical output");
    assert_eq!(body, "echoed");
}

/// A multi-step cycle (read → call_tool → emit → idle) runs as exactly ONE
/// cycle. `step_cap = 1` then retires after it. Confirms `step_cap` counts
/// cycles, not steps, and that a `Read` step feeds a note body back as an
/// observation the cycle proceeds from.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn multi_step_cycle_counts_as_one_cycle() {
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_millis(50)).await;
    mandate.step_cap = Some(1);
    // Pre-seed a note the Read step pulls.
    fs.apply_ops(vec![FsOp::WriteFile {
        path: "notes/plan.md".into(),
        content: "the standing plan".into(),
    }])
    .await
    .expect("seed note");
    let args = json!({"msg": "go"});
    let expected_ev = EvidenceId::new("echo", &args, &json!({"echoed": {"msg": "go"}}));

    let script = vec![
        Decision::Read {
            path: "notes/plan.md".into(),
        },
        Decision::CallTools {
            calls: vec![ToolCall::new("echo", args, ClaimSeed::new("s"))],
        },
        Decision::WriteOutput {
            body: "did the work".into(),
            citations: vec![expected_ev],
        },
        idle(),
    ];
    let agent = Agent::new(
        mandate,
        fs,
        MockDecide::new(script),
        registry_with_echo(),
        fresh_health(tmp.path()),
    );

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    // Four steps, one Idle → ONE cycle. step_cap=1 retires right after.
    assert_eq!(reason, "step_cap (1) reached");
    // The work products of the single cycle are all on disk.
    assert!(tmp.path().join("outputs").join("output.md").is_file());
    assert_eq!(agent_record_files(&tmp.path().join("evidence")).len(), 1);
    // Stayed Healthy — no failing steps.
    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(v.get("state").and_then(|x| x.as_str()), Some("Healthy"));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn step_cap_caps_cycles_and_writes_retirement() {
    // Mandate caps the loop at exactly 2 cycles. Idle period is small so
    // paused-time auto-advance drives both cycles via the deadline arm —
    // no signals needed. The script holds exactly 2 Idle steps (= 2
    // single-step cycles) and nothing else: if the cap fails to fire, the
    // loop attempts a third cycle, MockDecide returns "script exhausted",
    // and the cycle goes Unhealthy. So both behaviours are covered.
    let tmp = TempDir::new().expect("tempdir");
    let mandate = Mandate::new("max-cycles-test", Duration::from_millis(50), Some(2));
    let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
        .await
        .expect("open fs");

    let script = vec![idle(), idle()];
    let agent = Agent::new(
        mandate,
        fs,
        MockDecide::new(script),
        registry_with_echo(),
        fresh_health(tmp.path()),
    );

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");

    assert!(
        reason.contains("step_cap"),
        "expected step_cap retirement reason, got: {reason}"
    );
    assert!(
        reason.contains('2'),
        "reason should mention the cap value: {reason}"
    );
    assert!(tmp.path().join("retirement.json").is_file());
}

/// Model emits an unsatisfiable step (`CallTool` for an unregistered tool);
/// the runtime catches the apply-time failure, folds it into the session as
/// a failure observation, and the model adapts *within the same cycle* by
/// idling. The agent stays Healthy (one failure under the default budget).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn invalid_call_tool_is_recoverable_in_cycle_failure() {
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_millis(50)).await;
    mandate.step_cap = Some(1);
    let script = vec![
        // Step 1: model picks a tool that is not registered. Apply-time
        // failure → record_failure(Inference) → counter=1 (under default
        // budget of 1) → failure observation appended; the model adapts.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "no_such_tool",
                json!({"x": 1}),
                ClaimSeed::new("seed-1"),
            )],
        },
        // Step 2: the model adapts to the failure and idles, ending the
        // cycle cleanly.
        idle(),
    ];
    let agent = Agent::new(
        mandate,
        fs,
        MockDecide::new(script),
        registry_with_echo(),
        fresh_health(tmp.path()),
    );

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "step_cap (1) reached");

    // Provenance side-effect check: the bad CallTool must not have
    // produced an evidence record. The registry rejected the lookup
    // before any tool ran, so `evidence/` is either absent or empty.
    let evidence_dir = tmp.path().join("evidence");
    let entries = agent_record_files(&evidence_dir);
    assert!(
        entries.is_empty(),
        "no evidence should have been recorded for an unregistered tool"
    );

    // Health stays Healthy across the cycle — budget was consumed
    // (counter=1) but never exhausted.
    let health_path = tmp.path().join("health.json");
    let v: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&health_path).expect("read health"))
            .expect("parse health");
    assert_eq!(v.get("state").and_then(|x| x.as_str()), Some("Healthy"));
    assert!(
        !tmp.path().join("health").exists(),
        "no archive directory should be created when the agent never went Unhealthy"
    );
    assert!(tmp.path().join("retirement.json").is_file());
}

/// `WriteOutput` with an *empty* citations list is rejected by
/// `AgentFs::persist_output` with `FsError::EmptyEvidence`; that maps to a
/// recoverable in-cycle failure observation the model adapts to (here by
/// idling). Stays Healthy under the default budget.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn write_output_with_empty_evidence_is_recoverable_in_cycle() {
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_millis(50)).await;
    mandate.step_cap = Some(1);
    let script = vec![
        Decision::WriteOutput {
            body: "no provenance".into(),
            citations: vec![],
        },
        idle(),
    ];
    let agent = Agent::new(
        mandate,
        fs,
        MockDecide::new(script),
        registry_with_echo(),
        fresh_health(tmp.path()),
    );

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "step_cap (1) reached");

    // `outputs/` materialises on first write; treat absent dir as no
    // output persisted.
    let outputs_dir = tmp.path().join("outputs");
    if outputs_dir.exists() {
        assert!(std::fs::read_dir(&outputs_dir)
            .expect("read outputs")
            .next()
            .is_none());
    }

    // Stayed Healthy — single failure under the default budget.
    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(v.get("state").and_then(|x| x.as_str()), Some("Healthy"));
}

/// Same shape of failure driven through the `WriteOutput` step with a
/// well-formed-but-not-on-disk evidence id (`FsError::EvidenceNotFound`).
/// Mirrors the empty-evidence test above; the two cover the two distinct
/// `FsError` variants the in-cycle failure path catches.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn write_output_with_unknown_evidence_is_recoverable_in_cycle() {
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_millis(50)).await;
    mandate.step_cap = Some(1);
    let bogus = EvidenceId::from_hex("deadbeef".repeat(8));
    let script = vec![
        Decision::WriteOutput {
            body: "lying about provenance".into(),
            citations: vec![bogus],
        },
        idle(),
    ];
    let agent = Agent::new(
        mandate,
        fs,
        MockDecide::new(script),
        registry_with_echo(),
        fresh_health(tmp.path()),
    );

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "step_cap (1) reached");

    let outputs_dir = tmp.path().join("outputs");
    if outputs_dir.exists() {
        assert!(std::fs::read_dir(&outputs_dir)
            .expect("read outputs")
            .next()
            .is_none());
    }

    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(v.get("state").and_then(|x| x.as_str()), Some("Healthy"));
}

/// Persistent in-cycle failure exhausts the per-cycle inference budget
/// across two failing steps of the *same* cycle. The agent transitions to
/// `Unhealthy` and the cycle ends; the run loop **does not halt**, and the
/// next cycle (here, an `Idle`) recovers the tracker to `Healthy` while
/// archiving the prior incident.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn persistent_in_cycle_failure_exhausts_budget_and_recovers_next_cycle() {
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_millis(50)).await;
    mandate.step_cap = Some(2);
    // Budget `RetryBudget::new(1, 3)` → max_inference = 1, so the second
    // failing step in a cycle exhausts (counter 2 > 1).
    let script = vec![
        // Cycle 1, step 1: bad CallTool → record_failure ok (counter=1).
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "no_such_tool",
                json!({}),
                ClaimSeed::new("seed-1"),
            )],
        },
        // Cycle 1, step 2: bad CallTool again → counter=2 > max=1 →
        // BudgetExhausted → transition_to_unhealthy, cycle ends. Run loop
        // does NOT exit.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "no_such_tool",
                json!({}),
                ClaimSeed::new("seed-1"),
            )],
        },
        // Cycle 2: valid Idle → mark_tick_success → archives the Unhealthy
        // incident and flips back to Healthy. The step_cap cap then stops
        // the loop.
        idle(),
    ];
    let agent = Agent::new(
        mandate,
        fs,
        MockDecide::new(script),
        registry_with_echo(),
        fresh_health_with(tmp.path(), RetryBudget::new(1, 3)),
    );

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "step_cap (2) reached");

    // After recovery: the live `health.json` reflects Healthy state.
    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(
        v.get("state").and_then(|x| x.as_str()),
        Some("Healthy"),
        "agent must recover to Healthy after the next successful cycle"
    );
    assert!(v.get("incident").is_none() || v.get("incident").unwrap().is_null());

    // The prior Unhealthy incident must have been archived by recovery.
    let archive_dir = tmp.path().join("health");
    assert!(
        archive_dir.is_dir(),
        "archive dir should be created on recovery"
    );
    let archived: Vec<_> = std::fs::read_dir(&archive_dir)
        .expect("read archive")
        .map(|e| e.expect("dirent").path())
        .collect();
    assert_eq!(
        archived.len(),
        1,
        "exactly one archived incident expected, got: {archived:?}"
    );

    // The archived incident should describe the apply-time failure.
    let inc: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&archived[0]).expect("read archive"))
            .expect("parse archive");
    assert_eq!(
        inc.get("state").and_then(|x| x.as_str()),
        Some("Unhealthy"),
        "archived record must be the Unhealthy incident"
    );
    let failing = inc
        .get("incident")
        .and_then(|i| i.get("failing"))
        .expect("failing block");
    assert_eq!(
        failing.get("type").and_then(|x| x.as_str()),
        Some("Inference"),
        "apply-time failures count as inference failures"
    );
    let retry_trail = inc
        .get("incident")
        .and_then(|i| i.get("retry_trail"))
        .and_then(|x| x.as_array())
        .expect("retry_trail");
    assert_eq!(
        retry_trail.len(),
        2,
        "retry trail should record both failing steps before exhaustion"
    );
}

/// Decide-side `Err` (model adapter could not produce a `Decision`) is the
/// inference-retry-exhaustion signal at the cycle boundary: it transitions
/// the tracker to `Unhealthy` directly, without spending a budget slot, and
/// ends the cycle. The run loop keeps going; `step_cap` then retires.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn decide_err_transitions_to_unhealthy_and_keeps_loop_alive() {
    let tmp = TempDir::new().expect("tempdir");
    let mandate = Mandate::new("decide-err", Duration::from_millis(50), Some(1));
    let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
        .await
        .expect("open fs");

    // Empty script → `MockDecide::decide` returns `Err("script exhausted")`
    // on the first call of cycle 1. That is the Decide-Err we exercise.
    let agent = Agent::new(
        mandate,
        fs,
        MockDecide::new(vec![]),
        registry_with_echo(),
        fresh_health(tmp.path()),
    );
    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert!(
        reason.contains("step_cap"),
        "expected step_cap retirement, got: {reason}"
    );

    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(
        v.get("state").and_then(|x| x.as_str()),
        Some("Unhealthy"),
        "Decide-Err must transition the tracker to Unhealthy via the run loop"
    );
    assert_eq!(
        v.get("incident")
            .and_then(|i| i.get("failing"))
            .and_then(|f| f.get("type"))
            .and_then(|x| x.as_str()),
        Some("Inference"),
        "Decide-Err is an inference failure"
    );
    let last_err = v
        .get("incident")
        .and_then(|i| i.get("last_error"))
        .and_then(|x| x.as_str())
        .expect("last_error string");
    assert!(
        last_err.contains("script exhausted"),
        "incident's last_error should preserve the underlying Decide-Err message, got: {last_err}"
    );
}

/// Test-only `Tool` impl: fails its first `fail_count` calls with a
/// caller-supplied `anyhow::Error`, then succeeds. Used to exercise the
/// agent-side tool-failure path without standing up an MCP server. By the
/// time `tools.call(...)` returns `Err`, the tool has already exhausted
/// whatever retry policy it was configured with — "the tool errored" is the
/// only observable signal at the run-loop boundary.
struct FlakyTool {
    name: String,
    /// Remaining failures before the tool starts succeeding. Decremented
    /// on each call. `Mutex` because `Tool::call` takes `&self`.
    remaining_failures: Mutex<u32>,
}

impl FlakyTool {
    fn new(name: impl Into<String>, fail_count: u32) -> Self {
        Self {
            name: name.into(),
            remaining_failures: Mutex::new(fail_count),
        }
    }
}

#[async_trait]
impl Tool for FlakyTool {
    fn name(&self) -> &str {
        &self.name
    }
    async fn call(&self, args: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        let mut remaining = self.remaining_failures.lock().unwrap();
        if *remaining > 0 {
            *remaining -= 1;
            return Err(anyhow::anyhow!(
                "flaky tool {:?} failing (remaining_failures was {})",
                self.name,
                *remaining + 1
            ));
        }
        Ok(json!({"ok": true, "args": args}))
    }
}

fn registry_with_flaky(name: &str, fail_count: u32) -> ToolRegistry {
    let mut r = ToolRegistry::new();
    r.register(Arc::new(FlakyTool::new(name, fail_count)))
        .expect("register flaky");
    r
}

/// A tool that fails persistently exhausts the per-cycle
/// `FailureKind::ToolCall` budget and trips the tracker to `Unhealthy`.
/// We pin `max_tool = 0` so a single exhausted call exhausts the budget,
/// ending the cycle. The run loop must **not** halt.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn tool_call_exhausts_retry_budget_trips_unhealthy() {
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_millis(50)).await;
    mandate.step_cap = Some(1);
    let registry = registry_with_flaky("flaky", u32::MAX);
    let script = vec![
        // Cycle 1, step 1: model calls the flaky tool → tool errors →
        // ToolError → record_failure(ToolCall) → budget exhausted
        // (max_tool=0) → transition_to_unhealthy, cycle ends.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "flaky",
                json!({"x": 1}),
                ClaimSeed::new("seed-1"),
            )],
        },
    ];
    let agent = Agent::new(
        mandate,
        fs,
        MockDecide::new(script),
        registry,
        fresh_health_with(tmp.path(), RetryBudget::new(1, 0)),
    );

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "step_cap (1) reached");

    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(
        v.get("state").and_then(|x| x.as_str()),
        Some("Unhealthy"),
        "tool-call budget exhaustion must transition to Unhealthy"
    );
    let failing = v
        .get("incident")
        .and_then(|i| i.get("failing"))
        .expect("failing block");
    assert_eq!(
        failing.get("type").and_then(|x| x.as_str()),
        Some("ToolCall"),
        "incident must be tagged as a tool-call failure, not inference"
    );
    let details = failing.get("details").expect("details block");
    assert_eq!(
        details.get("tool").and_then(|x| x.as_str()),
        Some("flaky"),
        "incident details should record which tool failed"
    );
    assert!(
        details
            .get("error")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .contains("flaky tool"),
        "incident details should preserve the underlying error message"
    );
    let evidence_dir = tmp.path().join("evidence");
    assert!(
        agent_record_files(&evidence_dir).is_empty(),
        "no evidence should have been recorded for the failed call: {:?}",
        agent_record_files(&evidence_dir)
    );
}

/// After the per-cycle tool-call budget exhausts and trips `Unhealthy`, the
/// very next successful cycle must recover the tracker to `Healthy` and
/// archive the prior incident. We reuse the flaky tool but fail it only
/// once, so cycle 2's `CallTool` succeeds and that cycle's `Idle` marks
/// success.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn tool_call_exhaustion_recovers_on_next_successful_cycle() {
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_millis(50)).await;
    mandate.step_cap = Some(2);
    let registry = registry_with_flaky("flaky", 1);
    let script = vec![
        // Cycle 1: tool errors → budget=0 trips → Unhealthy, cycle ends.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "flaky",
                json!({"x": 1}),
                ClaimSeed::new("seed-1"),
            )],
        },
        // Cycle 2: tool succeeds → observation appended → idle →
        // mark_tick_success → archive incident and flip back to Healthy.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "flaky",
                json!({"x": 2}),
                ClaimSeed::new("seed-2"),
            )],
        },
        idle(),
    ];
    let agent = Agent::new(
        mandate,
        fs,
        MockDecide::new(script),
        registry,
        fresh_health_with(tmp.path(), RetryBudget::new(1, 0)),
    );

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "step_cap (2) reached");

    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(
        v.get("state").and_then(|x| x.as_str()),
        Some("Healthy"),
        "next successful cycle must recover the tracker to Healthy"
    );
    assert!(v.get("incident").is_none() || v.get("incident").unwrap().is_null());

    let archive_dir = tmp.path().join("health");
    assert!(
        archive_dir.is_dir(),
        "archive dir should be created on recovery"
    );
    let archived: Vec<_> = std::fs::read_dir(&archive_dir)
        .expect("read archive")
        .map(|e| e.expect("dirent").path())
        .collect();
    assert_eq!(
        archived.len(),
        1,
        "exactly one archived incident expected, got: {archived:?}"
    );
    let inc: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&archived[0]).expect("read archive"))
            .expect("parse archive");
    assert_eq!(
        inc.get("incident")
            .and_then(|i| i.get("failing"))
            .and_then(|f| f.get("type"))
            .and_then(|x| x.as_str()),
        Some("ToolCall"),
        "archived incident must preserve the tool-call kind"
    );

    let evidence_dir = tmp.path().join("evidence");
    assert!(
        evidence_dir.is_dir(),
        "evidence dir should exist after recovery"
    );
    let evs = agent_record_files(&evidence_dir);
    assert_eq!(
        evs.len(),
        1,
        "exactly one evidence record expected from the recovery cycle"
    );
}

/// Capturing `Decide` wrapper that snapshots every `Session` it sees and
/// defers to a `MockDecide` script. Used to assert on the session the run
/// loop hands the model on a post-failure step — that's the surface the
/// in-cycle corrective signal rides on (replacing the old cross-tick
/// `ContextBundle.correction`).
struct CapturingDecide {
    inner: MockDecide,
    seen: Arc<Mutex<Vec<Session>>>,
}

#[async_trait]
impl Decide for CapturingDecide {
    async fn decide(&self, session: &Session) -> anyhow::Result<Decision> {
        self.seen.lock().unwrap().push(session.clone());
        self.inner.decide(session).await
    }
}

/// When a `CallTool` exhausts its retry budget inside the tool (surfaces as
/// `Err` from `tools.call`) and the per-cycle `FailureKind::ToolCall` budget
/// still has room, the failure is folded into the session as a failure
/// observation the *next step of the same cycle* reasons over. This replaces
/// the old cross-tick correction: the model self-corrects inline.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn tool_call_failure_is_visible_as_in_cycle_observation() {
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_millis(50)).await;
    mandate.step_cap = Some(1);
    let registry = registry_with_flaky("flaky", u32::MAX);
    let script = vec![
        // Step 1: bad CallTool → tool errors → record_failure ok (budget
        // has room) → failure observation appended to the session.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "flaky",
                json!({"q": "what", "n": 3}),
                ClaimSeed::new("seed-1"),
            )],
        },
        // Step 2: idle so the cycle terminates. The session this decide()
        // sees must carry the failure observation from step 1.
        idle(),
    ];
    let seen: Arc<Mutex<Vec<Session>>> = Arc::new(Mutex::new(Vec::new()));
    let decide = CapturingDecide {
        inner: MockDecide::new(script),
        seen: seen.clone(),
    };
    let agent = Agent::new(
        mandate,
        fs,
        decide,
        registry,
        fresh_health_with(tmp.path(), RetryBudget::new(1, 3)),
    );

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "step_cap (1) reached");

    let captured = seen.lock().unwrap().clone();
    assert_eq!(
        captured.len(),
        2,
        "expected exactly two decide() invocations (two steps), got {}",
        captured.len()
    );

    // Step 1's session: empty — this is the failure-generating step.
    assert!(
        captured[0].steps.is_empty(),
        "step 1 must see an empty session: it generates the failure"
    );

    // Step 2's session: one step whose observation describes the tool
    // failure (tool name, args, error, next-step cue).
    assert_eq!(
        captured[1].steps.len(),
        1,
        "step 2 must see the failed step"
    );
    let obs = &captured[1].steps[0].observation;
    assert!(!obs.ok, "the recorded step's observation must be a failure");
    assert!(
        obs.content.contains("\"flaky\""),
        "observation should name the failed tool, got: {}",
        obs.content
    );
    assert!(
        obs.content.contains("{\"n\":3,\"q\":\"what\"}"),
        "observation should include args summary, got: {}",
        obs.content
    );
    assert!(
        obs.content.contains("flaky tool"),
        "observation should preserve underlying error message, got: {}",
        obs.content
    );
    assert!(
        obs.content.contains("different decision"),
        "observation should end with a next-step cue, got: {}",
        obs.content
    );

    // Sanity: the agent stayed Healthy — one failure under a 3-slot budget.
    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(
        v.get("state").and_then(|x| x.as_str()),
        Some("Healthy"),
        "single tool failure under budget should not transition to Unhealthy"
    );
}

/// After a tool failure becomes an in-cycle observation, the model uses it
/// to take a *different* step (here, a different tool that succeeds) within
/// the same cycle, then idles. The agent stays `Healthy`. Symmetric to
/// `invalid_call_tool_is_recoverable_in_cycle_failure`, asserting via a
/// `CapturingDecide` that the second step saw the failure observation.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn tool_call_failure_then_different_step_recovers_within_cycle() {
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_millis(50)).await;
    mandate.step_cap = Some(1);
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(FlakyTool::new("flaky", u32::MAX)))
        .expect("register flaky");
    registry
        .register(Arc::new(EchoTool))
        .expect("register echo");

    let script = vec![
        // Step 1: CallTool flaky → errors → failure observation appended.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "flaky",
                json!({"x": 1}),
                ClaimSeed::new("seed-flaky"),
            )],
        },
        // Step 2: model sees the failure observation and takes a different
        // step (echo) which succeeds.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "echo",
                json!({"msg": "recovered"}),
                ClaimSeed::new("seed-echo"),
            )],
        },
        // Step 3: idle, ending the cycle.
        idle(),
    ];
    let seen: Arc<Mutex<Vec<Session>>> = Arc::new(Mutex::new(Vec::new()));
    let decide = CapturingDecide {
        inner: MockDecide::new(script),
        seen: seen.clone(),
    };
    let agent = Agent::new(
        mandate,
        fs,
        decide,
        registry,
        fresh_health_with(tmp.path(), RetryBudget::new(1, 3)),
    );

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "step_cap (1) reached");

    let captured = seen.lock().unwrap().clone();
    assert_eq!(captured.len(), 3, "expected three decide() invocations");

    // Step 2 saw the flaky failure observation — that's how it "knew" to
    // choose a different tool.
    assert_eq!(captured[1].steps.len(), 1);
    assert!(
        captured[1].steps[0]
            .observation
            .content
            .contains("\"flaky\""),
        "step 2 must see the failure observation from step 1's tool failure, got: {}",
        captured[1].steps[0].observation.content
    );

    // Health stays Healthy across the cycle: one failure under the
    // per-cycle budget, then a success, then idle.
    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(
        v.get("state").and_then(|x| x.as_str()),
        Some("Healthy"),
        "agent must stay Healthy: failure was absorbed within the cycle"
    );
    assert!(
        !tmp.path().join("health").exists(),
        "no archive directory should exist when the agent never tripped Unhealthy"
    );

    // The recovery step's successful echo persisted exactly one evidence
    // record (the flaky tool produced none).
    let evidence_dir = tmp.path().join("evidence");
    let evs = agent_record_files(&evidence_dir);
    assert_eq!(
        evs.len(),
        1,
        "exactly one evidence record expected (echo's), got: {evs:?}"
    );
}

/// The cycle seed's FS index reaches the run loop. Pre-seed notes + outputs
/// on disk, then run an agent whose first step is `Idle`; the captured
/// session's seed must carry pointers (filenames) to those files — the
/// pull-navigation surface the model reads from, not file bodies.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn seed_index_reaches_the_run_loop() {
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_millis(50)).await;
    mandate.step_cap = Some(1);

    // Seed a note + an output via the production FS path.
    fs.apply_ops(vec![FsOp::WriteFile {
        path: "notes/plan.md".into(),
        content: "the plan".into(),
    }])
    .await
    .expect("seed note");
    let ev_id = fs
        .record_evidence(EvidenceRecord::new(
            "echo",
            json!({"k": 1}),
            json!({"v": 1}),
            Utc::now(),
        ))
        .await
        .expect("record evidence");
    let _out_id = fs
        .persist_output("a finding", &[ev_id])
        .await
        .expect("persist output");

    let seen: Arc<Mutex<Vec<Session>>> = Arc::new(Mutex::new(Vec::new()));
    let decide = CapturingDecide {
        inner: MockDecide::new(vec![idle()]),
        seen: seen.clone(),
    };
    let agent = Agent::new(
        mandate,
        fs,
        decide,
        registry_with_echo(),
        fresh_health(tmp.path()),
    );

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "step_cap (1) reached");

    let captured = seen.lock().unwrap().clone();
    assert_eq!(captured.len(), 1, "expected exactly one decide()");
    let seed = &captured[0].seed;
    assert_eq!(
        seed.index.notes,
        vec!["plan.md".to_string()],
        "seed index must point at the note filename"
    );
    assert_eq!(
        seed.index.outputs,
        vec!["output.md".to_string()],
        "seed index must point at the canonical output filename"
    );
}

/// K=3 parallel tool calls in a single step. All three succeed; the agent
/// loop must persist three distinct evidence records and the cycle proceeds
/// to `WriteOutput` then `Idle`. Models the "synthesize 3 file reads in one
/// step" path.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn parallel_call_tools_k3_all_succeed_persists_evidence() {
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_millis(50)).await;
    mandate.step_cap = Some(1);
    let args_a = json!({"path": "a.md"});
    let args_b = json!({"path": "b.md"});
    let args_c = json!({"path": "c.md"});
    let ev_a = EvidenceId::new("echo", &args_a, &json!({"echoed": args_a}));
    let ev_b = EvidenceId::new("echo", &args_b, &json!({"echoed": args_b}));
    let ev_c = EvidenceId::new("echo", &args_c, &json!({"echoed": args_c}));

    let script = vec![
        // Step 1: K=3 parallel call_tool batch — all echo, distinct args.
        Decision::CallTools {
            calls: vec![
                ToolCall::with_tool_use_id(
                    "echo",
                    args_a.clone(),
                    ClaimSeed::new("seed-a"),
                    "toolu_a",
                ),
                ToolCall::with_tool_use_id(
                    "echo",
                    args_b.clone(),
                    ClaimSeed::new("seed-b"),
                    "toolu_b",
                ),
                ToolCall::with_tool_use_id(
                    "echo",
                    args_c.clone(),
                    ClaimSeed::new("seed-c"),
                    "toolu_c",
                ),
            ],
        },
        // Step 2: cite all three.
        Decision::WriteOutput {
            body: "synthesized from 3 reads".into(),
            citations: vec![ev_a.clone(), ev_b.clone(), ev_c.clone()],
        },
        idle(),
    ];
    let agent = Agent::new(
        mandate,
        fs,
        MockDecide::new(script),
        registry_with_echo(),
        fresh_health(tmp.path()),
    );

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "step_cap (1) reached");

    // Three evidence files on disk, one per call.
    let evidence_dir = tmp.path().join("evidence");
    let evs: Vec<String> = agent_record_files(&evidence_dir)
        .into_iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();
    assert_eq!(evs.len(), 3, "expected 3 evidence files, got: {evs:?}");
    for id in [&ev_a, &ev_b, &ev_c] {
        let name = format!("{}.json", id);
        assert!(
            evs.contains(&name),
            "evidence {name} not on disk; got: {evs:?}"
        );
    }

    // WriteOutput succeeded — the canonical Output holds the synthesized body.
    let body = std::fs::read_to_string(tmp.path().join("outputs").join("output.md"))
        .expect("read canonical output");
    assert_eq!(body, "synthesized from 3 reads");

    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(v["state"].as_str(), Some("Healthy"));
}

/// Partial failure: K=3 parallel tool calls where the middle call fails.
/// Successful siblings persist their evidence (the model can cite them on a
/// later step), and the failure becomes an in-cycle observation describing
/// only the failed call. The per-cycle `ToolCall` budget is decremented by
/// exactly one slot — pinning the "K against budget" accounting.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn parallel_call_tools_k3_partial_failure_persists_successes_and_observes_failure() {
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_millis(50)).await;
    mandate.step_cap = Some(1);
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(EchoTool))
        .expect("register echo");
    registry
        .register(Arc::new(FlakyTool::new("flaky", u32::MAX)))
        .expect("register flaky");

    let script = vec![
        // Step 1: parallel batch — echo, flaky, echo. Two successes
        // sandwich one persistent failure.
        Decision::CallTools {
            calls: vec![
                ToolCall::with_tool_use_id(
                    "echo",
                    json!({"k": "a"}),
                    ClaimSeed::new("seed-a"),
                    "toolu_a",
                ),
                ToolCall::with_tool_use_id(
                    "flaky",
                    json!({"k": "b"}),
                    ClaimSeed::new("seed-b"),
                    "toolu_b",
                ),
                ToolCall::with_tool_use_id(
                    "echo",
                    json!({"k": "c"}),
                    ClaimSeed::new("seed-c"),
                    "toolu_c",
                ),
            ],
        },
        // Step 2: model sees the failure observation; idle to end the cycle.
        idle(),
    ];

    let seen: Arc<Mutex<Vec<Session>>> = Arc::new(Mutex::new(Vec::new()));
    let decide = CapturingDecide {
        inner: MockDecide::new(script),
        seen: seen.clone(),
    };
    let agent = Agent::new(
        mandate,
        fs,
        decide,
        registry,
        fresh_health_with(tmp.path(), RetryBudget::new(1, 3)),
    );

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "step_cap (1) reached");

    // Successful sibling evidence stays on disk (the load-bearing
    // "don't unwind on partial failure" property).
    let echo_a_id = EvidenceId::new("echo", &json!({"k": "a"}), &json!({"echoed": {"k": "a"}}));
    let echo_c_id = EvidenceId::new("echo", &json!({"k": "c"}), &json!({"echoed": {"k": "c"}}));
    let evidence_dir = tmp.path().join("evidence");
    let evs: Vec<String> = agent_record_files(&evidence_dir)
        .into_iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();
    assert_eq!(
        evs.len(),
        2,
        "two successful echo calls should persist evidence even when sibling failed; got: {evs:?}"
    );
    assert!(
        evs.contains(&format!("{}.json", echo_a_id)),
        "evidence for echo(k=a) missing; got: {evs:?}"
    );
    assert!(
        evs.contains(&format!("{}.json", echo_c_id)),
        "evidence for echo(k=c) missing; got: {evs:?}"
    );

    // The next step's session carries the in-cycle failure observation
    // naming only the failed tool.
    let captured = seen.lock().unwrap().clone();
    assert_eq!(captured.len(), 2);
    assert_eq!(captured[1].steps.len(), 1);
    let obs = &captured[1].steps[0].observation;
    assert!(!obs.ok);
    assert!(
        obs.content.contains("\"flaky\""),
        "observation must name the failed tool: {}",
        obs.content
    );
    assert!(
        !obs.content.contains("\"echo\""),
        "observation must not name successful siblings, got: {}",
        obs.content
    );

    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(v["state"].as_str(), Some("Healthy"));
}

/// K parallel failures consume K slots in the `FailureKind::ToolCall`
/// budget. With `max_tool = 2` and a K=3 batch of all-failing calls, the
/// budget exhausts on the third slot and the cycle ends `Unhealthy`. Pins
/// the documented "K against budget" choice.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn parallel_call_tools_k3_all_fail_consumes_k_budget_slots_and_trips_unhealthy() {
    let (tmp, fs, mut mandate) = fresh_fs(Duration::from_millis(50)).await;
    mandate.step_cap = Some(1);
    let registry = registry_with_flaky("flaky", u32::MAX);

    let script = vec![
        // K=3 all-failing batch. With max_tool=2, three failures exceed the
        // budget on the third recorded slot → Unhealthy, cycle ends.
        Decision::CallTools {
            calls: vec![
                ToolCall::new("flaky", json!({"i": 1}), ClaimSeed::new("seed-1")),
                ToolCall::new("flaky", json!({"i": 2}), ClaimSeed::new("seed-2")),
                ToolCall::new("flaky", json!({"i": 3}), ClaimSeed::new("seed-3")),
            ],
        },
    ];
    let agent = Agent::new(
        mandate,
        fs,
        MockDecide::new(script),
        registry,
        fresh_health_with(tmp.path(), RetryBudget::new(1, 2)),
    );

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "step_cap (1) reached");

    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(
        v["state"].as_str(),
        Some("Unhealthy"),
        "K=3 failures with max_tool=2 must trip Unhealthy",
    );
    let retry_trail = v["incident"]["retry_trail"]
        .as_array()
        .expect("retry_trail array");
    assert_eq!(
        retry_trail.len(),
        3,
        "every failed call in the batch should appear in the retry trail, got: {retry_trail:?}",
    );
    let failures = v["incident"]["failing"]["details"]["failures"]
        .as_array()
        .expect("failures array in incident details");
    assert_eq!(failures.len(), 3);
}

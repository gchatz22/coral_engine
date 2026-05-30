//! Integration tests for the `Agent` run loop.
//!
//! Exercises the public surface only (`Agent::new`, `Agent::signal`,
//! `Agent::run`) plus the FS root the agent writes to: signal/deadline
//! wakeups, `EmitOutput` / `Retire` / `RewriteFs` / `CallTool` arms,
//! `max_ticks` cap, the apply-time correction loop, tool-failure
//! correction, health budget exhaustion + recovery.
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
use coral_node::decision::{
    ClaimSeed, ContextBundle, Decide, Decision, FsOp, MockDecide, ToolCall,
};
use coral_node::evidence::{EvidenceId, EvidenceRecord};
use coral_node::fs::AgentFs;
use coral_node::health::{HealthTracker, RetryBudget};
use coral_node::mandate::{ContextPolicy, Mandate};
use coral_node::tools::{EchoTool, Tool, ToolRegistry};
use coral_node::trigger::Trigger;
use coral_node::trigger_queue::SignalSink;

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
async fn loop_wakes_on_injected_signal_and_retires() {
    // Idle period is huge so the test relies on the signal, not the
    // scheduled wake, to drive the first tick.
    let (tmp, fs, mandate) = fresh_fs(Duration::from_secs(3600)).await;
    let script = vec![Decision::Retire {
        reason: "signal-drove-me".into(),
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
    assert_eq!(reason, "signal-drove-me");
    assert!(tmp.path().join("retirement.json").is_file());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn loop_wakes_on_deadline_when_no_signal_arrives() {
    // Short idle period so the deadline-arm fires with paused-time auto-
    // advance. Script retires on the first tick; if it ran, the deadline
    // fired, we drained a `ScheduledWake`, and decide returned.
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50)).await;
    let script = vec![Decision::Retire {
        reason: "deadline-drove-me".into(),
    }];
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
    assert_eq!(reason, "deadline-drove-me");
    assert!(tmp.path().join("retirement.json").is_file());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn emit_output_with_valid_evidence_writes_file_under_outputs() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50)).await;
    // Pre-seed an evidence record on disk so the EmitOutput's evidence id
    // resolves. Computing the id here keeps the test independent of how
    // the agent would have produced it.
    let rec = EvidenceRecord::new(
        "echo",
        json!({"msg": "hi"}),
        json!({"echoed": {"msg": "hi"}}),
        chrono::Utc::now(),
    );
    let ev_id: EvidenceId = fs.record_evidence(rec).await.expect("seed evidence");

    let script = vec![
        Decision::EmitOutput {
            content: "the answer".into(),
            evidence: vec![ev_id.clone()],
        },
        Decision::Retire {
            reason: "done".into(),
        },
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

    // Exactly one file under outputs/, and it references the evidence id.
    let outputs_dir = tmp.path().join("outputs");
    let entries = agent_record_files(&outputs_dir);
    assert_eq!(entries.len(), 1, "expected exactly one output file");
    let bytes = std::fs::read(&entries[0]).expect("read output");
    let v: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(
        v.get("content").and_then(|s| s.as_str()),
        Some("the answer")
    );
    let ev_arr = v
        .get("evidence")
        .and_then(|x| x.as_array())
        .expect("evidence array");
    assert_eq!(ev_arr.len(), 1);
    assert_eq!(ev_arr[0].as_str(), Some(ev_id.as_str()));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn retire_writes_retirement_json_and_returns_reason() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50)).await;
    let script = vec![Decision::Retire {
        reason: "graceful exit".into(),
    }];
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
    assert_eq!(reason, "graceful exit");

    let path = tmp.path().join("retirement.json");
    assert!(path.is_file());
    let v: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).expect("read")).expect("json");
    assert_eq!(
        v.get("reason").and_then(|s| s.as_str()),
        Some("graceful exit")
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn rewrite_fs_writes_file_under_notes() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50)).await;
    let script = vec![
        Decision::RewriteFs {
            ops: vec![FsOp::WriteFile {
                path: "notes/scratch.md".into(),
                content: "hello from the loop".into(),
            }],
        },
        Decision::Retire {
            reason: "done".into(),
        },
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

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn call_tool_records_evidence_and_emit_output_consumes_it() {
    // Sanity sweep that exercises the CallTool arm end to end: the loop
    // calls echo, the resulting evidence is persisted, and a follow-up
    // EmitOutput with that evidence id succeeds.
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50)).await;
    // Compute the id we expect echo to produce so we can reference it in
    // the EmitOutput decision.
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
        Decision::EmitOutput {
            content: "echoed".into(),
            evidence: vec![expected_ev.clone()],
        },
        Decision::Retire {
            reason: "done".into(),
        },
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

    // Evidence file written by the CallTool arm.
    let ev_path = tmp
        .path()
        .join("evidence")
        .join(format!("{}.json", expected_ev));
    assert!(ev_path.is_file(), "expected evidence file at {ev_path:?}");

    // Output file written by the EmitOutput arm references that evidence.
    let outputs_dir = tmp.path().join("outputs");
    let entries = agent_record_files(&outputs_dir);
    assert_eq!(entries.len(), 1);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn max_ticks_caps_loop_iterations_and_writes_retirement() {
    // Mandate caps the loop at exactly 2 ticks. Idle period is small so
    // paused-time auto-advance drives both ticks via the deadline arm —
    // no signals needed. The script holds exactly 2 Idle decisions and
    // nothing else: if the cap fails to fire, the loop attempts a third
    // tick, MockDecide returns "script exhausted", and run() bubbles an
    // Err. So both behaviours are covered: the cap firing produces
    // Ok(retired); the cap not firing produces Err.
    let tmp = TempDir::new().expect("tempdir");
    let mandate = Mandate::new("max-ticks-test", Duration::from_millis(50), Some(2));
    let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
        .await
        .expect("open fs");

    let script = vec![
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
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

    assert!(
        reason.contains("max_ticks"),
        "expected max_ticks retirement reason, got: {reason}"
    );
    assert!(
        reason.contains('2'),
        "reason should mention the cap value: {reason}"
    );
    assert!(tmp.path().join("retirement.json").is_file());
}

/// Model emits an unsatisfiable `Decision` (`CallTool` for an
/// unregistered tool); the runtime must catch the apply-time failure,
/// stage a correction for the next tick, and the next tick must produce
/// a valid `Decision` that completes. The agent stays Healthy throughout.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn invalid_call_tool_stages_correction_then_recovers() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50)).await;
    let script = vec![
        // Tick 1: model picks a tool that is not registered. Apply-time
        // failure → record_failure(Inference) → counter=1 (under default
        // budget of 1) → pending_correction set for the next tick.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "no_such_tool",
                json!({"x": 1}),
                ClaimSeed::new("seed-1"),
            )],
        },
        // Tick 2: correction continuation; script's next decision is
        // Retire. dispatch returns Retire → loop exits.
        Decision::Retire {
            reason: "recovered".into(),
        },
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
    assert_eq!(reason, "recovered");

    // Provenance side-effect check: the bad CallTool must not have
    // produced an evidence record. The registry rejected the lookup
    // before any tool ran, so `evidence/` is either absent or empty.
    let evidence_dir = tmp.path().join("evidence");
    let entries = agent_record_files(&evidence_dir);
    assert!(
        entries.is_empty(),
        "no evidence should have been recorded for an unregistered tool"
    );

    // Health stays Healthy across the correction cycle — budget was
    // consumed (counter=1) but never exhausted.
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

/// `EmitOutput` with an *empty* evidence list is rejected by
/// `AgentFs::persist_output` with `FsError::EmptyEvidence`; that maps
/// to `ApplyOutcome::NeedsCorrection` and the next iteration runs as a
/// correction continuation whose script retires.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn emit_output_with_empty_evidence_stages_correction() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50)).await;
    let script = vec![
        Decision::EmitOutput {
            content: "no provenance".into(),
            evidence: vec![],
        },
        Decision::Retire {
            reason: "recovered".into(),
        },
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
    assert_eq!(reason, "recovered");

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

/// Same shape of failure driven through the `EmitOutput` arm with a
/// well-formed-but-not-on-disk evidence id (`FsError::EvidenceNotFound`).
/// Mirrors the empty-evidence test above; the two cover the two distinct
/// `FsError` variants the apply-time correction path catches.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn emit_output_with_unknown_evidence_stages_correction() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50)).await;
    let bogus = EvidenceId::from_hex("deadbeef".repeat(8));
    let script = vec![
        Decision::EmitOutput {
            content: "lying about provenance".into(),
            evidence: vec![bogus],
        },
        Decision::Retire {
            reason: "recovered".into(),
        },
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
    assert_eq!(reason, "recovered");

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

/// Persistent apply-time failure exhausts the per-tick inference budget
/// across the original attempt + one correction continuation. The agent
/// transitions to `Unhealthy`, the run loop **does not halt**, and a
/// subsequent successful tick (here, an `Idle` decision) recovers the
/// tracker to `Healthy` while archiving the prior incident.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn persistent_apply_time_failure_exhausts_budget_and_recovers_on_next_success() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50)).await;
    // Default budget is `RetryBudget::new(1, 3)` → max_inference = 1, so
    // total apply-time attempts before exhaustion = 2 (original + 1
    // retry inside the same fresh-tick window).
    let script = vec![
        // Attempt 1 (fresh tick): bad CallTool → record_failure ok →
        // synthetic correction injected.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "no_such_tool",
                json!({}),
                ClaimSeed::new("seed-1"),
            )],
        },
        // Attempt 2 (correction continuation tick — begin_tick skipped):
        // bad CallTool → counter=2 > max_inference=1 → BudgetExhausted →
        // transition_to_unhealthy. Run loop does NOT exit.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "no_such_tool",
                json!({}),
                ClaimSeed::new("seed-1"),
            )],
        },
        // Attempt 3 (next deadline-driven fresh tick): valid Idle.
        // dispatch returns Continue → mark_tick_success → archives the
        // Unhealthy incident and flips back to Healthy.
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
        // Attempt 4: valid Retire — exits the loop.
        Decision::Retire {
            reason: "after-recovery".into(),
        },
    ];
    // Anchor the budget explicitly so a future change to
    // `RetryBudget::default()` does not silently invalidate the
    // arithmetic in this test's commentary.
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
    assert_eq!(reason, "after-recovery");

    // After recovery: the live `health.json` reflects Healthy state with
    // a `since` timestamp at recovery time (not the agent's open time).
    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(
        v.get("state").and_then(|x| x.as_str()),
        Some("Healthy"),
        "agent must recover to Healthy after the next successful tick"
    );
    // `incident` is dropped on the Healthy record.
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
        "retry trail should record both attempts before exhaustion"
    );
}

/// Decide-side `Err` (model adapter could not produce a `Decision`) is
/// the inference-retry-exhaustion signal at the run loop boundary: it
/// transitions the tracker to `Unhealthy` directly, without spending
/// another budget slot. We verify the transition + persistence; the
/// rehydrate-then-recover half of the cycle is exercised by the
/// `persistent_apply_time_failure_*` test above.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn decide_err_transitions_to_unhealthy_and_keeps_loop_alive() {
    // Mandate caps at 1 tick so the test terminates: tick body runs once
    // (decide errors → transition_to_unhealthy → Continue), then iteration
    // 2 hits `tick >= max_ticks` and retires via the safety cap. That is
    // the cleanest way to verify the run loop **does not halt** on
    // Decide-Err while still bounding the test.
    let tmp = TempDir::new().expect("tempdir");
    let mandate = Mandate::new("decide-err", Duration::from_millis(50), Some(1));
    let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
        .await
        .expect("open fs");

    // Empty script → `MockDecide::decide` returns `Err("script exhausted")`
    // on the first call. That is the Decide-Err we exercise here.
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
    // max_ticks fired *after* the Decide-Err was caught and the tracker
    // transitioned — that's the property under test.
    assert!(
        reason.contains("max_ticks"),
        "expected max_ticks retirement, got: {reason}"
    );

    // The Decide-Err must have transitioned the tracker to Unhealthy
    // and the run loop kept going long enough for max_ticks to fire.
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

/// Regression test for the bug that motivated moving correction state off
/// the trigger queue: an external trigger landing in the queue between an
/// apply-time failure and the correction-continuation tick must NOT reset
/// the per-tick retry budget.
///
/// Before this fix, mid-correction continuation was signaled by a
/// self-injected synthetic trigger; the next tick classified itself as
/// "correction-only" by inspecting drained triggers. A racing external
/// trigger arriving in the same window made `is_correction_only` false,
/// which called `begin_tick` and reset the budget — so a noisy producer
/// could grant unlimited correction attempts. With `pending_correction`
/// stored on the agent, the classification is a stored fact; this
/// scenario must still exhaust the budget after the configured number of
/// failures.
///
/// Timing reproduction: tick 1's `Decide::decide` injects an external
/// trigger via a captured `SignalSink` *before* returning the bad
/// `CallTool`. By the time tick 1's dispatch sets `pending_correction`,
/// the external is buffered. Tick 2's drain sees `[External]`. Under the
/// old design, that drain (one external, zero synthetic-correction-kind
/// triggers) would have flipped `is_correction_only` to false and reset
/// the budget. Under the new design, `pending_correction.is_some()`
/// short-circuits the `begin_tick` call and the budget accumulates,
/// exhausting on tick 2's failure as required.
///
/// budget = `RetryBudget::new(1, 3)` → max_inference = 1 → two apply-time
/// failures within one continuous window exhaust.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn apply_failure_correction_budget_is_immune_to_concurrent_external_triggers() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50)).await;
    let script = vec![
        // Tick 1 (fresh): the wrapper Decide injects an external trigger,
        // then returns this bad CallTool. record_failure ok (counter=1)
        // → pending_correction set.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "no_such_tool",
                json!({}),
                ClaimSeed::new("seed-1"),
            )],
        },
        // Tick 2 (correction continuation; drain pulls the racing
        // external trigger, but pending_correction.is_some() so
        // begin_tick is skipped): bad CallTool again → counter=2 >
        // max=1 → BudgetExhausted → Unhealthy.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "no_such_tool",
                json!({}),
                ClaimSeed::new("seed-1"),
            )],
        },
        // Tick 3 (fresh — pending_correction cleared on the Unhealthy
        // transition): valid Idle → mark_tick_success → archives the
        // incident and flips back to Healthy.
        Decision::Idle {
            next_after: Duration::from_millis(50),
        },
        // Tick 4: retire cleanly.
        Decision::Retire {
            reason: "post-exhaustion".into(),
        },
    ];

    // The wrapper Decide needs a `SignalSink` for the same `TriggerQueue`
    // the agent runs against, but the agent owns its queue and only
    // exposes a sink via `signal()` — which we can't call until after
    // `Agent::new` consumes the Decide. Resolve the cycle with a deferred
    // slot: the wrapper holds `Arc<Mutex<Option<SignalSink>>>`, we move
    // the wrapper into the agent, then fill the slot with the agent's
    // sink before spawning `run()`.
    let pending_sink: Arc<Mutex<Option<SignalSink>>> = Arc::new(Mutex::new(None));

    struct DeferredSinkDecide {
        inner: MockDecide,
        sink_slot: Arc<Mutex<Option<SignalSink>>>,
        inject_on_call: u32,
        calls: Mutex<u32>,
    }
    #[async_trait]
    impl Decide for DeferredSinkDecide {
        async fn decide(&self, ctx: ContextBundle) -> anyhow::Result<Decision> {
            let n = {
                let mut c = self.calls.lock().unwrap();
                let n = *c;
                *c += 1;
                n
            };
            if n == self.inject_on_call {
                let guard = self.sink_slot.lock().unwrap();
                guard
                    .as_ref()
                    .expect("sink must be installed before run starts")
                    .send(coral_node::trigger::Trigger::External {
                        kind: "interfering_producer".into(),
                        payload: json!({"noise": true}),
                    })
                    .expect("inject");
            }
            self.inner.decide(ctx).await
        }
    }

    let decide = DeferredSinkDecide {
        inner: MockDecide::new(script),
        sink_slot: pending_sink.clone(),
        inject_on_call: 0,
        calls: Mutex::new(0),
    };

    let agent = Agent::new(
        mandate,
        fs,
        decide,
        registry_with_echo(),
        fresh_health_with(tmp.path(), RetryBudget::new(1, 3)),
    );
    *pending_sink.lock().unwrap() = Some(agent.signal());

    let handle = tokio::spawn(agent.run());

    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "post-exhaustion");

    // The archive must hold exactly one Unhealthy incident with a
    // retry_trail of length 2 — proof that the budget exhausted on tick 2
    // despite the racing trigger landing in tick 2's drain. Under the old
    // design, the racing trigger would have caused tick 2 to call
    // begin_tick, the test would never archive an incident, and this
    // assertion would fail.
    let archive_dir = tmp.path().join("health");
    assert!(
        archive_dir.is_dir(),
        "archive dir should be created on recovery — \
         absence means the budget was reset by the racing external trigger"
    );
    let archived: Vec<_> = std::fs::read_dir(&archive_dir)
        .expect("read archive")
        .map(|e| e.expect("dirent").path())
        .collect();
    assert_eq!(
        archived.len(),
        1,
        "exactly one archived incident expected (budget exhausted), got: {archived:?}"
    );

    let inc: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&archived[0]).expect("read archive"))
            .expect("parse archive");
    let retry_trail = inc
        .get("incident")
        .and_then(|i| i.get("retry_trail"))
        .and_then(|x| x.as_array())
        .expect("retry_trail");
    assert_eq!(
        retry_trail.len(),
        2,
        "retry trail should accumulate both apply-time attempts despite the race"
    );
}

/// Test-only `Tool` impl: fails its first `fail_count` calls with a
/// caller-supplied `anyhow::Error`, then succeeds. Used to exercise the
/// agent-side `ApplyOutcome::ToolError` path without standing up an MCP
/// server in tests. We pass an `anyhow::Error` as the failure mode rather
/// than going through `McpTool`'s `RetryPolicy` because:
///
/// 1. The unit tests in `src/mcp/tool.rs` already cover the
///    `RetryPolicy` mechanics — first-try success, second-try success
///    after a transient failure, exhaustion after the configured number
///    of attempts.
/// 2. From the agent run loop's perspective, "the tool errored" is the
///    only observable signal — by the time `tools.call(...)` returns
///    `Err`, the tool has already exhausted whatever retry policy it was
///    configured with. The integration tests below assert the run-loop
///    wiring (budget accounting + `Unhealthy` transition + recovery)
///    given that surface.
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

/// A tool that fails persistently exhausts the per-tick
/// `FailureKind::ToolCall` budget and trips the tracker to `Unhealthy`.
/// We pin `max_tool = 0` so a single exhausted call exhausts the budget
/// — each exhausted tool call counts as one tick-level slot, and with
/// budget = 0 the first one trips it. The run loop must **not** halt —
/// verified by the fact that the script
/// then runs a successful `Idle` and a `Retire` decision in the recovery
/// test below.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn tool_call_exhausts_retry_budget_trips_unhealthy() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50)).await;
    // Tool fails forever within the lifetime of this test.
    let registry = registry_with_flaky("flaky", u32::MAX);
    let script = vec![
        // Tick 1: model calls the flaky tool → tool errors →
        // ApplyOutcome::ToolError → record_failure(ToolCall, _) →
        // budget exhausted (max_tool=0) → transition_to_unhealthy.
        // Run loop continues.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "flaky",
                json!({"x": 1}),
                ClaimSeed::new("seed-1"),
            )],
        },
        // Tick 2: retire so the test terminates.
        Decision::Retire {
            reason: "after-tool-exhaustion".into(),
        },
    ];
    // Anchor budget shape explicitly: max_inference=1 keeps inference
    // path healthy, max_tool=0 trips on the first exhausted tool call.
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
    assert_eq!(reason, "after-tool-exhaustion");

    // Tick 1 transitioned to Unhealthy; the script's second decision was
    // a clean Retire which (per dispatch) does not mark a tick success,
    // so the live health.json should still be Unhealthy when the loop
    // exited. (Retire is the terminal path; recovery is exercised in the
    // companion test below.)
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
    // No evidence record was persisted for the failed call.
    let evidence_dir = tmp.path().join("evidence");
    assert!(
        agent_record_files(&evidence_dir).is_empty(),
        "no evidence should have been recorded for the failed call: {:?}",
        agent_record_files(&evidence_dir)
    );
}

/// After the per-tick tool-call budget exhausts and trips `Unhealthy`,
/// the very next successful tick must recover the tracker to `Healthy`
/// and archive the prior incident — the same recovery contract
/// `src/health.rs` defines for the inference path. We reuse the same
/// flaky tool but only fail it once, so the next tick's `CallTool`
/// succeeds and the tick is marked successful.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn tool_call_exhaustion_recovers_on_next_successful_tick() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50)).await;
    // Fail exactly once: tick 1 exhausts (budget=0 trips immediately),
    // tick 2's CallTool succeeds, mark_tick_success archives the
    // Unhealthy incident.
    let registry = registry_with_flaky("flaky", 1);
    let script = vec![
        // Tick 1: tool errors → Unhealthy.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "flaky",
                json!({"x": 1}),
                ClaimSeed::new("seed-1"),
            )],
        },
        // Tick 2 (fresh — tick 1's BudgetExhausted with max_tool=0
        // cleared pending_correction in the Unhealthy transition, so
        // begin_tick runs again): tool succeeds → ApplyOutcome::Continue
        // → mark_tick_success → archive incident and flip back to
        // Healthy.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "flaky",
                json!({"x": 2}),
                ClaimSeed::new("seed-2"),
            )],
        },
        // Tick 3: retire cleanly.
        Decision::Retire {
            reason: "after-recovery".into(),
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
    assert_eq!(reason, "after-recovery");

    // Live health.json now reflects Healthy with `since` at the
    // recovery tick's timestamp.
    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(
        v.get("state").and_then(|x| x.as_str()),
        Some("Healthy"),
        "next successful tick must recover the tracker to Healthy"
    );
    assert!(v.get("incident").is_none() || v.get("incident").unwrap().is_null());

    // Prior Unhealthy incident archived under health/<transitioned_at>.
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

    // Recovery tick's successful CallTool must have persisted evidence.
    let evidence_dir = tmp.path().join("evidence");
    assert!(
        evidence_dir.is_dir(),
        "evidence dir should exist after recovery"
    );
    let evs = agent_record_files(&evidence_dir);
    assert_eq!(
        evs.len(),
        1,
        "exactly one evidence record expected from the recovery tick"
    );
}

/// Capturing `Decide` wrapper that snapshots every `ContextBundle` it
/// sees and defers to a `MockDecide` script. Used by the tool-failure
/// correction tests to assert on the bundle the run loop hands the
/// model on the post-tool-failure tick — that's the surface the
/// corrective signal rides on.
struct CapturingDecide {
    inner: MockDecide,
    seen: Arc<Mutex<Vec<ContextBundle>>>,
}

#[async_trait]
impl Decide for CapturingDecide {
    async fn decide(&self, ctx: ContextBundle) -> anyhow::Result<Decision> {
        self.seen.lock().unwrap().push(ctx.clone());
        self.inner.decide(ctx).await
    }
}

/// When a `CallTool` exhausts its retry budget inside the tool
/// (surfaces as `Err` from `tools.call`) and the per-tick
/// `FailureKind::ToolCall` budget still has room, the run loop must
/// stage a `CorrectionContext` so the next tick's `ContextBundle`
/// carries a corrective signal describing the failure (tool name, args,
/// error). This mirrors the apply-time correction loop: the model gets
/// a chance to self-correct rather than rediscovering from scratch why
/// its last decision failed.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn tool_call_failure_stages_correction_visible_on_next_tick_bundle() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50)).await;
    // Tool fails twice: tick 1's CallTool errors (after the tool's
    // internal RetryPolicy gives up); tick 2's CallTool also errors but
    // the test exits before exhausting the budget — we only care that
    // tick 2 saw a correction in its bundle.
    let registry = registry_with_flaky("flaky", u32::MAX);
    let script = vec![
        // Tick 1: bad CallTool → tool errors → record_failure ok (budget
        // has room) → pending_correction set.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "flaky",
                json!({"q": "what", "n": 3}),
                ClaimSeed::new("seed-1"),
            )],
        },
        // Tick 2 (correction continuation): retire so the test
        // terminates. The bundle this decide() sees must carry the
        // correction from tick 1.
        Decision::Retire {
            reason: "stop-after-seeing-correction".into(),
        },
    ];
    // Anchor budget so tick 1 fits comfortably: max_tool=3 → tick 1's
    // single failure stays under the cap, and pending_correction is
    // staged rather than the tracker tripping to Unhealthy.
    let seen: Arc<Mutex<Vec<ContextBundle>>> = Arc::new(Mutex::new(Vec::new()));
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
    assert_eq!(reason, "stop-after-seeing-correction");

    let captured = seen.lock().unwrap().clone();
    assert_eq!(
        captured.len(),
        2,
        "expected exactly two decide() invocations, got {}",
        captured.len()
    );

    // Tick 1's bundle: no correction (this is the failure-generating
    // tick, not the continuation).
    assert!(
        captured[0].correction.is_none(),
        "tick 1 must not carry a correction: it generates the failure"
    );

    // Tick 2's bundle: a correction describing the tool failure.
    let correction = captured[1]
        .correction
        .as_ref()
        .expect("tick 2 must carry a correction staged by tick 1");
    let failure = &correction.failure;
    // Tool name surfaced (quoted, per the helper's contract).
    assert!(
        failure.contains("\"flaky\""),
        "correction should name the failed tool, got: {failure}"
    );
    // Args surfaced verbatim — model sees what it sent.
    assert!(
        failure.contains("{\"n\":3,\"q\":\"what\"}"),
        "correction should include args summary, got: {failure}"
    );
    // Error string preserved so the model can diagnose.
    assert!(
        failure.contains("flaky tool"),
        "correction should preserve underlying error message, got: {failure}"
    );
    // Concrete next-step instruction.
    assert!(
        failure.contains("different decision"),
        "correction should end with a next-step cue, got: {failure}"
    );

    // Sanity: the agent stayed Healthy because the per-tick tool-call
    // budget had room (max_tool=3, only one failure recorded before
    // retire). No archive directory should have been created.
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

/// After a tool failure stages a correction, the next tick uses the
/// corrective context to emit a *different* decision (here, a different
/// tool call that succeeds), the tick completes via
/// `ApplyOutcome::Continue`, and the agent stays / returns to `Healthy`.
/// Symmetric to `invalid_call_tool_stages_correction_then_recovers`.
///
/// We assert against a `CapturingDecide` again so we can show the second
/// tick saw the correction *and* emitted a different `Decision`.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn tool_call_failure_correction_then_different_decision_recovers_to_healthy() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50)).await;
    // Two tools: `flaky` fails forever; `echo` always succeeds. The
    // script drives the model to call `flaky` first, see the correction,
    // and then call `echo` on the continuation.
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(FlakyTool::new("flaky", u32::MAX)))
        .expect("register flaky");
    registry
        .register(Arc::new(EchoTool))
        .expect("register echo");

    let script = vec![
        // Tick 1: CallTool flaky → errors → pending_correction set.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "flaky",
                json!({"x": 1}),
                ClaimSeed::new("seed-flaky"),
            )],
        },
        // Tick 2 (correction continuation): the model sees the
        // correction in the bundle and emits a *different* decision
        // (calls echo instead). That succeeds → ApplyOutcome::Continue
        // → mark_tick_success → pending_correction cleared, tracker
        // stays Healthy.
        Decision::CallTools {
            calls: vec![ToolCall::new(
                "echo",
                json!({"msg": "recovered"}),
                ClaimSeed::new("seed-echo"),
            )],
        },
        // Tick 3: retire cleanly.
        Decision::Retire {
            reason: "recovered-after-tool-failure".into(),
        },
    ];
    let seen: Arc<Mutex<Vec<ContextBundle>>> = Arc::new(Mutex::new(Vec::new()));
    let decide = CapturingDecide {
        inner: MockDecide::new(script),
        seen: seen.clone(),
    };
    // Budget with enough rope: max_tool=3 so the single failure on tick
    // 1 stays under cap. The point of this test is the recovery happens
    // before exhaustion ever fires.
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
    assert_eq!(reason, "recovered-after-tool-failure");

    let captured = seen.lock().unwrap().clone();
    assert_eq!(captured.len(), 3, "expected three decide() invocations");

    // Tick 2 saw the correction — that's how it "knew" to choose a
    // different tool. This pins the correction-was-visible property.
    let correction = captured[1]
        .correction
        .as_ref()
        .expect("tick 2 must see the correction staged by tick 1's tool failure");
    assert!(
        correction.failure.contains("\"flaky\""),
        "correction should name the failed tool, got: {}",
        correction.failure
    );

    // Tick 3 is a fresh tick (recovery cleared pending_correction), so
    // it carries no correction.
    assert!(
        captured[2].correction.is_none(),
        "tick 3 must not carry a correction: tick 2 succeeded and cleared it"
    );

    // Health stays Healthy across the whole cycle: single failure under
    // the per-tick budget, then a success, then retire.
    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(
        v.get("state").and_then(|x| x.as_str()),
        Some("Healthy"),
        "agent must stay Healthy: failure was absorbed by the correction loop"
    );
    // No archive directory: tracker never transitioned to Unhealthy.
    assert!(
        !tmp.path().join("health").exists(),
        "no archive directory should exist when the agent never tripped Unhealthy"
    );

    // Recovery tick's successful CallTool must have persisted exactly
    // one evidence record (from echo). The flaky tool produced none.
    let evidence_dir = tmp.path().join("evidence");
    let evs = agent_record_files(&evidence_dir);
    assert_eq!(
        evs.len(),
        1,
        "exactly one evidence record expected (echo's), got: {evs:?}"
    );
}

/// Per-mandate `recent_outputs` cap reaches the run loop end-to-end.
///
/// Pre-seed 5 outputs on disk, then run an agent whose mandate's
/// `ContextPolicy::recent_outputs = 2`. The bundle the run loop hands
/// `Decide::decide` on the first tick must carry at most 2 outputs.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn per_mandate_recent_outputs_cap_reaches_the_run_loop() {
    let tmp = TempDir::new().expect("tempdir");
    let mandate = Mandate {
        text: "tiny window".into(),
        idle_period: Duration::from_millis(50),
        max_ticks: Some(1),
        retry_policy: None,
        context_policy: ContextPolicy {
            recent_outputs: 2,
            recent_evidence: 8,
            open_claims_max: 32,
        },
    };
    let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
        .await
        .expect("open fs");

    // Seed 5 outputs by going through the FS directly (the production
    // path); each cites the same evidence record so the provenance contract
    // is satisfied.
    let ev_id = fs
        .record_evidence(EvidenceRecord::new(
            "echo",
            json!({"k": 1}),
            json!({"v": 1}),
            Utc::now(),
        ))
        .await
        .expect("record evidence");
    for i in 0..5 {
        fs.persist_output(&format!("seed-output-{i}"), &[ev_id.clone()])
            .await
            .expect("persist output");
    }
    // Sanity: the FS layer agrees five outputs are on disk.
    assert_eq!(fs.list_recent_outputs(usize::MAX).await.unwrap().len(), 5);

    // Single-tick script: retire on the first decide so we exit immediately
    // after observing the bundle.
    let seen: Arc<Mutex<Vec<ContextBundle>>> = Arc::new(Mutex::new(Vec::new()));
    let decide = CapturingDecide {
        inner: MockDecide::new(vec![Decision::Retire {
            reason: "saw-bundle".into(),
        }]),
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
    assert_eq!(reason, "saw-bundle");

    let captured = seen.lock().unwrap().clone();
    assert_eq!(captured.len(), 1, "expected exactly one decide()");
    let bundle = &captured[0];
    assert_eq!(
        bundle.recent_outputs.len(),
        2,
        "per-mandate recent_outputs cap should have shrunk the bundle from 5 to 2"
    );
    // The cap is also reflected on the bundle's mandate snapshot (the
    // bundle clones the mandate verbatim).
    assert_eq!(bundle.mandate.context_policy.recent_outputs, 2);
}

/// K=3 parallel tool calls in a single tick. All three succeed; the
/// agent loop must persist three distinct evidence records in input
/// order and continue cleanly to the next tick's `EmitOutput`. Models
/// the "synthesize 3 file reads in one tick" path.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn parallel_call_tools_k3_all_succeed_persists_evidence_in_order() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50)).await;
    // Three distinct evidence ids the EmitOutput will cite. EchoTool
    // produces a content-addressed record per `(name, args, result)`
    // triple, so picking three distinct args guarantees three distinct
    // ids.
    let args_a = json!({"path": "a.md"});
    let args_b = json!({"path": "b.md"});
    let args_c = json!({"path": "c.md"});
    let result_a = json!({"echoed": args_a});
    let result_b = json!({"echoed": args_b});
    let result_c = json!({"echoed": args_c});
    let ev_a = EvidenceId::new("echo", &args_a, &result_a);
    let ev_b = EvidenceId::new("echo", &args_b, &result_b);
    let ev_c = EvidenceId::new("echo", &args_c, &result_c);

    let script = vec![
        // Tick 1: K=3 parallel call_tool batch — all echo, distinct args.
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
        // Tick 2: cite all three.
        Decision::EmitOutput {
            content: "synthesized from 3 reads".into(),
            evidence: vec![ev_a.clone(), ev_b.clone(), ev_c.clone()],
        },
        Decision::Retire {
            reason: "done".into(),
        },
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
    assert_eq!(reason, "done");

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

    // EmitOutput succeeded — one output file referencing all three ids.
    let outputs_dir = tmp.path().join("outputs");
    let outs = agent_record_files(&outputs_dir);
    assert_eq!(outs.len(), 1);
    let v: serde_json::Value = serde_json::from_slice(&std::fs::read(&outs[0]).unwrap()).unwrap();
    let ev_arr = v["evidence"].as_array().expect("evidence array");
    assert_eq!(ev_arr.len(), 3);

    // Health stayed Healthy across the parallel-dispatch tick.
    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(v["state"].as_str(), Some("Healthy"));
}

/// Partial failure: K=3 parallel tool calls where the middle call
/// fails. Successful siblings must persist their evidence (the model
/// can cite them next tick), the failure stages a correction describing
/// only the failed call, and the per-tick `ToolCall` budget is
/// decremented by exactly one slot — pinning the "K against budget"
/// accounting documented in `agent.rs`.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn parallel_call_tools_k3_partial_failure_persists_successes_and_stages_correction() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50)).await;
    // `echo` always succeeds; `flaky` always errors. Mix them in one
    // batch to exercise the partial-failure dispatch path.
    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(EchoTool))
        .expect("register echo");
    registry
        .register(Arc::new(FlakyTool::new("flaky", u32::MAX)))
        .expect("register flaky");

    let script = vec![
        // Tick 1: parallel batch — echo, flaky, echo. Two successes
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
        // Tick 2: model sees correction; just retire so the test
        // terminates with a deterministic reason.
        Decision::Retire {
            reason: "after-partial-failure".into(),
        },
    ];

    // Capture the bundle on tick 2 so we can confirm the correction
    // describes only the failed call.
    let seen: Arc<Mutex<Vec<ContextBundle>>> = Arc::new(Mutex::new(Vec::new()));
    let decide = CapturingDecide {
        inner: MockDecide::new(script),
        seen: seen.clone(),
    };

    // Generous budget so the single tool failure stages a correction
    // without exhausting (the test asserts on correction shape, not on
    // exhaustion).
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
    assert_eq!(reason, "after-partial-failure");

    // Successful sibling evidence stays on disk (the load-bearing
    // "don't unwind on partial failure" property the dispatch site
    // documents). The two persisted records correspond to the two
    // echo siblings — assert by content-addressed id so the test
    // pins *which* evidence survived, not just that two files exist.
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

    // The next tick's bundle carries the corrective signal naming the
    // failed tool/args.
    let captured = seen.lock().unwrap().clone();
    assert_eq!(captured.len(), 2);
    let correction = captured[1]
        .correction
        .as_ref()
        .expect("tick 2 must see a correction from the partial-batch failure");
    assert!(
        correction.failure.contains("\"flaky\""),
        "correction must name the failed tool: {}",
        correction.failure
    );
    // No mention of echo, which succeeded — the correction should
    // describe only what the model needs to fix.
    assert!(
        !correction.failure.contains("\"echo\""),
        "correction must not name successful siblings, got: {}",
        correction.failure
    );

    // Health stayed Healthy: one tool failure under a 3-slot budget.
    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(v["state"].as_str(), Some("Healthy"));
}

/// K parallel failures must consume K slots in the `FailureKind::ToolCall`
/// budget. With `max_tool = 2` and a K=3 batch of all-failing calls,
/// the budget exhausts and the tracker transitions to `Unhealthy`.
/// Pins the documented "K against budget" choice.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn parallel_call_tools_k3_all_fail_consumes_k_budget_slots_and_trips_unhealthy() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50)).await;
    let registry = registry_with_flaky("flaky", u32::MAX);

    let script = vec![
        // K=3 all-failing batch. With max_tool=2, three failures
        // exceed the budget on the third recorded slot.
        Decision::CallTools {
            calls: vec![
                ToolCall::new("flaky", json!({"i": 1}), ClaimSeed::new("seed-1")),
                ToolCall::new("flaky", json!({"i": 2}), ClaimSeed::new("seed-2")),
                ToolCall::new("flaky", json!({"i": 3}), ClaimSeed::new("seed-3")),
            ],
        },
        Decision::Retire {
            reason: "after-batch-exhaustion".into(),
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
    assert_eq!(reason, "after-batch-exhaustion");

    // K=3 failures with max_tool=2 trips Unhealthy. The archived
    // incident's retry trail captures all three failures so audit sees
    // the whole batch even though only the third one tripped the
    // budget.
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

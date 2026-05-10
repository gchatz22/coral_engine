//! Integration tests for the `Agent` run loop.
//!
//! These tests exercise the public surface only (`Agent::new`,
//! `Agent::signal`, `Agent::run`) plus the FS root the agent writes to.
//! They cover:
//!
//! * **JAR2-8** verification list (`scratch/minimal_node_backend.md` § 7):
//!   wakes on signal, wakes on deadline, `EmitOutput` with valid evidence
//!   writes a file, `Retire` exits cleanly, `RewriteFs` writes under
//!   `notes/`, `CallTool` records evidence, `max_ticks` caps the loop.
//! * **JAR2-19** synthetic-trigger correction loop and health
//!   transitions: `EmitOutput` with empty / unknown evidence and
//!   `CallTool` for an unregistered tool now route through
//!   `ApplyOutcome::NeedsCorrection` and inject a synthetic-correction
//!   trigger; persistent failure exhausts the inference budget and flips
//!   the tracker to `Unhealthy`; the next successful tick recovers and
//!   archives the prior incident; Decide-side `Err` transitions to
//!   `Unhealthy` directly while keeping the run loop alive.
//!
//! Time-sensitive tests use `#[tokio::test(flavor = "current_thread",
//! start_paused = true)]` so the runtime auto-advances when the only
//! pending task is a sleep, making the deadline-arm tests deterministic.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;

use jarvis_node::agent::{Agent, RetireReason};
use jarvis_node::decision::{ClaimSeed, Decision, FsOp, MockDecide};
use jarvis_node::evidence::{EvidenceId, EvidenceRecord};
use jarvis_node::fs::AgentFs;
use jarvis_node::health::{HealthTracker, RetryBudget};
use jarvis_node::mandate::Mandate;
use jarvis_node::tools::{EchoTool, ToolRegistry};
use jarvis_node::trigger::Trigger;

fn fresh_fs(idle_period: Duration) -> (TempDir, AgentFs, Mandate) {
    let tmp = TempDir::new().expect("tempdir");
    let mandate = Mandate::new("loop smoke", idle_period, None);
    let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate).expect("open fs");
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

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn loop_wakes_on_injected_signal_and_retires() {
    // Idle period is huge so the test relies on the signal, not the
    // scheduled wake, to drive the first tick.
    let (tmp, fs, mandate) = fresh_fs(Duration::from_secs(3600));
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
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50));
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
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50));
    // Pre-seed an evidence record on disk so the EmitOutput's evidence id
    // resolves. Computing the id here keeps the test independent of how
    // the agent would have produced it.
    let rec = EvidenceRecord::new(
        "echo",
        json!({"msg": "hi"}),
        json!({"echoed": {"msg": "hi"}}),
        chrono::Utc::now(),
    );
    let ev_id: EvidenceId = fs.record_evidence(rec).expect("seed evidence");

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
    let entries: Vec<_> = std::fs::read_dir(&outputs_dir)
        .expect("read outputs")
        .map(|e| e.expect("dirent").path())
        .collect();
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
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50));
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
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50));
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
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50));
    // Compute the id we expect echo to produce so we can reference it in
    // the EmitOutput decision.
    let args = json!({"msg": "hi"});
    let result = json!({"echoed": {"msg": "hi"}});
    let expected_ev = EvidenceId::new("echo", &args, &result);

    let script = vec![
        Decision::CallTool {
            name: "echo".into(),
            args: args.clone(),
            claim_seed: ClaimSeed::new("seed-1"),
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
    let entries: Vec<_> = std::fs::read_dir(&outputs_dir)
        .expect("read outputs")
        .map(|e| e.expect("dirent").path())
        .collect();
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
    let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate).expect("open fs");

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

// ---- JAR2-19: synthetic-trigger correction loop + health transitions ----

/// JAR2-19 acceptance test 1: model emits an unsatisfiable `Decision`
/// (`CallTool` for an unregistered tool); the runtime must catch the
/// apply-time failure, inject a corrective synthetic trigger into the
/// queue, and the next tick must produce a valid `Decision` that
/// completes. The agent stays Healthy throughout.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn invalid_call_tool_injects_synthetic_correction_then_recovers() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50));
    let script = vec![
        // Tick 1: model picks a tool that is not registered. Apply-time
        // failure → record_failure(Inference) → counter=1 (under default
        // budget of 1) → synthetic correction injected for the next tick.
        Decision::CallTool {
            name: "no_such_tool".into(),
            args: json!({"x": 1}),
            claim_seed: ClaimSeed::new("seed-1"),
        },
        // Tick 2: synthetic correction trigger arrives; script's next
        // decision is Retire. dispatch returns Retire → loop exits.
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
    if evidence_dir.exists() {
        let entries: Vec<_> = std::fs::read_dir(&evidence_dir)
            .expect("read evidence")
            .collect();
        assert!(
            entries.is_empty(),
            "no evidence should have been recorded for an unregistered tool"
        );
    }

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
/// `AgentFs::persist_output` with `FsError::EmptyEvidence`. Per JAR2-19,
/// that maps to `ApplyOutcome::NeedsCorrection` rather than the legacy
/// "log + continue" warn — the next iteration pulls the synthetic
/// correction trigger and the script's second decision retires.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn emit_output_with_empty_evidence_injects_synthetic_correction() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50));
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

    // No output file written (the failed EmitOutput never persisted).
    let outputs_dir = tmp.path().join("outputs");
    assert!(std::fs::read_dir(&outputs_dir)
        .expect("read outputs")
        .next()
        .is_none());

    // Stayed Healthy — single failure under the default budget.
    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(v.get("state").and_then(|x| x.as_str()), Some("Healthy"));
}

/// Same shape of failure but driven through the `EmitOutput` arm with a
/// well-formed-but-not-on-disk evidence id (`FsError::EvidenceNotFound`).
/// Mirrors the empty-evidence test above; the two cover the two distinct
/// `FsError` variants the apply-time correction path catches.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn emit_output_with_unknown_evidence_injects_synthetic_correction() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50));
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

    // No output file written (the failed EmitOutput never persisted).
    let outputs_dir = tmp.path().join("outputs");
    assert!(std::fs::read_dir(&outputs_dir)
        .expect("read outputs")
        .next()
        .is_none());

    // Stayed Healthy — single failure under the default budget.
    let v: serde_json::Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(v.get("state").and_then(|x| x.as_str()), Some("Healthy"));
}

/// JAR2-19 acceptance test 2: persistent apply-time failure exhausts the
/// per-tick inference budget across the original attempt + one
/// synthetic-correction continuation. The agent transitions to
/// `Unhealthy`, the run loop **does not halt**, and a subsequent
/// successful tick (here, an `Idle` decision) recovers the tracker to
/// `Healthy` while archiving the prior incident.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn persistent_apply_time_failure_exhausts_budget_and_recovers_on_next_success() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50));
    // Default budget is `RetryBudget::new(1, 3)` → max_inference = 1, so
    // total apply-time attempts before exhaustion = 2 (original + 1
    // retry inside the same fresh-tick window).
    let script = vec![
        // Attempt 1 (fresh tick): bad CallTool → record_failure ok →
        // synthetic correction injected.
        Decision::CallTool {
            name: "no_such_tool".into(),
            args: json!({}),
            claim_seed: ClaimSeed::new("seed-1"),
        },
        // Attempt 2 (correction continuation tick — begin_tick skipped):
        // bad CallTool → counter=2 > max_inference=1 → BudgetExhausted →
        // transition_to_unhealthy. Run loop does NOT exit.
        Decision::CallTool {
            name: "no_such_tool".into(),
            args: json!({}),
            claim_seed: ClaimSeed::new("seed-1"),
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
        "apply-time failures count as inference failures per JAR2-19"
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
    let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate).expect("open fs");

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
        "Decide-Err is an inference failure per JAR2-19"
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

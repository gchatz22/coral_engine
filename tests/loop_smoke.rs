//! Integration tests for the JAR2-8 `Agent` run loop.
//!
//! These tests exercise the public surface only (`Agent::new`,
//! `Agent::signal`, `Agent::run`) plus the FS root the agent writes to.
//! They cover the verification list from
//! `scratch/minimal_node_backend.md` § 7 / the JAR2-8 ticket:
//!
//! * Wakes on injected signal before deadline.
//! * Wakes on deadline if no signal (drains a `ScheduledWake`).
//! * `EmitOutput` with valid evidence → file under `outputs/`.
//! * `EmitOutput` with empty evidence → loop logs warn, agent stays alive.
//! * `EmitOutput` referencing a nonexistent evidence id → same.
//! * `Retire` exits cleanly; `retirement.json` written.
//! * `RewriteFs` writes the expected file under `notes/`.
//!
//! Time-sensitive tests use `#[tokio::test(flavor = "current_thread",
//! start_paused = true)]` so the runtime auto-advances when the only
//! pending task is a sleep, making the deadline-arm tests deterministic.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;

use jarvis_node::agent::{Agent, RetireReason};
use jarvis_node::decision::{ClaimSeed, Decision, FsOp, MockDecide};
use jarvis_node::evidence::{EvidenceId, EvidenceRecord};
use jarvis_node::fs::AgentFs;
use jarvis_node::mandate::Mandate;
use jarvis_node::tools::{EchoTool, ToolRegistry};
use jarvis_node::trigger::Trigger;

fn fresh_fs(idle_period: Duration) -> (TempDir, AgentFs, Mandate) {
    let tmp = TempDir::new().expect("tempdir");
    let mandate = Mandate::new("loop smoke", idle_period, None);
    let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate).expect("open fs");
    (tmp, fs, mandate)
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
    let agent = Agent::new(mandate, fs, MockDecide::new(script), registry_with_echo());
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
    let agent = Agent::new(mandate, fs, MockDecide::new(script), registry_with_echo());

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
    let agent = Agent::new(mandate, fs, MockDecide::new(script), registry_with_echo());

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
async fn emit_output_with_empty_evidence_keeps_agent_alive() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50));
    // First decision violates provenance (empty evidence). The loop must
    // log and continue. The next deadline pushes ScheduledWake, and the
    // second decision retires — proving the agent stayed alive.
    let script = vec![
        Decision::EmitOutput {
            content: "no provenance".into(),
            evidence: vec![],
        },
        Decision::Retire {
            reason: "still-alive".into(),
        },
    ];
    let agent = Agent::new(mandate, fs, MockDecide::new(script), registry_with_echo());

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "still-alive");

    // Provenance violation must not have produced an outputs/ file.
    let outputs_dir = tmp.path().join("outputs");
    let entries: Vec<_> = std::fs::read_dir(&outputs_dir)
        .expect("read outputs")
        .collect();
    assert!(
        entries.is_empty(),
        "no output should be written on empty-evidence violation"
    );
    assert!(tmp.path().join("retirement.json").is_file());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn emit_output_with_unknown_evidence_keeps_agent_alive() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50));
    // 64-hex bogus id — well-formed shape, no file on disk.
    let bogus = EvidenceId::from_hex("deadbeef".repeat(8));
    let script = vec![
        Decision::EmitOutput {
            content: "lying about provenance".into(),
            evidence: vec![bogus],
        },
        Decision::Retire {
            reason: "still-alive".into(),
        },
    ];
    let agent = Agent::new(mandate, fs, MockDecide::new(script), registry_with_echo());

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(5), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "still-alive");

    let outputs_dir = tmp.path().join("outputs");
    let entries: Vec<_> = std::fs::read_dir(&outputs_dir)
        .expect("read outputs")
        .collect();
    assert!(entries.is_empty());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn retire_writes_retirement_json_and_returns_reason() {
    let (tmp, fs, mandate) = fresh_fs(Duration::from_millis(50));
    let script = vec![Decision::Retire {
        reason: "graceful exit".into(),
    }];
    let agent = Agent::new(mandate, fs, MockDecide::new(script), registry_with_echo());

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
    let agent = Agent::new(mandate, fs, MockDecide::new(script), registry_with_echo());

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
    let agent = Agent::new(mandate, fs, MockDecide::new(script), registry_with_echo());

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
    let agent = Agent::new(mandate, fs, MockDecide::new(script), registry_with_echo());

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

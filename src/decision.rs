//! `Decision` — what the agent wants the runtime to do next.
//!
//! Pure data; the run loop (a later ticket) is what actually executes
//! these. Every variant carries enough information that a Decision can be
//! serialized, replayed, or audited without the original `Decide`
//! implementation in hand.
//!
//! `ClaimSeed` and `FsOp` live here because they exist only as parameters
//! to specific `Decision` variants; promoting them to their own modules
//! would be premature.
//!
//! This module also hosts the JAR2-6 surface area:
//!
//! * `ContextBundle` — the read-only snapshot the loop hands to a `Decide`
//!   implementation each tick.
//! * `Decide` — the trait every model adapter (mock, real LLM, etc.)
//!   implements; the run loop will only ever talk to this trait.
//! * `MockDecide` — a scripted `Decide` for tests.
//! * `assemble_context` — a plain async fn that reads the per-agent FS and
//!   packages the bundle. Kept as a free function for the bootstrap; it
//!   can graduate to its own trait once real LLM activities arrive.

use crate::evidence::{EvidenceId, EvidenceRecord};
use crate::fs::AgentFs;
use crate::mandate::{Mandate, Output};
use crate::trigger::Trigger;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

/// What the agent has decided to do this tick.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Decision {
    /// Invoke a tool by name with JSON args. `claim_seed` is an opaque
    /// hint the agent attaches so the resulting evidence can be linked to
    /// the claim it was meant to support.
    CallTool {
        name: String,
        args: serde_json::Value,
        claim_seed: ClaimSeed,
    },
    /// Emit a finished output. The run loop will refuse to persist an
    /// output whose evidence ids don't all resolve.
    EmitOutput {
        content: String,
        evidence: Vec<EvidenceId>,
    },
    /// Mutate the per-agent filesystem.
    RewriteFs { ops: Vec<FsOp> },
    /// Tell the scheduler to wait at least `next_after` before the next
    /// idle wake.
    Idle {
        #[serde(with = "crate::duration_ms")]
        next_after: Duration,
    },
    /// Stop running. The reason is persisted so retirement is auditable.
    Retire { reason: String },
}

/// Opaque seed used to deterministically derive a claim id from a tool
/// call. The kernel doesn't interpret it; it's a string the agent picks.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClaimSeed(pub String);

impl ClaimSeed {
    pub fn new(s: impl Into<String>) -> Self {
        ClaimSeed(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A single mutation against the per-agent filesystem. Paths are relative
/// to the agent's FS root; the FS layer (a later ticket) is responsible
/// for sandboxing and validation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum FsOp {
    /// Create or overwrite a file with `content`.
    WriteFile { path: String, content: String },
    /// Remove a file. Idempotent at the type level; the FS layer decides
    /// what to do if the path is absent.
    DeleteFile { path: String },
}

/// Snapshot the run loop hands to `Decide::decide` once per tick.
///
/// Owned data (not borrows) so an implementation can move the bundle into
/// an async task, queue it, or serialize it for audit/replay without
/// fighting lifetimes. The bootstrap shape mirrors the ticket spec
/// verbatim: mandate + the triggers that woke us + a small slice of recent
/// FS state. Trim fields here only when one is unambiguously dead.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContextBundle {
    pub mandate: Mandate,
    pub triggers: Vec<Trigger>,
    pub recent_outputs: Vec<Output>,
    pub recent_evidence: Vec<EvidenceRecord>,
}

/// Trait every model adapter (mock, real LLM, deterministic policy)
/// implements. The run loop (JAR2-8) talks to nothing else when it needs
/// "what should the agent do next?" — that constraint is the whole point
/// of this trait.
///
/// `Send + Sync` are required because the loop owns its `Decide`
/// implementation behind shared state (typically `Arc<dyn Decide>`) and
/// awaits across `.await` points on a multi-threaded runtime.
#[async_trait::async_trait]
pub trait Decide: Send + Sync {
    async fn decide(&self, ctx: ContextBundle) -> anyhow::Result<Decision>;
}

/// Scripted `Decide` for tests: pops decisions from a queue in FIFO order.
///
/// We use `std::sync::Mutex<VecDeque<Decision>>` rather than the tokio
/// flavor because the body of `decide` is purely synchronous (pop, return)
/// — no `.await` is held while the lock is taken, so the std mutex is
/// strictly cheaper and keeps the trait body free of an unnecessary
/// async-runtime dependency at the lock point.
///
/// When the script is exhausted, `decide` returns an error rather than
/// panicking so a misconfigured test surfaces as a normal `Result` failure
/// the harness can report.
pub struct MockDecide {
    script: Mutex<VecDeque<Decision>>,
}

impl MockDecide {
    /// Build a mock from a script of decisions to return, in order.
    pub fn new(script: Vec<Decision>) -> Self {
        Self {
            script: Mutex::new(script.into()),
        }
    }

    /// Number of decisions still queued. Useful in tests that want to
    /// assert the loop drained the script.
    pub fn remaining(&self) -> usize {
        self.script.lock().expect("MockDecide mutex poisoned").len()
    }
}

#[async_trait::async_trait]
impl Decide for MockDecide {
    async fn decide(&self, _ctx: ContextBundle) -> anyhow::Result<Decision> {
        let mut q = self.script.lock().expect("MockDecide mutex poisoned");
        q.pop_front()
            .ok_or_else(|| anyhow!("MockDecide script exhausted"))
    }
}

/// Bootstrap context window. The run loop reads at most this many recent
/// outputs and evidence records into each `ContextBundle`.
///
/// Eight is the value called out by the ticket; if it ever needs tuning,
/// promote to a field on `Mandate` or a kernel config rather than passing
/// it through this signature.
///
/// # Why a fixed window today (v1 vs v2)
///
/// This is bootstrap scaffolding, not the long-term shape of context
/// assembly. The v2 design (tracked in JAR2-10, "context-assembly v2")
/// splits context into two complementary paths:
///
/// 1. **Warm cache** — what `assemble_context` grows into. Small,
///    mandate-shaped, assembled by the runtime each tick and passed
///    unconditionally into `Decide::decide`: current mandate, triggers,
///    last few outputs, tail of conflict log. Shrinks vs. today, but
///    doesn't go to zero — re-discovering "what was I doing" via tool
///    calls every tick burns latency and cost on context most ticks
///    need anyway.
///
/// 2. **Self-directed retrieval** — tools the agent calls during
///    `decide` (`read_file`, `list_dir`, eventually a semantic-search
///    MCP server over the agent's own FS). The agent picks what to
///    pull. This is the FS-as-RAG path VISION § 4–5 calls for and what
///    `agent_runtime.md` § 6 means by "mandate-specific
///    selection/distillation".
///
/// `RECENT_WINDOW = 8` is a placeholder for piece (1): small enough
/// that the bundle stays cheap, big enough to be non-empty for the
/// MockDecide-driven loop tests in JAR2-7/8. The real policy (window
/// per mandate, time-vs-filename ordering, indexed reads, the
/// warm-cache/tools cut line) is empirical and waits on real model
/// latency + MCP roundtrip data, which we won't have until those
/// tickets land.
const RECENT_WINDOW: usize = 8;

/// Read FS state and package a `ContextBundle` for the given triggers.
///
/// For the bootstrap this is a plain async fn (per the ticket's plan
/// slice). Determinism: outputs and evidence are read via the FS helpers,
/// which sort filenames lexically and return the last `RECENT_WINDOW`
/// entries — see `AgentFs::list_recent_outputs` /
/// `AgentFs::list_recent_evidence`.
pub async fn assemble_context(
    fs: &AgentFs,
    triggers: &[Trigger],
    cfg: &Mandate,
) -> anyhow::Result<ContextBundle> {
    let recent_outputs = fs.list_recent_outputs(RECENT_WINDOW)?;
    let recent_evidence = fs.list_recent_evidence(RECENT_WINDOW)?;
    Ok(ContextBundle {
        mandate: cfg.clone(),
        triggers: triggers.to_vec(),
        recent_outputs,
        recent_evidence,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evidence::EvidenceId;
    use serde_json::json;

    #[test]
    fn call_tool_round_trip() {
        let d = Decision::CallTool {
            name: "echo".into(),
            args: json!({"msg": "hi"}),
            claim_seed: ClaimSeed::new("seed-1"),
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn emit_output_round_trip_non_empty_evidence() {
        let ev1 = EvidenceId::new("echo", &json!({"a": 1}), &json!({"r": 1}));
        let ev2 = EvidenceId::new("echo", &json!({"a": 2}), &json!({"r": 2}));
        let d = Decision::EmitOutput {
            content: "hello".into(),
            evidence: vec![ev1.clone(), ev2.clone()],
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
        if let Decision::EmitOutput { evidence, .. } = back {
            assert_eq!(evidence, vec![ev1, ev2]);
        } else {
            panic!("expected EmitOutput");
        }
    }

    #[test]
    fn emit_output_round_trip_empty_evidence() {
        let d = Decision::EmitOutput {
            content: "draft".into(),
            evidence: vec![],
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
        if let Decision::EmitOutput { evidence, .. } = back {
            assert!(evidence.is_empty());
        } else {
            panic!("expected EmitOutput");
        }
    }

    #[test]
    fn rewrite_fs_round_trip() {
        let d = Decision::RewriteFs {
            ops: vec![
                FsOp::WriteFile {
                    path: "notes/a.md".into(),
                    content: "hi".into(),
                },
                FsOp::DeleteFile {
                    path: "notes/old.md".into(),
                },
            ],
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn idle_round_trip_serializes_duration_as_ms() {
        let d = Decision::Idle {
            next_after: Duration::from_millis(2500),
        };
        let s = serde_json::to_string(&d).unwrap();
        assert!(s.contains("\"next_after\":2500"), "got {s}");
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn retire_round_trip() {
        let d = Decision::Retire {
            reason: "done".into(),
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn claim_seed_is_transparent() {
        let cs = ClaimSeed::new("abc");
        let s = serde_json::to_string(&cs).unwrap();
        assert_eq!(s, "\"abc\"");
        let back: ClaimSeed = serde_json::from_str(&s).unwrap();
        assert_eq!(back, cs);
        assert_eq!(back.as_str(), "abc");
    }

    #[test]
    fn fs_op_round_trip() {
        let op = FsOp::WriteFile {
            path: "p".into(),
            content: "c".into(),
        };
        let s = serde_json::to_string(&op).unwrap();
        assert!(s.contains("\"op\":\"write_file\""));
        let back: FsOp = serde_json::from_str(&s).unwrap();
        assert_eq!(op, back);

        let del = FsOp::DeleteFile { path: "p".into() };
        let s2 = serde_json::to_string(&del).unwrap();
        assert!(s2.contains("\"op\":\"delete_file\""));
        let back2: FsOp = serde_json::from_str(&s2).unwrap();
        assert_eq!(del, back2);
    }

    // ---- JAR2-6: Decide / MockDecide / assemble_context ---------------------

    use crate::evidence::EvidenceRecord;
    use crate::mandate::Mandate;
    use crate::trigger::Trigger;
    use chrono::{DateTime, Utc};
    use tempfile::TempDir;

    fn ts() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-05-03T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn dummy_mandate() -> Mandate {
        Mandate::new("research foo", Duration::from_millis(1000), Some(10))
    }

    fn dummy_bundle() -> ContextBundle {
        ContextBundle {
            mandate: dummy_mandate(),
            triggers: vec![],
            recent_outputs: vec![],
            recent_evidence: vec![],
        }
    }

    #[tokio::test]
    async fn mock_decide_returns_scripted_decisions_in_order() {
        let script = vec![
            Decision::Idle {
                next_after: Duration::from_millis(50),
            },
            Decision::Retire {
                reason: "done".into(),
            },
        ];
        let mock = MockDecide::new(script.clone());
        assert_eq!(mock.remaining(), 2);

        let first = mock.decide(dummy_bundle()).await.unwrap();
        assert_eq!(first, script[0]);
        assert_eq!(mock.remaining(), 1);

        let second = mock.decide(dummy_bundle()).await.unwrap();
        assert_eq!(second, script[1]);
        assert_eq!(mock.remaining(), 0);
    }

    #[tokio::test]
    async fn mock_decide_errors_when_script_exhausted() {
        let mock = MockDecide::new(vec![]);
        let err = mock.decide(dummy_bundle()).await.unwrap_err();
        assert!(
            err.to_string().contains("script exhausted"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn mock_decide_is_object_safe_via_dyn_decide() {
        // Compile-time check: Decide is dyn-compatible. The run loop will
        // hold an `Arc<dyn Decide>`, so this property is load-bearing.
        let mock: Box<dyn Decide> = Box::new(MockDecide::new(vec![Decision::Retire {
            reason: "ok".into(),
        }]));
        let d = mock.decide(dummy_bundle()).await.unwrap();
        assert!(matches!(d, Decision::Retire { .. }));
    }

    #[tokio::test]
    async fn assemble_context_includes_passed_in_triggers_verbatim() {
        let tmp = TempDir::new().unwrap();
        let mandate = dummy_mandate();
        let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate).unwrap();

        let triggers = vec![
            Trigger::ScheduledWake,
            Trigger::External {
                kind: "webhook".into(),
                payload: serde_json::json!({"x": 1}),
            },
        ];

        let bundle = assemble_context(&fs, &triggers, &mandate).await.unwrap();
        assert_eq!(bundle.triggers, triggers);
        assert_eq!(bundle.mandate, mandate);
        assert!(bundle.recent_outputs.is_empty());
        assert!(bundle.recent_evidence.is_empty());
    }

    #[tokio::test]
    async fn assemble_context_reads_outputs_and_evidence_deterministically() {
        let tmp = TempDir::new().unwrap();
        let mandate = dummy_mandate();
        let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate).unwrap();

        // Seed some evidence and outputs. We write more than RECENT_WINDOW
        // of each so the windowing path is also exercised.
        let mut ev_ids = Vec::new();
        for i in 0..(RECENT_WINDOW + 2) {
            let rec = EvidenceRecord::new(
                "echo",
                serde_json::json!({ "i": i }),
                serde_json::json!({ "echoed": i }),
                ts(),
            );
            let id = fs.record_evidence(rec).unwrap();
            ev_ids.push(id);
        }
        for (i, id) in ev_ids.iter().enumerate() {
            fs.persist_output(&format!("out-{i}"), &[id.clone()])
                .unwrap();
        }

        let triggers = vec![Trigger::ScheduledWake];
        let a = assemble_context(&fs, &triggers, &mandate).await.unwrap();
        let b = assemble_context(&fs, &triggers, &mandate).await.unwrap();

        // Determinism across calls: same inputs, same output order.
        assert_eq!(a.recent_outputs, b.recent_outputs);
        assert_eq!(a.recent_evidence, b.recent_evidence);

        // Window is honored.
        assert_eq!(a.recent_outputs.len(), RECENT_WINDOW);
        assert_eq!(a.recent_evidence.len(), RECENT_WINDOW);

        // Sanity: evidence records sort by their (hex) id; outputs by their
        // ulid filename. Both should be ascending, so the last entry is the
        // lexically greatest filename present on disk.
        let mut ev_sorted = a
            .recent_evidence
            .iter()
            .map(|r| r.id.as_str().to_string())
            .collect::<Vec<_>>();
        let original = ev_sorted.clone();
        ev_sorted.sort();
        assert_eq!(original, ev_sorted, "evidence not in sorted order");
    }
}

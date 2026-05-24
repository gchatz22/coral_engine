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
use crate::fs::{AgentFs, Claim, ClaimStatus};
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
    /// Invoke one or more tools in parallel for the same tick. A
    /// one-element vector preserves the original single-call semantics.
    /// The run loop dispatches every entry in the same tick, persists one
    /// `EvidenceRecord` per call, and stages the paired `tool_result`
    /// blocks for the next prompt bundle.
    ///
    /// Struct (not tuple) variant because serde's
    /// `#[serde(tag = "type")]` on this enum rejects tagged newtype
    /// variants carrying a sequence — `CallTools(Vec<ToolCall>)` would
    /// emit cleanly but fail to deserialize back. The named field keeps
    /// the wire form `{"type":"call_tools","calls":[...]}` flat and
    /// round-trippable.
    CallTools { calls: Vec<ToolCall> },
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

/// One element of a `Decision::CallTools`. `claim_seed` is an opaque
/// hint the agent attaches so the resulting evidence can be linked to
/// the claim it was meant to support. `tool_use_id` is the vendor's
/// `tool_use.id` from the model response that introduced this call —
/// the kernel propagates it back through the next prompt's
/// `tool_result` blocks so the assistant turn's `tool_use` ids stay
/// paired with their results across the tick boundary, per both
/// Anthropic's and Cohere's tool-use protocols.
///
/// Named `decision::ToolCall` to avoid confusion with the vendor-wire
/// `model_client::ToolCall` shape (`{ id, name, arguments }`). The two
/// types are deliberately distinct: this one is the kernel's intent;
/// the vendor one is the parsed wire payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCall {
    pub name: String,
    pub args: serde_json::Value,
    pub claim_seed: ClaimSeed,
    /// Vendor-supplied `tool_use.id` for the model response that emitted
    /// this call. `None` when the decision was synthesized by a non-model
    /// `Decide` (e.g. `MockDecide` in tests), in which case no
    /// `tool_result` pairing is required because there is no preceding
    /// assistant `tool_use` block to answer.
    ///
    /// Carried through the parser today; the current cross-tick prompt
    /// path conveys tool results via `recent_evidence` (text bullets in
    /// the next bundle), not via `tool_use`/`tool_result` block pairing.
    /// This field is reserved for the future cross-tick rendering path
    /// that would emit paired blocks across the tick boundary — kept on
    /// the wire today so a fixture or replay captured at this version
    /// stays useful after that path lands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_use_id: Option<String>,
}

impl ToolCall {
    /// Build a `ToolCall` with no vendor `tool_use_id`. Convenience for
    /// tests and non-model `Decide` impls that don't have one to thread
    /// through.
    pub fn new(name: impl Into<String>, args: serde_json::Value, claim_seed: ClaimSeed) -> Self {
        Self {
            name: name.into(),
            args,
            claim_seed,
            tool_use_id: None,
        }
    }

    /// Build a `ToolCall` with a vendor-supplied `tool_use.id`. Used by
    /// the `LlmDecide` parser when it has the id available from the
    /// model's response.
    pub fn with_tool_use_id(
        name: impl Into<String>,
        args: serde_json::Value,
        claim_seed: ClaimSeed,
        tool_use_id: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            args,
            claim_seed,
            tool_use_id: Some(tool_use_id.into()),
        }
    }
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
///
/// `correction` is `Some` when this tick is a continuation of a prior
/// attempt the runtime rejected (an unsatisfiable `Decision`). It carries
/// a human-readable description of why the previous attempt failed; the
/// prompt renderer surfaces it as a distinct section so the model can
/// self-correct. See `agent.rs` for the budget-accounting contract that
/// pairs with this field.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContextBundle {
    pub mandate: Mandate,
    pub triggers: Vec<Trigger>,
    pub recent_outputs: Vec<Output>,
    pub recent_evidence: Vec<EvidenceRecord>,
    /// Open claims (`claims/<slug>.json` with `status == Open`), capped by
    /// `mandate.context_policy.open_claims_max`. Surfaced in the warm cache
    /// so the seed-reuse convention (JAR2-28 / `scratch/claim_seed_persistence.md`)
    /// works without a tool roundtrip every tick. Order is inherited from
    /// `AgentFs::list_claims` (filename ascending) per
    /// `scratch/context_assembly_v2.md` § 8. `#[serde(default)]` keeps the
    /// wire format backward-compatible with pre-JAR2-36 bundles.
    #[serde(default)]
    pub open_claims: Vec<Claim>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correction: Option<CorrectionContext>,
}

/// "Your previous decision was unsatisfiable; here is why" — agent-internal
/// continuation state, distinct from the trigger stream that represents
/// outside-world events.
///
/// Set by the run loop when a `Decision` parses cleanly but cannot be
/// applied (an unregistered tool, an unresolvable evidence id). The next
/// tick threads it into the `ContextBundle` so the model gets a chance to
/// emit a satisfiable `Decision`. Kept off the trigger queue on purpose —
/// see `agent.rs`'s module doc for the rationale.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorrectionContext {
    /// Human-readable description of the failure that prompted this
    /// correction. Echoed to the model in the rendered prompt and stamped
    /// into the `HealthIncident` retry trail.
    pub failure: String,
}

impl CorrectionContext {
    pub fn new(failure: impl Into<String>) -> Self {
        Self {
            failure: failure.into(),
        }
    }
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

/// Read FS state and package a `ContextBundle` for the given triggers.
///
/// Window sizes are drawn from `cfg.context_policy`
/// (`crate::mandate::ContextPolicy`): per-mandate tuning replaces the
/// JAR2-6 hardcoded `RECENT_WINDOW = 8` constant retired in JAR2-36.
/// Defaults reproduce the pre-JAR2-36 behavior so existing graphs are
/// unaffected.
///
/// Determinism: outputs and evidence are read via the FS helpers, which
/// sort filenames lexically and return the last N entries — see
/// `AgentFs::list_recent_outputs` / `AgentFs::list_recent_evidence`. Open
/// claims are drawn from `AgentFs::list_claims` in its native filename
/// order; phase 1 inherits that ordering per
/// `scratch/context_assembly_v2.md` § 8.
///
/// `correction` carries continuation state from the run loop when the
/// previous tick produced an unsatisfiable `Decision`. When `Some`, the
/// rendered prompt surfaces it as a dedicated section.
pub async fn assemble_context(
    fs: &AgentFs,
    triggers: &[Trigger],
    cfg: &Mandate,
    correction: Option<CorrectionContext>,
) -> anyhow::Result<ContextBundle> {
    let policy = &cfg.context_policy;
    let recent_outputs = fs.list_recent_outputs(policy.recent_outputs).await?;
    let recent_evidence = fs.list_recent_evidence(policy.recent_evidence).await?;
    let open_claims: Vec<Claim> = fs
        .list_claims()
        .await?
        .into_iter()
        .filter(|c| c.status == ClaimStatus::Open)
        .take(policy.open_claims_max)
        .collect();
    // Field counts feed the JAR2-36 measurement spike + ongoing
    // observability — keeps the warm cache shape inspectable without
    // dumping potentially large bodies into the log.
    tracing::debug!(
        triggers = triggers.len(),
        recent_outputs = recent_outputs.len(),
        recent_evidence = recent_evidence.len(),
        open_claims = open_claims.len(),
        correction = correction.is_some(),
        "assemble_context bundle field counts"
    );
    Ok(ContextBundle {
        mandate: cfg.clone(),
        triggers: triggers.to_vec(),
        recent_outputs,
        recent_evidence,
        open_claims,
        correction,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evidence::EvidenceId;
    use serde_json::json;

    #[test]
    fn call_tools_single_call_round_trip() {
        let d = Decision::CallTools {
            calls: vec![ToolCall::new(
                "echo",
                json!({"msg": "hi"}),
                ClaimSeed::new("seed-1"),
            )],
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
        // tool_use_id absent on the wire when None (skip_serializing_if).
        assert!(!s.contains("tool_use_id"), "wire shape: {s}");
        // Tagged enum: tag stays `call_tools` and the vec lives under
        // `calls` — the field name is wire-stable.
        assert!(s.contains("\"type\":\"call_tools\""), "wire shape: {s}");
        assert!(s.contains("\"calls\":["), "wire shape: {s}");
    }

    #[test]
    fn call_tools_multi_call_round_trip() {
        let d = Decision::CallTools {
            calls: vec![
                ToolCall::with_tool_use_id(
                    "read_a",
                    json!({"path": "a.md"}),
                    ClaimSeed::new("seed-a"),
                    "toolu_a",
                ),
                ToolCall::with_tool_use_id(
                    "read_b",
                    json!({"path": "b.md"}),
                    ClaimSeed::new("seed-b"),
                    "toolu_b",
                ),
                ToolCall::with_tool_use_id(
                    "read_c",
                    json!({"path": "c.md"}),
                    ClaimSeed::new("seed-c"),
                    "toolu_c",
                ),
            ],
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&s).unwrap();
        assert_eq!(d, back);
        if let Decision::CallTools { calls } = back {
            assert_eq!(calls.len(), 3);
            assert_eq!(calls[0].tool_use_id.as_deref(), Some("toolu_a"));
            assert_eq!(calls[2].name, "read_c");
        } else {
            panic!("expected CallTools");
        }
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
            open_claims: vec![],
            correction: None,
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
        let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
            .await
            .unwrap();

        let triggers = vec![
            Trigger::ScheduledWake,
            Trigger::External {
                kind: "webhook".into(),
                payload: serde_json::json!({"x": 1}),
            },
        ];

        let bundle = assemble_context(&fs, &triggers, &mandate, None)
            .await
            .unwrap();
        assert_eq!(bundle.triggers, triggers);
        assert_eq!(bundle.mandate, mandate);
        assert!(bundle.recent_outputs.is_empty());
        assert!(bundle.recent_evidence.is_empty());
        assert!(bundle.correction.is_none());
    }

    #[tokio::test]
    async fn assemble_context_threads_correction_into_bundle() {
        let tmp = TempDir::new().unwrap();
        let mandate = dummy_mandate();
        let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
            .await
            .unwrap();

        let correction = CorrectionContext::new("call_tool: no tool registered under name \"x\"");
        let bundle = assemble_context(&fs, &[], &mandate, Some(correction.clone()))
            .await
            .unwrap();
        assert_eq!(bundle.correction.as_ref(), Some(&correction));
    }

    #[tokio::test]
    async fn assemble_context_reads_outputs_and_evidence_deterministically() {
        let tmp = TempDir::new().unwrap();
        let mandate = dummy_mandate();
        let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
            .await
            .unwrap();

        // Seed more than the default window so the windowing path is also
        // exercised. The default `ContextPolicy::recent_outputs` /
        // `recent_evidence` is 8 (pinned in `mandate.rs::tests`); we write
        // two extra of each.
        let default_window = mandate.context_policy.recent_outputs;
        assert_eq!(default_window, mandate.context_policy.recent_evidence);
        let mut ev_ids = Vec::new();
        for i in 0..(default_window + 2) {
            let rec = EvidenceRecord::new(
                "echo",
                serde_json::json!({ "i": i }),
                serde_json::json!({ "echoed": i }),
                ts(),
            );
            let id = fs.record_evidence(rec).await.unwrap();
            ev_ids.push(id);
        }
        for (i, id) in ev_ids.iter().enumerate() {
            fs.persist_output(&format!("out-{i}"), &[id.clone()])
                .await
                .unwrap();
        }

        let triggers = vec![Trigger::ScheduledWake];
        let a = assemble_context(&fs, &triggers, &mandate, None)
            .await
            .unwrap();
        let b = assemble_context(&fs, &triggers, &mandate, None)
            .await
            .unwrap();

        // Determinism across calls: same inputs, same output order.
        assert_eq!(a.recent_outputs, b.recent_outputs);
        assert_eq!(a.recent_evidence, b.recent_evidence);
        assert_eq!(a.open_claims, b.open_claims);
        assert_eq!(a.triggers, b.triggers);
        assert_eq!(a.correction, b.correction);

        // Window is honored.
        assert_eq!(a.recent_outputs.len(), default_window);
        assert_eq!(a.recent_evidence.len(), default_window);

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

    // ---- JAR2-36: ContextPolicy plumbing -------------------------------

    use crate::fs::{Claim, ClaimStatus};
    use crate::mandate::ContextPolicy;

    #[tokio::test]
    async fn assemble_context_honors_per_mandate_recent_outputs_cap() {
        let tmp = TempDir::new().unwrap();
        let mandate = Mandate {
            text: "tiny window".into(),
            idle_period: Duration::from_millis(100),
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
            .unwrap();

        let id = fs
            .record_evidence(EvidenceRecord::new(
                "echo",
                serde_json::json!({"k": 1}),
                serde_json::json!({"v": 1}),
                ts(),
            ))
            .await
            .unwrap();
        for i in 0..5 {
            fs.persist_output(&format!("o-{i}"), &[id.clone()])
                .await
                .unwrap();
        }

        let bundle = assemble_context(&fs, &[], &mandate, None).await.unwrap();
        assert_eq!(
            bundle.recent_outputs.len(),
            2,
            "per-mandate recent_outputs cap not honored"
        );
    }

    #[tokio::test]
    async fn assemble_context_honors_per_mandate_recent_evidence_cap() {
        let tmp = TempDir::new().unwrap();
        let mandate = Mandate {
            text: "tiny evidence window".into(),
            idle_period: Duration::from_millis(100),
            max_ticks: Some(1),
            retry_policy: None,
            context_policy: ContextPolicy {
                recent_outputs: 8,
                recent_evidence: 3,
                open_claims_max: 32,
            },
        };
        let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
            .await
            .unwrap();
        for i in 0..6 {
            fs.record_evidence(EvidenceRecord::new(
                "echo",
                serde_json::json!({"i": i}),
                serde_json::json!({"i": i}),
                ts(),
            ))
            .await
            .unwrap();
        }

        let bundle = assemble_context(&fs, &[], &mandate, None).await.unwrap();
        assert_eq!(bundle.recent_evidence.len(), 3);
    }

    #[tokio::test]
    async fn assemble_context_surfaces_only_open_claims_capped_in_filename_order() {
        let tmp = TempDir::new().unwrap();
        let mandate = Mandate {
            text: "claims window".into(),
            idle_period: Duration::from_millis(100),
            max_ticks: Some(1),
            retry_policy: None,
            context_policy: ContextPolicy {
                recent_outputs: 8,
                recent_evidence: 8,
                open_claims_max: 2,
            },
        };
        let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
            .await
            .unwrap();

        // Seed claims in mixed states so the filter has work to do. Use
        // alphabetic seeds so the on-disk slug order is alphabetic-prefix
        // (suffix hashes break exact equality of the slug body across
        // seeds, but the slug body order is what dominates).
        let claims = vec![
            ("alpha", ClaimStatus::Open),
            ("bravo", ClaimStatus::Resolved),
            ("charlie", ClaimStatus::Open),
            ("delta", ClaimStatus::Abandoned),
            ("echo-claim", ClaimStatus::Open),
        ];
        for (seed, status) in &claims {
            fs.write_claim(&Claim {
                seed: (*seed).into(),
                description: format!("desc-{seed}"),
                status: *status,
                created_at: ts(),
            })
            .await
            .unwrap();
        }

        let bundle = assemble_context(&fs, &[], &mandate, None).await.unwrap();

        // Cap is honored.
        assert_eq!(bundle.open_claims.len(), 2);
        // Every surfaced claim is Open.
        for c in &bundle.open_claims {
            assert_eq!(c.status, ClaimStatus::Open, "non-Open claim leaked through");
        }
        // Order matches the AgentFs::list_claims (filename ascending)
        // restricted to Open. We reproduce that ordering here and compare.
        let expected: Vec<Claim> = fs
            .list_claims()
            .await
            .unwrap()
            .into_iter()
            .filter(|c| c.status == ClaimStatus::Open)
            .take(2)
            .collect();
        assert_eq!(bundle.open_claims, expected);
    }

    #[tokio::test]
    async fn assemble_context_empty_claims_yields_empty_open_claims() {
        let tmp = TempDir::new().unwrap();
        let mandate = dummy_mandate();
        let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
            .await
            .unwrap();
        let bundle = assemble_context(&fs, &[], &mandate, None).await.unwrap();
        assert!(bundle.open_claims.is_empty());
    }

    #[tokio::test]
    async fn assemble_context_is_deterministic_across_repeat_calls_with_full_bundle() {
        // Determinism is the load-bearing property — re-asserted under the
        // JAR2-36 policy fields with every window populated.
        let tmp = TempDir::new().unwrap();
        let mandate = dummy_mandate();
        let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
            .await
            .unwrap();

        let ev = fs
            .record_evidence(EvidenceRecord::new(
                "echo",
                serde_json::json!({"k": 1}),
                serde_json::json!({"v": 1}),
                ts(),
            ))
            .await
            .unwrap();
        fs.persist_output("o-1", &[ev.clone()]).await.unwrap();
        fs.write_claim(&Claim {
            seed: "claim-1".into(),
            description: "d".into(),
            status: ClaimStatus::Open,
            created_at: ts(),
        })
        .await
        .unwrap();

        let triggers = vec![Trigger::ScheduledWake];
        let correction = Some(CorrectionContext::new("e"));
        let a = assemble_context(&fs, &triggers, &mandate, correction.clone())
            .await
            .unwrap();
        let b = assemble_context(&fs, &triggers, &mandate, correction)
            .await
            .unwrap();
        assert_eq!(a.recent_outputs, b.recent_outputs);
        assert_eq!(a.recent_evidence, b.recent_evidence);
        assert_eq!(a.open_claims, b.open_claims);
        assert_eq!(a.triggers, b.triggers);
        assert_eq!(a.correction, b.correction);
    }
}

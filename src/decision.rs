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

use crate::evidence::EvidenceId;
use serde::{Deserialize, Serialize};
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
}

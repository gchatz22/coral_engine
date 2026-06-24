//! `Mandate` — the standing instruction an agent runs against — and
//! `Output` — the thing it produces, with provenance back to evidence.

use crate::evidence::EvidenceId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Duration;

/// Retry policy for tool calls invoked under this mandate. Bounds attempts
/// within a single tool call and the fixed delay between them. Lives on
/// `Mandate` so a per-mandate override propagates into the `McpTool`s an
/// agent uses (see `ToolRegistry::register_mcp_server_with_policy`).
///
/// Defaults: 3 total attempts, 50 ms between retries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Total attempts including the first. `1` disables retry (one shot
    /// only). `0` is rejected at construction.
    pub max_attempts: u32,
    /// Fixed sleep between retries. Set to `Duration::ZERO` in tests so
    /// they do not pay for wall-clock backoff.
    #[serde(with = "crate::duration_ms")]
    pub backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff: Duration::from_millis(50),
        }
    }
}

impl RetryPolicy {
    /// Build a policy with `max_attempts` total attempts and `backoff`
    /// between them. `max_attempts` is clamped to at least `1` — a
    /// zero-attempt policy is a wiring bug, not a useful state.
    pub fn new(max_attempts: u32, backoff: Duration) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            backoff,
        }
    }

    /// Convenience for tests: retry-N with zero backoff so the retry loop
    /// runs at virtual-time speed under `tokio::test(start_paused = true)`.
    #[cfg(test)]
    pub fn test_immediate(max_attempts: u32) -> Self {
        Self::new(max_attempts, Duration::ZERO)
    }
}

/// What an agent has been told to do, and how patient to be about it.
///
/// `idle_period` is the wake cadence when no signal arrives. `max_ticks`
/// is an optional safety cap on loop iterations; `None` means "run until
/// `Retire`."
///
/// `retry_policy` is an optional per-mandate override for the retry
/// behaviour of any `McpTool`s registered for this agent (see
/// `ToolRegistry::register_mcp_server_with_policy`). `None` falls back to
/// `RetryPolicy::default()` at tool construction.
///
/// `context_policy` tunes how `assemble_context` shapes the warm
/// `ContextBundle` for this mandate (window sizes for recent outputs /
/// evidence, cap on open-claims surfaced into the bundle).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mandate {
    pub text: String,
    #[serde(with = "crate::duration_ms")]
    pub idle_period: Duration,
    pub max_ticks: Option<u64>,
    /// Per-mandate retry policy override. `None` (the default and the
    /// serialized shape when absent) leaves `RetryPolicy::default()` in
    /// place.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<RetryPolicy>,
    /// Per-mandate context-assembly policy. A missing field deserializes
    /// to `ContextPolicy::default()`.
    #[serde(default)]
    pub context_policy: ContextPolicy,
    /// Per-agent model override. `None` (the default and the serialized
    /// shape when absent) falls back to the worker's configured model. When
    /// set, the `decide` path sends this model id to the worker's configured
    /// vendor — a stronger model for a reconciling parent than its children,
    /// say. A model id the vendor doesn't recognize is an operator misconfig
    /// that surfaces as a runtime vendor error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// The tools this agent is assigned, by definition id (a subset of the
    /// graph's tool defs). Tool definitions are graph-scoped; assignment is
    /// per-agent config that rides this durable input. Surfaced to the model
    /// as its tool catalog and enforced at dispatch: a tool call whose
    /// advertised name resolves to no assigned def is rejected. Empty (the
    /// default) means no tools assigned — the agent can call none.
    #[serde(default)]
    pub tools: Vec<String>,
}

impl Mandate {
    /// Convenience constructor. Retry policy defaults to `None` (uses
    /// `RetryPolicy::default()` at tool-construction time) and context
    /// policy defaults to `ContextPolicy::default()`. `tools` defaults to
    /// empty — set it explicitly for an agent that calls tools.
    pub fn new(text: impl Into<String>, idle_period: Duration, max_ticks: Option<u64>) -> Self {
        Self {
            text: text.into(),
            idle_period,
            max_ticks,
            retry_policy: None,
            context_policy: ContextPolicy::default(),
            model: None,
            tools: Vec::new(),
        }
    }
}

/// Tuning knobs that shape warm-cache assembly for a given mandate. See
/// `scratch/context_assembly_v2.md` § 3 + § 6 for the design rationale and
/// `scratch/context_assembly_v1_measurements.md` for the empirical basis of
/// the default values below.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextPolicy {
    /// Max recent outputs to surface in the warm `ContextBundle`. Reads
    /// from `AgentFs::list_recent_outputs(recent_outputs)`.
    #[serde(default = "default_recent_outputs")]
    pub recent_outputs: usize,
    /// Max recent evidence records to surface in the warm `ContextBundle`.
    /// Reads from `AgentFs::list_recent_evidence(recent_evidence)`.
    #[serde(default = "default_recent_evidence")]
    pub recent_evidence: usize,
    /// Max open claims (`status == Open`) to surface in the warm
    /// `ContextBundle`. Drawn from `AgentFs::list_claims` in its native
    /// filename order; phase 1 inherits that ordering per
    /// `scratch/context_assembly_v2.md` § 8.
    #[serde(default = "default_open_claims_max")]
    pub open_claims_max: usize,
}

// Defaults pinned in `scratch/context_assembly_v1_measurements.md`.
fn default_recent_outputs() -> usize {
    8
}
fn default_recent_evidence() -> usize {
    8
}
fn default_open_claims_max() -> usize {
    32
}

impl Default for ContextPolicy {
    fn default() -> Self {
        Self {
            recent_outputs: default_recent_outputs(),
            recent_evidence: default_recent_evidence(),
            open_claims_max: default_open_claims_max(),
        }
    }
}

/// A produced artifact. Every output carries the evidence ids that justify
/// its content; the run loop refuses outputs whose evidence does not resolve
/// on disk (see `AgentFs::persist_output`).
///
/// `id` is **content-addressed**: `sha256` over the canonical JSON of
/// `{content, evidence: sorted_ids}`, mirroring `EvidenceId::new`'s
/// shape. Two ticks that emit byte-identical `(content, evidence)` produce
/// the same `OutputId`, which makes `persist_output` idempotent for free
/// under Temporal activity retries.
///
/// `created_at` is deliberately **not** part of the hash: the same logical
/// claim minted twice on different ticks must collapse to one file, not
/// two. Wall-clock drift is a cosmetic property of the file's on-disk
/// timestamp, not an identity contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Output {
    pub id: OutputId,
    pub content: String,
    pub evidence: Vec<EvidenceId>,
    pub created_at: DateTime<Utc>,
}

impl Output {
    /// Build an output whose id is derived from `(content, evidence)`.
    /// Two calls with the same content and evidence (in any input order)
    /// produce the same `OutputId`.
    pub fn new(
        content: impl Into<String>,
        evidence: Vec<EvidenceId>,
        created_at: DateTime<Utc>,
    ) -> Self {
        let content = content.into();
        let id = OutputId::new(&content, &evidence);
        Self {
            id,
            content,
            evidence,
            created_at,
        }
    }
}

/// Hex-encoded sha256 identifying an `Output`. Content-addressed: same
/// `(content, evidence)` → same id. Wraps the hex digits as a `String`
/// so the on-disk filename is the id verbatim.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OutputId(String);

impl OutputId {
    /// Derive an id from `(content, evidence)`. Evidence ids are sorted
    /// before hashing so caller-side ordering doesn't fragment the id
    /// space. Mirrors `EvidenceId::new`'s canonical-JSON envelope:
    /// fixed lexical-order keys, no whitespace, recursive canonical
    /// sub-encoding.
    pub fn new(content: &str, evidence: &[EvidenceId]) -> Self {
        let mut sorted: Vec<&EvidenceId> = evidence.iter().collect();
        sorted.sort();

        // Build the canonical JSON form of {content, evidence: [..]} via
        // serde_json::Value so we share `EvidenceId`'s rules for sorted
        // object keys and stringly-quoted scalars. The wrapping object
        // has exactly two keys (`content`, `evidence`) in lexical order
        // by construction.
        let envelope = serde_json::json!({
            "content": content,
            "evidence": sorted
                .iter()
                .map(|e| serde_json::Value::String(e.as_str().to_string()))
                .collect::<Vec<_>>(),
        });
        let bytes =
            serde_json::to_vec(&envelope).expect("canonical envelope serialization is infallible");
        let digest = Sha256::digest(&bytes);
        OutputId(hex::encode(digest))
    }

    /// Wrap a pre-computed hex string. Trusts the caller; useful for
    /// deserialization paths and tests.
    pub fn from_hex(hex: impl Into<String>) -> Self {
        OutputId(hex.into())
    }

    /// Borrow the hex digits.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for OutputId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evidence::EvidenceId;
    use serde_json::json;

    fn ts() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-05-03T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn mandate_round_trip() {
        let m = Mandate::new("research foo", Duration::from_millis(1500), Some(42));
        let s = serde_json::to_string(&m).unwrap();
        // Sanity-check the wire format of the duration.
        assert!(s.contains("\"idle_period\":1500"));
        let back: Mandate = serde_json::from_str(&s).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn mandate_round_trip_with_no_max_ticks() {
        let m = Mandate::new("watch", Duration::from_secs(30), None);
        let s = serde_json::to_string(&m).unwrap();
        let back: Mandate = serde_json::from_str(&s).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn duration_serializes_as_millis_truncating_sub_ms() {
        let m = Mandate::new("x", Duration::from_micros(1500), None);
        let s = serde_json::to_string(&m).unwrap();
        // 1500us = 1ms (sub-millisecond truncated).
        assert!(s.contains("\"idle_period\":1"), "got {s}");
    }

    #[test]
    fn mandate_default_omits_retry_policy_field() {
        // `skip_serializing_if = "Option::is_none"`: a default `Mandate`
        // must not emit `retry_policy` on the wire.
        let m = Mandate::new("x", Duration::from_millis(100), None);
        let s = serde_json::to_string(&m).unwrap();
        assert!(
            !s.contains("retry_policy"),
            "default mandate JSON should omit retry_policy, got {s}"
        );
    }

    #[test]
    fn mandate_round_trip_with_retry_policy_override() {
        let m = Mandate {
            text: "tune retry".into(),
            idle_period: Duration::from_millis(100),
            max_ticks: Some(1),
            retry_policy: Some(RetryPolicy::new(5, Duration::from_millis(10))),
            context_policy: ContextPolicy::default(),
            model: None,
            tools: Vec::new(),
        };
        let s = serde_json::to_string(&m).unwrap();
        // Verify both subfields land on the wire under stable names.
        assert!(
            s.contains("\"max_attempts\":5"),
            "expected max_attempts on wire, got {s}"
        );
        assert!(
            s.contains("\"backoff\":10"),
            "expected backoff as ms on wire, got {s}"
        );
        let back: Mandate = serde_json::from_str(&s).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn mandate_deserializes_legacy_json_without_retry_policy_field() {
        // Legacy mandate JSON without the `retry_policy` or
        // `context_policy` keys: `#[serde(default)]` must fill in the
        // defaults rather than reject the input.
        let legacy = r#"{"text":"old","idle_period":250,"max_ticks":null}"#;
        let back: Mandate = serde_json::from_str(legacy).unwrap();
        assert!(back.retry_policy.is_none());
        assert_eq!(back.text, "old");
        assert_eq!(back.idle_period, Duration::from_millis(250));
        assert_eq!(back.max_ticks, None);
        assert_eq!(back.context_policy, ContextPolicy::default());
    }

    #[test]
    fn mandate_deserializes_input_still_carrying_removed_persistent_field() {
        // Persistence is now universal — `Mandate` carries no `persistent`
        // field. An in-flight durable `AgentInput` serialized before the
        // removal may still carry `persistent`; `Mandate` has no
        // `deny_unknown_fields`, so the stale key is dropped on deserialize
        // rather than rejected. This keeps continue-as-new replay safe
        // across the field removal.
        let stale = r#"{"text":"old","idle_period":250,"max_ticks":null,"persistent":true}"#;
        let back: Mandate = serde_json::from_str(stale).unwrap();
        assert_eq!(back.text, "old");
        assert_eq!(back.idle_period, Duration::from_millis(250));
    }

    #[test]
    fn mandate_new_defaults_model_to_none() {
        let m = Mandate::new("x", Duration::from_millis(100), None);
        assert!(m.model.is_none());
    }

    #[test]
    fn mandate_round_trip_with_model_override() {
        let mut m = Mandate::new("reconcile", Duration::from_millis(100), None);
        m.model = Some("claude-opus-4-8".into());
        let s = serde_json::to_string(&m).unwrap();
        assert!(
            s.contains("\"model\":\"claude-opus-4-8\""),
            "expected model on wire, got {s}"
        );
        let back: Mandate = serde_json::from_str(&s).unwrap();
        assert_eq!(m, back);
        assert_eq!(back.model.as_deref(), Some("claude-opus-4-8"));
    }

    #[test]
    fn mandate_omits_model_from_wire_when_none() {
        let m = Mandate::new("x", Duration::from_millis(100), None);
        let s = serde_json::to_string(&m).unwrap();
        assert!(
            !s.contains("model"),
            "default mandate JSON should omit model, got {s}"
        );
    }

    #[test]
    fn mandate_deserializes_legacy_json_without_model_field() {
        // A serialized mandate from before the field existed (e.g. an
        // in-flight Temporal continue-as-new `AgentInput`) must
        // deserialize to `model: None`, not reject.
        let legacy = r#"{"text":"old","idle_period":250,"max_ticks":null}"#;
        let back: Mandate = serde_json::from_str(legacy).unwrap();
        assert!(back.model.is_none());
    }

    #[test]
    fn context_policy_default_values_are_pinned() {
        // Defaults pinned per `scratch/context_assembly_v1_measurements.md`.
        let p = ContextPolicy::default();
        assert_eq!(p.recent_outputs, 8);
        assert_eq!(p.recent_evidence, 8);
        assert_eq!(p.open_claims_max, 32);
    }

    #[test]
    fn context_policy_round_trip_through_serde() {
        let p = ContextPolicy {
            recent_outputs: 2,
            recent_evidence: 4,
            open_claims_max: 16,
        };
        let s = serde_json::to_string(&p).unwrap();
        let back: ContextPolicy = serde_json::from_str(&s).unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn context_policy_deserializes_empty_object_to_defaults() {
        // Each field carries its own `#[serde(default = "...")]`, so an
        // empty `{}` must fill every knob with the default value. This is
        // the round-trip safety net for YAML graph snippets that omit some
        // of the knobs but not others.
        let back: ContextPolicy = serde_json::from_str("{}").unwrap();
        assert_eq!(back, ContextPolicy::default());
    }

    #[test]
    fn context_policy_deserializes_partial_object_with_per_field_defaults() {
        let back: ContextPolicy = serde_json::from_str(r#"{"recent_outputs": 1}"#).unwrap();
        assert_eq!(back.recent_outputs, 1);
        assert_eq!(back.recent_evidence, default_recent_evidence());
        assert_eq!(back.open_claims_max, default_open_claims_max());
    }

    #[test]
    fn mandate_round_trip_with_context_policy_override() {
        let m = Mandate {
            text: "tune context".into(),
            idle_period: Duration::from_millis(100),
            max_ticks: Some(1),
            retry_policy: None,
            context_policy: ContextPolicy {
                recent_outputs: 2,
                recent_evidence: 3,
                open_claims_max: 4,
            },
            model: None,
            tools: Vec::new(),
        };
        let s = serde_json::to_string(&m).unwrap();
        assert!(
            s.contains("\"recent_outputs\":2"),
            "expected recent_outputs on wire, got {s}"
        );
        let back: Mandate = serde_json::from_str(&s).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn retry_policy_default_is_3_attempts_50ms() {
        // Pinned here (not just in `mcp::tool::tests`) because the policy
        // type now lives in this module — the default is part of the
        // `Mandate` API contract: `retry_policy: None` means "this".
        let p = RetryPolicy::default();
        assert_eq!(p.max_attempts, 3);
        assert_eq!(p.backoff, Duration::from_millis(50));
    }

    #[test]
    fn retry_policy_new_clamps_zero_to_one() {
        assert_eq!(RetryPolicy::new(0, Duration::ZERO).max_attempts, 1);
    }

    #[test]
    fn output_round_trip_with_non_empty_evidence() {
        let ev1 = EvidenceId::new("echo", &json!({"a": 1}), &json!({"r": 1}));
        let ev2 = EvidenceId::new("echo", &json!({"a": 2}), &json!({"r": 2}));
        let out = Output::new("hello", vec![ev1, ev2], ts());
        let s = serde_json::to_string(&out).unwrap();
        let back: Output = serde_json::from_str(&s).unwrap();
        assert_eq!(out, back);
        assert_eq!(back.evidence.len(), 2);
    }

    #[test]
    fn output_round_trip_with_empty_evidence() {
        let out = Output::new("nothing yet", vec![], ts());
        let s = serde_json::to_string(&out).unwrap();
        let back: Output = serde_json::from_str(&s).unwrap();
        assert_eq!(out, back);
        assert!(back.evidence.is_empty());
    }

    #[test]
    fn output_id_is_deterministic_for_same_content_and_evidence() {
        let ev1 = EvidenceId::new("echo", &json!({"a": 1}), &json!({"r": 1}));
        let ev2 = EvidenceId::new("echo", &json!({"a": 2}), &json!({"r": 2}));
        let a = OutputId::new("hello", &[ev1.clone(), ev2.clone()]);
        let b = OutputId::new("hello", &[ev1.clone(), ev2.clone()]);
        assert_eq!(a, b);
    }

    #[test]
    fn output_id_is_stable_across_evidence_ordering() {
        // Evidence ids must be sorted before hashing — same set, different
        // insertion order → same id. This is what makes
        // `Decision::EmitOutput` idempotent even when the agent shuffles
        // its evidence vector between retries.
        let ev1 = EvidenceId::new("echo", &json!({"a": 1}), &json!({"r": 1}));
        let ev2 = EvidenceId::new("echo", &json!({"a": 2}), &json!({"r": 2}));
        let a = OutputId::new("hello", &[ev1.clone(), ev2.clone()]);
        let b = OutputId::new("hello", &[ev2, ev1]);
        assert_eq!(a, b);
    }

    #[test]
    fn output_id_differs_for_different_content() {
        let ev = EvidenceId::new("echo", &json!({"a": 1}), &json!({"r": 1}));
        let a = OutputId::new("hello", &[ev.clone()]);
        let b = OutputId::new("world", &[ev]);
        assert_ne!(a, b);
    }

    #[test]
    fn output_id_differs_for_different_evidence() {
        let ev1 = EvidenceId::new("echo", &json!({"a": 1}), &json!({"r": 1}));
        let ev2 = EvidenceId::new("echo", &json!({"a": 2}), &json!({"r": 2}));
        let a = OutputId::new("hello", &[ev1]);
        let b = OutputId::new("hello", &[ev2]);
        assert_ne!(a, b);
    }

    #[test]
    fn output_id_is_independent_of_created_at() {
        // The hash domain is `(content, evidence)`; timestamps must not
        // leak in or two ticks emitting the same claim at different
        // wall-clock times would land in different files, defeating
        // dedup. `Output::new` derives the id from `(content, evidence)`
        // alone, so two calls with different `created_at` produce the
        // same `OutputId`.
        let ev = EvidenceId::new("echo", &json!({"a": 1}), &json!({"r": 1}));
        let t1 = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let t2 = DateTime::parse_from_rfc3339("2030-12-31T23:59:59Z")
            .unwrap()
            .with_timezone(&Utc);
        let a = Output::new("hello", vec![ev.clone()], t1);
        let b = Output::new("hello", vec![ev], t2);
        assert_eq!(a.id, b.id);
        // ...but the per-call timestamps remain distinct on the record.
        assert_ne!(a.created_at, b.created_at);
    }

    #[test]
    fn output_id_is_64_hex_chars() {
        let ev = EvidenceId::new("echo", &json!(null), &json!(null));
        let id = OutputId::new("x", &[ev]);
        assert_eq!(id.as_str().len(), 64);
        assert!(id.as_str().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn output_id_round_trip() {
        let ev = EvidenceId::new("echo", &json!({"k": "v"}), &json!(1));
        let id = OutputId::new("hello", &[ev]);
        let s = serde_json::to_string(&id).unwrap();
        // `transparent` should serialize as a bare JSON string.
        assert!(s.starts_with('"') && s.ends_with('"'));
        let back: OutputId = serde_json::from_str(&s).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn output_id_from_hex_round_trips() {
        let id = OutputId::from_hex("abc123");
        assert_eq!(id.as_str(), "abc123");
        assert_eq!(id.to_string(), "abc123");
    }
}

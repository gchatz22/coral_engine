//! `Mandate` — the standing instruction an agent runs against — and
//! `Output` — the thing it produces, with provenance back to evidence.

use crate::evidence::EvidenceId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use ulid::Ulid;

/// Retry policy for tool calls invoked under this mandate. Bounds attempts
/// within a single tool call and the fixed delay between them. Lives on
/// `Mandate` so a per-mandate override propagates into the `McpTool`s an
/// agent uses (see `ToolRegistry::register_mcp_server_with_policy`). Was
/// previously inside `mcp::tool` (JAR2-25); JAR2-31 hoists it to the
/// `mandate` module so the field on `Mandate` does not have to be feature-
/// gated and so the wire format is identical across `--features` configs.
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
/// `ToolRegistry::register_mcp_server_with_policy`). `None` keeps the
/// `RetryPolicy::default()` semantics JAR2-25 wired at construction.
///
/// `context_policy` tunes how `assemble_context` shapes the warm
/// `ContextBundle` for this mandate (window sizes for recent outputs /
/// evidence, cap on open-claims surfaced into the bundle). Defaults
/// reproduce the pre-JAR2-36 hardcoded `RECENT_WINDOW = 8` behavior so
/// existing graphs round-trip unchanged.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mandate {
    pub text: String,
    #[serde(with = "crate::duration_ms")]
    pub idle_period: Duration,
    pub max_ticks: Option<u64>,
    /// Per-mandate retry policy override. `None` (the default and the
    /// serialized shape when absent) leaves `RetryPolicy::default()` in
    /// place. `#[serde(default, skip_serializing_if = "Option::is_none")]`
    /// keeps the wire format backward-compatible with pre-JAR2-31 mandate
    /// JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<RetryPolicy>,
    /// Per-mandate context-assembly policy. `#[serde(default)]` keeps the
    /// wire format backward-compatible with pre-JAR2-36 mandate JSON; a
    /// missing field deserializes to `ContextPolicy::default()`, which
    /// matches the pre-JAR2-36 hardcoded window sizes.
    #[serde(default)]
    pub context_policy: ContextPolicy,
}

impl Mandate {
    /// Convenience constructor. Retry policy defaults to `None` (uses
    /// `RetryPolicy::default()` at tool-construction time) and context
    /// policy defaults to `ContextPolicy::default()`.
    pub fn new(text: impl Into<String>, idle_period: Duration, max_ticks: Option<u64>) -> Self {
        Self {
            text: text.into(),
            idle_period,
            max_ticks,
            retry_policy: None,
            context_policy: ContextPolicy::default(),
        }
    }
}

/// Tuning knobs that shape warm-cache assembly for a given mandate. See
/// `scratch/context_assembly_v2.md` § 3 + § 6 for the design rationale and
/// `scratch/context_assembly_v1_measurements.md` for the empirical basis of
/// the default values below.
///
/// Defaults match pre-JAR2-36 behavior (`RECENT_WINDOW = 8`) for
/// `recent_outputs` / `recent_evidence` so existing graphs round-trip
/// unchanged. `open_claims_max` is new in JAR2-36; its default is the
/// strawman value from the design doc, retained after the measurement spike
/// found no recorded-fixture mandate accumulating more than a handful of
/// claims.
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

// Defaults pinned in `scratch/context_assembly_v1_measurements.md`. The
// recent_* values match the pre-JAR2-36 `RECENT_WINDOW = 8` constant so
// existing graphs are unaffected; `open_claims_max` is the design-doc
// strawman, unchanged after the spike.
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
/// its content; the run loop will later refuse outputs whose evidence does
/// not resolve on disk. For now this type is pure data.
///
/// `id` is a ULID so outputs sort lexically by creation time and have a
/// short, URL-safe textual form.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Output {
    pub id: OutputId,
    pub content: String,
    pub evidence: Vec<EvidenceId>,
    pub created_at: DateTime<Utc>,
}

impl Output {
    /// Build an output with a freshly minted id and the supplied timestamp.
    pub fn new(
        content: impl Into<String>,
        evidence: Vec<EvidenceId>,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id: OutputId::new(),
            content: content.into(),
            evidence,
            created_at,
        }
    }
}

/// Newtype around a ULID identifying an `Output`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OutputId(pub Ulid);

impl OutputId {
    /// Mint a new id from the ULID monotonic generator.
    pub fn new() -> Self {
        OutputId(Ulid::new())
    }
}

impl Default for OutputId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for OutputId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
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
        // `skip_serializing_if = "Option::is_none"` is the backward-compat
        // contract for pre-JAR2-31 mandate JSON: a default `Mandate` must
        // not emit `retry_policy` so old fixtures still round-trip.
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
        // Pre-JAR2-31 mandate JSON had no `retry_policy` key. `#[serde(default)]`
        // must fill in `None` rather than reject the input.
        let legacy = r#"{"text":"old","idle_period":250,"max_ticks":null}"#;
        let back: Mandate = serde_json::from_str(legacy).unwrap();
        assert!(back.retry_policy.is_none());
        assert_eq!(back.text, "old");
        assert_eq!(back.idle_period, Duration::from_millis(250));
        assert_eq!(back.max_ticks, None);
        // Pre-JAR2-36 mandate JSON also had no `context_policy`. The
        // `#[serde(default)]` on the field fills in `ContextPolicy::default()`.
        assert_eq!(back.context_policy, ContextPolicy::default());
    }

    #[test]
    fn context_policy_default_values_match_pre_jar2_36_behavior() {
        // Pinned: the warm-cache defaults must reproduce the pre-JAR2-36
        // `RECENT_WINDOW = 8` behavior so existing graphs are unaffected.
        // `open_claims_max` is the design-doc strawman, retained after the
        // measurement spike (see `scratch/context_assembly_v1_measurements.md`).
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
    fn output_ids_are_unique() {
        let a = OutputId::new();
        let b = OutputId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn output_id_round_trip() {
        let id = OutputId::new();
        let s = serde_json::to_string(&id).unwrap();
        let back: OutputId = serde_json::from_str(&s).unwrap();
        assert_eq!(id, back);
    }
}

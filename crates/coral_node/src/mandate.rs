//! `Mandate` — the standing instruction an agent runs against — and
//! `OutputId` — the fingerprint of the single Output it keeps current.

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

/// Interim runaway backstop applied when a mandate carries no explicit
/// `step_cap`: the loop retires once its cycle counter reaches this value.
/// A coarse ceiling sized to catch a genuinely runaway agent, not to bound a
/// legitimate long-lived monitor — but it *is* a hard ceiling, so a monitor
/// that cycles long enough will eventually hit it. Temporary scaffolding
/// until the budget primitive becomes the sole runaway guard (the design's
/// end state has no iteration cap); it is not an authoring knob.
pub const INTERIM_STEP_CAP: u64 = 1_000_000;

/// What an agent has been told to do, and how patient to be about it.
///
/// `idle_period` is the self-wake cadence: `Some(d)` wakes the agent every
/// `d` when no signal arrives; `None` is the **"never"** cadence — the agent
/// self-wakes only its first cycle, then waits for triggers (child/upstream
/// updates, human ops, external signals) and never re-arms a self-wake
/// timer. An agent never self-terminates; the loop stops only via a
/// retire signal, teardown, or the runaway `step_cap` backstop.
///
/// `step_cap` is the interim runaway backstop, in cycles. `None` falls back
/// to [`INTERIM_STEP_CAP`]. It is deliberately **not** part of the authoring
/// surface (no `graph.yaml` field): the test harness sets a small value to
/// terminate hermetically; operators rely on cadence + budget.
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
    #[serde(with = "crate::duration_ms_opt")]
    pub idle_period: Option<Duration>,
    #[serde(default)]
    pub step_cap: Option<u64>,
    /// Per-mandate retry policy override. `None` (the default and the
    /// serialized shape when absent) leaves `RetryPolicy::default()` in
    /// place.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<RetryPolicy>,
    /// Per-mandate context-assembly policy. A missing field deserializes
    /// to `ContextPolicy::default()`.
    #[serde(default)]
    pub context_policy: ContextPolicy,
    /// Per-agent model, as a qualified `provider/model` name (e.g.
    /// `anthropic/claude-opus-4-8`, `cohere/command-a`). The `decide` path
    /// resolves the `provider` prefix against the registry of clients booted
    /// from available keys, so a reconciling parent can run on a different
    /// provider than its children. A bare name without a prefix routes to the
    /// registry's default provider. `None` (the default and the serialized
    /// shape when absent) uses the default provider's default model. A
    /// provider the registry doesn't carry is an operator misconfig that
    /// surfaces as a runtime error.
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
    /// Convenience constructor for the recurring-cadence case: the agent
    /// self-wakes every `idle_period`. Retry policy defaults to `None` (uses
    /// `RetryPolicy::default()` at tool-construction time) and context
    /// policy defaults to `ContextPolicy::default()`. `tools` defaults to
    /// empty — set it explicitly for an agent that calls tools. `step_cap`
    /// is the interim runaway backstop; `None` falls back to
    /// [`INTERIM_STEP_CAP`].
    pub fn new(text: impl Into<String>, idle_period: Duration, step_cap: Option<u64>) -> Self {
        Self {
            text: text.into(),
            idle_period: Some(idle_period),
            step_cap,
            retry_policy: None,
            context_policy: ContextPolicy::default(),
            model: None,
            tools: Vec::new(),
        }
    }

    /// Constructor for the **"never"** cadence: the agent self-wakes only its
    /// first cycle, then waits for triggers and never re-arms a self-wake
    /// timer. Other defaults match [`Mandate::new`].
    pub fn new_never(text: impl Into<String>, step_cap: Option<u64>) -> Self {
        Self {
            text: text.into(),
            idle_period: None,
            step_cap,
            retry_policy: None,
            context_policy: ContextPolicy::default(),
            model: None,
            tools: Vec::new(),
        }
    }

    /// Whether this mandate's cadence is **"never"** — no recurring self-wake.
    pub fn is_never(&self) -> bool {
        self.idle_period.is_none()
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

/// Hex-encoded sha256 fingerprint of an output's prose body. Identifies a
/// specific version of an agent's single, kept-current Output: a re-emit of
/// the same body produces the same id, a changed body a new one. Carried on
/// the `ChildOutput` trigger and the workflow's `last_output_id` so a child
/// signalling "my output changed" is idempotent under activity retries.
///
/// The body lives at the canonical path `outputs/output.md` (content-only in
/// the FS); the id is a fingerprint, not the filename.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OutputId(String);

impl OutputId {
    /// Derive an id from the output body. Same body → same id.
    pub fn new(body: &str) -> Self {
        let digest = Sha256::digest(body.as_bytes());
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
    fn mandate_round_trip_with_no_step_cap() {
        let m = Mandate::new("watch", Duration::from_secs(30), None);
        let s = serde_json::to_string(&m).unwrap();
        let back: Mandate = serde_json::from_str(&s).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn never_mandate_round_trips_with_null_idle_period() {
        let m = Mandate::new_never("wait for children", Some(3));
        assert!(m.is_never());
        let s = serde_json::to_string(&m).unwrap();
        // The "never" cadence serializes as a null idle_period.
        assert!(s.contains("\"idle_period\":null"), "got {s}");
        let back: Mandate = serde_json::from_str(&s).unwrap();
        assert_eq!(m, back);
        assert!(back.is_never());
        assert_eq!(back.step_cap, Some(3));
    }

    #[test]
    fn new_is_recurring_cadence_not_never() {
        let m = Mandate::new("watch", Duration::from_secs(30), None);
        assert!(!m.is_never());
        assert_eq!(m.idle_period, Some(Duration::from_secs(30)));
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
            idle_period: Some(Duration::from_millis(100)),
            step_cap: Some(1),
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
        let legacy = r#"{"text":"old","idle_period":250}"#;
        let back: Mandate = serde_json::from_str(legacy).unwrap();
        assert!(back.retry_policy.is_none());
        assert_eq!(back.text, "old");
        assert_eq!(back.idle_period, Some(Duration::from_millis(250)));
        assert_eq!(back.step_cap, None);
        assert_eq!(back.context_policy, ContextPolicy::default());
    }

    #[test]
    fn mandate_deserializes_input_still_carrying_removed_max_ticks_field() {
        // `max_ticks` was relocated to the harness-only `step_cap`. An
        // in-flight durable `AgentInput` serialized before the removal may
        // still carry `max_ticks`; `Mandate` has no `deny_unknown_fields`, so
        // the stale key is dropped on deserialize rather than rejected (and
        // `step_cap` falls back to its default). This keeps continue-as-new
        // replay safe across the field removal.
        let stale = r#"{"text":"old","idle_period":250,"max_ticks":8,"persistent":true}"#;
        let back: Mandate = serde_json::from_str(stale).unwrap();
        assert_eq!(back.text, "old");
        assert_eq!(back.idle_period, Some(Duration::from_millis(250)));
        assert_eq!(back.step_cap, None);
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
            idle_period: Some(Duration::from_millis(100)),
            step_cap: Some(1),
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
    fn output_id_is_deterministic_for_same_body() {
        assert_eq!(OutputId::new("hello"), OutputId::new("hello"));
    }

    #[test]
    fn output_id_differs_for_different_body() {
        assert_ne!(OutputId::new("hello"), OutputId::new("world"));
    }

    #[test]
    fn output_id_is_64_hex_chars() {
        let id = OutputId::new("x");
        assert_eq!(id.as_str().len(), 64);
        assert!(id.as_str().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn output_id_round_trip() {
        let id = OutputId::new("hello");
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

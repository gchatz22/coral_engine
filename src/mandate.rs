//! `Mandate` — the standing instruction an agent runs against — and
//! `Output` — the thing it produces, with provenance back to evidence.

use crate::evidence::EvidenceId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use ulid::Ulid;

/// What an agent has been told to do, and how patient to be about it.
///
/// `idle_period` is the wake cadence when no signal arrives. `max_ticks`
/// is an optional safety cap on loop iterations; `None` means "run until
/// `Retire`."
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mandate {
    pub text: String,
    #[serde(with = "crate::duration_ms")]
    pub idle_period: Duration,
    pub max_ticks: Option<u64>,
}

impl Mandate {
    /// Convenience constructor.
    pub fn new(text: impl Into<String>, idle_period: Duration, max_ticks: Option<u64>) -> Self {
        Self {
            text: text.into(),
            idle_period,
            max_ticks,
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

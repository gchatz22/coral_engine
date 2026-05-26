//! JAR2-83 (stage 5.6) — `ConflictRecord` + content-addressed `ConflictId`.
//!
//! The kernel-visible side of the conflict-log FS schema. Every disagreement
//! the parent's `reconcile_children` activity records lands as
//! `<agent_root>/conflicts/<id>.json`, content-addressed over
//! `(alternatives, resolution)` so a retried activity PUTs byte-identical
//! bytes under the same key and `put_if_absent` dedupes cleanly.
//!
//! ## Field layout
//!
//! ```text
//! ConflictRecord {
//!     id,              // ConflictId, derived from (alternatives, resolution)
//!     timestamp,       // when the record was minted; NOT part of the id
//!     kind,            // HeldOpen | Resolved — derived from resolution.is_some()
//!     alternatives,    // >= 2; validated by AgentFs::write_conflict
//!     resolution,      // None iff HeldOpen
//! }
//! ```
//!
//! `timestamp` is excluded from the id for the same reason `Output`'s
//! `created_at` is: a retry on a different wall-clock minute must still
//! collapse to the same content-addressed file. `kind` is reader
//! convenience (so an audit tool doesn't have to inspect
//! `resolution.is_some()` to colour the row); it's not on the wire of
//! the `ConflictRecordIntent` the LLM emits and not part of the hash.
//!
//! ## Canonical form
//!
//! Mirrors `OutputId::new`'s `serde_json::to_vec(&Value::Object(..))`
//! approach: `serde_json::to_value(&alternatives)` /
//! `serde_json::to_value(&resolution)` route through `Value::Object`'s
//! `BTreeMap` (no `preserve_order` feature in this workspace's
//! `serde_json`), so struct fields land in lex-sorted key order without
//! manual canonicalization. Then `serde_json::to_vec` of the wrapping
//! envelope (two keys: `alternatives` and `resolution`, already in lex
//! order by construction) yields the bytes hashed with sha256.
//!
//! `resolution: None` is serialized as JSON `null` so the canonical-form
//! bytes are stable across the `HeldOpen` / `Resolved` split (vs.
//! omitting the key entirely, which would make the bytes — and the id —
//! differ in ways unrelated to record identity).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub use crate::decision::{ConflictAlternative, ConflictId, ConflictResolution};

/// Whether the parent picked a winning alternative or left the
/// disagreement on the table. Stored on the record for audit-tool
/// ergonomics; derivable from `resolution.is_some()`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictKind {
    HeldOpen,
    Resolved,
}

impl ConflictKind {
    /// Derive the kind from the presence/absence of a resolution.
    /// `ConflictRecord::new` calls this internally; exposed so callers
    /// that already have a `ConflictResolution` in hand can label a
    /// record consistently without round-tripping through the
    /// constructor.
    pub fn from_resolution(resolution: &Option<ConflictResolution>) -> Self {
        if resolution.is_some() {
            ConflictKind::Resolved
        } else {
            ConflictKind::HeldOpen
        }
    }
}

/// On-disk shape of `<agent_root>/conflicts/<id>.json`. Written by
/// [`crate::fs::AgentFs::write_conflict`] from the parent's
/// `reconcile_children` activity.
///
/// Constructed via [`ConflictRecord::new`] so `id` and `kind` are
/// derived from the other fields (single source of truth — callers
/// cannot construct a record whose `id` doesn't match its content or
/// whose `kind` disagrees with its `resolution`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictRecord {
    pub id: ConflictId,
    pub timestamp: DateTime<Utc>,
    pub kind: ConflictKind,
    pub alternatives: Vec<ConflictAlternative>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<ConflictResolution>,
}

impl ConflictRecord {
    /// Build a record from its substantive fields. Derives
    /// `id = ConflictId::new(&alternatives, &resolution)` and
    /// `kind = ConflictKind::from_resolution(&resolution)`.
    ///
    /// Validation of `alternatives.len() >= 2` lives at the FS writer
    /// (`AgentFs::write_conflict`) and the activity boundary
    /// (`ReconciliationError::ConflictAlternativesTooFew`) — not here.
    /// Mirrors `Output::new`, which doesn't re-check
    /// `EvidenceId`/`evidence` presence (that lives in `persist_output`).
    pub fn new(
        timestamp: DateTime<Utc>,
        alternatives: Vec<ConflictAlternative>,
        resolution: Option<ConflictResolution>,
    ) -> Self {
        let id = ConflictId::new(&alternatives, &resolution);
        let kind = ConflictKind::from_resolution(&resolution);
        Self {
            id,
            timestamp,
            kind,
            alternatives,
            resolution,
        }
    }
}

/// Content-addressing constructor for [`ConflictId`].
///
/// Hashes the canonical JSON form of `{alternatives, resolution}` —
/// `timestamp` and `kind` are deliberately excluded:
///
/// - `timestamp` would defeat retry idempotency (every Temporal retry
///   produces a fresh `now`; we want the second retry's `put_if_absent`
///   to dedupe against the first attempt's file).
/// - `kind` is fully determined by `resolution.is_some()`, so including
///   it would be redundant entropy with no information gain.
///
/// Mirrors `OutputId::new`'s envelope shape (two-key object, keys in
/// lex order by construction, `serde_json::to_value` for the inner
/// values to canonicalize struct field order via BTreeMap).
impl ConflictId {
    /// Derive an id from `(alternatives, resolution)`. Two records
    /// with the same alternatives and resolution produce the same id;
    /// changing any alternative, the resolution, or adding/removing a
    /// resolution changes the id.
    pub fn new(
        alternatives: &[ConflictAlternative],
        resolution: &Option<ConflictResolution>,
    ) -> Self {
        // `to_value` on a struct produces `Value::Object`, which is a
        // BTreeMap under this workspace's `serde_json` (no
        // `preserve_order` feature). BTreeMap iteration is lex-sorted,
        // so the serialized bytes have canonical key order without a
        // manual walk.
        let alts: Vec<serde_json::Value> = alternatives
            .iter()
            .map(|a| {
                serde_json::to_value(a).expect("ConflictAlternative serialization is infallible")
            })
            .collect();
        let res = serde_json::to_value(resolution)
            .expect("Option<ConflictResolution> serialization is infallible");
        let envelope = serde_json::json!({
            "alternatives": alts,
            "resolution": res,
        });
        let bytes =
            serde_json::to_vec(&envelope).expect("canonical envelope serialization is infallible");
        let digest = Sha256::digest(&bytes);
        ConflictId::from_hex(hex::encode(digest))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_ref::{AgentId, AgentRef};
    use crate::mandate::OutputId;
    use uuid::Uuid;

    fn ts() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-05-25T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn agent_ref(s: &str, uuid: &str) -> AgentRef {
        AgentRef::new(s, AgentId::new(Uuid::parse_str(uuid).unwrap()))
    }

    fn alt_a() -> ConflictAlternative {
        ConflictAlternative {
            source_child: agent_ref(
                "graphs/g1/agents/child-a",
                "11111111-1111-1111-1111-111111111111",
            ),
            source_output_id: OutputId::from_hex("aa".repeat(32)),
            claim: "value is 42".into(),
        }
    }

    fn alt_b() -> ConflictAlternative {
        ConflictAlternative {
            source_child: agent_ref(
                "graphs/g1/agents/child-b",
                "22222222-2222-2222-2222-222222222222",
            ),
            source_output_id: OutputId::from_hex("bb".repeat(32)),
            claim: "value is 43".into(),
        }
    }

    fn resolution_pick_a() -> ConflictResolution {
        ConflictResolution {
            chosen_alternative_idx: 0,
            reasoning: "primary source has higher confidence".into(),
        }
    }

    #[test]
    fn conflict_id_is_deterministic_for_same_inputs() {
        let a = ConflictId::new(&[alt_a(), alt_b()], &None);
        let b = ConflictId::new(&[alt_a(), alt_b()], &None);
        assert_eq!(a, b);
    }

    #[test]
    fn conflict_id_changes_when_alternatives_change() {
        let base = ConflictId::new(&[alt_a(), alt_b()], &None);
        let other_alt = ConflictAlternative {
            claim: "value is 99".into(),
            ..alt_b()
        };
        let other = ConflictId::new(&[alt_a(), other_alt], &None);
        assert_ne!(base, other);
    }

    #[test]
    fn conflict_id_changes_when_resolution_changes() {
        let held = ConflictId::new(&[alt_a(), alt_b()], &None);
        let resolved = ConflictId::new(&[alt_a(), alt_b()], &Some(resolution_pick_a()));
        assert_ne!(held, resolved);
    }

    #[test]
    fn conflict_id_is_independent_of_timestamp() {
        // Timestamp is NOT in the hash — two records minted at
        // different wall-clock instants over the same content must
        // collapse to the same id (retry idempotency).
        let r1 = ConflictRecord::new(ts(), vec![alt_a(), alt_b()], None);
        let r2 = ConflictRecord::new(Utc::now(), vec![alt_a(), alt_b()], None);
        assert_eq!(r1.id, r2.id, "ConflictId must be timestamp-independent");
    }

    #[test]
    fn conflict_id_is_64_hex_chars() {
        let id = ConflictId::new(&[alt_a(), alt_b()], &None);
        assert_eq!(id.as_str().len(), 64);
        assert!(id.as_str().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn record_new_derives_held_open_when_resolution_is_none() {
        let r = ConflictRecord::new(ts(), vec![alt_a(), alt_b()], None);
        assert_eq!(r.kind, ConflictKind::HeldOpen);
        assert!(r.resolution.is_none());
    }

    #[test]
    fn record_new_derives_resolved_when_resolution_is_some() {
        let r = ConflictRecord::new(ts(), vec![alt_a(), alt_b()], Some(resolution_pick_a()));
        assert_eq!(r.kind, ConflictKind::Resolved);
        assert!(r.resolution.is_some());
    }

    #[test]
    fn record_new_id_matches_constructor() {
        // The id stored on the record matches what `ConflictId::new`
        // would produce from the same inputs.
        let alts = vec![alt_a(), alt_b()];
        let res = Some(resolution_pick_a());
        let r = ConflictRecord::new(ts(), alts.clone(), res.clone());
        let recomputed = ConflictId::new(&alts, &res);
        assert_eq!(r.id, recomputed);
    }

    #[test]
    fn record_round_trips_through_serde_held_open() {
        let r = ConflictRecord::new(ts(), vec![alt_a(), alt_b()], None);
        let s = serde_json::to_string(&r).unwrap();
        let back: ConflictRecord = serde_json::from_str(&s).unwrap();
        assert_eq!(r, back);
        // Wire-form sanity: `kind` is serialized as `held_open` /
        // `resolved` (snake_case), `resolution` omitted via
        // `skip_serializing_if` so the HeldOpen file is lean.
        assert!(s.contains("\"kind\":\"held_open\""), "wire shape: {s}");
        assert!(!s.contains("resolution"), "wire shape: {s}");
    }

    #[test]
    fn record_round_trips_through_serde_resolved() {
        let r = ConflictRecord::new(ts(), vec![alt_a(), alt_b()], Some(resolution_pick_a()));
        let s = serde_json::to_string(&r).unwrap();
        let back: ConflictRecord = serde_json::from_str(&s).unwrap();
        assert_eq!(r, back);
        assert!(s.contains("\"kind\":\"resolved\""), "wire shape: {s}");
        assert!(
            s.contains("\"chosen_alternative_idx\":0"),
            "wire shape: {s}"
        );
    }

    #[test]
    fn conflict_kind_from_resolution_helper() {
        assert_eq!(ConflictKind::from_resolution(&None), ConflictKind::HeldOpen);
        assert_eq!(
            ConflictKind::from_resolution(&Some(resolution_pick_a())),
            ConflictKind::Resolved
        );
    }
}

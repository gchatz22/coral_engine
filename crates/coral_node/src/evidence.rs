//! Evidence — content-addressed records of tool calls.
//!
//! `EvidenceId` is the hex-encoded sha256 of the canonical JSON
//! serialization of the `(tool, args, result)` triple. Object keys are
//! sorted during serialization so logically-equal records hash equal
//! regardless of in-memory key order. The full `result` is hashed by
//! design: hashing only `(tool, args)` would let two calls with the same
//! inputs but different outputs collide in the evidence store and
//! silently corrupt provenance chains.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

/// Hex-encoded sha256 of a tool call's canonical JSON form.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EvidenceId(String);

impl EvidenceId {
    /// Build an id by hashing the canonical JSON of `(tool, args, result)`.
    pub fn new(tool: &str, args: &serde_json::Value, result: &serde_json::Value) -> Self {
        let mut buf = Vec::with_capacity(256);
        // Three fixed keys emitted in lexical order so the envelope is
        // itself canonical.
        buf.push(b'{');
        write_json_string(&mut buf, "args");
        buf.push(b':');
        write_canonical(&mut buf, args);
        buf.push(b',');
        write_json_string(&mut buf, "result");
        buf.push(b':');
        write_canonical(&mut buf, result);
        buf.push(b',');
        write_json_string(&mut buf, "tool");
        buf.push(b':');
        write_canonical(&mut buf, &serde_json::Value::String(tool.to_string()));
        buf.push(b'}');

        let digest = Sha256::digest(&buf);
        EvidenceId(hex::encode(digest))
    }

    /// Wrap a pre-computed hex string. Trusts the caller; useful for
    /// deserialization paths where the id was produced elsewhere.
    pub fn from_hex(hex: impl Into<String>) -> Self {
        EvidenceId(hex.into())
    }

    /// Borrow the hex digits.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EvidenceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A single tool-call observation. The id is derivable from the other
/// fields; we still store it so consumers can index/look up without
/// recomputing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceRecord {
    pub id: EvidenceId,
    pub tool: String,
    pub args: serde_json::Value,
    pub result: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

impl EvidenceRecord {
    /// Build a record, computing the id from `(tool, args, result)`.
    pub fn new(
        tool: impl Into<String>,
        args: serde_json::Value,
        result: serde_json::Value,
        created_at: DateTime<Utc>,
    ) -> Self {
        let tool = tool.into();
        let id = EvidenceId::new(&tool, &args, &result);
        Self {
            id,
            tool,
            args,
            result,
            created_at,
        }
    }
}

/// Recursively serialize `v` into `buf` with sorted object keys.
///
/// Numbers are emitted via `serde_json::Number`'s `Display`, strings are
/// JSON-escaped, and object members are written in lexical sort of their
/// UTF-8 keys.
fn write_canonical(buf: &mut Vec<u8>, v: &serde_json::Value) {
    match v {
        serde_json::Value::Null => buf.extend_from_slice(b"null"),
        serde_json::Value::Bool(true) => buf.extend_from_slice(b"true"),
        serde_json::Value::Bool(false) => buf.extend_from_slice(b"false"),
        serde_json::Value::Number(n) => {
            buf.extend_from_slice(n.to_string().as_bytes());
        }
        serde_json::Value::String(s) => write_json_string(buf, s),
        serde_json::Value::Array(items) => {
            buf.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    buf.push(b',');
                }
                write_canonical(buf, item);
            }
            buf.push(b']');
        }
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            buf.push(b'{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    buf.push(b',');
                }
                write_json_string(buf, k);
                buf.push(b':');
                write_canonical(buf, &map[*k]);
            }
            buf.push(b'}');
        }
    }
}

/// Write `s` as a JSON-escaped string (including the surrounding quotes).
fn write_json_string(buf: &mut Vec<u8>, s: &str) {
    let v = serde_json::Value::String(s.to_string());
    let bytes = serde_json::to_vec(&v).expect("string serialization is infallible");
    buf.extend_from_slice(&bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ts() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-05-03T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn evidence_id_is_deterministic_for_same_inputs() {
        let a = EvidenceId::new("echo", &json!({"msg": "hi"}), &json!({"echoed": "hi"}));
        let b = EvidenceId::new("echo", &json!({"msg": "hi"}), &json!({"echoed": "hi"}));
        assert_eq!(a, b);
    }

    #[test]
    fn evidence_id_is_stable_across_object_key_orderings() {
        let a = EvidenceId::new(
            "echo",
            &json!({"a": 1, "b": 2, "nested": {"x": 1, "y": 2}}),
            &json!({}),
        );
        let mut map = serde_json::Map::new();
        let mut nested = serde_json::Map::new();
        nested.insert("y".into(), json!(2));
        nested.insert("x".into(), json!(1));
        map.insert("nested".into(), serde_json::Value::Object(nested));
        map.insert("b".into(), json!(2));
        map.insert("a".into(), json!(1));
        let b = EvidenceId::new("echo", &serde_json::Value::Object(map), &json!({}));
        assert_eq!(a, b);
    }

    #[test]
    fn evidence_id_differs_for_different_inputs() {
        let base = EvidenceId::new("echo", &json!({"msg": "hi"}), &json!({"echoed": "hi"}));
        let other_tool = EvidenceId::new("echo2", &json!({"msg": "hi"}), &json!({"echoed": "hi"}));
        let other_args = EvidenceId::new("echo", &json!({"msg": "bye"}), &json!({"echoed": "hi"}));
        let other_result =
            EvidenceId::new("echo", &json!({"msg": "hi"}), &json!({"echoed": "bye"}));
        assert_ne!(base, other_tool);
        assert_ne!(base, other_args);
        assert_ne!(base, other_result);
        assert_ne!(other_tool, other_args);
    }

    #[test]
    fn evidence_id_is_64_hex_chars() {
        let id = EvidenceId::new("t", &json!(null), &json!(null));
        assert_eq!(id.as_str().len(), 64);
        assert!(id.as_str().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn evidence_id_serde_round_trip() {
        let id = EvidenceId::new("echo", &json!({"k": "v"}), &json!(1));
        let s = serde_json::to_string(&id).unwrap();
        assert!(s.starts_with('"') && s.ends_with('"'));
        let back: EvidenceId = serde_json::from_str(&s).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn evidence_record_round_trip() {
        let rec = EvidenceRecord::new("echo", json!({"msg": "hi"}), json!({"echoed": "hi"}), ts());
        let s = serde_json::to_string(&rec).unwrap();
        let back: EvidenceRecord = serde_json::from_str(&s).unwrap();
        assert_eq!(rec, back);
        let recomputed = EvidenceId::new(&rec.tool, &rec.args, &rec.result);
        assert_eq!(rec.id, recomputed);
    }

    #[test]
    fn from_hex_round_trips() {
        let id = EvidenceId::from_hex("abc123");
        assert_eq!(id.as_str(), "abc123");
        assert_eq!(id.to_string(), "abc123");
    }
}

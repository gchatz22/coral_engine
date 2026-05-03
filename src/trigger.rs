//! `Trigger` — what wakes the agent loop.
//!
//! The bootstrap taxonomy is intentionally minimal (see
//! `scratch/minimal_node_backend.md` § 6 row 1): scheduled wake, an
//! external signal carrying an opaque payload, and a human override
//! carrying an opaque op. ChildOutput / SiblingBatch / MandateUpdate
//! are deferred until the parent–child topology lands.

use serde::{Deserialize, Serialize};

/// Reason the agent loop is being asked to step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Trigger {
    /// The scheduler's idle deadline elapsed.
    ScheduledWake,
    /// An external system raised an event addressed to this agent.
    External {
        kind: String,
        payload: serde_json::Value,
    },
    /// A human is forcing a mutation. Shape is deliberately opaque for
    /// the bootstrap; the kernel does not yet enforce override semantics.
    HumanOverride { op: HumanOp },
}

/// Opaque newtype around the JSON payload describing a human override op.
/// We don't validate the shape yet — that lands when override semantics
/// do.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HumanOp(pub serde_json::Value);

impl HumanOp {
    pub fn new(value: serde_json::Value) -> Self {
        HumanOp(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn scheduled_wake_round_trip() {
        let t = Trigger::ScheduledWake;
        let s = serde_json::to_string(&t).unwrap();
        let back: Trigger = serde_json::from_str(&s).unwrap();
        assert_eq!(t, back);
    }

    #[test]
    fn external_round_trip() {
        let t = Trigger::External {
            kind: "webhook".into(),
            payload: json!({"x": 1, "y": [true, null]}),
        };
        let s = serde_json::to_string(&t).unwrap();
        let back: Trigger = serde_json::from_str(&s).unwrap();
        assert_eq!(t, back);
    }

    #[test]
    fn human_override_round_trip() {
        let t = Trigger::HumanOverride {
            op: HumanOp::new(json!({"action": "pause"})),
        };
        let s = serde_json::to_string(&t).unwrap();
        let back: Trigger = serde_json::from_str(&s).unwrap();
        assert_eq!(t, back);
    }

    #[test]
    fn human_op_is_transparent() {
        let op = HumanOp::new(json!({"k": "v"}));
        let s = serde_json::to_string(&op).unwrap();
        // `transparent` means we should see the inner JSON object, not a
        // wrapping struct.
        assert_eq!(s, r#"{"k":"v"}"#);
    }

    #[test]
    fn external_uses_kind_payload_keys() {
        let t = Trigger::External {
            kind: "k".into(),
            payload: json!(1),
        };
        let s = serde_json::to_string(&t).unwrap();
        assert!(s.contains("\"kind\":\"k\""));
        assert!(s.contains("\"payload\":1"));
        assert!(s.contains("\"type\":\"external\""));
    }
}

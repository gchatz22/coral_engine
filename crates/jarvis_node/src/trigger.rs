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

/// Opaque newtype around the JSON payload describing a mandate patch.
///
/// Carried by the `mandate_update` signal on the workflow host (JAR2-59)
/// — operators ship updated routing/budget/tooling preferences mid-flight
/// and the agent picks them up on the next tick. The kernel does not yet
/// enforce a schema; stage 6 wires the consumption path and stage 1's
/// three-layer resolution (`scratch/temporal_staged_plan.md` § 4 decision
/// 4) defines what fields are legal. Today this is structural plumbing
/// only — semantics are deferred.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MandatePatch(pub serde_json::Value);

impl MandatePatch {
    pub fn new(value: serde_json::Value) -> Self {
        MandatePatch(value)
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
    fn mandate_patch_is_transparent() {
        // Same opaque-JSON contract as `HumanOp`. Lock today's behaviour
        // so the signal payload over the wire matches the inner JSON.
        let p = MandatePatch::new(json!({"model": "gpt-x"}));
        let s = serde_json::to_string(&p).unwrap();
        assert_eq!(s, r#"{"model":"gpt-x"}"#);
        let back: MandatePatch = serde_json::from_str(&s).unwrap();
        assert_eq!(p, back);
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

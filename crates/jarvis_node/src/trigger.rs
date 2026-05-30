//! `Trigger` — what wakes the agent loop. Five classes: scheduled wake,
//! external signal with opaque payload, human override with opaque op,
//! and the two cross-agent variants `ChildOutput` / `ChildRetired`. The
//! cross-agent variants carry `agent_name` on the payload so the prompt
//! renderer doesn't need a `GraphStore` lookup in `assemble_context`.
//!
//! # Cross-agent variants + priority invariant
//!
//! [`Trigger::ChildOutput`] and [`Trigger::ChildRetired`] carry the
//! child's stable [`AgentRef`] plus its operator-readable `agent_name`
//! and the variant-specific payload ([`crate::mandate::OutputId`] or
//! retirement reason). The renderer surfaces `agent_name` as a
//! distinct field — it is *not* folded into the opaque `payload` blob
//! that [`Trigger::External`] carries, so prompts and the TUI can
//! distinguish "a child agent said this" from "an unknown external
//! webhook said this" without an out-of-band lookup.
//!
//! [`crate::trigger_queue::TriggerQueue::drain_ordered`] sorts triggers
//! by class so the agent loop always sees them in the same order
//! regardless of arrival interleaving:
//!
//! ```text
//! Human > External > ChildOutput / ChildRetired > Scheduled
//! ```
//!
//! Operator signals always preempt cross-agent traffic; cross-agent
//! traffic always preempts idle timers. `ChildRetired` shares the
//! cross-agent class with `ChildOutput` (both represent a child telling
//! the parent something actionable); within a class the order is FIFO
//! by arrival.

use crate::agent_ref::AgentRef;
use crate::mandate::OutputId;
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
    /// A human is forcing a mutation. Shape is deliberately opaque; the
    /// kernel does not yet enforce override semantics.
    HumanOverride { op: HumanOp },
    /// A child agent emitted an output. Carries the child's structural
    /// reference + the `OutputId` the parent can later cite in a
    /// `ReconcileChildren` decision. `agent_name` is the human-readable
    /// child name surfaced into the prompt — kept on the payload so the
    /// renderer doesn't need a `GraphStore` handle.
    ChildOutput {
        child_ref: AgentRef,
        agent_name: String,
        output_id: OutputId,
    },
    /// A child agent retired. The parent observes via the normal trigger
    /// drain and decides what (if anything) to do — possibly spawn a
    /// replacement via `Decision::ReplaceChild`. `agent_name` and
    /// `reason` mirror `ChildOutput`'s payload-carries-rendering-data
    /// stance.
    ChildRetired {
        child_ref: AgentRef,
        agent_name: String,
        reason: String,
    },
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
/// Carried by the `mandate_update` signal on the workflow host — operators
/// ship updated routing/budget/tooling preferences mid-flight and the
/// agent picks them up on the next tick. The kernel does not yet enforce
/// a schema; this is structural plumbing only — semantics are deferred.
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

    // ---- Cross-agent variants -----------------------------------------

    use crate::agent_ref::AgentId;
    use uuid::Uuid;

    fn child_agent_id() -> AgentId {
        AgentId::new(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap())
    }

    fn child_ref() -> AgentRef {
        AgentRef::new("graphs/g-1/agents/agent-7", child_agent_id())
    }

    #[test]
    fn child_output_round_trip() {
        let t = Trigger::ChildOutput {
            child_ref: child_ref(),
            agent_name: "fda_scraper".into(),
            output_id: OutputId::from_hex("ab".repeat(32)),
        };
        let s = serde_json::to_string(&t).unwrap();
        let back: Trigger = serde_json::from_str(&s).unwrap();
        assert_eq!(t, back);
    }

    #[test]
    fn child_output_uses_snake_case_tag_and_carries_agent_name() {
        let t = Trigger::ChildOutput {
            child_ref: child_ref(),
            agent_name: "fda_scraper".into(),
            output_id: OutputId::from_hex("ab".repeat(32)),
        };
        let s = serde_json::to_string(&t).unwrap();
        assert!(s.contains("\"type\":\"child_output\""), "wire shape: {s}");
        // `agent_name` is on the payload so the renderer can surface it
        // without a DB lookup.
        assert!(
            s.contains("\"agent_name\":\"fda_scraper\""),
            "wire shape: {s}"
        );
        assert!(s.contains("\"output_id\":"), "wire shape: {s}");
        assert!(s.contains("\"child_ref\":"), "wire shape: {s}");
    }

    #[test]
    fn child_retired_round_trip() {
        let t = Trigger::ChildRetired {
            child_ref: child_ref(),
            agent_name: "fda_scraper".into(),
            reason: "mandate satisfied".into(),
        };
        let s = serde_json::to_string(&t).unwrap();
        let back: Trigger = serde_json::from_str(&s).unwrap();
        assert_eq!(t, back);
    }

    #[test]
    fn child_retired_uses_snake_case_tag_and_carries_reason() {
        let t = Trigger::ChildRetired {
            child_ref: child_ref(),
            agent_name: "fda_scraper".into(),
            reason: "mandate satisfied".into(),
        };
        let s = serde_json::to_string(&t).unwrap();
        assert!(s.contains("\"type\":\"child_retired\""), "wire shape: {s}");
        assert!(
            s.contains("\"agent_name\":\"fda_scraper\""),
            "wire shape: {s}"
        );
        assert!(
            s.contains("\"reason\":\"mandate satisfied\""),
            "wire shape: {s}"
        );
    }
}

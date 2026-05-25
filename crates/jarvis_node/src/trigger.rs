//! `Trigger` — what wakes the agent loop.
//!
//! The bootstrap taxonomy (see `scratch/minimal_node_backend.md` § 6 row 1)
//! was minimal: scheduled wake, an external signal carrying an opaque
//! payload, and a human override carrying an opaque op. Stage 5
//! (`scratch/temporal_staged_plan.md` § 5) adds two cross-agent variants:
//! `ChildOutput` (a child emitted an output the parent may want to
//! reconcile) and `ChildRetired` (a child stopped, parent decides if/how
//! to replace it). Carrying `agent_name` on the payload is the cheapest
//! path — keeps the prompt renderer from needing a `GraphStore` lookup
//! in `assemble_context`.

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
    /// A human is forcing a mutation. Shape is deliberately opaque for
    /// the bootstrap; the kernel does not yet enforce override semantics.
    HumanOverride { op: HumanOp },
    /// A child agent emitted an output. Carries the child's structural
    /// reference + the `OutputId` the parent can later cite in a
    /// `ReconcileChildren` decision (Stage 5.5). `agent_name` is the
    /// human-readable child name surfaced into the prompt — kept on the
    /// payload so the renderer doesn't need a `GraphStore` handle.
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

/// Structural pointer to another agent (a child, for the two `Trigger`
/// variants above). The sibling Stage 5.1 ticket (JAR2-78) owns the
/// canonical definition of this type in `decision.rs`; the placeholder
/// here lets this PR compile in isolation. When 5.1 lands, this stub is
/// deleted and the import switches to `crate::decision::AgentRef`.
///
/// Fields are the minimum the Stage 5 Project description (decisions
/// 6 and 8) names: a stable `agent_id` (the structural-DB identity) and
/// the `workflow_id` the Temporal layer routes signals to. Both are
/// strings today — `jarvis_node` does not depend on `uuid` and pulling
/// it in just for the placeholder would be wasted churn.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRef {
    pub agent_id: String,
    pub workflow_id: String,
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

    // ---- JAR2-79: Stage 5.2 cross-agent variants -----------------------

    fn child_ref() -> AgentRef {
        AgentRef {
            agent_id: "agent-7".into(),
            workflow_id: "graphs/g-1/agents/agent-7".into(),
        }
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
        // without a DB lookup (Stage 5 Project decision 5).
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

    #[test]
    fn agent_ref_round_trip() {
        let r = AgentRef {
            agent_id: "agent-7".into(),
            workflow_id: "graphs/g-1/agents/agent-7".into(),
        };
        let s = serde_json::to_string(&r).unwrap();
        let back: AgentRef = serde_json::from_str(&s).unwrap();
        assert_eq!(r, back);
    }
}

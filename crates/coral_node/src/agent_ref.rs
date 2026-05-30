//! `AgentId` + `AgentRef` — the kernel's stable handles for naming agents.
//!
//! Lives in `coral_node` (not `coral_temporal`) so the kernel can talk
//! about other agents — through `Decision`'s parent-child topology
//! variants and `Trigger::ChildOutput` — without depending on a particular
//! host's workflow plumbing. `ParentRef` (in `coral_temporal::workflow`)
//! stays distinct: it is the workflow-side delivery target carrying
//! `workflow_id` + `signal`; `AgentRef` is the kernel-native, persistable
//! handle.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

/// Newtype over the structural DB's `agents.id` column (a `Uuid`). The
/// kernel never *generates* an `AgentId` — values originate from the
/// `register_child_in_structural_db` activity or from `coral apply`'s
/// YAML walker. Both surfaces hand the id back to the kernel as an
/// opaque token; this newtype keeps that opacity at the type level so a
/// stray `Uuid` from somewhere else in the codebase can't be mistaken
/// for an agent identifier.
///
/// Mirrors `OutputId` / `EvidenceId` in `mandate.rs` / `evidence.rs`:
/// transparent serde so the on-disk and wire forms are the underlying
/// UUID string, plus `Display` for log/trace formatting and `FromStr`
/// for parsing back out of a string.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentId(Uuid);

impl AgentId {
    /// Wrap a pre-allocated `Uuid`. The caller is the source of the value
    /// (structural DB activity or apply walker); this constructor exists
    /// so kernel-side code can route an id through without unwrapping.
    pub fn new(uuid: Uuid) -> Self {
        AgentId(uuid)
    }

    /// Borrow the underlying `Uuid`.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Move the underlying `Uuid` out.
    pub fn into_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Default UUID `Display` is the hyphenated form, matching the
        // structural-DB text representation and what `coral_graph` logs.
        self.0.fmt(f)
    }
}

impl FromStr for AgentId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(AgentId)
    }
}

impl From<Uuid> for AgentId {
    fn from(u: Uuid) -> Self {
        AgentId(u)
    }
}

/// Newtype over the structural DB's `graphs.id` column (a `Uuid`). Mirrors
/// [`AgentId`]'s shape — transparent serde, `Display`, `FromStr`, `From<Uuid>` —
/// so a `Uuid` from anywhere else in the codebase cannot be mistaken for a
/// graph identifier.
///
/// Threaded through child-registration activities so the workflow id
/// (`graphs/<graph_id>/agents/<agent_id>`) can be constructed and
/// `GraphStore::add_agent` (which keys agents by `graph_id`) can be called
/// without a DB lookup.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GraphId(Uuid);

impl GraphId {
    /// Wrap a pre-allocated `Uuid`. Mirrors [`AgentId::new`]'s rationale.
    pub fn new(uuid: Uuid) -> Self {
        GraphId(uuid)
    }

    /// Borrow the underlying `Uuid`.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Move the underlying `Uuid` out.
    pub fn into_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for GraphId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for GraphId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(GraphId)
    }
}

impl From<Uuid> for GraphId {
    fn from(u: Uuid) -> Self {
        GraphId(u)
    }
}

/// Kernel-side handle for an agent, sufficient for the parent-child
/// topology decisions to name a sibling or child.
///
/// Carries both the structural id and the workflow id so the workflow
/// host can route signals via
/// `WorkflowContext::signal_external_workflow(workflow_id, ..)` without
/// looking the id up against the DB on every send. The workflow-id
/// scheme (`graphs/<graph_id>/agents/<agent_id>`) is flat — reparenting
/// does not rewrite ids — so caching the string here is safe.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AgentRef {
    pub workflow_id: String,
    pub agent_id: AgentId,
}

impl AgentRef {
    pub fn new(workflow_id: impl Into<String>, agent_id: AgentId) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            agent_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixed_uuid() -> Uuid {
        // Hand-picked, valid UUID v4. Deterministic — used across the
        // tests in this module so the wire-form assertions are exact.
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
    }

    #[test]
    fn agent_id_transparent_serde_round_trip() {
        let id = AgentId::new(fixed_uuid());
        let s = serde_json::to_string(&id).unwrap();
        // Transparent: the wire form is exactly the hyphenated UUID
        // string, no wrapping object.
        assert_eq!(s, "\"550e8400-e29b-41d4-a716-446655440000\"");
        let back: AgentId = serde_json::from_str(&s).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn agent_id_display_matches_uuid_hyphenated_form() {
        let id = AgentId::new(fixed_uuid());
        assert_eq!(id.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn agent_id_from_str_round_trips_display() {
        let id = AgentId::new(fixed_uuid());
        let parsed: AgentId = id.to_string().parse().unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn agent_id_from_str_rejects_garbage() {
        assert!("not-a-uuid".parse::<AgentId>().is_err());
    }

    #[test]
    fn agent_id_from_uuid_conversion() {
        let u = fixed_uuid();
        let id: AgentId = u.into();
        assert_eq!(id.as_uuid(), &u);
        assert_eq!(id.into_uuid(), u);
    }

    #[test]
    fn agent_ref_round_trip_carries_workflow_id_and_agent_id() {
        let r = AgentRef::new("graphs/g1/agents/a1", AgentId::new(fixed_uuid()));
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(
            v,
            json!({
                "workflow_id": "graphs/g1/agents/a1",
                "agent_id": "550e8400-e29b-41d4-a716-446655440000",
            })
        );
        let back: AgentRef = serde_json::from_value(v).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn agent_ref_field_order_on_wire_is_struct_definition_order() {
        // serde emits struct fields in definition order; locking the
        // expected key ordering keeps the wire-shape contract obvious.
        let r = AgentRef::new("wf", AgentId::new(fixed_uuid()));
        let s = serde_json::to_string(&r).unwrap();
        let workflow_pos = s.find("workflow_id").unwrap();
        let agent_pos = s.find("agent_id").unwrap();
        assert!(workflow_pos < agent_pos, "wire shape: {s}");
    }

    // GraphId mirror tests — keep its type-level shape honest against
    // `AgentId` so a future divergence (e.g. a non-transparent serde form
    // on one but not the other) shows up here.

    #[test]
    fn graph_id_transparent_serde_round_trip() {
        let id = GraphId::new(fixed_uuid());
        let s = serde_json::to_string(&id).unwrap();
        assert_eq!(s, "\"550e8400-e29b-41d4-a716-446655440000\"");
        let back: GraphId = serde_json::from_str(&s).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn graph_id_display_matches_uuid_hyphenated_form() {
        let id = GraphId::new(fixed_uuid());
        assert_eq!(id.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn graph_id_from_str_round_trips_display() {
        let id = GraphId::new(fixed_uuid());
        let parsed: GraphId = id.to_string().parse().unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn graph_id_from_str_rejects_garbage() {
        assert!("not-a-uuid".parse::<GraphId>().is_err());
    }

    #[test]
    fn graph_id_from_uuid_conversion() {
        let u = fixed_uuid();
        let id: GraphId = u.into();
        assert_eq!(id.as_uuid(), &u);
        assert_eq!(id.into_uuid(), u);
    }
}

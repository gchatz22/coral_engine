//! `jarvis_node` — single-node runtime for the Jarvis Engine.
//!
//! This ticket (JAR2-3) lands the typed core: `Mandate`, `Trigger`,
//! `Decision`, `Evidence`, `Output`. Pure data + serde, no behavior beyond
//! constructors, validation, and ID generation. Persistence, the trigger
//! queue, the `Decide` trait, tool dispatch, and the run loop arrive in
//! later tickets (JAR2-4+).

pub mod agent;
// JAR2-57 (stage 3.1): pure per-tick logic — `drain_triggers`, `decide`,
// `dispatch` + `DispatchOutcome`. Hosted by today's `Agent::run` and (in
// stage 3.4+) by `AgentWorkflow`. See `agent_core.rs` module doc.
pub mod agent_core;
pub mod decide_llm;
pub mod decision;
pub mod evidence;
pub mod fs;
pub mod health;
pub mod mandate;
#[cfg(feature = "mcp")]
pub mod mcp;
pub mod model_client;
pub mod scheduler;
// JAR2-51: pluggable per-agent storage abstraction. Today's `AgentFs`
// (`src/fs.rs`) becomes a facade over `Arc<dyn AgentStorage>` in JAR2-53;
// `LocalStorage` (the on-disk backend) lands in JAR2-52. See
// `scratch/agent_storage.md` for the design.
pub mod storage;
pub mod tools;
pub mod trigger;
pub mod trigger_queue;

pub(crate) mod duration_ms {
    //! Serialize/deserialize `std::time::Duration` as `u64` milliseconds.
    //!
    //! Used via `#[serde(with = "crate::duration_ms")]` on `Duration`
    //! fields. Sub-millisecond precision is truncated on the way out.

    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(d: &Duration, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // `as_millis` returns u128; durations large enough to overflow u64
        // ms (~584M years) are not a real concern here, but saturate just
        // in case rather than panic.
        let ms = u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
        s.serialize_u64(ms)
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ms = u64::deserialize(d)?;
        Ok(Duration::from_millis(ms))
    }
}

/// Returns the crate name. Trivial helper from the bootstrap ticket; safe
/// to remove once a richer public surface lands.
pub fn crate_name() -> &'static str {
    "jarvis_node"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crate_name_is_jarvis_node() {
        assert_eq!(crate_name(), "jarvis_node");
    }
}

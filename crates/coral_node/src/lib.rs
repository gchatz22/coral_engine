//! `coral_node` — single-node runtime for the Coral Engine.

pub mod agent;
pub mod agent_core;
pub mod agent_ref;
pub mod conflict;
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
        // Saturate rather than panic: u64-ms overflow would require a
        // ~584M-year duration.
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

/// Returns the crate name.
pub fn crate_name() -> &'static str {
    "coral_node"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crate_name_is_coral_node() {
        assert_eq!(crate_name(), "coral_node");
    }
}

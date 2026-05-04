//! `jarvis_node` — single-node runtime for the Jarvis Engine.
//!
//! This ticket (JAR2-3) lands the typed core: `Mandate`, `Trigger`,
//! `Decision`, `Evidence`, `Output`. Pure data + serde, no behavior beyond
//! constructors, validation, and ID generation. Persistence, the trigger
//! queue, the `Decide` trait, tool dispatch, and the run loop arrive in
//! later tickets (JAR2-4+).

pub mod decision;
pub mod evidence;
pub mod fs;
pub mod mandate;
pub mod scheduler;
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

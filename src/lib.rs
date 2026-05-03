//! `jarvis_node` — single-node runtime for the Jarvis Engine.
//!
//! This crate is a bootstrap skeleton. Subsequent tickets land the agent
//! types, per-agent filesystem, trigger queue, decision trait, tool
//! registry, and run loop on top of this scaffold.

/// Returns the crate name. Trivial helper used to exercise the test harness
/// from the bootstrap ticket; safe to remove once real public API lands.
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

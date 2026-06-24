//! Per-agent idle-deadline stub. Computes "when should the loop next wake
//! if no signal arrives?" for a single agent; `next_deadline` is always
//! `Instant::now() + next_after` so it never drifts past wall time during
//! long ticks.

use std::time::Duration;

use tokio::time::Instant;

/// Whether the loop should arm a self-wake timer for the upcoming cycle.
///
/// A **"never"**-cadence node (`Mandate::idle_period == None`) must still
/// fire its *first* cycle so a leaf with no inbound trigger produces once and
/// a parent spawns its children — otherwise an all-`never` graph would block
/// forever and never propagate a result. So the first wake always arms;
/// afterwards a `never` node waits on triggers alone and never re-arms.
pub fn arm_self_wake(never: bool, is_first_wake: bool) -> bool {
    !never || is_first_wake
}

/// Per-agent scheduler stub. Holds the cadence at which the loop should
/// wake when no external signal has arrived.
#[derive(Debug, Clone)]
pub struct Scheduler {
    next_after: Duration,
}

impl Scheduler {
    /// Build a scheduler with the supplied default cadence. Callers will
    /// typically pass `Mandate::idle_period` here.
    pub fn new(default: Duration) -> Self {
        Self {
            next_after: default,
        }
    }

    /// The next moment the loop should consider itself due to wake. Always
    /// computed relative to `Instant::now()` so the deadline never falls
    /// into the past while the loop is busy.
    pub fn next_deadline(&self) -> Instant {
        Instant::now() + self.next_after
    }

    /// Replace the cadence. The loop calls this when the agent's
    /// `Decision::Idle { next_after }` arm fires.
    pub fn set_next_after(&mut self, d: Duration) {
        self.next_after = d;
    }

    /// Inspect the currently configured cadence. Useful for tests and
    /// telemetry; the run loop itself reads through `next_deadline`.
    pub fn next_after(&self) -> Duration {
        self.next_after
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::{self, Instant};

    use super::*;

    #[test]
    fn arm_self_wake_always_arms_the_first_wake() {
        // A recurring node arms every wake.
        assert!(arm_self_wake(false, true));
        assert!(arm_self_wake(false, false));
        // A `never` node arms only its first wake (so an all-`never` graph's
        // leaves still produce once), then waits on triggers alone.
        assert!(arm_self_wake(true, true));
        assert!(!arm_self_wake(true, false));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn next_deadline_is_now_plus_next_after() {
        let cadence = Duration::from_millis(250);
        let s = Scheduler::new(cadence);
        let before = Instant::now();
        let deadline = s.next_deadline();
        assert_eq!(deadline - before, cadence);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn next_deadline_advances_by_idle_period_after_set_next_after() {
        // The scheduler's deadline advances by `idle_period` once
        // `set_next_after(idle_period)` is called.
        let idle_period = Duration::from_secs(5);
        let mut s = Scheduler::new(Duration::from_millis(100));

        // Sanity: initial cadence is the constructor's value.
        let t0 = Instant::now();
        assert_eq!(s.next_deadline() - t0, Duration::from_millis(100));

        s.set_next_after(idle_period);

        // Advance virtual time so `now` moves; the new deadline must still
        // be `now + idle_period`, i.e. the cadence change took effect.
        time::advance(Duration::from_secs(1)).await;
        let t1 = Instant::now();
        let deadline = s.next_deadline();
        assert_eq!(deadline - t1, idle_period);

        // And it advanced by `idle_period` relative to the moment we set
        // it (give or take the 1s we advanced).
        assert_eq!(deadline - t0, Duration::from_secs(1) + idle_period);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn next_deadline_recomputes_from_now_each_call() {
        let s = Scheduler::new(Duration::from_secs(2));
        let d1 = s.next_deadline();
        time::advance(Duration::from_millis(500)).await;
        let d2 = s.next_deadline();
        // Second call's deadline should be 500ms further out than the
        // first, because `now` moved.
        assert_eq!(d2 - d1, Duration::from_millis(500));
    }

    #[test]
    fn next_after_accessor_round_trips() {
        let mut s = Scheduler::new(Duration::from_millis(10));
        assert_eq!(s.next_after(), Duration::from_millis(10));
        s.set_next_after(Duration::from_millis(777));
        assert_eq!(s.next_after(), Duration::from_millis(777));
    }
}

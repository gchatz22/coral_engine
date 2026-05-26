//! `TriggerQueue` — the per-agent inbox for [`Trigger`]s.
//!
//! Sits behind a `tokio::sync::mpsc::unbounded_channel` (bootstrap choice;
//! backpressure tuning is explicitly out of scope per JAR2-5). External
//! producers hold a clonable [`SignalSink`] and call [`SignalSink::send`].
//! The agent loop owns the [`TriggerQueue`] and uses two operations:
//!
//! * [`TriggerQueue::wait_nonempty`] — async, resolves once at least one
//!   trigger is buffered. **Does not consume** the trigger; the next
//!   [`TriggerQueue::drain_ordered`] returns it.
//! * [`TriggerQueue::drain_ordered`] — synchronous, drains everything
//!   currently buffered (both the internal staging buffer and any pending
//!   items in the channel) and returns them sorted by the rule from
//!   `scratch/minimal_node_backend.md` § 4, tightened in Stage 5
//!   (`scratch/temporal_staged_plan.md` § 5 ticket 5.2 / JAR2-79):
//!   **human > external > child_output > scheduled, FIFO within each
//!   class**. Operator-driven signals always preempt cross-agent traffic;
//!   cross-agent traffic preempts idle timers. `ChildRetired` is treated
//!   as a `ChildOutput`-class signal for ordering purposes — both
//!   represent a child telling the parent something actionable.
//!
//! [`TriggerQueue::push`] is a synchronous self-send used by the scheduler
//! to inject `ScheduledWake` from inside the loop. It routes through the
//! same mpsc sender so there is a single ingress path; this keeps
//! `wait_nonempty` correct without any extra notification plumbing.

use std::collections::VecDeque;

use tokio::sync::mpsc::{self, error::TryRecvError, UnboundedReceiver, UnboundedSender};

use crate::trigger::Trigger;

/// Re-export of the channel send error so callers don't have to depend on
/// `tokio::sync::mpsc` directly. The error fires when every [`SignalSink`]
/// and the queue itself have been dropped, which is effectively "the agent
/// went away."
pub type SendError = mpsc::error::SendError<Trigger>;

/// Clonable handle for external producers to push triggers onto the queue.
#[derive(Clone, Debug)]
pub struct SignalSink {
    tx: UnboundedSender<Trigger>,
}

impl SignalSink {
    /// Push a trigger onto the queue. Returns the trigger back inside a
    /// [`SendError`] if the queue has been dropped.
    pub fn send(&self, t: Trigger) -> Result<(), SendError> {
        self.tx.send(t)
    }
}

/// Per-agent trigger inbox.
///
/// Owns the receiving end of the mpsc channel plus a small staging buffer
/// so that [`wait_nonempty`](Self::wait_nonempty) can observe arrival
/// without consuming the trigger.
#[derive(Debug)]
pub struct TriggerQueue {
    /// Cloned and handed out via [`Self::sink`]; also retained so the loop
    /// can [`Self::push`] synchronously.
    tx: UnboundedSender<Trigger>,
    rx: UnboundedReceiver<Trigger>,
    /// Triggers observed by `wait_nonempty` but not yet returned by
    /// `drain_ordered`. Preserves arrival order; sort happens at drain.
    buffer: VecDeque<Trigger>,
}

impl TriggerQueue {
    /// Build a fresh queue. Returns the queue and a [`SignalSink`] that can
    /// be cloned and handed to external producers.
    pub fn new() -> (Self, SignalSink) {
        let (tx, rx) = mpsc::unbounded_channel();
        let sink = SignalSink { tx: tx.clone() };
        (
            Self {
                tx,
                rx,
                buffer: VecDeque::new(),
            },
            sink,
        )
    }

    /// Mint another [`SignalSink`] for the same queue. Equivalent to
    /// cloning the one returned by [`Self::new`].
    pub fn sink(&self) -> SignalSink {
        SignalSink {
            tx: self.tx.clone(),
        }
    }

    /// Synchronously push a trigger onto the queue. Used by the scheduler
    /// to inject `Trigger::ScheduledWake` from inside the agent loop.
    /// Routes through the mpsc sender so there is a single ingress path
    /// and `wait_nonempty` stays correct without extra notification.
    ///
    /// Self-send into our own retained sender cannot fail (the receiver is
    /// owned by `self`), so this returns `()`.
    pub fn push(&mut self, t: Trigger) {
        // The retained sender's receiver is `self.rx`, which is alive for
        // the lifetime of `&mut self`. An error here would mean the
        // receiver was somehow dropped while we hold it — impossible.
        self.tx
            .send(t)
            .expect("TriggerQueue self-send: receiver is owned by self");
    }

    /// Resolve once at least one trigger is buffered. Does **not** remove
    /// the trigger; the next [`Self::drain_ordered`] will return it.
    ///
    /// If the staging buffer is already non-empty, returns immediately
    /// without touching the receiver. Otherwise awaits the next item on
    /// the channel and stashes it in the buffer.
    pub async fn wait_nonempty(&mut self) {
        if !self.buffer.is_empty() {
            return;
        }
        // `recv()` returns `None` only when every sender (including the
        // one we retain) has been dropped. Since `self` holds a sender,
        // that cannot happen while `&mut self` is live.
        match self.rx.recv().await {
            Some(t) => self.buffer.push_back(t),
            None => {
                unreachable!("TriggerQueue::wait_nonempty: channel closed despite owned sender")
            }
        }
    }

    /// Drain everything currently buffered and return it sorted by class
    /// (Human > External > ChildOutput/ChildRetired > Scheduled), FIFO
    /// within each class. See the module doc for the rationale on slotting
    /// cross-agent signals between operator signals and idle timers.
    ///
    /// Synchronous: pulls the staging buffer first, then non-blockingly
    /// drains any items already sitting in the channel. Items that arrive
    /// after this call will be picked up by the next `wait_nonempty` /
    /// `drain_ordered` cycle.
    pub fn drain_ordered(&mut self) -> Vec<Trigger> {
        let mut out: Vec<Trigger> = self.buffer.drain(..).collect();
        loop {
            match self.rx.try_recv() {
                Ok(t) => out.push(t),
                Err(TryRecvError::Empty) => break,
                // Disconnected can't actually happen — we hold a sender —
                // but treat it the same as Empty for robustness.
                Err(TryRecvError::Disconnected) => break,
            }
        }
        // Stable sort preserves arrival order within each class, giving
        // us FIFO-within-class for free.
        out.sort_by_key(class_priority);
        out
    }
}

/// Lower number = higher priority. Stable-sorting by this key yields the
/// rule from `scratch/minimal_node_backend.md` § 4 tightened by Stage 5
/// (JAR2-79): human > external > child_output/child_retired > scheduled,
/// FIFO within each class. `ChildOutput` and `ChildRetired` share a
/// priority class — both are cross-agent signals from a child, and the
/// ordering between them is FIFO by arrival.
fn class_priority(t: &Trigger) -> u8 {
    match t {
        Trigger::HumanOverride { .. } => 0,
        Trigger::External { .. } => 1,
        Trigger::ChildOutput { .. } | Trigger::ChildRetired { .. } => 2,
        Trigger::ScheduledWake => 3,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use tokio::time::timeout;

    use super::*;
    use crate::agent_ref::{AgentId, AgentRef};
    use crate::mandate::OutputId;
    use crate::trigger::{HumanOp, Trigger};
    use uuid::Uuid;

    fn ext(kind: &str) -> Trigger {
        Trigger::External {
            kind: kind.into(),
            payload: json!({}),
        }
    }

    fn human(action: &str) -> Trigger {
        Trigger::HumanOverride {
            op: HumanOp::new(json!({ "action": action })),
        }
    }

    fn child_ref(name: &str) -> AgentRef {
        // Deterministic UUID derived from the name's first byte so the
        // test fixture is stable across runs without pulling in
        // `uuid/v5`. The two existing call sites pass distinct names
        // ("alpha", "beta"), so distinct UUIDs land.
        let mut bytes = [0u8; 16];
        bytes[0] = name.as_bytes().first().copied().unwrap_or(0);
        let uuid = Uuid::from_bytes(bytes);
        AgentRef::new(format!("graphs/g/agents/{name}"), AgentId::new(uuid))
    }

    fn child_output(name: &str) -> Trigger {
        Trigger::ChildOutput {
            child_ref: child_ref(name),
            agent_name: name.into(),
            output_id: OutputId::from_hex("ab".repeat(32)),
        }
    }

    fn child_retired(name: &str) -> Trigger {
        Trigger::ChildRetired {
            child_ref: child_ref(name),
            agent_name: name.into(),
            reason: "done".into(),
        }
    }

    #[tokio::test]
    async fn drain_returns_human_first_then_external_then_scheduled() {
        let (mut q, sink) = TriggerQueue::new();
        // Push in the worst possible order for a naive FIFO drain.
        sink.send(Trigger::ScheduledWake).unwrap();
        sink.send(ext("webhook")).unwrap();
        sink.send(human("pause")).unwrap();

        // Make sure the sender side is observed by wait_nonempty before we
        // drain (drain itself also pulls from the channel, but this also
        // exercises the buffer path).
        q.wait_nonempty().await;
        let drained = q.drain_ordered();

        assert_eq!(drained.len(), 3);
        assert!(matches!(drained[0], Trigger::HumanOverride { .. }));
        assert!(matches!(drained[1], Trigger::External { .. }));
        assert!(matches!(drained[2], Trigger::ScheduledWake));
    }

    #[tokio::test]
    async fn drain_returns_human_external_child_output_then_scheduled() {
        // JAR2-79: Stage 5 ordering invariant — operator signals preempt
        // cross-agent traffic preempts idle timers.
        // `Human > External > ChildOutput > Scheduled`.
        let (mut q, sink) = TriggerQueue::new();
        // Push in the worst possible order for a naive FIFO drain — the
        // newly-added cross-agent class is the load-bearing assertion.
        sink.send(Trigger::ScheduledWake).unwrap();
        sink.send(child_output("scraper")).unwrap();
        sink.send(ext("webhook")).unwrap();
        sink.send(human("pause")).unwrap();

        q.wait_nonempty().await;
        let drained = q.drain_ordered();

        assert_eq!(drained.len(), 4);
        assert!(matches!(drained[0], Trigger::HumanOverride { .. }));
        assert!(matches!(drained[1], Trigger::External { .. }));
        assert!(matches!(drained[2], Trigger::ChildOutput { .. }));
        assert!(matches!(drained[3], Trigger::ScheduledWake));
    }

    #[tokio::test]
    async fn child_retired_shares_priority_class_with_child_output_and_preempts_scheduled() {
        // `ChildRetired` is the same priority class as `ChildOutput`
        // (both are "a child told us something"); both preempt
        // `ScheduledWake`. Order between them is FIFO by arrival.
        let (mut q, sink) = TriggerQueue::new();
        sink.send(Trigger::ScheduledWake).unwrap();
        sink.send(child_retired("worker_a")).unwrap();
        sink.send(child_output("worker_b")).unwrap();

        q.wait_nonempty().await;
        let drained = q.drain_ordered();

        assert_eq!(drained.len(), 3);
        // FIFO within the shared cross-agent class: retired-a, then
        // output-b, then the scheduled wake.
        assert!(matches!(drained[0], Trigger::ChildRetired { .. }));
        assert!(matches!(drained[1], Trigger::ChildOutput { .. }));
        assert!(matches!(drained[2], Trigger::ScheduledWake));
    }

    #[tokio::test]
    async fn fifo_within_external_class() {
        let (mut q, sink) = TriggerQueue::new();
        sink.send(ext("first")).unwrap();
        sink.send(ext("second")).unwrap();
        sink.send(ext("third")).unwrap();

        q.wait_nonempty().await;
        let drained = q.drain_ordered();

        let kinds: Vec<&str> = drained
            .iter()
            .map(|t| match t {
                Trigger::External { kind, .. } => kind.as_str(),
                _ => panic!("expected External"),
            })
            .collect();
        assert_eq!(kinds, vec!["first", "second", "third"]);
    }

    #[tokio::test]
    async fn fifo_within_human_class_even_when_interleaved_with_other_classes() {
        let (mut q, sink) = TriggerQueue::new();
        sink.send(human("a")).unwrap();
        sink.send(ext("k1")).unwrap();
        sink.send(human("b")).unwrap();
        sink.send(Trigger::ScheduledWake).unwrap();
        sink.send(human("c")).unwrap();

        q.wait_nonempty().await;
        let drained = q.drain_ordered();

        // First three: human a, b, c (FIFO).
        let humans: Vec<&str> = drained
            .iter()
            .take(3)
            .map(|t| match t {
                Trigger::HumanOverride { op } => {
                    op.0.get("action")
                        .and_then(|v| v.as_str())
                        .expect("action string")
                }
                other => panic!("expected human, got {other:?}"),
            })
            .collect();
        assert_eq!(humans, vec!["a", "b", "c"]);
        // Then the external, then the scheduled wake.
        assert!(matches!(drained[3], Trigger::External { .. }));
        assert!(matches!(drained[4], Trigger::ScheduledWake));
    }

    #[tokio::test]
    async fn wait_nonempty_resolves_promptly_when_a_trigger_is_pushed() {
        let (mut q, sink) = TriggerQueue::new();
        // Push first, then wait — must resolve essentially immediately.
        sink.send(Trigger::ScheduledWake).unwrap();

        timeout(Duration::from_millis(100), q.wait_nonempty())
            .await
            .expect("wait_nonempty did not resolve in time");

        // The drain that follows still sees the trigger (i.e. wait_nonempty
        // did not consume it).
        let drained = q.drain_ordered();
        assert_eq!(drained.len(), 1);
        assert!(matches!(drained[0], Trigger::ScheduledWake));
    }

    #[tokio::test]
    async fn wait_nonempty_returns_immediately_when_buffer_already_has_an_item() {
        let (mut q, sink) = TriggerQueue::new();
        sink.send(human("urgent")).unwrap();
        // First wait stashes into the buffer.
        q.wait_nonempty().await;
        // Second wait must not block — buffer already non-empty.
        timeout(Duration::from_millis(50), q.wait_nonempty())
            .await
            .expect("wait_nonempty re-blocked despite non-empty buffer");
    }

    #[tokio::test]
    async fn push_injects_scheduled_wake_synchronously() {
        let (mut q, _sink) = TriggerQueue::new();
        q.push(Trigger::ScheduledWake);
        q.wait_nonempty().await;
        let drained = q.drain_ordered();
        assert_eq!(drained, vec![Trigger::ScheduledWake]);
    }

    #[tokio::test]
    async fn drain_picks_up_items_arriving_after_wait_nonempty() {
        let (mut q, sink) = TriggerQueue::new();
        sink.send(Trigger::ScheduledWake).unwrap();
        q.wait_nonempty().await;
        // Now push two more before drain — they must all show up, sorted.
        sink.send(human("late")).unwrap();
        sink.send(ext("late_ext")).unwrap();
        let drained = q.drain_ordered();
        assert_eq!(drained.len(), 3);
        assert!(matches!(drained[0], Trigger::HumanOverride { .. }));
        assert!(matches!(drained[1], Trigger::External { .. }));
        assert!(matches!(drained[2], Trigger::ScheduledWake));
    }

    #[test]
    fn signal_sink_is_clonable_and_both_clones_deliver() {
        let (mut q, sink_a) = TriggerQueue::new();
        let sink_b = sink_a.clone();
        sink_a.send(ext("a")).unwrap();
        sink_b.send(ext("b")).unwrap();
        let drained = q.drain_ordered();
        assert_eq!(drained.len(), 2);
    }
}

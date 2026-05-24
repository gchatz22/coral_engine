//! Stage 3.2 (JAR2-58) — Jarvis worker registration helpers.
//! Stage 3.4 (JAR2-60) — replaces `NoopActivities` with [`AgentActivities`].
//! Stage 3.4.5 (JAR2-69) — process-wide `AgentStorage` install/access for
//! activity bodies (the OnceLock pattern from
//! `scratch/temporal_rust_sdk_smoke.md` § 3.4).
//! Stage 3.6 (JAR2-62) — process-wide [`Decide`] install/access for the
//! `decide_next_action` activity body. Mirrors the storage shape
//! exactly: install once at worker boot, panic on double install, panic
//! on access-before-install. The `Decide` trait is vendor-neutral and
//! always available (un-gated in `jarvis_node::decision`), so this
//! library compiles regardless of which vendor features are turned on.
//! `bin/worker.rs` is the only place that picks the concrete
//! [`LlmDecide`] vendor and is itself `#[cfg]`-gated.
//!
//! Lives in the library so both the `worker` binary and integration
//! tests (in `tests/`) share the same registration call site.
//!
//! Stage 3.5–3.10 fills in real activity bodies inside
//! [`crate::activities::AgentActivities`]; the registration call here is
//! unchanged across those tickets. Those bodies will reach for the
//! shared storage via [`agent_storage`] to build per-tick [`AgentFs`]
//! instances.

use std::sync::{Arc, OnceLock};

use anyhow::Result;
use jarvis_node::decision::Decide;
use jarvis_node::storage::AgentStorage;
use temporalio_client::Client;
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::CoreRuntime;

use crate::activities::AgentActivities;
use crate::workflow::AgentWorkflow;

/// Default task queue. Live tests override via `TEMPORAL_TASK_QUEUE`.
pub const DEFAULT_TASK_QUEUE: &str = "jarvis-agents";

/// Process-wide [`AgentStorage`] backend. Installed once at worker boot
/// (or test setup) via [`install_agent_storage`]; accessed from inside
/// activity bodies via [`agent_storage`].
///
/// Per `scratch/temporal_rust_sdk_smoke.md` § 3.4 the activity macro
/// owns the registered value, so shared state has to live behind a
/// `static` rather than be threaded through the activity impl block.
static AGENT_STORAGE: OnceLock<Arc<dyn AgentStorage>> = OnceLock::new();

/// Install the process-wide [`AgentStorage`] backend.
///
/// Worker binaries call this at boot (against a configured FS root, see
/// `bin/worker.rs`); test harnesses call this with a `MemoryStorage`
/// instance during their setup.
///
/// **Panics** on double install. This is loud-on-misuse rather than
/// silent because two backends in one process would silently disagree
/// about where evidence lives — better to crash early.
pub fn install_agent_storage(storage: Arc<dyn AgentStorage>) {
    AGENT_STORAGE
        .set(storage)
        .map_err(|_| ())
        .expect("install_agent_storage called twice; one process, one backend");
}

/// Access the installed [`AgentStorage`] backend.
///
/// Returns a cheap [`Arc`] clone — activity bodies hand the result to
/// `AgentFs::new_with_storage(storage, &handle.prefix, &mandate).await`
/// each tick, since `AgentFs` is per-prefix while the underlying
/// storage is process-wide.
///
/// **Panics** if [`install_agent_storage`] hasn't been called.
/// Activities only run after the worker has booted (which installs
/// before `worker.run()`), so callers from activity bodies are
/// structurally safe.
pub fn agent_storage() -> Arc<dyn AgentStorage> {
    AGENT_STORAGE
        .get()
        .cloned()
        .expect("agent_storage() accessed before install_agent_storage()")
}

/// Process-wide [`Decide`] implementation used by the
/// `decide_next_action` activity body (JAR2-62). Installed once at
/// worker boot via [`install_decide`]; accessed from the activity body
/// via [`decide_impl`].
///
/// Same rationale as [`AGENT_STORAGE`]: the activity macro takes a
/// value-typed bundle (smoke § 3.4), so shared state has to live behind
/// a `static`. Vendor selection per `scratch/temporal_staged_plan.md`
/// § 8 decision 4 happens at the worker-boot layer (env-driven today,
/// structural DB later); the activity body itself doesn't relitigate
/// vendor per tick.
///
/// The trait object is `Arc<dyn Decide>` rather than a concrete
/// `LlmDecide` so:
/// 1. Hermetic tests can install a `MockDecide` without dragging vendor
///    features into the `jarvis_temporal` test build.
/// 2. The library compiles with zero vendor features — only the
///    worker binary needs to gate vendor-specific constructors.
static DECIDE_IMPL: OnceLock<Arc<dyn Decide>> = OnceLock::new();

/// Install the process-wide [`Decide`] implementation used by the
/// `decide_next_action` activity body.
///
/// Worker binaries call this at boot after constructing an
/// [`LlmDecide`] from env-driven vendor selection (see
/// `bin/worker.rs`). Test harnesses install a `MockDecide` directly.
///
/// **Panics** on double install. Mirrors [`install_agent_storage`] —
/// two implementations in one process would silently disagree about
/// vendor / model routing and crashing loudly is friendlier than
/// silently diverging.
pub fn install_decide(decide: Arc<dyn Decide>) {
    DECIDE_IMPL
        .set(decide)
        .map_err(|_| ())
        .expect("install_decide called twice; one process, one Decide impl");
}

/// Access the installed [`Decide`] implementation.
///
/// Returns a cheap [`Arc`] clone — the `decide_next_action` activity
/// body holds the clone only for the duration of one
/// `Decide::decide(...)` call. The trait method takes `&self`, so the
/// clone is purely for ownership at the call site, not for
/// concurrency.
///
/// **Panics** if [`install_decide`] hasn't been called. The
/// `decide_next_action` activity body checks the test-injected
/// `DECISION_SCRIPT` *before* reaching for the installed implementation
/// (guardrail 5 of the ticket), so unit tests that script every
/// decision don't need to install one.
pub fn decide_impl() -> Arc<dyn Decide> {
    DECIDE_IMPL
        .get()
        .cloned()
        .expect("decide_impl() accessed before install_decide()")
}

/// Build a worker registering [`AgentWorkflow`] + [`AgentActivities`] on
/// the given task queue.
///
/// `Worker::new` returns `Box<dyn Error>` (not `Send + Sync`); we wrap
/// it via `anyhow::anyhow!("{e}")` so `?` works against `anyhow::Result`.
/// See `scratch/temporal_rust_sdk_smoke.md` § 3.5.
///
/// `register_activities` takes the bare value, not `Arc<T>` — smoke
/// § 3.4. The macro impls `ActivityImplementer for AgentActivities` and
/// `register_activities` wraps in `Arc` internally.
pub fn build_worker(runtime: &CoreRuntime, client: Client, task_queue: &str) -> Result<Worker> {
    let opts = WorkerOptions::new(task_queue)
        .register_workflow::<AgentWorkflow>()
        .register_activities(AgentActivities)
        .build();
    Worker::new(runtime, client, opts).map_err(|e| anyhow::anyhow!("Worker::new failed: {e}"))
}

#[cfg(test)]
mod tests {
    //! Coverage for the install/access shape. The `OnceLock` is
    //! process-wide, so these run under a single shared
    //! `#[serial]`-style guard (the same pattern JAR2-60 used for
    //! `DECISION_SCRIPT`).
    //!
    //! Note: a test that asserts `install_agent_storage` panics on
    //! double-install can only run once per process — once the storage
    //! is installed, any subsequent test that also wanted to install
    //! would also panic. We resolve this by having a single test that
    //! installs (succeeding), then re-installs (catching the panic),
    //! then accesses (succeeding) — covering all three behaviors in
    //! one shot.
    //!
    //! JAR2-62 extends this with the `install_decide` / `decide_impl`
    //! pair, which has the same once-per-process constraint. The
    //! storage and decide statics are independent `OnceLock`s, so we
    //! cover both inside the same `#[test]` (which by virtue of being a
    //! single test runs sequentially with itself; cargo's parallel
    //! runner won't multiplex it). This keeps the file's one-test-per-
    //! process invariant intact.
    use super::*;
    use jarvis_node::decision::{Decide, Decision, MockDecide};
    use jarvis_node::storage::MemoryStorage;
    use std::time::Duration;

    #[test]
    fn install_then_access_then_double_install_panics() {
        // ---- storage half ------------------------------------------------
        // First install succeeds.
        install_agent_storage(Arc::new(MemoryStorage::new()));

        // Access returns a usable Arc.
        let s = agent_storage();
        // `Arc::strong_count >= 2` (the OnceLock holds one, we hold one).
        assert!(Arc::strong_count(&s) >= 2);

        // Second install panics — caught with `catch_unwind` so the
        // process stays alive for any other tests.
        let result = std::panic::catch_unwind(|| {
            install_agent_storage(Arc::new(MemoryStorage::new()));
        });
        assert!(result.is_err(), "double install_agent_storage should panic");

        // ---- decide half (JAR2-62) --------------------------------------
        // First install succeeds. `MockDecide` is the trait's
        // test-only implementation in `jarvis_node::decision`; we use
        // an empty script because we only assert the install/access
        // wiring, not the trait's behavior (covered by JAR2-19).
        let decide: Arc<dyn Decide> = Arc::new(MockDecide::new(vec![Decision::Idle {
            next_after: Duration::from_millis(1),
        }]));
        install_decide(decide);

        // Access returns a usable Arc.
        let d = decide_impl();
        assert!(Arc::strong_count(&d) >= 2);

        // Second install panics.
        let result = std::panic::catch_unwind(|| {
            install_decide(Arc::new(MockDecide::new(vec![])));
        });
        assert!(result.is_err(), "double install_decide should panic");
    }
}

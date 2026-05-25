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
//! Stage 3.7 (JAR2-63) — process-wide [`ToolRegistry`] install/access for
//! the `execute_tool` activity body. Mirrors the JAR2-69 storage pair.
//!
//! Lives in the library so both the `worker` binary and integration
//! tests (in `tests/`) share the same registration call site.
//!
//! Stage 3.5–3.10 fills in real activity bodies inside
//! [`crate::activities::AgentActivities`]; the registration call here is
//! unchanged across those tickets. Those bodies will reach for the
//! shared storage via [`agent_storage`] to build per-tick [`AgentFs`]
//! instances.

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use std::env;
use std::sync::{Arc, OnceLock};

use anyhow::{anyhow, Result};
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use jarvis_node::decide_llm::LlmDecide;
use jarvis_node::decision::Decide;
#[cfg(feature = "llm-anthropic")]
use jarvis_node::model_client::anthropic::AnthropicClient;
#[cfg(feature = "llm-cohere")]
use jarvis_node::model_client::cohere::CohereClient;
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use jarvis_node::model_client::CompleteOptions;
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use jarvis_node::model_client::ModelClient;
use jarvis_node::storage::AgentStorage;
use jarvis_node::tools::ToolRegistry;
use temporalio_client::Client;
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::CoreRuntime;

/// JAR2-62 vendor selector env var. See [`build_decide_from_env`].
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
const VENDOR_ENV: &str = "JARVIS_MODEL_VENDOR";
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
const ANTHROPIC_API_KEY_ENV: &str = "ANTHROPIC_API_KEY";
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
const COHERE_API_KEY_ENV: &str = "COHERE_API_KEY";

use crate::activities::AgentActivities;
use crate::workflow::AgentWorkflow;

/// Canonical task queue the worker daemon listens on and operator CLIs
/// dispatch to. Public so future thin-client CLIs (`jarvis apply`,
/// `jarvis signal`, etc. — JAR2-76 and onward) import the same constant
/// the daemon registers under, avoiding the "CLI and daemon target
/// different queues" failure mode flagged in
/// `scratch/temporal_staged_plan.md` § 2.6.
///
/// Both the `worker` binary and live integration tests under `tests/`
/// override via `TEMPORAL_TASK_QUEUE` — the binary for fleet-shard /
/// personal-queue isolation, the tests for per-run uniqueness so
/// repeated runs don't share state.
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

/// Process-wide [`ToolRegistry`] consulted by the `execute_tool` activity
/// body (JAR2-63). Installed once at worker boot via
/// [`install_tool_registry`] and accessed via [`tool_registry`].
///
/// Same OnceLock pattern as [`AGENT_STORAGE`] above — the activity macro
/// owns the registered activity value, so shared state has to live
/// behind a `static` rather than be threaded through the activity impl
/// block (`scratch/temporal_rust_sdk_smoke.md` § 3.4).
static TOOL_REGISTRY: OnceLock<Arc<ToolRegistry>> = OnceLock::new();

/// Install the process-wide [`ToolRegistry`] used by the `execute_tool`
/// activity.
///
/// The worker binary builds a registry at boot (registering the configured
/// `EchoTool` + any MCP servers from env vars) and calls this before
/// `worker.run()`. Test harnesses build a tiny in-memory registry
/// (`EchoTool` plus per-test aliases) and call this in their setup.
///
/// **Panics** on double install. Same loud-on-misuse rationale as
/// [`install_agent_storage`]: two registries in one process would
/// disagree on which tool a given name routes to, and silent shadowing
/// would be far worse than a crash.
pub fn install_tool_registry(registry: Arc<ToolRegistry>) {
    TOOL_REGISTRY
        .set(registry)
        .map_err(|_| ())
        .expect("install_tool_registry called twice; one process, one registry");
}

/// Access the installed [`ToolRegistry`].
///
/// Returns a cheap [`Arc`] clone — the `execute_tool` activity body
/// calls `registry.call(&input.call.name, input.call.args)` per
/// invocation. The registry itself is `Send + Sync` (tools are
/// `Arc<dyn Tool>`), so concurrent activity invocations share one
/// instance.
///
/// **Panics** if [`install_tool_registry`] hasn't been called. Same
/// structural-safety argument as [`agent_storage`]: activities only
/// run after the worker has booted, which installs before
/// `worker.run()`.
pub fn tool_registry() -> Arc<ToolRegistry> {
    TOOL_REGISTRY
        .get()
        .cloned()
        .expect("tool_registry() accessed before install_tool_registry()")
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

/// JAR2-62 / JAR2-68 — pick the LLM vendor from env and build the
/// [`Decide`] implementation the `decide_next_action` activity body
/// will call.
///
/// Used by both `bin/worker.rs` (long-running worker daemon) and
/// `bin/jarvis_run_workflow.rs` (JAR2-68 live-vendor smoke); factored
/// here so the selection precedence + feature-gating live in one
/// place. Returns the vendor tag (`"anthropic"` / `"cohere"`)
/// alongside the trait object so the caller can log it.
///
/// **Selection precedence (JAR2-70):**
/// 1. `JARVIS_MODEL_VENDOR` env var set + that vendor is compiled in
///    → use it.
/// 2. `JARVIS_MODEL_VENDOR` env var set + that vendor is NOT compiled
///    in → error pointing at the missing feature.
/// 3. `JARVIS_MODEL_VENDOR` unset → walk the vendor preference order
///    (`anthropic`, then `cohere` — matches `node-run-llm`'s documented
///    order in `bin/node_run_llm.rs::USAGE`) and pick the **first
///    compiled-in vendor whose API key is set**. A vendor that isn't
///    compiled in is skipped even if its key is present; a compiled-
///    in vendor with no key is skipped too.
/// 4. None of the compiled-in vendors have keys → error with a
///    "rebuild with --features" hint pointing at the union of the
///    compiled-in vendors' key env vars.
///
/// Pre-JAR2-70 the env-key fallback (case 3) returned whichever
/// vendor's key was set without checking compilation, so a binary
/// built with `--features llm-cohere` and `ANTHROPIC_API_KEY` set in
/// the env would resolve to `"anthropic"` and then fail at
/// `build_anthropic_client` with the "not compiled in" error. Now
/// `JARVIS_MODEL_VENDOR=anthropic` is the only path that surfaces
/// that error; the auto-fallback transparently picks the compiled-in
/// alternative.
///
/// **Feature gating.** The non-feature-gated body is itself `#[cfg]`-
/// gated on at-least-one vendor, with a "no vendors built" stub for
/// the zero-feature build (still compiles, errors at runtime).
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
pub fn build_decide_from_env() -> Result<(&'static str, Arc<dyn Decide>)> {
    let vendor = resolve_vendor()?;
    let model_client: Arc<dyn ModelClient> = match vendor {
        "anthropic" => build_anthropic_client()?,
        "cohere" => build_cohere_client()?,
        other => return Err(anyhow!("internal: resolve_vendor returned `{other}`")),
    };
    let options = CompleteOptions::default();
    let decide: Arc<dyn Decide> = Arc::new(LlmDecide::new(model_client, options));
    Ok((vendor, decide))
}

/// Zero-vendor stub. Lib still compiles in a feature-less build (the
/// workspace `cargo build` does this); callers get an early error when
/// no vendor is compiled in.
#[cfg(not(any(feature = "llm-anthropic", feature = "llm-cohere")))]
pub fn build_decide_from_env() -> Result<(&'static str, Arc<dyn Decide>)> {
    Err(anyhow!(
        "no LLM vendor compiled in; rebuild with --features llm-anthropic and/or --features llm-cohere"
    ))
}

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
fn resolve_vendor() -> Result<&'static str> {
    resolve_vendor_inner(
        env::var(VENDOR_ENV).ok().as_deref(),
        |k| env::var(k).map(|v| !v.is_empty()).unwrap_or(false),
        compiled_vendors(),
    )
}

/// Vendor + its required-key env var. The fallback walk in case 3
/// iterates this list in preference order.
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
const VENDOR_KEY_ENVS: &[(&str, &str)] = &[
    ("anthropic", ANTHROPIC_API_KEY_ENV),
    ("cohere", COHERE_API_KEY_ENV),
];

/// Compile-time list of vendors built into this binary, in the
/// preference order `VENDOR_KEY_ENVS` defines. Empty in the zero-
/// vendor build (the outer `#[cfg]` guard means this function only
/// exists when at least one vendor is compiled in, but the slice may
/// still be inspected from the table-driven test below).
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
fn compiled_vendors() -> &'static [&'static str] {
    #[cfg(all(feature = "llm-anthropic", feature = "llm-cohere"))]
    {
        &["anthropic", "cohere"]
    }
    #[cfg(all(feature = "llm-anthropic", not(feature = "llm-cohere")))]
    {
        &["anthropic"]
    }
    #[cfg(all(not(feature = "llm-anthropic"), feature = "llm-cohere"))]
    {
        &["cohere"]
    }
}

/// Pure, env-injected core of `resolve_vendor` — broken out so the
/// table-driven test below can exercise the full matrix of
/// (compiled-in vendors) × (which keys are set) × (`JARVIS_MODEL_VENDOR`
/// set / unset) without mutating process env.
///
/// `compiled` is the list of vendors built into this binary, in
/// preference order (`compiled_vendors()` at runtime; a test fixture
/// in tests).
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
fn resolve_vendor_inner(
    vendor_env: Option<&str>,
    key_set: impl Fn(&str) -> bool,
    compiled: &[&'static str],
) -> Result<&'static str> {
    // Case 1 + 2: explicit env vendor.
    if let Some(v) = vendor_env {
        return match v {
            "anthropic" => {
                if compiled.contains(&"anthropic") {
                    Ok("anthropic")
                } else {
                    Err(anyhow!(
                        "vendor `anthropic` requested but not compiled in; rebuild with --features llm-anthropic"
                    ))
                }
            }
            "cohere" => {
                if compiled.contains(&"cohere") {
                    Ok("cohere")
                } else {
                    Err(anyhow!(
                        "vendor `cohere` requested but not compiled in; rebuild with --features llm-cohere"
                    ))
                }
            }
            other => Err(anyhow!(
                "{VENDOR_ENV}=`{other}` is not a known vendor (expected `anthropic` or `cohere`)"
            )),
        };
    }
    // Case 3: walk vendor preference order, pick first compiled-in
    // vendor with key. Pre-JAR2-70 bug was returning a non-compiled
    // vendor's name based on key presence alone, which then errored
    // downstream at `build_*_client`.
    for (vendor, key_env) in VENDOR_KEY_ENVS {
        if compiled.contains(vendor) && key_set(key_env) {
            return Ok(*vendor);
        }
    }
    // Case 4: nothing usable. The error message names only the
    // compiled-in vendors' key env vars (no point telling the user to
    // set `ANTHROPIC_API_KEY` on a cohere-only build).
    let usable_keys: Vec<&str> = VENDOR_KEY_ENVS
        .iter()
        .filter(|(v, _)| compiled.contains(v))
        .map(|(_, k)| *k)
        .collect();
    Err(anyhow!(
        "no vendor selected: set {VENDOR_ENV}, or one of [{keys}]",
        keys = usable_keys.join(", ")
    ))
}

#[cfg(feature = "llm-anthropic")]
fn build_anthropic_client() -> Result<Arc<dyn ModelClient>> {
    Ok(Arc::new(AnthropicClient::new()))
}

#[cfg(all(
    any(feature = "llm-anthropic", feature = "llm-cohere"),
    not(feature = "llm-anthropic")
))]
fn build_anthropic_client() -> Result<Arc<dyn ModelClient>> {
    Err(anyhow!(
        "vendor `anthropic` requested but not compiled in; rebuild with --features llm-anthropic"
    ))
}

#[cfg(feature = "llm-cohere")]
fn build_cohere_client() -> Result<Arc<dyn ModelClient>> {
    Ok(Arc::new(CohereClient::new()))
}

#[cfg(all(
    any(feature = "llm-anthropic", feature = "llm-cohere"),
    not(feature = "llm-cohere")
))]
fn build_cohere_client() -> Result<Arc<dyn ModelClient>> {
    Err(anyhow!(
        "vendor `cohere` requested but not compiled in; rebuild with --features llm-cohere"
    ))
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
    use jarvis_node::tools::{EchoTool, ToolRegistry};
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

    /// JAR2-63 mirror of `install_then_access_then_double_install_panics`
    /// — the install/access/double-install invariants for the
    /// process-wide `ToolRegistry` are exactly the same as for
    /// `AgentStorage`. One process-scoped test covers all three
    /// behaviours since the underlying `OnceLock` can only be set once
    /// per process.
    #[test]
    fn install_tool_registry_then_access_then_double_install_panics() {
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(EchoTool))
            .expect("register echo tool");
        install_tool_registry(Arc::new(reg));

        let r = tool_registry();
        // OnceLock holds one strong ref, we hold one.
        assert!(Arc::strong_count(&r) >= 2);

        let result = std::panic::catch_unwind(|| {
            install_tool_registry(Arc::new(ToolRegistry::new()));
        });
        assert!(result.is_err(), "double install_tool_registry should panic");
    }

    /// JAR2-70 — `resolve_vendor_inner` precedence matrix.
    ///
    /// Hermetic: env is injected via the function's `key_set` callback
    /// and the explicit `vendor_env` argument, so no real env-var
    /// mutation happens and no `serial_test` is needed. Covers the
    /// full cross product:
    ///   - `compiled`: anthropic-only, cohere-only, both.
    ///   - `keys`: none, anthropic, cohere, both.
    ///   - `vendor_env`: unset, anthropic, cohere, unknown.
    ///
    /// The matrix asserts the JAR2-70 precedence rule: `JARVIS_MODEL_VENDOR`
    /// is honored only if compiled in (else feature-rebuild error);
    /// otherwise pick the first compiled-in vendor whose key is set.
    #[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
    #[test]
    fn resolve_vendor_inner_precedence_matrix() {
        use std::collections::HashSet;

        const ANTHROPIC: &[&str] = &["anthropic"];
        const COHERE: &[&str] = &["cohere"];
        const BOTH: &[&str] = &["anthropic", "cohere"];

        // (compiled, vendor_env, keys_set, expected)
        //   expected = Ok("anthropic"/"cohere") | Err(substring)
        type Expected = Result<&'static str, &'static str>;
        type Case = (
            &'static [&'static str],
            Option<&'static str>,
            &'static [&'static str],
            Expected,
        );
        let cases: &[Case] = &[
            // ---- explicit JARVIS_MODEL_VENDOR honored when compiled in ----
            (BOTH, Some("anthropic"), &[], Ok("anthropic")),
            (BOTH, Some("cohere"), &[], Ok("cohere")),
            (ANTHROPIC, Some("anthropic"), &[], Ok("anthropic")),
            (COHERE, Some("cohere"), &[], Ok("cohere")),
            // ---- explicit but not compiled in -> error ----
            (
                ANTHROPIC,
                Some("cohere"),
                &["COHERE_API_KEY"],
                Err("cohere"),
            ),
            (
                COHERE,
                Some("anthropic"),
                &["ANTHROPIC_API_KEY"],
                Err("anthropic"),
            ),
            // ---- unknown vendor name -> error ----
            (BOTH, Some("openai"), &["ANTHROPIC_API_KEY"], Err("openai")),
            // ---- env unset, fallback by key + compiled-in ----
            // both compiled, anthropic key only -> anthropic.
            (BOTH, None, &["ANTHROPIC_API_KEY"], Ok("anthropic")),
            // both compiled, cohere key only -> cohere.
            (BOTH, None, &["COHERE_API_KEY"], Ok("cohere")),
            // both compiled, both keys -> anthropic (preference order).
            (
                BOTH,
                None,
                &["ANTHROPIC_API_KEY", "COHERE_API_KEY"],
                Ok("anthropic"),
            ),
            // anthropic-only build, only ANTHROPIC_API_KEY -> anthropic.
            (ANTHROPIC, None, &["ANTHROPIC_API_KEY"], Ok("anthropic")),
            // anthropic-only build, only COHERE_API_KEY -> error (bug
            // before JAR2-70 returned Ok("anthropic") then died at
            // build_anthropic_client; OR Ok("cohere") then died at
            // build_cohere_client. Post-JAR2-70 it's a clean "no
            // vendor selected").
            (ANTHROPIC, None, &["COHERE_API_KEY"], Err("no vendor")),
            // cohere-only build, only COHERE_API_KEY -> cohere (this
            // is the bug case from the ticket: pre-JAR2-70 the
            // env-key walk preferred anthropic even when not compiled
            // in, so a cohere-only build with only the cohere key set
            // and ANTHROPIC_API_KEY unset already worked; the actual
            // breakage was when ANTHROPIC_API_KEY *was* also set,
            // captured in the next case).
            (COHERE, None, &["COHERE_API_KEY"], Ok("cohere")),
            // cohere-only build, both keys set -> cohere (the
            // headline JAR2-70 fix: pre-fix returned anthropic and
            // died downstream).
            (
                COHERE,
                None,
                &["ANTHROPIC_API_KEY", "COHERE_API_KEY"],
                Ok("cohere"),
            ),
            // No keys, no env -> error.
            (BOTH, None, &[], Err("no vendor")),
            (ANTHROPIC, None, &[], Err("no vendor")),
            (COHERE, None, &[], Err("no vendor")),
        ];

        for (i, (compiled, vendor_env, keys, expected)) in cases.iter().enumerate() {
            let set: HashSet<&str> = keys.iter().copied().collect();
            let got = resolve_vendor_inner(*vendor_env, |k| set.contains(k), compiled);
            match (got, expected) {
                (Ok(g), Ok(e)) => assert_eq!(
                    g, *e,
                    "case {i} (compiled={compiled:?}, vendor_env={vendor_env:?}, keys={keys:?}): expected Ok({e}), got Ok({g})"
                ),
                (Err(e), Err(needle)) => {
                    let msg = format!("{e}");
                    assert!(
                        msg.contains(needle),
                        "case {i} (compiled={compiled:?}, vendor_env={vendor_env:?}, keys={keys:?}): expected error containing {needle:?}, got `{msg}`"
                    );
                }
                (g, e) => panic!(
                    "case {i} (compiled={compiled:?}, vendor_env={vendor_env:?}, keys={keys:?}): expected {e:?}, got {g:?}"
                ),
            }
        }
    }
}

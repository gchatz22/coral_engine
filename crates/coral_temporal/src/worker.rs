//! Worker registration helpers and process-wide handles for activity bodies.
//!
//! Lives in the library so both the `worker` binary and integration tests
//! share one registration call site. Process-wide singletons
//! ([`AgentStorage`], [`Decide`], [`ToolRegistryProvider`], [`StructuralDbStore`])
//! are installed once at worker boot and accessed from activity bodies via
//! the matching getter. The `OnceLock` shape is required because the
//! Temporal SDK's activity macro takes a value-typed bundle, so shared
//! state cannot be threaded through the impl block.

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use std::env;
use std::sync::{Arc, OnceLock};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use coral_node::agent_ref::{AgentId, GraphId};
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use coral_node::decide_llm::LlmDecide;
use coral_node::decision::Decide;
#[cfg(feature = "llm-anthropic")]
use coral_node::model_client::anthropic::AnthropicClient;
#[cfg(feature = "llm-cohere")]
use coral_node::model_client::cohere::CohereClient;
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use coral_node::model_client::CompleteOptions;
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
use coral_node::model_client::ModelClient;
use coral_node::storage::AgentStorage;
use coral_node::tools::ToolRegistry;
use temporalio_client::Client;
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::CoreRuntime;

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
const VENDOR_ENV: &str = "CORAL_MODEL_VENDOR";
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
const ANTHROPIC_API_KEY_ENV: &str = "ANTHROPIC_API_KEY";
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
const COHERE_API_KEY_ENV: &str = "COHERE_API_KEY";

use crate::activities::AgentActivities;
use crate::workflow::AgentWorkflow;

/// Canonical task queue the worker daemon listens on and operator CLIs
/// dispatch to. Overridden via `TEMPORAL_TASK_QUEUE` for fleet-shard
/// isolation or per-test uniqueness.
pub const DEFAULT_TASK_QUEUE: &str = "coral-agents";

/// Process-wide [`AgentStorage`] backend installed via
/// [`install_agent_storage`] and accessed via [`agent_storage`].
static AGENT_STORAGE: OnceLock<Arc<dyn AgentStorage>> = OnceLock::new();

/// Install the process-wide [`AgentStorage`] backend. Panics on double
/// install — two backends in one process would silently disagree about
/// where evidence lives.
pub fn install_agent_storage(storage: Arc<dyn AgentStorage>) {
    AGENT_STORAGE
        .set(storage)
        .map_err(|_| ())
        .expect("install_agent_storage called twice; one process, one backend");
}

/// Access the installed [`AgentStorage`] backend. Panics if
/// [`install_agent_storage`] hasn't been called.
pub fn agent_storage() -> Arc<dyn AgentStorage> {
    AGENT_STORAGE
        .get()
        .cloned()
        .expect("agent_storage() accessed before install_agent_storage()")
}

/// Process-wide [`Decide`] implementation used by the
/// `decide_next_action` activity body. Installed via [`install_decide`]
/// and accessed via [`decide_impl`].
///
/// The trait object is `Arc<dyn Decide>` so hermetic tests can install a
/// `MockDecide` without dragging vendor features into the test build,
/// and the library compiles with zero vendor features.
static DECIDE_IMPL: OnceLock<Arc<dyn Decide>> = OnceLock::new();

/// Install the process-wide [`Decide`] implementation. Panics on double
/// install.
pub fn install_decide(decide: Arc<dyn Decide>) {
    DECIDE_IMPL
        .set(decide)
        .map_err(|_| ())
        .expect("install_decide called twice; one process, one Decide impl");
}

/// Access the installed [`Decide`] implementation. Panics if
/// [`install_decide`] hasn't been called. Unit tests that script every
/// decision via `DECISION_SCRIPT` don't reach this path.
pub fn decide_impl() -> Arc<dyn Decide> {
    DECIDE_IMPL
        .get()
        .cloned()
        .expect("decide_impl() accessed before install_decide()")
}

/// Resolves the [`ToolRegistry`] the `execute_tool` activity dispatches
/// through, keyed by the calling agent's `graph_id`. The worker daemon
/// installs a DB-backed implementation that builds one registry per graph
/// (builtin `echo` plus that graph's MCP servers, read from the structural
/// DB); tests install a static or map-backed double.
///
/// Defined as a trait (not a concrete type) so the registry-building path —
/// which connects MCP servers and therefore needs `coral_node/mcp` — lives
/// in the composition root (`coral_worker`) and this library stays
/// feature-agnostic, mirroring how [`StructuralDbStore`] is defined here but
/// implemented by `GraphStore`.
#[async_trait]
pub trait ToolRegistryProvider: Send + Sync {
    /// Return the registry for `graph_id`. Implementations may build it
    /// lazily on first call and cache it; the returned `Arc` keeps any
    /// spawned MCP subprocesses alive for as long as it is held.
    async fn registry_for_graph(&self, graph_id: GraphId) -> anyhow::Result<Arc<ToolRegistry>>;
}

/// A [`ToolRegistryProvider`] that ignores `graph_id` and hands back one
/// shared registry for every graph — the builtin-only base. Installed via
/// [`install_tool_registry`].
struct StaticToolRegistryProvider {
    registry: Arc<ToolRegistry>,
}

#[async_trait]
impl ToolRegistryProvider for StaticToolRegistryProvider {
    async fn registry_for_graph(&self, _graph_id: GraphId) -> anyhow::Result<Arc<ToolRegistry>> {
        Ok(self.registry.clone())
    }
}

/// Process-wide [`ToolRegistryProvider`] consulted by the `execute_tool`
/// activity body. Installed via [`install_tool_registry_provider`] (or
/// [`install_tool_registry`] for the static base) and accessed via
/// [`tool_registry_provider`].
static TOOL_REGISTRY_PROVIDER: OnceLock<Arc<dyn ToolRegistryProvider>> = OnceLock::new();

/// Install the process-wide [`ToolRegistryProvider`]. Panics on double
/// install.
pub fn install_tool_registry_provider(provider: Arc<dyn ToolRegistryProvider>) {
    TOOL_REGISTRY_PROVIDER
        .set(provider)
        .map_err(|_| ())
        .expect("install_tool_registry_provider called twice; one process, one provider");
}

/// Install a single graph-agnostic [`ToolRegistry`] as the process-wide
/// provider: every graph resolves to `registry`. Convenience for tests and
/// builtin-only deployments. Panics on double install.
pub fn install_tool_registry(registry: Arc<ToolRegistry>) {
    install_tool_registry_provider(Arc::new(StaticToolRegistryProvider { registry }));
}

/// Access the installed [`ToolRegistryProvider`]. Panics if neither
/// [`install_tool_registry_provider`] nor [`install_tool_registry`] has been
/// called.
pub fn tool_registry_provider() -> Arc<dyn ToolRegistryProvider> {
    TOOL_REGISTRY_PROVIDER
        .get()
        .cloned()
        .expect("tool_registry_provider() accessed before install_tool_registry[_provider]()")
}

/// Structural-DB writer surface the `register_child_in_structural_db`
/// activity reaches for. Defined as a trait here (not a concrete type) to
/// avoid a `coral_temporal` -> `coral_graph` dependency cycle, mirroring
/// how [`AgentStorage`] handles per-agent FS.
#[async_trait]
pub trait StructuralDbStore: Send + Sync {
    /// Insert an agent row into a graph and return the freshly-allocated
    /// `AgentId`. The row is identity + topology only; the child's config
    /// travels via `AgentInput.mandate`, not the DB.
    async fn add_agent(&self, graph_id: GraphId, name: &str) -> anyhow::Result<AgentId>;

    /// Insert a parent → child edge. Returns an error on UNIQUE
    /// violation so the workflow's retry / correction path takes over
    /// rather than silently swallowing a double-spawn.
    async fn add_edge(
        &self,
        parent_agent_id: AgentId,
        child_agent_id: AgentId,
    ) -> anyhow::Result<()>;
}

/// Process-wide [`StructuralDbStore`] backend the
/// `register_child_in_structural_db` activity reaches for.
static STRUCTURAL_DB: OnceLock<Arc<dyn StructuralDbStore>> = OnceLock::new();

/// Install the process-wide [`StructuralDbStore`] backend. Panics on
/// double install.
pub fn install_structural_db_store(store: Arc<dyn StructuralDbStore>) {
    STRUCTURAL_DB
        .set(store)
        .map_err(|_| ())
        .expect("install_structural_db_store called twice; one process, one structural DB");
}

/// Access the installed [`StructuralDbStore`] backend. Panics if
/// [`install_structural_db_store`] hasn't been called.
pub fn structural_db_store() -> Arc<dyn StructuralDbStore> {
    STRUCTURAL_DB
        .get()
        .cloned()
        .expect("structural_db_store() accessed before install_structural_db_store()")
}

/// Build a worker registering [`AgentWorkflow`] + [`AgentActivities`] on
/// the given task queue.
pub fn build_worker(runtime: &CoreRuntime, client: Client, task_queue: &str) -> Result<Worker> {
    let opts = WorkerOptions::new(task_queue)
        .register_workflow::<AgentWorkflow>()
        .register_activities(AgentActivities)
        .build();
    Worker::new(runtime, client, opts).map_err(|e| anyhow::anyhow!("Worker::new failed: {e}"))
}

/// Pick the LLM vendor from env and build the [`Decide`] implementation
/// the `decide_next_action` activity body will call. Returns the vendor
/// tag alongside the trait object so the caller can log it.
///
/// Selection precedence:
/// 1. `CORAL_MODEL_VENDOR` set + that vendor compiled in → use it.
/// 2. `CORAL_MODEL_VENDOR` set + vendor NOT compiled in → error.
/// 3. `CORAL_MODEL_VENDOR` unset → pick the first compiled-in vendor
///    (in preference order `anthropic`, `cohere`) whose API key is set.
/// 4. Nothing usable → error with a "rebuild with --features" hint.
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

/// Zero-vendor stub so the library still compiles in a feature-less
/// build; callers get an early error when no vendor is compiled in.
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

/// Vendor + its required-key env var, in preference order.
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
const VENDOR_KEY_ENVS: &[(&str, &str)] = &[
    ("anthropic", ANTHROPIC_API_KEY_ENV),
    ("cohere", COHERE_API_KEY_ENV),
];

/// Compile-time list of vendors built into this binary, in the same
/// preference order `VENDOR_KEY_ENVS` defines.
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
/// table-driven test below can exercise the full matrix without
/// mutating process env. `compiled` is the list of vendors built into
/// this binary in preference order.
#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
fn resolve_vendor_inner(
    vendor_env: Option<&str>,
    key_set: impl Fn(&str) -> bool,
    compiled: &[&'static str],
) -> Result<&'static str> {
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
    for (vendor, key_env) in VENDOR_KEY_ENVS {
        if compiled.contains(vendor) && key_set(key_env) {
            return Ok(*vendor);
        }
    }
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
    //! Coverage for the install/access shape. Each `OnceLock` can only
    //! be set once per process, so each install + access + double-install
    //! sequence lives inside a single test. Statics are independent, so
    //! one test per static keeps them isolated.
    use super::*;
    use coral_node::decision::{Decide, Decision, MockDecide};
    use coral_node::storage::MemoryStorage;
    use coral_node::tools::{EchoTool, ToolRegistry};
    use std::time::Duration;

    /// Minimal `StructuralDbStore` whose methods panic when called.
    /// The install/access test only exercises the `OnceLock` plumbing;
    /// panicking on call flags any accidental dispatch.
    mod structural_db_test_double {
        use super::*;

        pub struct PanicStructuralDbStore;

        #[async_trait]
        impl StructuralDbStore for PanicStructuralDbStore {
            async fn add_agent(&self, _graph_id: GraphId, _name: &str) -> anyhow::Result<AgentId> {
                panic!("PanicStructuralDbStore::add_agent must not be called from this test")
            }

            async fn add_edge(
                &self,
                _parent_agent_id: AgentId,
                _child_agent_id: AgentId,
            ) -> anyhow::Result<()> {
                panic!("PanicStructuralDbStore::add_edge must not be called from this test")
            }
        }
    }

    #[test]
    fn install_then_access_then_double_install_panics() {
        install_agent_storage(Arc::new(MemoryStorage::new()));

        let s = agent_storage();
        assert!(Arc::strong_count(&s) >= 2);

        let result = std::panic::catch_unwind(|| {
            install_agent_storage(Arc::new(MemoryStorage::new()));
        });
        assert!(result.is_err(), "double install_agent_storage should panic");

        let decide: Arc<dyn Decide> = Arc::new(MockDecide::new(vec![Decision::Idle {
            next_after: Duration::from_millis(1),
        }]));
        install_decide(decide);

        let d = decide_impl();
        assert!(Arc::strong_count(&d) >= 2);

        let result = std::panic::catch_unwind(|| {
            install_decide(Arc::new(MockDecide::new(vec![])));
        });
        assert!(result.is_err(), "double install_decide should panic");
    }

    #[tokio::test]
    async fn install_tool_registry_then_access_then_double_install_panics() {
        let mut reg = ToolRegistry::new();
        reg.register(Arc::new(EchoTool))
            .expect("register echo tool");
        install_tool_registry(Arc::new(reg));

        // The static provider hands the same registry back for any graph.
        let provider = tool_registry_provider();
        let g = GraphId::new(uuid::Uuid::nil());
        let registry = provider
            .registry_for_graph(g)
            .await
            .expect("static provider resolves any graph");
        assert!(registry.contains("echo"));

        let result = std::panic::catch_unwind(|| {
            install_tool_registry(Arc::new(ToolRegistry::new()));
        });
        assert!(result.is_err(), "double install_tool_registry should panic");
    }

    /// The seam `execute_tool` relies on: a provider hands each graph its
    /// own registry, so an agent can only reach its own graph's tools. The
    /// live MCP-on-the-workflow path (real servers, two graphs on one
    /// worker) is the env-gated smoke that ships with the example graph.
    #[tokio::test]
    async fn provider_routes_each_graph_to_its_own_registry() {
        use coral_node::tools::Tool;
        use std::collections::HashMap;

        struct NamedTool(&'static str);
        #[async_trait]
        impl Tool for NamedTool {
            fn name(&self) -> &str {
                self.0
            }
            async fn call(&self, args: serde_json::Value) -> anyhow::Result<serde_json::Value> {
                Ok(args)
            }
        }

        struct MapProvider(HashMap<GraphId, Arc<ToolRegistry>>);
        #[async_trait]
        impl ToolRegistryProvider for MapProvider {
            async fn registry_for_graph(
                &self,
                graph_id: GraphId,
            ) -> anyhow::Result<Arc<ToolRegistry>> {
                self.0
                    .get(&graph_id)
                    .cloned()
                    .ok_or_else(|| anyhow!("no registry for this graph"))
            }
        }

        let ga = GraphId::new(uuid::Uuid::from_u128(0xA));
        let gb = GraphId::new(uuid::Uuid::from_u128(0xB));
        let mut ra = ToolRegistry::new();
        ra.register(Arc::new(NamedTool("alpha"))).unwrap();
        let mut rb = ToolRegistry::new();
        rb.register(Arc::new(NamedTool("beta"))).unwrap();
        let mut map = HashMap::new();
        map.insert(ga, Arc::new(ra));
        map.insert(gb, Arc::new(rb));
        let provider: Arc<dyn ToolRegistryProvider> = Arc::new(MapProvider(map));

        let reg_a = provider.registry_for_graph(ga).await.expect("graph a");
        assert!(reg_a.contains("alpha") && !reg_a.contains("beta"));
        let reg_b = provider.registry_for_graph(gb).await.expect("graph b");
        assert!(reg_b.contains("beta") && !reg_b.contains("alpha"));

        let unknown = GraphId::new(uuid::Uuid::from_u128(0xC));
        assert!(provider.registry_for_graph(unknown).await.is_err());
    }

    #[test]
    fn install_structural_db_store_then_access_then_double_install_panics() {
        let fake: Arc<dyn StructuralDbStore> =
            Arc::new(structural_db_test_double::PanicStructuralDbStore);
        install_structural_db_store(fake);
        let s = structural_db_store();
        assert!(Arc::strong_count(&s) >= 2);

        let result = std::panic::catch_unwind(|| {
            install_structural_db_store(Arc::new(
                structural_db_test_double::PanicStructuralDbStore,
            ));
        });
        assert!(
            result.is_err(),
            "double install_structural_db_store should panic"
        );
    }

    /// `resolve_vendor_inner` precedence matrix.
    ///
    /// Hermetic: env is injected via the `key_set` callback and the
    /// explicit `vendor_env` argument, so no process env mutation
    /// happens. Covers the cross product of `compiled` × `keys` ×
    /// `vendor_env` against the precedence rule: `CORAL_MODEL_VENDOR`
    /// honored only if compiled in (else feature-rebuild error);
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
            // explicit CORAL_MODEL_VENDOR honored when compiled in
            (BOTH, Some("anthropic"), &[], Ok("anthropic")),
            (BOTH, Some("cohere"), &[], Ok("cohere")),
            (ANTHROPIC, Some("anthropic"), &[], Ok("anthropic")),
            (COHERE, Some("cohere"), &[], Ok("cohere")),
            // explicit but not compiled in -> error
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
            // unknown vendor name -> error
            (BOTH, Some("openai"), &["ANTHROPIC_API_KEY"], Err("openai")),
            // env unset, fallback by key + compiled-in
            (BOTH, None, &["ANTHROPIC_API_KEY"], Ok("anthropic")),
            (BOTH, None, &["COHERE_API_KEY"], Ok("cohere")),
            (
                BOTH,
                None,
                &["ANTHROPIC_API_KEY", "COHERE_API_KEY"],
                Ok("anthropic"),
            ),
            (ANTHROPIC, None, &["ANTHROPIC_API_KEY"], Ok("anthropic")),
            (ANTHROPIC, None, &["COHERE_API_KEY"], Err("no vendor")),
            (COHERE, None, &["COHERE_API_KEY"], Ok("cohere")),
            (
                COHERE,
                None,
                &["ANTHROPIC_API_KEY", "COHERE_API_KEY"],
                Ok("cohere"),
            ),
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

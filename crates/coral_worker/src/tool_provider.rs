//! Per-graph [`ToolRegistry`] provider backed by the structural DB.
//!
//! `graph.yaml` is the runtime source of truth for a graph's tools: the
//! validator persists `kind: mcp` rows, and the worker reads them back here
//! to build one registry per graph (builtin `echo` plus that graph's MCP
//! servers). An agent's `execute_tool` activity resolves its registry by
//! `graph_id`, so a graph can only reach its own servers.
//!
//! Lifecycle: registries are built lazily on first dispatch for a graph and
//! cached for the worker's lifetime (fleet-lifetime retention — no idle
//! teardown in v1). A cached registry owns its `Arc<McpClient>`s, which own
//! the spawned subprocesses; those are torn down when the worker process
//! exits and the OS reaps its children.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use coral_graph::{GraphStore, ToolRecord};
use coral_node::agent_ref::GraphId;
use coral_node::mcp::McpClient;
use coral_node::tools::{EchoTool, ToolRegistry};
use coral_temporal::worker::ToolRegistryProvider;
use tokio::sync::Mutex;
use tracing::{info, warn};

/// A deduplicated MCP server to spawn for a graph. `env` is sorted so two
/// rows naming the same `(command, args, env)` collapse to one spawn.
#[derive(Debug, PartialEq, Eq)]
struct McpServerSpec {
    command: String,
    args: Vec<String>,
    env: Vec<(String, String)>,
}

/// Builds per-graph [`ToolRegistry`]s from the structural DB and caches them.
pub struct DbToolRegistryProvider {
    store: Arc<GraphStore>,
    cache: Mutex<HashMap<GraphId, Arc<ToolRegistry>>>,
}

impl DbToolRegistryProvider {
    pub fn new(store: Arc<GraphStore>) -> Self {
        Self {
            store,
            cache: Mutex::new(HashMap::new()),
        }
    }

    async fn build_registry(&self, graph_id: GraphId) -> Result<ToolRegistry> {
        let rows = self
            .store
            .list_tools_for_graph(graph_id.into_uuid())
            .await
            .context("reading graph tools from structural DB")?;
        let specs = mcp_specs_from_rows(&rows)?;

        // MCP servers register first; the builtin `echo` is a fallback added
        // only if no server already provides that name. A name collision
        // (between servers, or with the builtin) skips the colliding tool
        // rather than aborting the whole graph's registry — first-wins.
        let mut registry = ToolRegistry::new();
        let mut tool_names: Vec<String> = Vec::new();
        for spec in &specs {
            let args_refs: Vec<&str> = spec.args.iter().map(String::as_str).collect();
            let client = Arc::new(
                McpClient::connect_stdio_with_env(&spec.command, &args_refs, &spec.env)
                    .await
                    .with_context(|| format!("connecting MCP server {:?}", spec.command))?,
            );
            let outcome = registry
                .register_mcp_server_skipping_existing(client)
                .await
                .with_context(|| format!("registering tools from MCP server {:?}", spec.command))?;
            for name in &outcome.skipped {
                warn!(
                    graph_id = %graph_id,
                    server = %spec.command,
                    tool = %name,
                    "skipped MCP tool: a tool with this name is already registered"
                );
            }
            tool_names.extend(outcome.registered);
        }

        if !registry.contains("echo") {
            registry
                .register(Arc::new(EchoTool))
                .context("registering builtin echo in per-graph registry")?;
            tool_names.push("echo".to_string());
        }

        info!(
            graph_id = %graph_id,
            mcp_servers = specs.len(),
            tools = ?tool_names,
            "built per-graph tool registry"
        );
        Ok(registry)
    }
}

#[async_trait]
impl ToolRegistryProvider for DbToolRegistryProvider {
    async fn registry_for_graph(&self, graph_id: GraphId) -> Result<Arc<ToolRegistry>> {
        // The lock is held across the build so concurrent first-dispatches
        // for the same graph don't double-spawn its servers. Different
        // graphs serialize their first build too; acceptable for v1 since a
        // build is one-time per graph.
        let mut cache = self.cache.lock().await;
        if let Some(existing) = cache.get(&graph_id) {
            return Ok(existing.clone());
        }
        let registry = Arc::new(self.build_registry(graph_id).await?);
        cache.insert(graph_id, registry.clone());
        Ok(registry)
    }
}

/// Canonical dedup key for an MCP server: `(command, args, sorted env)`.
type McpSpecKey = (String, Vec<String>, Vec<(String, String)>);

/// Parse the `mcp`-kind rows of a graph into deduplicated server specs.
/// `builtin` rows are skipped (every registry already gets `echo`).
fn mcp_specs_from_rows(rows: &[ToolRecord]) -> Result<Vec<McpServerSpec>> {
    let mut seen: HashSet<McpSpecKey> = HashSet::new();
    let mut specs = Vec::new();
    for row in rows {
        if row.kind != "mcp" {
            continue;
        }
        let command = row
            .command
            .clone()
            .ok_or_else(|| anyhow!("mcp tool row has no command"))?;
        let args = parse_args(&row.args)
            .with_context(|| format!("parsing args for mcp command {command:?}"))?;
        let env = parse_env(&row.env_refs)
            .with_context(|| format!("parsing env for mcp command {command:?}"))?;
        let key = (command.clone(), args.clone(), env.clone());
        if seen.insert(key) {
            specs.push(McpServerSpec { command, args, env });
        }
    }
    Ok(specs)
}

fn parse_args(value: &serde_json::Value) -> Result<Vec<String>> {
    match value {
        serde_json::Value::Array(items) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| anyhow!("mcp args must be an array of strings"))
            })
            .collect(),
        _ => Err(anyhow!("mcp args must be a JSON array")),
    }
}

/// Parse the stored `env_refs` into sorted `(name, value)` pairs. MCP rows
/// persist an object (`{}` when absent); tolerate an empty array / null too.
fn parse_env(value: &serde_json::Value) -> Result<Vec<(String, String)>> {
    match value {
        serde_json::Value::Object(map) => {
            let pairs: BTreeMap<String, String> = map
                .iter()
                .map(|(k, v)| {
                    v.as_str()
                        .map(|s| (k.clone(), s.to_owned()))
                        .ok_or_else(|| anyhow!("mcp env values must be strings"))
                })
                .collect::<Result<_>>()?;
            Ok(pairs.into_iter().collect())
        }
        serde_json::Value::Array(items) if items.is_empty() => Ok(Vec::new()),
        serde_json::Value::Null => Ok(Vec::new()),
        _ => Err(anyhow!("mcp env_refs must be a JSON object")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    fn tool(
        kind: &str,
        command: Option<&str>,
        args: serde_json::Value,
        env: serde_json::Value,
    ) -> ToolRecord {
        ToolRecord {
            id: Uuid::nil(),
            graph_id: Uuid::nil(),
            kind: kind.to_string(),
            command: command.map(str::to_owned),
            args,
            env_refs: env,
            created_at: chrono::DateTime::parse_from_rfc3339("2026-05-30T00:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        }
    }

    #[test]
    fn skips_builtin_and_parses_mcp_rows() {
        let rows = vec![
            tool("builtin", Some("echo"), json!([]), json!([])),
            tool(
                "mcp",
                Some("mcp-web"),
                json!(["--json", "--quiet"]),
                json!({"API_KEY": "k", "LOG": "debug"}),
            ),
        ];
        let specs = mcp_specs_from_rows(&rows).expect("parse");
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].command, "mcp-web");
        assert_eq!(specs[0].args, vec!["--json", "--quiet"]);
        // env sorted by key for a canonical dedup key + deterministic spawn.
        assert_eq!(
            specs[0].env,
            vec![
                ("API_KEY".to_string(), "k".to_string()),
                ("LOG".to_string(), "debug".to_string()),
            ]
        );
    }

    #[test]
    fn dedups_identical_servers() {
        let a = tool("mcp", Some("srv"), json!(["--x"]), json!({"E": "1"}));
        let b = tool("mcp", Some("srv"), json!(["--x"]), json!({"E": "1"}));
        // Same command/args, different env → distinct spec.
        let c = tool("mcp", Some("srv"), json!(["--x"]), json!({"E": "2"}));
        let specs = mcp_specs_from_rows(&[a, b, c]).expect("parse");
        assert_eq!(
            specs.len(),
            2,
            "identical (cmd,args,env) collapse; differing env stays"
        );
    }

    #[test]
    fn empty_env_object_yields_no_pairs() {
        let rows = vec![tool("mcp", Some("srv"), json!([]), json!({}))];
        let specs = mcp_specs_from_rows(&rows).expect("parse");
        assert_eq!(specs.len(), 1);
        assert!(specs[0].env.is_empty());
        assert!(specs[0].args.is_empty());
    }

    #[test]
    fn rejects_non_string_args_and_missing_command() {
        let bad_args = vec![tool("mcp", Some("srv"), json!([1, 2]), json!({}))];
        assert!(mcp_specs_from_rows(&bad_args).is_err());

        let no_command = vec![tool("mcp", None, json!([]), json!({}))];
        assert!(mcp_specs_from_rows(&no_command).is_err());
    }
}

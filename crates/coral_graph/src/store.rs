//! `GraphStore` — CRUD API over the structural DB. Concrete struct
//! over a `PgPool` rather than a trait (no second implementation yet
//! to motivate one). All queries use compile-time-checked `sqlx::query!`
//! / `sqlx::query_as!` macros backed by the workspace `.sqlx/` offline
//! cache, so `cargo build` succeeds without a live DB.

use crate::types::{AgentRecord, Edge, Graph, ToolRecord};
use crate::yaml::{AppliedGraph, GraphYaml, ResolvedAgent, ResolvedAgentWorkflow, ToolKind};
use async_trait::async_trait;
use coral_node::agent_ref::{AgentId, GraphId};
use coral_temporal::worker::StructuralDbStore;
use coral_temporal::workflow::agent_workflow_id;
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::HashMap;
use uuid::Uuid;

/// Typed error surface for [`GraphStore::create_from_yaml`]. The single
/// domain-typed variant lets `coral-apply` pattern-match the
/// `metadata.name` collision and print a clean operator-facing error;
/// other failure paths stay opaque via `Sqlx`.
#[derive(Debug, thiserror::Error)]
pub enum GraphStoreError {
    /// A graph with this `metadata.name` already exists in the structural
    /// DB. `coral apply` is CREATE-only.
    #[error(
        "graph {name:?} already exists in the structural DB; `coral apply` is CREATE-only. \
         Drop the existing graph or use a different `metadata.name`."
    )]
    GraphAlreadyExists { name: String },

    /// Any other DB-level failure (FK violation, connection drop, etc.).
    /// Surfaced verbatim so operators can see the underlying `sqlx`
    /// error text; the `coral-apply` binary chains it into anyhow at
    /// the call site.
    #[error("structural DB write failed: {0}")]
    Sqlx(#[from] sqlx::Error),
}

/// Thin wrapper over a `PgPool` exposing the structural-DB CRUD surface
/// `coral apply` writes into and downstream consumers read from at
/// startup.
///
/// `Clone` is cheap — `PgPool` is `Arc`-shaped internally — so passing a
/// `GraphStore` by value into per-request handlers is the intended
/// pattern.
#[derive(Clone, Debug)]
pub struct GraphStore {
    pool: PgPool,
}

impl GraphStore {
    /// Wrap an existing `PgPool`. Caller is responsible for migrations
    /// (apply via `coral_graph::MIGRATOR` before constructing).
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Expose the underlying pool for callers that need to mix in their
    /// own queries. Intentionally `pub` rather than `pub(crate)`:
    /// `GraphStore` is a thin shell over the pool, with no realistic
    /// kernel reason to hide it.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    // --- writes -----------------------------------------------------

    /// Insert a graph row with a freshly minted UUID and return the
    /// populated `Graph`. `metadata` is `serde_json::Value` so callers
    /// can pass `serde_json::json!({...})` or `Value::Null` (which
    /// becomes the DB's default `{}` only if absent — explicit `Null`
    /// is preserved).
    pub async fn create_graph(
        &self,
        name: &str,
        metadata: serde_json::Value,
    ) -> sqlx::Result<Graph> {
        let id = Uuid::new_v4();
        let row = sqlx::query_as!(
            Graph,
            r#"
            INSERT INTO graphs (id, name, metadata)
            VALUES ($1, $2, $3)
            RETURNING id, name, metadata as "metadata!: serde_json::Value", created_at as "created_at!: chrono::DateTime<chrono::Utc>"
            "#,
            id,
            name,
            metadata,
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    /// Insert an agent into a graph. `mandate_ref` is an opaque text
    /// handle per the schema decision in `migrations/0001_initial.sql`
    /// — authored mandates live outside this DB (git-versioned
    /// `graph.yaml`), so there's no FK target.
    pub async fn add_agent(
        &self,
        graph_id: Uuid,
        name: &str,
        mandate_ref: Option<&str>,
    ) -> sqlx::Result<AgentRecord> {
        let id = Uuid::new_v4();
        let row = sqlx::query_as!(
            AgentRecord,
            r#"
            INSERT INTO agents (id, graph_id, name, mandate_ref)
            VALUES ($1, $2, $3, $4)
            RETURNING id, graph_id, name, mandate_ref, created_at as "created_at!: chrono::DateTime<chrono::Utc>"
            "#,
            id,
            graph_id,
            name,
            mandate_ref,
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    /// Insert a parent->child edge. Returns a `sqlx::Error::Database`
    /// with a unique-violation code if the edge already exists (the
    /// schema's `UNIQUE (parent_agent_id, child_agent_id)`).
    pub async fn add_edge(
        &self,
        parent_agent_id: Uuid,
        child_agent_id: Uuid,
    ) -> sqlx::Result<Edge> {
        let id = Uuid::new_v4();
        let row = sqlx::query_as!(
            Edge,
            r#"
            INSERT INTO edges (id, parent_agent_id, child_agent_id)
            VALUES ($1, $2, $3)
            RETURNING id, parent_agent_id, child_agent_id, created_at as "created_at!: chrono::DateTime<chrono::Utc>"
            "#,
            id,
            parent_agent_id,
            child_agent_id,
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    /// Insert a tool registration. `args` / `env_refs` default to empty
    /// JSON arrays in the schema; callers can pass `serde_json::json!([])`
    /// explicitly for clarity.
    pub async fn register_tool(
        &self,
        kind: &str,
        command: Option<&str>,
        args: serde_json::Value,
        env_refs: serde_json::Value,
    ) -> sqlx::Result<ToolRecord> {
        let id = Uuid::new_v4();
        let row = sqlx::query_as!(
            ToolRecord,
            r#"
            INSERT INTO tools (id, kind, command, args, env_refs)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, kind, command, args as "args!: serde_json::Value", env_refs as "env_refs!: serde_json::Value", created_at as "created_at!: chrono::DateTime<chrono::Utc>"
            "#,
            id,
            kind,
            command,
            args,
            env_refs,
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    /// Attach a tool to an agent via the `agent_tools` M:N junction.
    /// Idempotent in spirit (a re-attach is the same edge) but enforced
    /// by the composite PK at the DB level — callers see a unique
    /// violation on re-insert. We intentionally don't `ON CONFLICT DO
    /// NOTHING`; visible failures are the cheap signal that something
    /// upstream is over-eager.
    pub async fn attach_tool_to_agent(&self, agent_id: Uuid, tool_id: Uuid) -> sqlx::Result<()> {
        sqlx::query!(
            "INSERT INTO agent_tools (agent_id, tool_id) VALUES ($1, $2)",
            agent_id,
            tool_id,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Transactional wrapper: in one DB transaction, CREATE the graph
    /// row, every agent row (DFS parents-first across the whole forest),
    /// every parent→child `edges` row, every tool row, and every
    /// `agent_tools` attachment.
    ///
    /// Returns [`AppliedGraph`] carrying the freshly-allocated
    /// `graph_id`, a parents-first list of [`ResolvedAgent`]s, and the
    /// `operator_id → (db_agent_id, workflow_id)` map the apply
    /// binary's workflow-start phase consumes. Single-agent YAML is the
    /// degenerate one-element case.
    ///
    /// Returns [`GraphStoreError::GraphAlreadyExists`] when a graph with
    /// the same `metadata.name` already exists. `coral apply` is
    /// CREATE-only. Collision is detected via a `SELECT 1 FROM graphs
    /// WHERE name = $1` inside the transaction. `graphs.name` also
    /// carries a DB-level UNIQUE constraint (`graphs_name_unique`); the
    /// INSERT below catches Postgres SQLSTATE 23505 and maps it to the
    /// same variant — defense in depth for the two-concurrent-applies
    /// race the SELECT-then-INSERT alone cannot rule out.
    ///
    /// `graph.policy` is preserved verbatim into `graphs.metadata` under
    /// a `"policy"` key (pass-through; not enforced); metadata is `{}`
    /// when no policy is declared.
    ///
    /// Caller invariant: `graph` must already have passed
    /// [`crate::yaml::validate`] (or [`crate::yaml::parse_and_validate`]).
    ///
    /// Tool-row shape: `kind = "builtin"`, `command = Some(<builtin
    /// name>)`, `args = []`, `env_refs = []`.
    pub async fn create_from_yaml(
        &self,
        graph: &GraphYaml,
    ) -> Result<AppliedGraph, GraphStoreError> {
        let mut tx: Transaction<'_, Postgres> = self.pool.begin().await?;

        // Collision check inside the transaction. A plain SELECT is
        // sufficient for the operator-driven (non-concurrent) case; the
        // SQLSTATE 23505 catch on the INSERT below is the structural-
        // race backstop.
        let existing: Option<(Uuid,)> =
            sqlx::query_as("SELECT id FROM graphs WHERE name = $1 LIMIT 1")
                .bind(&graph.metadata.name)
                .fetch_optional(&mut *tx)
                .await?;
        if existing.is_some() {
            return Err(GraphStoreError::GraphAlreadyExists {
                name: graph.metadata.name.clone(),
            });
        }

        // --- graph row -------------------------------------------------
        // `policy:` is pass-through into `graphs.metadata` under the
        // `policy` key; absent => `{}`.
        let graph_metadata: serde_json::Value = match &graph.policy {
            Some(p) => serde_json::json!({ "policy": p.0 }),
            None => serde_json::json!({}),
        };
        let graph_id = Uuid::new_v4();
        let graph_row = match sqlx::query_as!(
            Graph,
            r#"
            INSERT INTO graphs (id, name, metadata)
            VALUES ($1, $2, $3)
            RETURNING id, name, metadata as "metadata!: serde_json::Value", created_at as "created_at!: chrono::DateTime<chrono::Utc>"
            "#,
            graph_id,
            &graph.metadata.name,
            graph_metadata,
        )
        .fetch_one(&mut *tx)
        .await
        {
            Ok(row) => row,
            Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("23505") => {
                return Err(GraphStoreError::GraphAlreadyExists {
                    name: graph.metadata.name.clone(),
                });
            }
            Err(e) => return Err(e.into()),
        };

        // --- tool rows -------------------------------------------------
        // Tools are graph-scoped. Indexed by operator id so each agent's
        // `tools[]` reference can resolve to a UUID.
        let mut tool_uuid_by_id: HashMap<&str, Uuid> = HashMap::with_capacity(graph.tools.len());
        for tool in &graph.tools {
            let tool_uuid = Uuid::new_v4();
            let (kind_text, command_text): (&str, Option<&str>) = match &tool.kind {
                ToolKind::Builtin { builtin } => ("builtin", Some(builtin.as_str())),
                ToolKind::Mcp { .. } => {
                    return Err(GraphStoreError::Sqlx(sqlx::Error::Protocol(format!(
                        "internal: create_from_yaml saw kind: mcp for tool {:?}; \
                             validator should have rejected it",
                        tool.id,
                    ))));
                }
            };
            sqlx::query_as!(
                ToolRecord,
                r#"
                INSERT INTO tools (id, kind, command, args, env_refs)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id, kind, command, args as "args!: serde_json::Value", env_refs as "env_refs!: serde_json::Value", created_at as "created_at!: chrono::DateTime<chrono::Utc>"
                "#,
                tool_uuid,
                kind_text,
                command_text,
                serde_json::json!([]),
                serde_json::json!([]),
            )
            .fetch_one(&mut *tx)
            .await?;
            tool_uuid_by_id.insert(tool.id.as_str(), tool_uuid);
        }

        // --- agent rows + edges + agent_tools (DFS parents-first) ------
        // The walker emits one row per agent (parents before children),
        // one `edges` row per parent→child pair, and one `agent_tools`
        // attachment per tool reference. The accumulator
        // (`resolved_agents`) carries the (operator_id, db_agent_id,
        // parent_db_id) triples downstream into [`AppliedGraph`].
        let mut resolved_agents: Vec<ResolvedAgent> = Vec::new();
        let mut id_map: HashMap<String, ResolvedAgentWorkflow> = HashMap::new();
        for root_yaml in &graph.agents {
            walk_agent_tree(
                &mut tx,
                graph_id,
                root_yaml,
                None,
                &tool_uuid_by_id,
                &mut resolved_agents,
                &mut id_map,
            )
            .await?;
        }

        tx.commit().await?;
        Ok(AppliedGraph {
            graph_id: GraphId::new(graph_row.id),
            graph_name: graph_row.name,
            agents: resolved_agents,
            id_map,
        })
    }

    // --- reads ------------------------------------------------------
    // (walk_agent_tree is a free function below — recursive async fns
    // need explicit boxing on the recursive call site.)
}

/// Recursive DFS walker for `create_from_yaml`. Writes one `agents`
/// row, optionally one `edges` row (when `parent_db_id` is `Some`),
/// every `agent_tools` join, then recurses into children.
///
/// Recursive `async fn` requires boxing on the recursive call (the
/// future's size would otherwise be unbounded); the function returns a
/// boxed future via `Box::pin` for the recursion.
fn walk_agent_tree<'a>(
    tx: &'a mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    yaml_agent: &'a crate::yaml::Agent,
    parent_db_id: Option<AgentId>,
    tool_uuid_by_id: &'a HashMap<&'a str, Uuid>,
    resolved_agents: &'a mut Vec<ResolvedAgent>,
    id_map: &'a mut HashMap<String, ResolvedAgentWorkflow>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), GraphStoreError>> + Send + 'a>> {
    Box::pin(async move {
        // Allocate this agent's UUID + insert its row.
        let agent_uuid = Uuid::new_v4();
        sqlx::query_as!(
            AgentRecord,
            r#"
            INSERT INTO agents (id, graph_id, name, mandate_ref)
            VALUES ($1, $2, $3, $4)
            RETURNING id, graph_id, name, mandate_ref, created_at as "created_at!: chrono::DateTime<chrono::Utc>"
            "#,
            agent_uuid,
            graph_id,
            &yaml_agent.id,
            // Mandate travels via `AgentInput.mandate` directly today.
            None as Option<&str>,
        )
        .fetch_one(&mut **tx)
        .await?;

        let db_agent_id = AgentId::new(agent_uuid);

        // Edge row: parent → this agent.
        if let Some(parent) = parent_db_id {
            sqlx::query!(
                "INSERT INTO edges (id, parent_agent_id, child_agent_id) VALUES ($1, $2, $3)",
                Uuid::new_v4(),
                parent.into_uuid(),
                agent_uuid,
            )
            .execute(&mut **tx)
            .await?;
        }

        // Agent-tool attachments.
        for tool_ref in &yaml_agent.tools {
            let tool_uuid = tool_uuid_by_id
                .get(tool_ref.as_str())
                .copied()
                .ok_or_else(|| {
                    GraphStoreError::Sqlx(sqlx::Error::Protocol(format!(
                        "internal: create_from_yaml could not resolve tool ref {tool_ref:?}; \
                     validator should have caught this"
                    )))
                })?;
            sqlx::query!(
                "INSERT INTO agent_tools (agent_id, tool_id) VALUES ($1, $2)",
                agent_uuid,
                tool_uuid,
            )
            .execute(&mut **tx)
            .await?;
        }

        // The workflow id is the UUID-shaped flat form:
        // cross-agent FS reads look agents up by UUID, so the
        // `FsHandle::for_agent` prefix must match the workflow id format.
        let workflow_id = agent_workflow_id(&graph_id.to_string(), &agent_uuid.to_string());
        id_map.insert(
            yaml_agent.id.clone(),
            ResolvedAgentWorkflow {
                db_agent_id,
                workflow_id,
            },
        );
        resolved_agents.push(ResolvedAgent {
            operator_id: yaml_agent.id.clone(),
            db_agent_id,
            parent_db_agent_id: parent_db_id,
        });

        // Recurse into children (parents-first DFS order: this agent's
        // row + edge + tool rows are already written before any child
        // row).
        for child in &yaml_agent.children {
            walk_agent_tree(
                tx,
                graph_id,
                child,
                Some(db_agent_id),
                tool_uuid_by_id,
                resolved_agents,
                id_map,
            )
            .await?;
        }

        Ok(())
    })
}

impl GraphStore {
    /// Fetch a single agent by id. Returns `Ok(None)` if no row matches —
    /// the call shape startup code expects (lookup that may legitimately
    /// miss).
    pub async fn get_agent(&self, agent_id: Uuid) -> sqlx::Result<Option<AgentRecord>> {
        let row = sqlx::query_as!(
            AgentRecord,
            r#"
            SELECT id, graph_id, name, mandate_ref, created_at as "created_at!: chrono::DateTime<chrono::Utc>"
            FROM agents
            WHERE id = $1
            "#,
            agent_id,
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// All agents in a graph, ordered by `created_at` ASC so the result
    /// is stable across calls. Empty `Vec` if the graph has no agents
    /// (or doesn't exist — we don't distinguish; the structural DB
    /// reads are "what's there", not "does this graph exist").
    pub async fn list_agents_in_graph(&self, graph_id: Uuid) -> sqlx::Result<Vec<AgentRecord>> {
        let rows = sqlx::query_as!(
            AgentRecord,
            r#"
            SELECT id, graph_id, name, mandate_ref, created_at as "created_at!: chrono::DateTime<chrono::Utc>"
            FROM agents
            WHERE graph_id = $1
            ORDER BY created_at ASC
            "#,
            graph_id,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// All children of an agent, returned as the child's `AgentRecord`
    /// (not the edge).
    pub async fn list_children(&self, parent_agent_id: Uuid) -> sqlx::Result<Vec<AgentRecord>> {
        let rows = sqlx::query_as!(
            AgentRecord,
            r#"
            SELECT a.id, a.graph_id, a.name, a.mandate_ref,
                   a.created_at as "created_at!: chrono::DateTime<chrono::Utc>"
            FROM agents a
            JOIN edges e ON e.child_agent_id = a.id
            WHERE e.parent_agent_id = $1
            ORDER BY a.created_at ASC
            "#,
            parent_agent_id,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// All edges whose parent or child belongs to a graph. The
    /// same-graph invariant lives in application code (see schema
    /// decision in `migrations/0001_initial.sql`).
    pub async fn list_edges_in_graph(&self, graph_id: Uuid) -> sqlx::Result<Vec<Edge>> {
        let rows = sqlx::query_as!(
            Edge,
            r#"
            SELECT DISTINCT e.id, e.parent_agent_id, e.child_agent_id,
                            e.created_at as "created_at!: chrono::DateTime<chrono::Utc>"
            FROM edges e
            JOIN agents a ON a.id = e.parent_agent_id OR a.id = e.child_agent_id
            WHERE a.graph_id = $1
            ORDER BY e.created_at ASC
            "#,
            graph_id,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// All tools attached to an agent via `agent_tools`.
    pub async fn list_tools_for_agent(&self, agent_id: Uuid) -> sqlx::Result<Vec<ToolRecord>> {
        let rows = sqlx::query_as!(
            ToolRecord,
            r#"
            SELECT t.id, t.kind, t.command,
                   t.args as "args!: serde_json::Value",
                   t.env_refs as "env_refs!: serde_json::Value",
                   t.created_at as "created_at!: chrono::DateTime<chrono::Utc>"
            FROM tools t
            JOIN agent_tools at ON at.tool_id = t.id
            WHERE at.agent_id = $1
            ORDER BY t.created_at ASC
            "#,
            agent_id,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }
}

// `StructuralDbStore` lives in `coral_temporal` because the
// `OnceLock<Arc<dyn ...>>` install is read from inside an activity body
// that can't depend on `coral_graph` without cycling the crate graph.
// This adapter narrows `GraphStore`'s raw-`Uuid` / `AgentRecord` surface
// to the trait's kernel-newtype shape.
#[async_trait]
impl StructuralDbStore for GraphStore {
    async fn add_agent(
        &self,
        graph_id: GraphId,
        name: &str,
        mandate_ref: Option<&str>,
    ) -> anyhow::Result<AgentId> {
        let record = GraphStore::add_agent(self, graph_id.into_uuid(), name, mandate_ref).await?;
        Ok(AgentId::new(record.id))
    }

    async fn add_edge(
        &self,
        parent_agent_id: AgentId,
        child_agent_id: AgentId,
    ) -> anyhow::Result<()> {
        GraphStore::add_edge(
            self,
            parent_agent_id.into_uuid(),
            child_agent_id.into_uuid(),
        )
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Per-method tests against an ephemeral per-test Postgres via
    //! `#[sqlx::test(migrator = "crate::MIGRATOR")]`. Each test gets
    //! its own freshly migrated DB, so tests are fully isolated and
    //! can rely on row counts. Happy path + one failure mode (e.g. FK
    //! violation) per method; SQL-parse-style failures are caught at
    //! compile time by `query!` and not retested here.
    use super::*;
    use sqlx::PgPool;

    async fn seed_graph(store: &GraphStore) -> Graph {
        store
            .create_graph("test", serde_json::json!({}))
            .await
            .expect("create_graph")
    }

    async fn seed_agent(store: &GraphStore, graph_id: Uuid, name: &str) -> AgentRecord {
        store
            .add_agent(graph_id, name, None)
            .await
            .expect("add_agent")
    }

    // --- create_graph ---

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn create_graph_writes_and_returns_row(pool: PgPool) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let g = store
            .create_graph("hello", serde_json::json!({"author": "tests"}))
            .await?;
        assert_eq!(g.name, "hello");
        assert_eq!(g.metadata, serde_json::json!({"author": "tests"}));
        Ok(())
    }

    // --- add_agent ---

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn add_agent_happy_path(pool: PgPool) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let graph = seed_graph(&store).await;
        let agent = store.add_agent(graph.id, "alice", Some("v1")).await?;
        assert_eq!(agent.graph_id, graph.id);
        assert_eq!(agent.name, "alice");
        assert_eq!(agent.mandate_ref.as_deref(), Some("v1"));
        Ok(())
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn add_agent_fk_violation_on_unknown_graph(pool: PgPool) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let bad_graph = Uuid::new_v4();
        let err = store
            .add_agent(bad_graph, "ghost", None)
            .await
            .expect_err("FK violation expected");
        // Postgres surfaces FK violations with SQLSTATE 23503.
        assert_eq!(
            err.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("23503")
        );
        Ok(())
    }

    // --- add_edge ---

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn add_edge_happy_path(pool: PgPool) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let graph = seed_graph(&store).await;
        let parent = seed_agent(&store, graph.id, "parent").await;
        let child = seed_agent(&store, graph.id, "child").await;
        let edge = store.add_edge(parent.id, child.id).await?;
        assert_eq!(edge.parent_agent_id, parent.id);
        assert_eq!(edge.child_agent_id, child.id);
        Ok(())
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn add_edge_rejects_duplicate(pool: PgPool) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let graph = seed_graph(&store).await;
        let parent = seed_agent(&store, graph.id, "parent").await;
        let child = seed_agent(&store, graph.id, "child").await;
        store.add_edge(parent.id, child.id).await?;
        let err = store
            .add_edge(parent.id, child.id)
            .await
            .expect_err("UNIQUE violation expected");
        // SQLSTATE 23505 is unique-violation in Postgres.
        assert_eq!(
            err.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("23505")
        );
        Ok(())
    }

    // --- register_tool ---

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn register_tool_happy_path(pool: PgPool) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let tool = store
            .register_tool(
                "echo",
                Some("/bin/echo"),
                serde_json::json!(["hello"]),
                serde_json::json!([]),
            )
            .await?;
        assert_eq!(tool.kind, "echo");
        assert_eq!(tool.command.as_deref(), Some("/bin/echo"));
        assert_eq!(tool.args, serde_json::json!(["hello"]));
        Ok(())
    }

    // --- attach_tool_to_agent ---

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn attach_tool_to_agent_happy_path(pool: PgPool) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let graph = seed_graph(&store).await;
        let agent = seed_agent(&store, graph.id, "a").await;
        let tool = store
            .register_tool("echo", None, serde_json::json!([]), serde_json::json!([]))
            .await?;
        store.attach_tool_to_agent(agent.id, tool.id).await?;
        let tools = store.list_tools_for_agent(agent.id).await?;
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].id, tool.id);
        Ok(())
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn attach_tool_to_agent_rejects_unknown_agent(pool: PgPool) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let tool = store
            .register_tool("echo", None, serde_json::json!([]), serde_json::json!([]))
            .await?;
        let err = store
            .attach_tool_to_agent(Uuid::new_v4(), tool.id)
            .await
            .expect_err("FK violation expected");
        assert_eq!(
            err.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("23503")
        );
        Ok(())
    }

    // --- get_agent ---

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn get_agent_returns_some_for_known_id(pool: PgPool) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let graph = seed_graph(&store).await;
        let agent = seed_agent(&store, graph.id, "a").await;
        let fetched = store.get_agent(agent.id).await?;
        assert_eq!(fetched.as_ref().map(|a| a.id), Some(agent.id));
        Ok(())
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn get_agent_returns_none_for_unknown_id(pool: PgPool) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let fetched = store.get_agent(Uuid::new_v4()).await?;
        assert!(fetched.is_none());
        Ok(())
    }

    // --- list_agents_in_graph ---

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn list_agents_in_graph_returns_all_in_creation_order(pool: PgPool) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let graph = seed_graph(&store).await;
        let a = seed_agent(&store, graph.id, "first").await;
        // Tiny sleep so the second agent's created_at is strictly later
        // than the first under Postgres's microsecond resolution. (At
        // most one of these in the whole test file.)
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let b = seed_agent(&store, graph.id, "second").await;
        let agents = store.list_agents_in_graph(graph.id).await?;
        assert_eq!(agents.len(), 2);
        assert_eq!(agents[0].id, a.id);
        assert_eq!(agents[1].id, b.id);
        Ok(())
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn list_agents_in_graph_returns_empty_for_unknown_graph(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let agents = store.list_agents_in_graph(Uuid::new_v4()).await?;
        assert!(agents.is_empty());
        Ok(())
    }

    // --- list_children ---

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn list_children_returns_child_records(pool: PgPool) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let graph = seed_graph(&store).await;
        let parent = seed_agent(&store, graph.id, "parent").await;
        let child = seed_agent(&store, graph.id, "child").await;
        store.add_edge(parent.id, child.id).await?;
        let children = store.list_children(parent.id).await?;
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].id, child.id);
        Ok(())
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn list_children_is_empty_for_childless_parent(pool: PgPool) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let graph = seed_graph(&store).await;
        let solo = seed_agent(&store, graph.id, "solo").await;
        let children = store.list_children(solo.id).await?;
        assert!(children.is_empty());
        Ok(())
    }

    // --- list_edges_in_graph ---

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn list_edges_in_graph_returns_all_edges_in_graph(pool: PgPool) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let graph = seed_graph(&store).await;
        let p = seed_agent(&store, graph.id, "parent").await;
        let c = seed_agent(&store, graph.id, "child").await;
        let edge = store.add_edge(p.id, c.id).await?;
        let edges = store.list_edges_in_graph(graph.id).await?;
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].id, edge.id);
        Ok(())
    }

    // --- list_tools_for_agent ---

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn list_tools_for_agent_is_empty_with_no_attachments(pool: PgPool) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let graph = seed_graph(&store).await;
        let agent = seed_agent(&store, graph.id, "a").await;
        let tools = store.list_tools_for_agent(agent.id).await?;
        assert!(tools.is_empty());
        Ok(())
    }

    // --- create_from_yaml --------------------------------------------

    /// Canonical happy-path fixture for `create_from_yaml`. Kept inline
    /// (not shared with the validator tests) to avoid coupling this
    /// module's pass/fail to the yaml-test module's shape.
    const HAPPY_YAML: &str = r#"
apiVersion: coral.engine/v1alpha1
kind: Graph
metadata:
  name: smoke
  description: store-test fixture
tools:
  - id: echo
    kind: builtin
    builtin: echo
agents:
  - id: root
    mandate:
      text: do the thing
      idle_period: 1s
      max_ticks: 4
    tools: [echo]
seed:
  triggers:
    - agent: root
      at: start
      external:
        kind: kickoff
        payload: {}
"#;

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn create_from_yaml_happy_path_writes_graph_agent_tool_rows(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let g = crate::yaml::parse_and_validate(HAPPY_YAML).expect("validator green on fixture");
        let applied = store.create_from_yaml(&g).await.expect("create_from_yaml");

        assert_eq!(applied.graph_name, "smoke");
        assert_eq!(applied.agents.len(), 1);
        assert_eq!(applied.agents[0].operator_id, "root");
        assert!(applied.agents[0].parent_db_agent_id.is_none());
        // id_map round-trips the operator id to the allocated UUIDs.
        let resolved = applied.id_map.get("root").expect("root id in id_map");
        assert_eq!(resolved.db_agent_id, applied.agents[0].db_agent_id);

        let agents = store
            .list_agents_in_graph(applied.graph_id.into_uuid())
            .await?;
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].name, "root");
        assert_eq!(AgentId::new(agents[0].id), applied.agents[0].db_agent_id);
        assert!(
            agents[0].mandate_ref.is_none(),
            "v1 leaves mandate_ref None; got {:?}",
            agents[0].mandate_ref,
        );

        let tools = store.list_tools_for_agent(agents[0].id).await?;
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].kind, "builtin");
        assert_eq!(tools[0].command.as_deref(), Some("echo"));
        Ok(())
    }

    /// Multi-agent hermetic test — parent + 2 children fixture against
    /// ephemeral Postgres. Verifies:
    ///   1. `agents` has 3 rows in the same graph
    ///   2. `edges` has 2 parent→child rows
    ///   3. `agent_tools` joins each agent to its declared tools
    ///   4. `AppliedGraph.id_map` contains all 3 operator ids mapped to
    ///      matching UUIDs (operator-authored ids resolve to the same
    ///      UUIDs `agents.id` holds)
    ///   5. `AppliedGraph.agents` is in DFS parents-first order
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn create_from_yaml_multi_agent_writes_tree_and_id_map_matches_agents_table(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        const MULTI: &str = r#"
apiVersion: coral.engine/v1alpha1
kind: Graph
metadata:
  name: multi-smoke
tools:
  - id: echo
    kind: builtin
    builtin: echo
agents:
  - id: parent
    mandate:
      text: parent
      idle_period: 1s
    tools: [echo]
    children:
      - id: child-a
        mandate:
          text: a
          idle_period: 1s
        tools: [echo]
      - id: child-b
        mandate:
          text: b
          idle_period: 1s
        tools: []
seed:
  triggers:
    - agent: parent
      at: start
      external:
        kind: kickoff
        payload: {}
"#;
        let store = GraphStore::new(pool);
        let g = crate::yaml::parse_and_validate(MULTI).expect("validator green");
        let applied = store.create_from_yaml(&g).await.expect("create_from_yaml");

        // 3 agents, all in the same graph, parents-first DFS.
        let agents = store
            .list_agents_in_graph(applied.graph_id.into_uuid())
            .await?;
        assert_eq!(agents.len(), 3);
        assert_eq!(applied.agents.len(), 3);
        assert_eq!(applied.agents[0].operator_id, "parent");
        assert!(applied.agents[0].parent_db_agent_id.is_none());
        // Child entries reference the parent's db UUID.
        let parent_db_id = applied.agents[0].db_agent_id;
        for child in &applied.agents[1..] {
            assert_eq!(child.parent_db_agent_id, Some(parent_db_id));
        }

        // 2 edges (parent → child-a, parent → child-b).
        let edges = store
            .list_edges_in_graph(applied.graph_id.into_uuid())
            .await?;
        assert_eq!(edges.len(), 2);
        assert!(edges
            .iter()
            .all(|e| AgentId::new(e.parent_agent_id) == parent_db_id));

        // agent_tools joins: parent + child-a each have echo; child-b has none.
        let parent_resolved = applied.id_map.get("parent").unwrap();
        let parent_tools = store
            .list_tools_for_agent(parent_resolved.db_agent_id.into_uuid())
            .await?;
        assert_eq!(parent_tools.len(), 1);
        let child_a_resolved = applied.id_map.get("child-a").unwrap();
        let child_a_tools = store
            .list_tools_for_agent(child_a_resolved.db_agent_id.into_uuid())
            .await?;
        assert_eq!(child_a_tools.len(), 1);
        let child_b_resolved = applied.id_map.get("child-b").unwrap();
        let child_b_tools = store
            .list_tools_for_agent(child_b_resolved.db_agent_id.into_uuid())
            .await?;
        assert_eq!(child_b_tools.len(), 0);

        // id_map UUIDs match agents.id UUIDs verbatim.
        for resolved in &applied.agents {
            let row = agents
                .iter()
                .find(|a| AgentId::new(a.id) == resolved.db_agent_id)
                .unwrap_or_else(|| panic!("agent row for {:?} not found", resolved.operator_id));
            assert_eq!(row.name, resolved.operator_id);
        }
        Ok(())
    }

    /// `policy:` round-trips into `graphs.metadata` jsonb.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn create_from_yaml_persists_policy_into_graph_metadata(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        const WITH_POLICY: &str = r#"
apiVersion: coral.engine/v1alpha1
kind: Graph
metadata:
  name: with-policy
tools:
  - id: echo
    kind: builtin
    builtin: echo
agents:
  - id: root
    mandate:
      text: x
      idle_period: 1s
    tools: [echo]
seed:
  triggers:
    - agent: root
      at: start
      external:
        kind: k
        payload: {}
policy:
  cost_budget:
    daily_usd: 50
  on_budget_exhausted: pause
"#;
        let store = GraphStore::new(pool);
        let g = crate::yaml::parse_and_validate(WITH_POLICY).expect("validator green");
        let applied = store.create_from_yaml(&g).await.expect("create_from_yaml");
        // Re-fetch the graph row to verify metadata.
        let row: (serde_json::Value,) = sqlx::query_as("SELECT metadata FROM graphs WHERE id = $1")
            .bind(applied.graph_id.into_uuid())
            .fetch_one(store.pool())
            .await?;
        let policy = row.0.get("policy").expect("policy key persists");
        assert_eq!(
            policy.get("on_budget_exhausted").and_then(|v| v.as_str()),
            Some("pause")
        );
        assert_eq!(
            policy
                .get("cost_budget")
                .and_then(|v| v.get("daily_usd"))
                .and_then(|v| v.as_i64()),
            Some(50),
        );
        Ok(())
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn create_from_yaml_returns_typed_error_on_metadata_name_collision(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let g = crate::yaml::parse_and_validate(HAPPY_YAML).expect("validator green");
        // First apply lands.
        store.create_from_yaml(&g).await.expect("first apply");

        // Second apply with the same `metadata.name` must surface the
        // typed `GraphAlreadyExists` variant so the binary can print
        // the operator-facing CREATE-only error.
        let err = store
            .create_from_yaml(&g)
            .await
            .expect_err("second apply must reject");
        match err {
            crate::GraphStoreError::GraphAlreadyExists { name } => {
                assert_eq!(name, "smoke");
            }
            other => panic!("expected GraphAlreadyExists, got {other:?}"),
        }
        Ok(())
    }

    /// Concurrent-race regression: spawn N tasks that all try to
    /// `create_from_yaml` the same graph against the same pool, and
    /// assert exactly one wins.
    ///
    /// Two concurrent applies could both pass the SELECT before either
    /// INSERT lands. The DB-level UNIQUE constraint
    /// (`graphs_name_unique`) makes the race structurally impossible;
    /// this test exercises it by racing N=8 spawned tasks and verifying:
    ///
    /// 1. Exactly one task returned `Ok`.
    /// 2. The remaining `N-1` tasks returned
    ///    `Err(GraphStoreError::GraphAlreadyExists { name })` —
    ///    *regardless* of whether the SELECT fast path or the SQLSTATE
    ///    23505 catch surfaced the collision. The caller-visible error
    ///    shape is identical, which is the contract `coral-apply`
    ///    relies on.
    /// 3. No other error variant leaked (a `Sqlx(_)` here would mean
    ///    the unique-violation catch didn't fire).
    ///
    /// N=8 is small enough to keep the test cheap but large enough to
    /// reliably interleave the SELECTs (Postgres handles each spawned
    /// task on its own connection from the `PgPool`).
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn create_from_yaml_concurrent_apply_exactly_one_wins(pool: PgPool) -> sqlx::Result<()> {
        const N: usize = 8;
        let store = GraphStore::new(pool);
        let graph = crate::yaml::parse_and_validate(HAPPY_YAML).expect("validator green");
        let expected_name = graph.metadata.name.clone();

        // Spawn N concurrent applies of the same YAML. Each task owns
        // its own `GraphStore` clone (cheap — `PgPool` is `Arc`-shaped)
        // and its own parsed `GraphYaml` clone so the tasks share no
        // state beyond the pool. `JoinSet` is the dep-free
        // alternative to `futures::join_all` (no `futures` in the
        // crate's `[dependencies]`).
        let mut joinset: tokio::task::JoinSet<
            Result<crate::yaml::AppliedGraph, crate::GraphStoreError>,
        > = tokio::task::JoinSet::new();
        for _ in 0..N {
            let store = store.clone();
            let graph = graph.clone();
            joinset.spawn(async move { store.create_from_yaml(&graph).await });
        }

        let mut ok_count = 0usize;
        let mut already_exists_count = 0usize;
        while let Some(joined) = joinset.join_next().await {
            let res = joined.expect("spawned task did not panic");
            match res {
                Ok(a) => {
                    assert_eq!(a.graph_name, expected_name);
                    ok_count += 1;
                }
                Err(crate::GraphStoreError::GraphAlreadyExists { name }) => {
                    assert_eq!(name, expected_name);
                    already_exists_count += 1;
                }
                Err(other) => {
                    panic!("expected Ok or GraphAlreadyExists from concurrent apply, got {other:?}")
                }
            }
        }

        assert_eq!(
            ok_count, 1,
            "exactly one concurrent apply must win; got {ok_count}",
        );
        assert_eq!(
            already_exists_count,
            N - 1,
            "the other {} applies must return GraphAlreadyExists; got {}",
            N - 1,
            already_exists_count,
        );

        // Belt-and-braces: the DB should hold exactly one row.
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM graphs WHERE name = $1")
            .bind(&expected_name)
            .fetch_one(store.pool())
            .await?;
        assert_eq!(
            row.0, 1,
            "graphs table must hold exactly one row for {expected_name:?}"
        );
        Ok(())
    }

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn create_from_yaml_is_transactional_on_collision_no_partial_writes(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        // The collision check runs inside the transaction before any
        // INSERT touches `graphs`/`agents`/`tools`. We can't directly
        // observe a rolled-back partial write (because nothing is
        // attempted before the SELECT), but we can assert the
        // post-collision DB state hasn't gained extra rows from the
        // second apply attempt — proving the transaction's
        // collision-short-circuit didn't leak.
        let store = GraphStore::new(pool);
        let g = crate::yaml::parse_and_validate(HAPPY_YAML).expect("validator green");
        store.create_from_yaml(&g).await.expect("first apply");

        // Count tools after the first apply.
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM tools")
            .fetch_one(store.pool())
            .await?;
        let tools_before = row.0;

        // Second apply errors.
        let _ = store
            .create_from_yaml(&g)
            .await
            .expect_err("must reject collision");

        // No new tool row was inserted.
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM tools")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(
            row.0, tools_before,
            "collision must short-circuit before any tool INSERT",
        );
        Ok(())
    }

    // --- StructuralDbStore bridge ------------------------------------

    /// The `StructuralDbStore` impl is a thin shim over `add_agent` +
    /// `add_edge`; the load-bearing claim is that the freshly-allocated
    /// child id round-trips through the kernel newtype and the edge
    /// row reads back via `list_children`. Routes through the trait
    /// method so a future trait-only refactor of the activity body has
    /// coverage.
    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn structural_db_store_trait_adds_agent_and_edge(pool: PgPool) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let graph = seed_graph(&store).await;
        let parent_record = seed_agent(&store, graph.id, "parent").await;
        let parent_id = AgentId::new(parent_record.id);
        let graph_id = GraphId::new(graph.id);

        // Trait dispatch: returns AgentId, not AgentRecord.
        let child_id: AgentId =
            <GraphStore as StructuralDbStore>::add_agent(&store, graph_id, "child", None)
                .await
                .expect("trait add_agent");
        <GraphStore as StructuralDbStore>::add_edge(&store, parent_id, child_id)
            .await
            .expect("trait add_edge");

        // Read back via the existing query API — the freshly-allocated
        // child id matches what `list_children` reports for the parent.
        let children = store.list_children(parent_record.id).await?;
        assert_eq!(children.len(), 1);
        assert_eq!(AgentId::new(children[0].id), child_id);
        assert_eq!(children[0].name, "child");
        Ok(())
    }
}

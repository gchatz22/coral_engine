//! `GraphStore` — CRUD API over the structural DB (stage 1.4, JAR2-49).
//!
//! Concrete struct over a `PgPool` rather than a trait. We're at the
//! single-implementation stage; promoting to a trait now would be
//! abstraction-for-its-own-sake (per `DEVELOPMENT.md` § 2). Mocking
//! pressure will come from stage 3's `AgentCore` refactor — if a test
//! wants to bypass the DB, the seam is `Arc<GraphStore>` swapped for a
//! trait-object then, with the trait extracted at the moment of need.
//!
//! All queries use `sqlx::query!` / `sqlx::query_as!` so the schema is
//! verified at compile time. The `.sqlx/` offline cache (committed via
//! `cargo sqlx prepare --workspace`) lets `cargo build` succeed without
//! a live DB; CI then runs the tests against a live Postgres.
//!
//! Method shapes match the union of the parent ticket (JAR2-44) and the
//! sub-ticket (JAR2-49) — `list_children`, `list_edges_in_graph`, and
//! `list_tools_for_agent` are all in scope.

use crate::types::{AgentRecord, Edge, Graph, ToolRecord};
use crate::yaml::{GraphYaml, ToolKind};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

/// Typed error surface for [`GraphStore::create_from_yaml`].
///
/// Stage 4.2 (JAR2-73) introduces a single domain-typed variant —
/// `GraphAlreadyExists` — so the `jarvis-apply` binary can pattern-match
/// the `metadata.name` collision and print a clean, operator-targeted
/// error (rather than letting a generic `sqlx::Error` bubble through).
/// Other failure paths stay opaque via `Sqlx`; the v1 CREATE-only
/// semantics don't need finer-grained typing yet (see <JAR2-71> for the
/// deferred reconciliation work that will).
#[derive(Debug, thiserror::Error)]
pub enum GraphStoreError {
    /// A graph with this `metadata.name` already exists in the structural
    /// DB. v1 is CREATE-only; reconciliation lives in Stage 5+.
    /// See <JAR2-71>'s parent-ticket decision matrix.
    #[error(
        "graph {name:?} already exists in the structural DB; v1 of `jarvis apply` is CREATE-only \
         (reconciliation is deferred to Stage 5 — see <JAR2-71>). Drop the existing graph or use \
         a different `metadata.name`."
    )]
    GraphAlreadyExists { name: String },

    /// Any other DB-level failure (FK violation, connection drop, etc.).
    /// Surfaced verbatim so operators can see the underlying `sqlx`
    /// error text; the `jarvis-apply` binary chains it into anyhow at
    /// the call site.
    #[error("structural DB write failed: {0}")]
    Sqlx(#[from] sqlx::Error),
}

/// Thin wrapper over a `PgPool` exposing the structural-DB CRUD surface
/// stage-4's `jarvis apply` will write into and downstream consumers
/// will read from at startup.
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
    /// (apply via `jarvis_graph::MIGRATOR` before constructing).
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Expose the underlying pool for callers that need to mix in their
    /// own queries (e.g. stage-5 multi-graph tests). Intentionally
    /// `pub` rather than `pub(crate)` because there's no realistic
    /// kernel reason to hide it: `GraphStore` is a thin shell, and the
    /// pool is what it shells.
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

    /// Stage 4.2 (JAR2-73) transactional wrapper: in one DB transaction,
    /// CREATE the graph row + the (single, v1) agent row + every tool
    /// row + every `agent_tools` attachment.
    ///
    /// Returns [`GraphStoreError::GraphAlreadyExists`] when a graph with
    /// the same `metadata.name` already exists. v1 of `jarvis apply` is
    /// CREATE-only; the reconciliation alternative (compare-and-update,
    /// or `--prune`) is locked to Stage 5+ per the parent ticket
    /// (<JAR2-71>). The collision is detected via a
    /// `SELECT 1 FROM graphs WHERE name = $1` inside the transaction —
    /// the fast path for the common, operator-driven case. As of JAR2-77
    /// `graphs.name` also carries a DB-level UNIQUE constraint
    /// (`graphs_name_unique`), and the INSERT below additionally catches
    /// the resulting Postgres SQLSTATE 23505 and maps it to the same
    /// `GraphAlreadyExists` variant — defense in depth for the two-
    /// concurrent-applies race the SELECT-then-INSERT alone cannot
    /// rule out.
    ///
    /// Caller invariant: `graph` must already have passed
    /// [`crate::yaml::validate`] (or [`crate::yaml::parse_and_validate`]),
    /// since this function takes the v1 "exactly one agent" guarantee as
    /// a typed witness via `graph.agents[0]`.
    ///
    /// Tool-row shape (mirror of the schema's free-form `kind` column):
    /// `kind = "builtin"`, `command = Some(<builtin name>)`,
    /// `args = []`, `env_refs = []`. v1's only tool variant is
    /// `kind: builtin`; the validator rejects `kind: mcp` so an
    /// `unreachable!()`-style match arm would be the only alternative
    /// here, which adds nothing.
    pub async fn create_from_yaml(&self, graph: &GraphYaml) -> Result<Graph, GraphStoreError> {
        let mut tx: Transaction<'_, Postgres> = self.pool.begin().await?;

        // Collision check inside the transaction. `FOR UPDATE` would be
        // the textbook race-free shape, but `graphs.name` has no index
        // and `jarvis apply` is operator-driven (not concurrent), so a
        // plain SELECT is sufficient for v1. A two-`jarvis apply`-at-
        // once race is a vanishingly unlikely scenario, and even when
        // it does occur both inserts would succeed today (no UNIQUE
        // constraint) — the right fix is the deferred schema migration,
        // not adding locking that papers over its absence.
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
            // `metadata` jsonb is `{}` for v1; the description lives on
            // the YAML side only (operator authoring surface), not in
            // the structural DB. Stage 5+ may bubble selected metadata
            // up if a need surfaces.
            serde_json::json!({}),
        )
        .fetch_one(&mut *tx)
        .await
        {
            Ok(row) => row,
            // JAR2-77 — the SELECT above is the fast path for the
            // common, operator-driven case. The `graphs_name_unique`
            // constraint (migration `0002_graphs_name_unique.sql`) is
            // the DB-level backstop for the two-concurrent-applies
            // race: two SELECTs can both miss, but only one INSERT
            // wins; the loser's `fetch_one` resolves to
            // `sqlx::Error::Database` with SQLSTATE 23505 (Postgres
            // unique-violation). Surface it as the same typed
            // `GraphAlreadyExists` variant the fast path uses so the
            // caller (the `jarvis-apply` binary) renders one clean
            // error message regardless of which check fired.
            //
            // The transaction is dropped on the `return` below, which
            // Postgres rolls back implicitly — no agent / tool rows
            // can leak from this branch because the only writes so
            // far are the failed INSERT itself.
            Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("23505") => {
                return Err(GraphStoreError::GraphAlreadyExists {
                    name: graph.metadata.name.clone(),
                });
            }
            Err(e) => return Err(e.into()),
        };

        // --- agent row (v1: exactly one) -------------------------------
        let agent_yaml = &graph.agents[0];
        let agent_id = Uuid::new_v4();
        sqlx::query_as!(
            AgentRecord,
            r#"
            INSERT INTO agents (id, graph_id, name, mandate_ref)
            VALUES ($1, $2, $3, $4)
            RETURNING id, graph_id, name, mandate_ref, created_at as "created_at!: chrono::DateTime<chrono::Utc>"
            "#,
            agent_id,
            graph_id,
            &agent_yaml.id,
            // `mandate_ref` is the opaque text handle to the authored
            // mandate. v1's mandate travels via `AgentInput.mandate`
            // directly (no resolver looks `mandate_ref` up), so leave
            // it `None` rather than seed a placeholder value the
            // structural DB would not interpret. See the no-back-compat-
            // scaffolding memo.
            None as Option<&str>,
        )
        .fetch_one(&mut *tx)
        .await?;

        // --- tool rows + agent_tools attachments -----------------------
        // The validator rejects `kind: mcp`, so every tool entry is a
        // `ToolKind::Builtin` here. We persist `command = <builtin name>`
        // so the structural DB row preserves the operator's choice
        // (e.g. `"echo"`) — the worker side doesn't read this column
        // back today, but writing it correctly future-proofs Stage 5+
        // for MCP-tool registration (which would set `command =
        // Some(<spawn argv[0]>)`). A `HashMap` indexes the freshly
        // minted UUIDs so the agent-tools attachment loop can resolve
        // tool ids by name without re-querying.
        let mut tool_uuid_by_id: std::collections::HashMap<&str, Uuid> =
            std::collections::HashMap::with_capacity(graph.tools.len());
        for tool in &graph.tools {
            let tool_uuid = Uuid::new_v4();
            let (kind_text, command_text): (&str, Option<&str>) = match &tool.kind {
                ToolKind::Builtin { builtin } => ("builtin", Some(builtin.as_str())),
                // v1's validator rejects `kind: mcp` before this code
                // runs — but the match must be exhaustive. A panic
                // here would be a validator bug; surface it as such.
                ToolKind::Mcp { .. } => {
                    return Err(GraphStoreError::Sqlx(sqlx::Error::Protocol(format!(
                        "internal: create_from_yaml saw kind: mcp for tool {:?}; \
                             validator should have rejected it (see <JAR2-71>)",
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

        for tool_id in &agent_yaml.tools {
            // Validator guarantees every `agent.tools[i]` resolves to a
            // top-level `tools[].id`; the lookup is therefore
            // infallible. We propagate a typed error if it isn't
            // rather than panic, so the validator bug (if any) surfaces
            // as a clean operator-facing message.
            let tool_uuid = tool_uuid_by_id
                .get(tool_id.as_str())
                .copied()
                .ok_or_else(|| {
                    GraphStoreError::Sqlx(sqlx::Error::Protocol(format!(
                        "internal: create_from_yaml could not resolve tool ref {tool_id:?}; \
                     validator should have caught this (see <JAR2-71>)"
                    )))
                })?;
            sqlx::query!(
                "INSERT INTO agent_tools (agent_id, tool_id) VALUES ($1, $2)",
                agent_id,
                tool_uuid,
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(graph_row)
    }

    // --- reads ------------------------------------------------------

    /// Fetch a single agent by id. Returns `Ok(None)` if no row matches —
    /// the call shape stage-3 startup code expects (lookup that may
    /// legitimately miss).
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
    /// (not the edge). The integration test in 1.5 uses this to assert
    /// `list_children(parent_id) == [child_id]`.
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

    /// All edges whose parent or child belongs to a graph. Useful for
    /// stage-4 `jarvis apply` round-trips that want to verify the full
    /// edge set after a write. Same-graph invariant lives in
    /// application code (see schema decision in
    /// `migrations/0001_initial.sql`).
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

    /// All tools attached to an agent via `agent_tools`. The integration
    /// test in 1.5 uses this to assert
    /// `list_tools_for_agent(child_id) == [tool_id]`.
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

#[cfg(test)]
mod tests {
    //! Per-method tests against an ephemeral per-test Postgres via
    //! `#[sqlx::test(migrator = "crate::MIGRATOR")]`. Each test gets
    //! its own freshly migrated DB, so tests are fully isolated and
    //! can rely on row counts.
    //!
    //! Acceptance per JAR2-49: "happy path + one failure mode (e.g. FK
    //! violation)" for each method. Failure modes that aren't reachable
    //! via the public API (e.g. SQL parse errors) are skipped — those
    //! are caught at compile time by `query!`.
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

    // --- create_from_yaml (Stage 4.2, JAR2-73) -----------------------

    /// Canonical happy-path fixture for `create_from_yaml`. Mirror of
    /// the validator-tests `HAPPY_YAML` (kept inline to avoid coupling
    /// the store-test module's pass/fail to the yaml-test module's
    /// shape).
    const HAPPY_YAML: &str = r#"
apiVersion: jarvis.engine/v1alpha1
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
        let graph_row = store.create_from_yaml(&g).await.expect("create_from_yaml");

        assert_eq!(graph_row.name, "smoke");
        let agents = store.list_agents_in_graph(graph_row.id).await?;
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].name, "root");
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

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn create_from_yaml_returns_typed_error_on_metadata_name_collision(
        pool: PgPool,
    ) -> sqlx::Result<()> {
        let store = GraphStore::new(pool);
        let g = crate::yaml::parse_and_validate(HAPPY_YAML).expect("validator green");
        // First apply lands.
        store.create_from_yaml(&g).await.expect("first apply");

        // Second apply with the same `metadata.name` must surface the
        // typed `GraphAlreadyExists` variant — the binary uses this to
        // print the operator-facing v1-CREATE-only error.
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

    /// JAR2-77 — concurrent-race regression: spawn N tasks that all try
    /// to `create_from_yaml` the same graph against the same pool, and
    /// assert exactly one wins.
    ///
    /// Today's SELECT-then-INSERT fast path is sufficient for the
    /// operator-driven case (single caller, no concurrency), but two
    /// concurrent applies *could* both pass the SELECT before either
    /// INSERT lands. The DB-level UNIQUE constraint
    /// (`graphs_name_unique`, migration `0002`) is what makes the
    /// race structurally impossible; this test exercises it by racing
    /// N=8 spawned tasks and verifying:
    ///
    /// 1. Exactly one task returned `Ok`.
    /// 2. The remaining `N-1` tasks returned
    ///    `Err(GraphStoreError::GraphAlreadyExists { name })` —
    ///    *regardless* of whether the SELECT fast path or the SQLSTATE
    ///    23505 catch surfaced the collision. The caller-visible error
    ///    shape is identical, which is the contract `jarvis-apply`
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
        let mut joinset: tokio::task::JoinSet<Result<Graph, crate::GraphStoreError>> =
            tokio::task::JoinSet::new();
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
                Ok(g) => {
                    assert_eq!(g.name, expected_name);
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
}

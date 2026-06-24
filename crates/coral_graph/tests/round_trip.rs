//! Cold-start integration test: write a graph with a parent + child +
//! edge + tool, then read every piece back via the `GraphStore` API and
//! assert the graph reconstructs. Uses `#[sqlx::test(migrator =
//! "coral_graph::MIGRATOR")]` for an ephemeral per-test database;
//! requires `DATABASE_URL` at run time.

use coral_graph::{GraphStore, MIGRATOR};
use sqlx::PgPool;

#[sqlx::test(migrator = "MIGRATOR")]
async fn applies_a_simple_parent_child_graph_and_reads_it_back(pool: PgPool) -> sqlx::Result<()> {
    // Step 1: build the GraphStore wrapper around the ephemeral test
    // pool. In production, the worker / `coral apply` constructs this
    // once at startup with `MIGRATOR.run(&pool)` already applied.
    let store = GraphStore::new(pool);

    // Step 2: create the graph itself. `coral apply` will pass the
    // YAML-authored name + a metadata blob (e.g. the source file path)
    // here.
    let graph = store
        .create_graph("smoke", serde_json::json!({"source": "round_trip.rs"}))
        .await?;

    // Step 3: add the two agents (identity + topology only).
    let parent = store.add_agent(graph.id, "parent").await?;
    let child = store.add_agent(graph.id, "child").await?;

    // Step 4: wire the parent->child edge. The schema's UNIQUE
    // constraint means a duplicate `add_edge` would fail — `coral
    // apply` is responsible for idempotency at the operator level.
    let edge = store.add_edge(parent.id, child.id).await?;

    // Step 5: register a graph-scoped tool definition. The tool kind
    // matches an operator-recognized name (`echo` here); `args` /
    // `env_refs` are JSONB blobs the worker interprets per kind. Per-agent
    // assignment is no longer a DB row — it rides each agent's mandate.
    let tool = store
        .register_tool(
            graph.id,
            "echo",
            Some("/bin/echo"),
            serde_json::json!(["hello", "from", "round_trip"]),
            serde_json::json!([]),
        )
        .await?;

    // --- read-back -------------------------------------------------

    // Step 6: list every agent in the graph. Two rows, parent first
    // (created earlier).
    let agents = store.list_agents_in_graph(graph.id).await?;
    assert_eq!(agents.len(), 2, "expected exactly two agents in the graph");
    assert_eq!(agents[0].id, parent.id);
    assert_eq!(agents[1].id, child.id);

    // Step 7: list every edge in the graph. Just the one.
    let edges = store.list_edges_in_graph(graph.id).await?;
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].id, edge.id);

    // Step 8: walk parent -> children.
    let children = store.list_children(parent.id).await?;
    assert_eq!(
        children.iter().map(|a| a.id).collect::<Vec<_>>(),
        vec![child.id],
        "list_children(parent_id) must equal [child_id]"
    );

    // Step 9: the graph-scoped tool def is listed for the graph.
    let tools_for_graph = store.list_tools_for_graph(graph.id).await?;
    assert_eq!(
        tools_for_graph.iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![tool.id],
        "list_tools_for_graph(graph_id) must equal [tool_id]"
    );

    // Step 10: spot-check a `get_agent` round-trip. This is the API a
    // worker uses at startup to resolve its own AgentRecord from the
    // workflow-id-derived UUID.
    let fetched = store.get_agent(child.id).await?;
    assert_eq!(fetched.as_ref().map(|a| a.id), Some(child.id));
    assert_eq!(fetched.as_ref().map(|a| a.name.as_str()), Some("child"));

    // Step 11 (negative): a parent agent with no edges has no children.
    // Catches a regression where `list_children` mistakenly returns
    // every child in the graph.
    let stranger = store.add_agent(graph.id, "stranger").await?;
    let stranger_children = store.list_children(stranger.id).await?;
    assert!(
        stranger_children.is_empty(),
        "an agent with no outgoing edges must have no children, got {:?}",
        stranger_children
    );

    Ok(())
}

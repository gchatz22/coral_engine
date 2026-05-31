# `smoke_mcp_temporal` — MCP-on-the-workflow-path smoke fixture

Single-file `graph.yaml` fixture proving a `kind: mcp` tool works
end-to-end on the Temporal worker path: `coral apply` persists the MCP
tool row, and the worker builds the agent's **per-graph** tool registry
by reading that row from the structural DB and spawning the MCP server.
This is the workflow-path counterpart to the in-process
[`smoke_mcp`](../smoke_mcp/README.md) / [`smoke_llm_mcp`](../smoke_llm_mcp/README.md)
fixtures (which drive MCP through `node-run-mcp` / `node-run-llm`, not
the worker).

## How the worker sources MCP servers (per graph)

`graph.yaml` is the runtime source of truth for a graph's tools. `coral
apply` writes the `tools` / `agent_tools` rows; the worker daemon never
reads the YAML. At first tool dispatch for a graph the worker reads that
graph's tool rows from the DB and builds a registry: the builtin `echo`
plus one `McpClient` per declared MCP server (deduplicated by
`command + args + env`). Registries are cached per `graph_id` for the
worker's lifetime, so two graphs on one worker each reach only their own
servers. (`server-everything` itself advertises an `echo` tool; it wins
the `echo` name for this graph, and the builtin is the fallback.)

## Prerequisites

- The dev stack (Postgres + Temporal) and a worker daemon running — see
  the [top-level README's Dev Environment section](../../README.md#dev-environment).
- **Node.js + npm** on the worker's `$PATH`: the worker spawns the MCP
  server via `npx -y @modelcontextprotocol/server-everything@<pinned>`.
  No paid API key is required.

## Files

- `graph.yaml` — one agent, one `kind: mcp` tool wired to the pinned
  reference server, mandate asking for a `get-sum` call + a cited output,
  then retire. Consumed by `coral apply`.

## Run

Bring up the dev stack + the worker daemon (the worker needs Node on its
`PATH`), then apply:

```sh
DATABASE_URL=postgres://coral:coral@localhost:5432/coral_structural \
cargo run -p coral_graph --bin coral-apply -- \
    examples/smoke_mcp_temporal/graph.yaml
```

The binary writes the structural rows (including the MCP tool), dispatches
the workflow onto the `coral-agents` task queue, and prints the workflow
ID. Execution happens on the daemon; the first `get-sum` call triggers the
worker to spawn the MCP server for this graph. Follow the run via the
Temporal Web UI at <http://localhost:8233>.

## Automated smoke

The env-gated live test
`crates/coral_worker/tests/mcp_graph_live.rs` drives this exact path with
a scripted decision sequence (no LLM): it applies the graph, hosts a
worker with the real DB-backed registry provider, runs `get-sum`, and
asserts the emitted Output's evidence traces back to the MCP call. It is
gated behind `TEMPORAL_LIVE_TEST=1`, `DATABASE_URL`, and `CORAL_SMOKE_MCP=1`
(plus Node for the server); see that file's header for the run command.

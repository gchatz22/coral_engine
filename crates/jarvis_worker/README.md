# jarvis_worker

The long-lived **worker daemon**. It hosts the Temporal `AgentWorkflow`
runtime (`jarvis_temporal`) and installs the structural-DB store
(`jarvis_graph::GraphStore`) so the full agent lifecycle — including
`Decision::SpawnChild` — works in production.

This crate is the composition root: `jarvis_graph` depends on
`jarvis_temporal`, so only a third crate above both can wire a real
`GraphStore` into the worker's `StructuralDbStore` slot without a Cargo
dependency cycle.

## Run it

The daemon expects backing services from the top-level `docker-compose.yml`
(Postgres + Temporal). Bring those up first (`make up`), then:

```sh
# Native dev loop (recommended): uses values from `.env`.
cargo run -p jarvis_worker --bin worker

# With an LLM vendor compiled in (otherwise boot errors on Decide setup):
cargo run -p jarvis_worker --bin worker --features llm-anthropic
```

Or as a container: `make worker` (or
`docker compose --profile container-worker up worker`). The image compiles
both vendors in; supply an API key (and optionally `JARVIS_MODEL_VENDOR`)
via the `worker` service env in `docker-compose.yml` to pick one at runtime.

## Environment

| Var | Required | Purpose |
|---|---|---|
| `DATABASE_URL` | **yes** | Postgres URL for the structural DB. The daemon installs a `GraphStore` over it at boot; without it the worker exits before serving. Dev value lives in `.env.example` (`postgres://jarvis:jarvis@localhost:5432/jarvis_structural`); the `worker` compose service sets the in-network equivalent. |
| `TEMPORAL_ADDRESS` | no | Temporal frontend gRPC endpoint (default `http://localhost:7233`). |
| `TEMPORAL_NAMESPACE` | no | Temporal namespace (default `default`). |
| `TEMPORAL_TASK_QUEUE` | no | Queue to listen on (default `jarvis-agents`). |
| `AGENT_FS_ROOT` | no | Per-agent FS root (default `./agent-fs`). |
| `JARVIS_MODEL_VENDOR` / `ANTHROPIC_API_KEY` / `COHERE_API_KEY` | no | LLM vendor selection for the `Decide` backend; see `jarvis_temporal::worker::build_decide_from_env`. |

The worker does **not** run schema migrations — apply the structural-DB
schema via `jarvis apply` (it runs `jarvis_graph::MIGRATOR`) before pointing
the daemon at a fresh database.

## Tests

`tests/spawn_child_db_live.rs` is a live, double-gated test
(`TEMPORAL_LIVE_TEST=1` + `DATABASE_URL`) that drives a parent through
`SpawnChild` and asserts the child `agents` + `edges` rows land in Postgres
via the real `GraphStore`. It is skipped in a plain `cargo test`.

# Coral Engine

*An open, forkable substrate for continuously running autonomous research — a graph of subagents that read the world, reason about it, and keep a current model of any topic alive forever after.*

The Coral Engine is an OS for autonomous research: a runtime for graphs of long-lived agents that wake on signal, do narrow work, and feed outputs to their parents. The graph — not the request — is the unit of computation. See `VISION.md` for the full product/architecture vision and `DEVELOPMENT.md` for the rules every contributor (human or agent) follows.

## Repo layout

- `crates/coral_node` — runtime types and agent core (`Agent::run`, `AgentFs`, `Mandate`, `Trigger`, `Decision`, ...). Today's library; foundation for the workspace.
- `crates/coral_temporal` — Temporal-hosted agent workflow runtime (library + `temporal-smoke` bin).
- `crates/coral_graph` — structural DB (graphs, agents, edges, tools) + the `coral-apply` operator CLI.
- `crates/coral_worker` — the long-lived worker daemon binary; composition root that wires the runtime to the structural-DB store.
- `scratch/` — design notes. `scratch/temporal_staged_plan.md` is the current execution plan; read it before non-trivial work.
- `examples/` — runnable smokes (FS, MCP, LLM-driven).
- `docker-compose.yml` + `crates/coral_worker/Dockerfile` — local dev environment, documented below.

## Dev environment

The day-to-day dev loop runs **backing services in Docker** and the **worker natively** via `cargo run`. Native worker = fast incremental builds, native debugger, no Docker rebuild between iterations. The container-shape worker exists in `docker-compose.yml` (profile `container-worker`) so the production path stays exercised — but it's not the default.

**Operator CLIs dispatch to the daemon.** Per `scratch/temporal_staged_plan.md` § 2.6, operator-facing CLIs (`coral apply`, future `coral signal` / `inspect` / `retire`) are thin Temporal clients — they connect to Temporal, dispatch onto the canonical task queue `coral-agents` (exported as `coral_temporal::worker::DEFAULT_TASK_QUEUE`), and exit. The long-lived worker daemon — the binary at `crates/coral_worker/src/bin/worker.rs`, either run natively or as the `worker` compose service — is what executes the workflows.

### Prerequisites

- Docker + Docker Compose v2 (`docker compose ...`, not `docker-compose ...`).
- Rust toolchain pinned by `rust-toolchain.toml` (1.88). `rustup` picks it up automatically.

### Bring the stack up

```sh
cp .env.example .env             # one-time; edit if defaults don't suit
make up                          # or: docker compose up -d
make ps                          # verify Postgres + Temporal + UI are healthy
```

What this starts:

| Service | Image | Host port | Purpose |
|---|---|---|---|
| `postgres` | `postgres:16-alpine3.23` | `5432` | Backs both Temporal (its own DBs) and the structural DB (`coral_structural`). |
| `temporal` | `temporalio/auto-setup:1.29.6` | `7233` | Temporal frontend (gRPC). Single-container "all services in one" dev image; production-shape splits these. |
| `temporal-ui` | `temporalio/ui:2.50.0` | `8233` | Temporal Web UI. Reachable at <http://localhost:8233>. |
| `worker` | built locally (`coral-worker:dev`) | — | Worker container scaffold. Built on demand; not started by default (profile `container-worker`). |

### Run the worker natively (recommended)

```sh
# Backing services already up from `make up`. The worker uses values from
# `.env` (host-network endpoints).
cargo run -p coral_worker --bin worker
```

The worker connects to the Temporal frontend at `TEMPORAL_ADDRESS`, registers `AgentWorkflow` + the activity bundle, and listens on the canonical task queue **`coral-agents`** (overrideable via `TEMPORAL_TASK_QUEUE` — see `.env.example`). It also installs the structural-DB store from **`DATABASE_URL`** (required — the daemon exits at boot without it) so `Decision::SpawnChild` can register child agents; it does not run migrations, so apply the schema via `coral apply` first. Once the log lines `installed StructuralDbStore backend ...` and `coral worker starting; registered: AgentWorkflow + AgentActivities` show up with `task_queue="coral-agents"`, operator CLIs (and `temporal workflow start --task-queue coral-agents ...`) can dispatch to it.

### Dispatch a workflow against the running daemon

```sh
# In a separate terminal, with the dev stack + native worker both up:
temporal workflow start \
    --address localhost:7233 --namespace default \
    --task-queue coral-agents \
    --type AgentWorkflow \
    --workflow-id 'graphs/<graph_id>/agents/<agent_id>' \
    --input '{ ... AgentInput JSON ... }'
```

The Temporal Web UI at <http://localhost:8233> shows the queued + running workflows.

### Run the worker as a container (production-shape)

```sh
make worker-build                # build only
make worker                      # build + run, attached
```

Or directly: `docker compose --profile container-worker up worker`. The container listens on the same `coral-agents` queue, so operator CLIs don't care which path is running.  Worker reads service-name endpoints (`postgres:5432`, `temporal:7233`) inside the compose network — no `.env` changes needed.

### Inspect state

- **Temporal UI:** <http://localhost:8233> — workflow histories, signals, task queues.
- **Postgres:** `make psql` opens a `psql` shell against the `coral_structural` database. From the host: `psql postgres://coral:coral@localhost:5432/coral_structural`.
- **Per-agent FS:** the host directory `./agent-fs/` is bind-mounted to `/agent-fs` inside the worker container. Inspect it with the usual shell tools.

### Reset

```sh
make down                        # stop services, keep volumes (Temporal/Postgres state survives restart)
make reset                       # stop services AND drop volumes — fresh state
```

`make reset` is the right move between experiments where you want Temporal history and Postgres data nuked. The per-agent FS (`./agent-fs/`) is host-mounted, so it persists across both `down` and `reset` — clear it by hand if needed.

If you brought up the in-container worker (via `make worker`), `docker compose` will leave its stopped container behind on a plain `down`; sweep it with `docker compose --profile container-worker down -v` once you're done with that path.

### Verify the workspace builds

```sh
cargo build --workspace
cargo test --workspace
```

The dev-environment work doesn't add Rust deps; if either command starts failing after a compose change, something else is in motion.

## Status

Pre-production. See `scratch/temporal_staged_plan.md` for the staged plan. Today's `coral_node` is a single in-process agent loop with provenance-enforced FS state.

See `DEVELOPMENT.md` for contribution rules: smallest correct diff, tests with the change, GitHub-Issues-driven planning, Graphite-managed stacked PRs.

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE).

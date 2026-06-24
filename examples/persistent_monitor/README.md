# `persistent_monitor` — continuous-monitor fixture

A reduced `graph.yaml` proving the **continuous loop** end-to-end on the
Temporal worker path: one parent (`analyst`) + two children
(`researcher-alpha`, `researcher-beta`). Persistence is universal — agents
never self-terminate — so they cycle (emit → idle → refresh), the parent
re-reconciles newer child outputs into refreshed reports, and the graph
stops only at a runtime guardrail.

One file serves two run modes, because `graph.yaml` never picks the
`Decide` implementation — only the topology, cadence, and budget.

## Mode 1 — deterministic contract smoke (no key, no Node)

The env-gated live test
[`crates/coral_worker/tests/persistent_monitor_live.rs`](../../crates/coral_worker/tests/persistent_monitor_live.rs)
applies this graph, hosts a worker with a **deterministic cycling `Decide`**,
and asserts the loop's runtime contract:

- each agent emits **≥2 distinct outputs** (the refresh cycle repeats),
- the parent performs **≥1 re-reconciliation** of a newer child output
  (≥2 distinct child outputs folded into synthetic evidence), and
- every agent stops via **`step_cap (N) reached`** (agents never self-terminate;
  the test injects a small `step_cap` as a harness-only cap — it is not a
  `graph.yaml` field).

The children cite planted evidence and the parent reconciles real child
outputs, so no model key and no Node are needed — only a local Temporal
Server and a Postgres:

```sh
TEMPORAL_LIVE_TEST=1 \
  DATABASE_URL=postgres://coral:coral@localhost:5432/coral_structural \
  cargo test -p coral_worker --test persistent_monitor_live -- --nocapture
```

It self-skips when either gate is absent, so the default `cargo test` stays
hermetic. (The always-on `example_graph_parses_and_validates` test runs with
no live deps and guards that the fixture still parses and validates.)

This proves the **machinery**. It does **not** exercise the lifecycle prompt
clauses (only a real model reads them; already snapshot-covered) or answer
whether a model can drive the loop — that's Mode 2.

## Mode 2 — real-model loop-viability run (manual)

To watch a real model drive the loop, bring up the dev stack + a worker
daemon built with a vendor (e.g. `--features llm-anthropic`, with
`ANTHROPIC_API_KEY` set), then apply this graph:

```sh
DATABASE_URL=postgres://coral:coral@localhost:5432/coral_structural \
cargo run -p coral_graph --bin coral-apply -- \
    examples/persistent_monitor/graph.yaml
```

Now the children call the MCP `get-sum` tool (so the worker needs **Node on
its `PATH`** — `npx -y @modelcontextprotocol/server-everything@<pinned>`)
and the parent reconciles their outputs. Follow it in the Temporal Web UI at
<http://localhost:8233>.

Notes for a real run:
- **Raise `idle_period`.** The sub-second cadence here is tuned so the
  deterministic smoke finishes in seconds; a real model wants a slower
  cadence. There is no per-agent iteration cap to author — the deterministic
  smoke injects a harness-only `step_cap`, and a real run stops via a retire
  signal or teardown, or the coarse interim `step_cap` backstop if it cycles
  long enough (budget will replace that backstop once it lands).
- **Tool naming.** The mandates reference `get-sum`, the server's advertised
  name. Pointing the children at a real web-search MCP server additionally
  needs the tool-catalog surfacing follow-up (issue #107) and per-server env
  substitution so the model knows which tool names it may call.
- The `get-sum` tool is cheap and deterministic on purpose — this run
  exercises *cycling and reconciliation*, not research quality.

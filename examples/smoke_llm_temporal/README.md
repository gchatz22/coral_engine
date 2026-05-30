# `smoke_llm_temporal` — live-vendor smoke fixture

Single-file `graph.yaml` fixture consumed by `jarvis-apply` against a
running worker daemon. The temporal worker only registers the
bootstrap `EchoTool` (MCP-server wiring through env vars is a planned
follow-up), so this fixture's mandate prompt is echo-only — diverging
from `smoke_llm_mcp/config.json`'s `get-sum` ask.

## Thin-client shape

`jarvis apply` is a thin Temporal client: it writes the structural DB,
dispatches the workflow onto the daemon's canonical task queue
(`jarvis-agents`), signals seed triggers, prints the workflow ID, and
exits. **Execution lives on a separately-running worker daemon** — see
the [top-level README's Dev Environment
section](../../README.md#dev-environment) for the recommended dev loop
(`cargo run -p jarvis_temporal --bin worker` in a separate terminal,
or the `worker` compose service).

## Files

- `graph.yaml` — Single operator-authored fixture. Encodes the mandate
  (call `echo`, emit a cited output, retire) + the kickoff seed
  trigger in the `apiVersion: jarvis.engine/v1alpha1` schema.
  Consumed by `jarvis apply`.

## Run

Bring up the dev stack + the worker daemon (see the top-level README),
then:

```sh
DATABASE_URL=postgres://jarvis:jarvis@localhost:5432/jarvis_structural \
cargo run -p jarvis_graph --bin jarvis-apply -- \
    examples/smoke_llm_temporal/graph.yaml
```

The binary returns in <1s with a printed workflow ID + a runnable
`temporal workflow describe ...` hint. Workflow execution happens on
the daemon; follow it via the Temporal Web UI at
<http://localhost:8233>, or via `temporal workflow show
--workflow-id <id>` from the CLI.

The daemon writes artifacts under its configured `AGENT_FS_ROOT`:

- `<root>/outputs/<sha>.json`
- `<root>/retirement.json`
- `<root>/decisions/<tick>.jsonl`
- `<root>/evidence/<sha>.json`
- `<root>/mandate.json`

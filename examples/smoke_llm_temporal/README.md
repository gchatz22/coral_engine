# `smoke_llm_temporal` — live-vendor smoke fixture

Single-file `graph.yaml` fixture consumed by `coral-apply` against a
running worker daemon. This fixture's mandate prompt is echo-only (it
uses the builtin `echo` tool); for the worker driving a real MCP server
from a `kind: mcp` graph, see
[`smoke_mcp_temporal`](../smoke_mcp_temporal/README.md).

## Thin-client shape

`coral apply` is a thin Temporal client: it writes the structural DB,
dispatches the workflow onto the daemon's canonical task queue
(`coral-agents`), signals seed triggers, prints the workflow ID, and
exits. **Execution lives on a separately-running worker daemon** — see
the [top-level README's Dev Environment
section](../../README.md#dev-environment) for the recommended dev loop
(`cargo run -p coral_worker --bin worker` in a separate terminal,
or the `worker` compose service).

## Files

- `graph.yaml` — Single operator-authored fixture. Encodes the mandate
  (call `echo`, emit a cited output, retire) + the kickoff seed
  trigger in the `apiVersion: coral.engine/v1alpha1` schema.
  Consumed by `coral apply`.

## Run

Bring up the dev stack + the worker daemon (see the top-level README),
then:

```sh
DATABASE_URL=postgres://coral:coral@localhost:5432/coral_structural \
cargo run -p coral_graph --bin coral-apply -- \
    examples/smoke_llm_temporal/graph.yaml
```

The binary returns in <1s with a printed workflow ID + a runnable
`temporal workflow describe ...` hint. Workflow execution happens on
the daemon; follow it via the Temporal Web UI at
<http://localhost:8233>, or via `temporal workflow show
--workflow-id <id>` from the CLI.

The daemon writes artifacts under its configured `AGENT_FS_ROOT`:

- `<root>/outputs/output.md`
- `<root>/retirement.json`
- `<root>/decisions/<tick>.jsonl`
- `<root>/evidence/<sha>.json`
- `<root>/mandate.json`

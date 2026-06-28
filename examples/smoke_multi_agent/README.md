# `smoke_multi_agent` — multi-agent integration fixture

Parent + 2 children + scripted disagreement. The smallest fixture that
exercises the multi-agent stack end-to-end: child workflows emit
deliberately-conflicting outputs, the parent reconciles them through
the `reconcile_children` activity (which writes synthetic evidence
records into the parent's `evidence/` directory and a `ConflictRecord`
into the parent's `conflicts/` directory), and the parent then emits
one final output citing the synthetic evidence ids before retiring.

## What this fixture proves

The integration test in `crates/coral_graph/tests/multi_agent.rs`
asserts every artifact below lands on disk in its byte-checkable
shape, and that the cross-agent provenance trail resolves
transitively:

- Each child's `outputs/output.md` carries the scripted claim
  (child-a: `"claim-A says X"`; child-b: `"claim-B says NOT-X"`).
- Parent's `evidence/` contains exactly 2 synthetic records (`tool ==
  "reconcile"`), one per cited child output. Each record's `args`
  carries the right `(child_agent_id, child_workflow_id,
  source_output_id)` triple.
- Parent's `outputs/` contains exactly 1 reconciled output citing both
  synthetic evidence ids.
- Parent's `conflicts/` contains exactly 1 `ConflictRecord` of `kind
  == "held_open"` with 2 alternatives whose `source_child` +
  `source_output_id` match the children's emitted outputs.
- **Cross-FS provenance trail (load-bearing):** parent's output →
  cited synthetic evidence → `args.source_output_id` → child's
  `outputs/output.md` resolves across two agent FS
  roots without ambiguity. This is the property the synthetic-
  evidence pattern was designed to guarantee, and the integration
  test pins it.

The children each cite one planted `EvidenceRecord` (`tool == "echo"`)
in their `WriteOutput` — `AgentFs::persist_output` rejects empty
evidence, and the test never runs a real tool, so the planted record
stands in for the tool call output. This is harness scaffolding, not
a contract; the parent's synthetic-evidence trail and the children's
own evidence trails are independent.

## Topology

```text
root  (parent, MockDecide-scripted)
├── child-a  (MockDecide-scripted; emits "claim-A says X")
└── child-b  (MockDecide-scripted; emits "claim-B says NOT-X")
```

Mandate text on each agent (`multi-agent-parent`, `multi-agent-child-a`,
`multi-agent-child-b`) is the routing discriminator the test's
`RoutingDecide` matches on at decide time.

## Run

This fixture is **integration-test-only**. The acceptance bar is the
`TEMPORAL_LIVE_TEST=1`-gated test in
`crates/coral_graph/tests/multi_agent.rs`, not an interactive
`coral apply` invocation.

```sh
# Bring up the dev stack (Temporal + Postgres) — see top-level README.
DATABASE_URL=postgres://coral:coral@localhost:5432/coral_structural \
TEMPORAL_LIVE_TEST=1 \
cargo test -p coral_graph --test multi_agent --all-features -- --nocapture
```

The test takes <30s on a healthy local stack. Without
`TEMPORAL_LIVE_TEST=1` set the test prints a one-line skip and returns
`Ok`.

## Test, not CLI — implementer's choice

The production `coral apply` binary always wires `LlmDecide` (real
LLM) through the worker daemon — there is no production-supported
seam for swapping in `MockDecide` from a YAML field or CLI flag. The
two viable patterns were:

- **Pattern A** *(chosen)*: bypass `coral apply` and construct the
  multi-agent topology directly via `client.start_workflow` per agent
  with `MockDecide` installed at the worker — the shape every
  multi-agent live test uses (`spawn_child_live.rs`,
  `child_parent_signal.rs`, `reconcile_children_live.rs`,
  `lifecycle_ops_live.rs`).
- **Pattern B**: extend the worker / `coral apply` with a
  `--decide=mock` flag or env var so MockDecide scripts load off disk.
  More test-realistic; more invasive (production-code surface that
  must be `#[cfg(test)]`- or env-gated so MockDecide doesn't run in
  production).

Pattern A was chosen for "smallest correct diff"; Pattern B is queued
as a follow-up if the apply-path coverage gap matters. The fixture
YAML is still parsed by the test at startup so the schema +
multi-agent topology shape is regression-protected.

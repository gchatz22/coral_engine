# `smoke_llm_temporal` — JAR2-68 live-vendor smoke fixture

Sibling of `examples/smoke_llm_mcp/`. The temporal worker today only
registers the bootstrap `EchoTool` (MCP-server wiring through env vars
is JAR2-63's flagged follow-up), so this fixture's mandate prompt is
echo-only — diverging from `smoke_llm_mcp/config.json`'s `get-sum`
ask. Kept separate to preserve guardrail 1 of JAR2-68 ("`node-run-llm`
stays untouched").

## Deprecation note (Stage 4.3, JAR2-74)

The recommended path is now `graph.yaml` via the `jarvis-apply` binary
(`crates/jarvis_graph/src/bin/jarvis_apply.rs`). The `config.json` +
`triggers.jsonl` pair is retained because `jarvis-run-workflow` still
consumes them as-is. Both files are scheduled for removal once Stage 6
lands the thin-client refactor that promotes `graph.yaml` as the
single fixture shape (see § 2.6 of `scratch/temporal_staged_plan.md`).
Until then, the JAR2-74 integration test
(`crates/jarvis_graph/tests/jarvis_apply_smoke.rs`) pins the YAML's
end-state to JAR2-68's smoke, so any drift between the two fixtures
fails CI.

## Files

- `graph.yaml` — Single operator-authored fixture. Encodes the same
  mandate + seed triggers as the `config.json` + `triggers.jsonl`
  pair, in the `apiVersion: jarvis.engine/v1alpha1` schema JAR2-72
  defines. Consumed by `jarvis apply` (JAR2-73). **This is the
  preferred path going forward.**
- `config.json` — `Mandate` JSON consumed by the `jarvis-run-workflow`
  binary. Asks the LLM to call `echo`, then emit an output citing the
  resulting evidence, then retire.
- `triggers.jsonl` — Single kickoff `External` trigger that wakes the
  agent on its first tick. Required because the workflow's
  `wait_condition(triggers_pending) || timer(next_wake)` race would
  otherwise drain an empty queue → `assemble_context` returns an empty
  bundle → `decide_next_action` sends the LLM a zero-length user
  message → vendor 400. Mirrors `smoke_llm_mcp/triggers.jsonl`.

## Run

Bring up a Temporal Server (`temporal server start-dev`), then either:

**Preferred (`jarvis apply`, JAR2-73):**

```sh
DATABASE_URL=postgres://jarvis:jarvis@localhost:5432/jarvis_structural \
ANTHROPIC_API_KEY=sk-... \
cargo run -p jarvis_graph --features "llm-anthropic" --bin jarvis-apply -- \
    examples/smoke_llm_temporal/graph.yaml \
    /tmp/jarvis-smoke-temporal-fs
```

**Legacy (`jarvis-run-workflow`, JAR2-68):**

```sh
ANTHROPIC_API_KEY=sk-... \
cargo run --features "llm-anthropic" --bin jarvis-run-workflow -- \
    examples/smoke_llm_temporal/config.json \
    /tmp/jarvis-smoke-temporal-fs \
    examples/smoke_llm_temporal/triggers.jsonl
```

The binary stamps a fresh timestamped subdirectory under the supplied
parent (`/tmp/jarvis-smoke-temporal-fs`), prints the resolved path on
the first stdout line (`jarvis-run-workflow: fs_root=...`), and exits
when the workflow returns `Retired`. Artifacts land under that path:

- `<root>/outputs/<ulid>.json`
- `<root>/retirement.json`
- `<root>/decisions/<tick>.jsonl`
- `<root>/evidence/<sha>.json`
- `<root>/mandate.json`

See JAR2-68 PR body for the artifact-by-artifact contract.

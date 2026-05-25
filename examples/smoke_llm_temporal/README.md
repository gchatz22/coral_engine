# `smoke_llm_temporal` — JAR2-68 live-vendor smoke fixture

Sibling of `examples/smoke_llm_mcp/`. The temporal worker today only
registers the bootstrap `EchoTool` (MCP-server wiring through env vars
is JAR2-63's flagged follow-up), so this fixture's mandate prompt is
echo-only — diverging from `smoke_llm_mcp/config.json`'s `get-sum`
ask. Kept separate to preserve guardrail 1 of JAR2-68 ("`node-run-llm`
stays untouched").

## Files

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

Bring up a Temporal Server (`temporal server start-dev`), then:

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

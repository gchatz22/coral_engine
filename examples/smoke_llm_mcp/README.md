# smoke_llm_mcp — end-to-end smoke for the LLM-driven Decide path

This fixture proves the **LLM-driven** loop end-to-end on a fresh checkout:
an agent boots with `LlmDecide` over a real `ModelClient` (Anthropic),
the registry auto-registers every tool that
`@modelcontextprotocol/server-everything` advertises, and the model itself
chooses to call `get-sum`, observes the result as an evidence record, and
emits an `Output` that cites that evidence id.

It is the runbook companion to JAR2-37 and closes the parent acceptance
of JAR2-12: *"Smoke fixture exercises a non-trivial decision path: model
is asked, emits `CallTool`, runtime executes, model receives result,
emits `EmitOutput` whose evidence resolves."* The mock-decide variant
(`examples/smoke_mcp/`) still covers the deterministic MCP wiring; this
one adds the live model.

## Prerequisites

* **Node.js + npm** on `$PATH`. Tested with Node 20.x / npm 10.x.
* **Rust toolchain** matching `rust-toolchain.toml`.
* Both the `mcp` and `llm-anthropic` cargo features (the `node-run-llm`
  binary declares `required-features = ["mcp", "llm-anthropic"]`, so a
  plain `cargo build` skips it).
* **`ANTHROPIC_API_KEY`** in the environment. The adapter surfaces a
  `ModelError::Auth` if it is missing; the binary prints it and exits 1.

The MCP server itself is fetched on demand via `npx -y`; no global
install is required for the runbook.

## One-shot command

From the repo root:

```bash
ANTHROPIC_API_KEY=sk-ant-... \
cargo run --features "mcp llm-anthropic" --bin node-run-llm -- \
    --vendor anthropic \
    examples/smoke_llm_mcp/config.json \
    examples/smoke_llm_mcp/triggers.jsonl \
    /tmp/jarvis-smoke-llm-mcp-fs \
    -- npx -y @modelcontextprotocol/server-everything
```

Everything after the inner `--` is the MCP server spawn command. The
binary spawns it as a subprocess, completes the MCP handshake, and
shuts it down when the agent retires.

Optional flags (insert before the positional args):

* `--model <id>` — override the default Anthropic model id. Without
  this the adapter reads `ANTHROPIC_MODEL` and falls back to its
  hardcoded default.
* `--max-tokens N` — sampling cap on the model's reply per tick
  (default 1024).
* `--temperature F` — sampling temperature; omitted from
  `CompleteOptions` when absent.

## What to expect

The agent runs under a `max_ticks = 8` cap. The model is instructed by
the mandate text to:

1. Call `get-sum` with `{"a": 2, "b": 3}` via the `call_tool` decision.
2. Once the tool result arrives, emit an output via `emit_output` whose
   `content` mentions the sum and whose `evidence` array cites the
   evidence id minted by step 1.
3. Retire.

A typical successful run takes 3–5 ticks (1 for the tool call, 1 for
the emit, optionally 1 for an explicit retire); the looser cap is in
place because the LLM may interleave an `idle` or re-read context
before settling on the answer.

On success, stdout includes:

```
node-run-llm: registered 13 MCP tool(s): echo, get-annotated-message, get-env, ...
node-run-llm: vendor=anthropic model=claude-haiku-4-5
node-run-llm: agent retired: <model's reason or max_ticks (8) reached>
node-run-llm: fs tree at /tmp/jarvis-smoke-llm-mcp-fs:
/tmp/jarvis-smoke-llm-mcp-fs
├── claims
├── evidence
│   └── <hex>.json
├── health.json
├── mandate.json
├── notes
├── outputs
│   └── 01<ulid>.json
└── retirement.json
```

Key files in the per-agent FS tree:

* `evidence/<hex>.json` — the `(get-sum, {a:2,b:3}, result)` triple the
  MCP server returned, content-addressed by sha256. There may be more
  than one record if the model retried.
* `outputs/<ulid>.json` — the `EmitOutput` payload, with its
  `evidence: [...]` array pointing at the evidence record(s) above.
* `retirement.json` — terminal marker with the reason the loop ended.
* `health.json` — `"state": "Healthy"` on a clean run.

The `13` tool count is what server-everything advertises today
(`echo`, `get-sum`, `get-tiny-image`, etc.); the count and tool names
will drift if the server's release bumps its tool surface.

## Cleanup

```bash
rm -rf /tmp/jarvis-smoke-llm-mcp-fs
```

The MCP server subprocess is shut down by the binary on retirement;
nothing leaks past the `cargo run` invocation.

## Hermeticity note

`cargo test` (with or without `--features "mcp llm-anthropic"`) does
**not** depend on the npm-installed MCP server *or* on a live Anthropic
key — the default suite stays offline. The optional integration test
under `tests/smoke_llm_mcp_anthropic.rs` is gated behind **both**
`JARVIS_SMOKE_LLM_MCP=1` *and* `ANTHROPIC_API_KEY` being set, and is
skipped with a printed reason otherwise:

```bash
JARVIS_SMOKE_LLM_MCP=1 ANTHROPIC_API_KEY=sk-ant-... \
cargo test --features "mcp llm-anthropic" \
    --test smoke_llm_mcp_anthropic -- --nocapture
```

Without either env var the test prints the skip reason and exits
success.

## Adjacent follow-ups

* **Cohere vendor arm.** `--vendor cohere` is rejected at parse time
  with a hint to rebuild with `--features llm-cohere` and add the
  dispatch arm. Not blocking JAR2-37 per its out-of-scope list.
* **Reliability tuning.** The mandate text is calibrated for the
  default `claude-haiku-4-5` model. If a future ticket targets a
  smaller / cheaper model, expect to widen `max_ticks` or rephrase
  the mandate.

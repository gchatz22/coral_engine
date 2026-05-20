# smoke_llm_mcp — end-to-end smoke for the LLM-driven Decide path

This fixture proves the **LLM-driven** loop end-to-end on a fresh checkout:
an agent boots with `LlmDecide` over a real `ModelClient` (Anthropic **or**
Cohere), the registry auto-registers every tool that
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
* The `mcp` feature plus the vendor feature you intend to use
  (`llm-anthropic`, `llm-cohere`, or both). The `node-run-llm` binary
  drops `required-features`, so a plain `cargo build` now compiles it —
  but every `--vendor` choice errors at runtime in that case, asking you
  to rebuild with the matching `--features`.
* **`ANTHROPIC_API_KEY`** in the environment for `--vendor anthropic`,
  and/or **`COHERE_API_KEY`** for `--vendor cohere`. Each adapter
  surfaces a `ModelError::Auth` if its key is missing; the binary prints
  it and exits 1.

The MCP server itself is fetched on demand via `npx -y`; no global
install is required for the runbook.

## One-shot commands

From the repo root, pick a vendor:

### Anthropic

```bash
ANTHROPIC_API_KEY=sk-ant-... \
cargo run --features "mcp llm-anthropic" --bin node-run-llm -- \
    --vendor anthropic \
    examples/smoke_llm_mcp/config.json \
    examples/smoke_llm_mcp/triggers.jsonl \
    /tmp/jarvis-smoke-llm-mcp-fs \
    -- npx -y @modelcontextprotocol/server-everything
```

### Cohere

```bash
COHERE_API_KEY=... \
cargo run --features "mcp llm-cohere" --bin node-run-llm -- \
    --vendor cohere \
    examples/smoke_llm_mcp/config.json \
    examples/smoke_llm_mcp/triggers.jsonl \
    /tmp/jarvis-smoke-llm-mcp-fs \
    -- npx -y @modelcontextprotocol/server-everything
```

You can also build both vendors into the same binary with
`--features "mcp llm-anthropic llm-cohere"` and pick a vendor at runtime
via `--vendor`.

Everything after the inner `--` is the MCP server spawn command. The
binary spawns it as a subprocess, completes the MCP handshake, and
shuts it down when the agent retires.

Optional flags (insert before the positional args):

* `--model <id>` — override the default model id for the chosen vendor.
  Without this the adapter reads `ANTHROPIC_MODEL` / `COHERE_MODEL` and
  falls back to its hardcoded default.
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

The `<fs_root>` positional you pass on the CLI is treated as a
*parent directory*: each invocation stamps a fresh
`<YYYY-MM-DDTHH-MM-SS-sssZ>` subdirectory inside it and writes the
per-agent FS there. The first line of stdout prints the absolute path
of that resolved subdirectory so you can `cd` into the right run when
inspecting after the fact. Two successive invocations against the
same parent **accumulate** — they do not clobber each other — so
comparing prompt/mandate tweaks across runs is trivial.

On success, stdout includes:

```
node-run-llm: fs_root=/tmp/jarvis-smoke-llm-mcp-fs/2026-05-20T04-30-07-123Z
node-run-llm: registered 13 MCP tool(s): echo, get-annotated-message, get-env, ...
node-run-llm: vendor=anthropic model=claude-haiku-4-5
node-run-llm: agent retired: <model's reason or max_ticks (8) reached>
node-run-llm: fs tree at /tmp/jarvis-smoke-llm-mcp-fs/2026-05-20T04-30-07-123Z:
/tmp/jarvis-smoke-llm-mcp-fs/2026-05-20T04-30-07-123Z
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

(For the Cohere run the `vendor=` line reads `vendor=cohere model=command-...`
instead. The `fs_root=` timestamp is the literal UTC instant the
binary started; expect a different one on each run.)

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

This nukes the *parent* directory and every accumulated
`<timestamp>/` subdirectory inside it. Successive `node-run-llm`
invocations don't overwrite each other — each run gets its own
timestamped subdir under the parent — so the parent can grow large
over an iteration session. Drop the whole tree when you're done, or
delete individual `<timestamp>/` subdirs if you want to keep a subset
for comparison.

The MCP server subprocess is shut down by the binary on retirement;
nothing leaks past the `cargo run` invocation.

## Hermeticity note

`cargo test` (with or without the LLM/MCP features) does **not** depend
on the npm-installed MCP server *or* on a live model key — the default
suite stays offline. The two optional integration tests are gated
independently:

* `tests/smoke_llm_mcp_anthropic.rs` — needs both `JARVIS_SMOKE_LLM_MCP=1`
  and `ANTHROPIC_API_KEY`; compiled only with `mcp` + `llm-anthropic`.
* `tests/smoke_llm_mcp_cohere.rs` — needs both `JARVIS_SMOKE_LLM_MCP=1`
  and `COHERE_API_KEY`; compiled only with `mcp` + `llm-cohere`.

Run them explicitly:

```bash
JARVIS_SMOKE_LLM_MCP=1 ANTHROPIC_API_KEY=sk-ant-... \
cargo test --features "mcp llm-anthropic" \
    --test smoke_llm_mcp_anthropic -- --nocapture

JARVIS_SMOKE_LLM_MCP=1 COHERE_API_KEY=... \
cargo test --features "mcp llm-cohere" \
    --test smoke_llm_mcp_cohere -- --nocapture
```

Without either env var the matching test prints the skip reason and
exits success.

## Adjacent follow-ups

* **Reliability tuning.** The mandate text is calibrated for the default
  `claude-haiku-4-5` / Cohere `command-a-03-2025` models. If a future
  ticket targets a smaller / cheaper model, expect to widen `max_ticks`
  or rephrase the mandate.

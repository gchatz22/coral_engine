# smoke_mcp — end-to-end smoke against `@modelcontextprotocol/server-everything`

This fixture proves the MCP wiring end-to-end on a fresh checkout: an
agent boots, the registry auto-registers every tool the reference MCP
server advertises, the run loop drives a scripted decision that calls
one of those tools, persists an `EvidenceRecord` for the call, and
emits an `Output` that references that evidence by id.

The `MockDecide` script keeps the demo deterministic; the real-LLM
end-to-end smoke lives in `smoke_llm_mcp/` and runs separately.

## Prerequisites

* **Node.js + npm** on `$PATH`. Tested with Node 20.x / npm 10.x.
* **Rust toolchain** matching `rust-toolchain.toml`.
* The `mcp` cargo feature (the `node-run-mcp` binary declares
  `required-features = ["mcp"]`, so plain `cargo build` skips it).

The MCP server itself is fetched on demand via `npx -y`; no global
install is required for the runbook.

## One-shot command

From the repo root:

```bash
cargo run --features mcp --bin node-run-mcp -- \
    examples/smoke_mcp/config.json \
    examples/smoke_mcp/triggers.jsonl \
    /tmp/coral-smoke-mcp-fs \
    -- npx -y @modelcontextprotocol/server-everything
```

Everything after the inner `--` is the MCP server spawn command. The
binary spawns it as a subprocess, completes the MCP handshake, and
shuts it down when the agent retires.

## What to expect

The agent runs three scripted ticks under a `step_cap = 3` cap:

1. **Tick 1** — `CallTool { name: "get-sum", args: {"a": 2, "b": 3} }`.
   Dispatched through `ToolRegistry::call`, which forwards into the
   MCP client and writes an `EvidenceRecord` to disk.
2. **Tick 2** — `EmitOutput` referencing the evidence id from tick 1.
3. **Tick 3** — `Idle`. The mandate's `step_cap` retires the
   agent at the end of this tick.

On success, stdout includes:

```
node-run-mcp: registered 13 MCP tool(s): echo, get-annotated-message, get-env, ...
node-run-mcp: agent retired: step_cap (3) reached
node-run-mcp: fs tree at /tmp/coral-smoke-mcp-fs:
/tmp/coral-smoke-mcp-fs
├── claims
├── evidence
│   └── 10c7854829677f9d39acaafa5aa5ae4e9fa59ae5cba2a1dafc941c4c9f4938de.json
├── health.json
├── mandate.json
├── notes
├── outputs
│   └── 01<ulid>.json
└── retirement.json
```

Key files:

* `evidence/10c78548...json` — the `(get-sum, {a:2,b:3}, result)`
  triple the MCP server returned, content-addressed by sha256.
* `outputs/<ulid>.json` — the `EmitOutput` payload, with its
  `evidence: [...]` array pointing at the evidence record above.
* `retirement.json` — `{"reason": "step_cap (3) reached", ...}`.
* `health.json` — `"state": "Healthy"`.

The `13` tool count is what server-everything advertises today
(`echo`, `get-sum`, `get-tiny-image`, etc.); the count and tool names
will drift if the server's release bumps its tool surface.

## Recomputing the evidence id

`decisions.jsonl` hardcodes the sha256 hex of the
`(tool="get-sum", args={"a":2,"b":3}, result=<CallToolResult>)` triple
that the server returns. If the canonical-JSON encoder in
`src/evidence.rs` ever changes — or if the server's `CallToolResult`
envelope drifts — the hash goes stale and the `EmitOutput` tick will
fail with `evidence <id> not found on disk`.

To recompute:

```bash
# 1. Run the smoke with the placeholder hash; the call_tool tick still
#    succeeds and writes the real evidence file.
# 2. Read the actual id from the filename under `evidence/`.
# 3. Paste it back into decisions.jsonl.
```

Alternatively, capture the `(args, result)` triple and feed it to
`cargo run --bin compute-evidence-id -- get-sum '<args-json>' '<result-json>'`.

## Cleanup

```bash
rm -rf /tmp/coral-smoke-mcp-fs
```

The MCP server subprocess is shut down by the binary on retirement;
nothing leaks past the `cargo run` invocation.

## Hermeticity note

`cargo test` (with or without `--features mcp`) does **not** depend on
the npm-installed MCP server — the default suite stays offline. The
optional integration test under
`tests/smoke_mcp_server_everything.rs` is gated behind
`CORAL_SMOKE_MCP=1` and is skipped by default:

```bash
CORAL_SMOKE_MCP=1 cargo test --features mcp \
    --test smoke_mcp_server_everything -- --nocapture
```

This is the gate the test prints when it is skipped.

## Adjacent follow-ups

* **CI integration.** The `mcp-smoke` and `runbook-smoke` jobs in
  `.github/workflows/ci.yml` pin
  `@modelcontextprotocol/server-everything` via the workflow-level
  `MCP_SERVER_EVERYTHING_VERSION` env var; bumping that version
  requires recomputing the evidence id hardcoded in
  `decisions.jsonl` (see "Recomputing the evidence id" above) and
  updating the `assert_smoke.sh` expectation in the same PR.
* **Stale npm cache permissions.** A long-lived dev environment can
  end up with `~/.npm/_cacache/content-v2/sha512/<xx>/` directories
  owned by `root` (a stray `sudo npm install` years ago), which makes
  `npx -y` fail with `EACCES`. Workaround: install once with a
  user-owned cache, e.g.
  `npm install -g --cache /tmp/coral-npm-cache --prefix /tmp/coral-npm-prefix @modelcontextprotocol/server-everything`,
  then point the spawn command at
  `/tmp/coral-npm-prefix/bin/mcp-server-everything`. Not a Coral
  issue; flagged here so anyone tripping on it knows the fix.

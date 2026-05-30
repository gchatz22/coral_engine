## Post-bootstrap follow-ups — Group A

*Status: planning surface. Ideas surfaced by the JAR2-1 bootstrap (PRs #1–#11) that did not make the cut for the bootstrap itself but are now reasonable next moves. Each entry has motivation, scope, dependencies, sizing, and open questions. This file holds Group A only — items that are independent and shippable anytime — so they can be worked in isolation. Group B (real-scale prerequisites) and Group C (strategic forks) live in `scratch/post_bootstrap_followups_later.md`.*

*Read order: `VISION.md` § 4–5, `scratch/agent_runtime.md`, `scratch/minimal_node_backend.md`, then this.*

---

### Status of the bootstrap

PRs #1–#11 deliver the entire spec from `scratch/minimal_node_backend.md`:

- `coral_node` crate, single-crate, stable Rust 1.84.
- Typed core: `Mandate`, `Trigger`, `Decision`, `Output`, `Evidence{Id,Record}`, `FsOp`, `HumanOp`, `ClaimSeed`, `RetireReason`.
- `AgentFs` with the load-bearing FS schema (`mandate.json`, `outputs/<ulid>.json`, `evidence/<sha256>.json`, `notes/`, `retirement.json`) and the provenance-by-construction invariant (`persist_output` rejects empty/unresolvable evidence).
- `TriggerQueue` with `Human > External > Scheduled` ordering, `Scheduler` stub, `Decide` trait + `MockDecide`, `assemble_context`, `Tool` trait + `ToolRegistry` + `EchoTool`.
- `Agent::run` integrating all of the above, with `tracing` spans, provenance keep-alive, and a `Mandate.max_ticks` cap.
- `node-run` binary + smoke fixture under `examples/smoke/`.
- 71 tests green; smoke runs end-to-end.

Two PRs (#10, #11) are in review at the time of writing; everything below assumes they merge as-is.

---

## Group A — Independent, ship anytime

These do not depend on each other or on any architectural decision still open. Pick whichever has the most pull in your current work.

### A1. Real `Decide` adapter (LLM-backed)

**Why.** `MockDecide` covers tests and the smoke binary. To make the agent do anything useful, `Decide::decide` has to ask a real model. This is the single largest unlock between bootstrap and "does real work."

**Scope.**
- A new `src/decide_llm.rs` (or a sibling crate if we adopt the workspace approach raised in our earlier crate-naming conversation).
- A small `ModelClient` trait — our own thin model-agnostic abstraction — with two implementations from day one: Anthropic and Cohere. VISION §4 ("open kernel, sovereign default") wants vendor-substitutable, and the maintainer wants both vendors live off the bat. Trait surface stays minimal: `complete(messages, tools, options) -> Response`; vendor-specific knobs live behind the impl.
- An `LlmDecide` struct holding model config + a `Box<dyn ModelClient>`.
- Prompt assembly from `ContextBundle`: render the mandate, recent triggers, recent outputs, and recent evidence into a structured prompt; emit a `Decision` via **tool use** (one tool per `Decision` variant, or a single `emit_decision` tool with a tagged-union schema — pick whichever is more reliable in prompt iteration).
- Schema validation: the model's tool-use payload must parse into `Decision`. On parse failure, retry once with a corrective system message; on second failure, return `Err` (which the agent-health policy below treats as an inference-retry exhaustion).
- **Correction-context loop.** When the model emits a `Decision` we cannot satisfy at apply-time (e.g. `CallTool` for an unregistered tool, or `EmitOutput` whose evidence does not resolve), the runtime stages a `CorrectionContext` describing the failure in the agent's `pending_correction` field and continues; the next iteration assembles a `ContextBundle` that carries the correction and gives the model a chance to self-correct. Corrections are agent-internal continuation state, not queue triggers — the trigger queue stays the boundary with the outside world. See `src/agent.rs` module docs for the rationale (an earlier draft routed corrections through a self-injected synthetic `Trigger`; a racing external trigger could reset the per-tick retry budget). This counts toward an inference-retry budget (see agent-health below).
- **Agent health / retry policy.** Both inference failures (parse/transport/rate-limit-after-backoff) and tool-call failures (see A2) feed a per-tick retry budget. When the budget is exhausted on a tick, the agent transitions to an `Unhealthy` state, that tick aborts, and a `health.json` records the failing decision, the retry trail, and the last error. **The run loop does not halt** — the agent stays subscribed to its trigger queue. The next trigger wakes it normally; if that tick completes without exhausting retries, the agent flips back to `Healthy` and the prior `health.json` is archived (kept for audit, e.g. moved to `health/<timestamp>.json`). If it fails again, `health.json` updates and the agent stays `Unhealthy`. `retirement.json` is *not* written by this path — health is orthogonal to retirement. This shape is shared between A1 and A2 and should be a small `health` module, not duplicated.
- Cost + latency accounting per call (rough; real metering is its own ticket).
- A feature flag or build profile to keep `LlmDecide` optional so the test suite stays free of network calls.

**Decided.**
- **Caching: drop from v1.** Anthropic prompt caching and Cohere's caching story are vendor-shaped differently and a model-agnostic abstraction is not worth blocking the ticket on. File a follow-up to revisit once both impls are live and we have real cost data to motivate the abstraction's shape.
- **Default models.** Cost-optimized: `claude-haiku-4-5` for the Anthropic path, `command-a` for the Cohere path. Both flag-overridable. Larger models are an explicit per-mandate opt-in.
- **Test isolation.** Recorded-response fixtures (VCR-style) for CI; a small live-test target gated behind an env var for occasional verification against both vendors.

**Dependencies.** None on the bootstrap. Pulls in a real HTTP client (`reqwest`); SDKs hand-rolled against the trait (avoids dragging in two vendor SDKs with their own dep trees).

**Sizing.** ~2 weeks of focused work for a credible v1 including prompt iteration. Sub-tickets: `ModelClient` trait + Anthropic impl, Cohere impl, prompt template, tool-use `Decision` schema, retry/validation + apply-time correction-context loop, agent-health module (shared with A2), cost accounting hooks, integration tests against recorded fixtures for both vendors.

---

### A2. Real tools via MCP

**Why.** `EchoTool` proves the dispatch path; agents need real data. Per `VISION.md` §4 ("data flows through MCP") the connector ecosystem is MCP, not a parallel framework.

**Scope.**
- Implement `Tool` for an `McpTool` that wraps an MCP server connection and exposes one or more tools (one MCP server can expose multiple).
- Connection lifecycle: spawn the server (stdio or SSE transport), register handshake, route `Tool::call` through `tools/call` JSON-RPC.
- Wire the `ToolRegistry::register` path so an MCP server's advertised tools show up automatically — possibly a `ToolRegistry::register_mcp_server(...)` helper that introspects via `tools/list`.
- Smoke fixture using a trivial MCP server (`@modelcontextprotocol/server-everything` is a good first target).

**Decided.**
- **Topology: one MCP server per agent for now.** `agent_runtime.md` §11.6 flags multiplexing across siblings as a kernel concern; this ticket explicitly punts that. **Follow-up to revisit:** a single shared server pool with per-agent multiplexing once we have multi-agent traffic — file when C2 (parent–child) lands so we have real load to design against.
- **Auth.** Env-var based for the bootstrap; real secret manager is a later ticket.
- **Schema trust.** Trust the MCP server's advertised tool schemas; no in-engine re-validation.
- **Streaming.** Punt to a follow-up. `Tool::call` stays single-shot; if the first server we try requires streaming, that becomes its own ticket rather than expanding this one.
- **Rust MCP client.** Use `rmcp` (the official Rust SDK). Hand-rolled is the fallback only if we hit a blocker during implementation.
- **Process supervision.** When an MCP server crashes mid-call: surface as a tool error and let the retry policy below handle it; do not auto-restart at bootstrap stage.
- **Retry + agent health.** Failed tool calls retry up to a configurable max (start with 3). On exhaustion the call surfaces as a tool error and trips the same agent-health path A1 defines: agent transitions to `Unhealthy`, the tick aborts, `health.json` records the failing call and retry trail. The run loop keeps running; a subsequent successful tick flips the agent back to `Healthy`. The retry+health module is shared between A1 (inference) and A2 (tool calls) — implement it once.

**Dependencies.** None on the bootstrap. Composes with A1 — once an LLM is asking for tools, we want real ones to give it, and the two share the agent-health module.

**Sizing.** ~1 week for the client + registry plumbing + one server smoke. Sub-tickets: MCP client integration, `McpTool` impl, registry registration helper, retry policy wiring (calls into the shared health module), smoke fixture against `server-everything`.

---

### A3. `compute-evidence-id` helper binary

**Why.** The smoke fixture (`examples/smoke/decisions.jsonl`) hardcodes a sha256 hex (`1d6a153a...`). Any change to the canonical-JSON encoding in `src/evidence.rs` silently breaks the fixture's `EmitOutput` arm at runtime — there is no static check that the hash matches the `(tool, args, result)` it claims to summarize. A small CLI to compute the id keeps fixtures honest and is also useful when authoring future fixtures.

**Scope.**
- New binary at `src/bin/compute_evidence_id.rs`.
- CLI: `compute-evidence-id <tool-name> <args-json> <result-json>` → prints the hex `EvidenceId` to stdout.
- Optional `--from-file <decisions.jsonl>` mode that walks a script and prints any `EmitOutput { evidence }` ids alongside the previous `CallTool` they appear to refer to (best-effort; helps audit fixtures).
- One unit test against a known-good triple.

**Dependencies.** None.

**Sizing.** A few hours.

**Open questions.** None worth flagging.

---

## Minor cleanups (no tickets needed; folded into the next nearby change)

- **`node-run` trigger feeder** silently `break`s if the queue receiver is gone. Acceptable for a smoke binary; a real long-lived runner will want a typed shutdown channel.
- **`node-run print_tree`** re-reads each subdirectory eagerly with no `--max-entries` flag. Fine for fixtures; pathological agents would want elision (same family of FS-scale problems as B1 in `post_bootstrap_followups_later.md`).
- **Wrong-arg-count exit** in `node-run` uses `std::process::exit(2)` from inside a sync helper, skipping destructor cleanup of the (not-yet-built) tokio runtime. Fine because no resources held; would graduate to a typed error if the binary grew.

---

## Decision needed before filing issues

1. **Issue shape.** Single issue vs. parent issue with sub-issues vs. Project board. My read: A3 is a single issue; A1 and A2 are parent issues (each has 5+ sub-issues identified in the Sizing sections).
2. **Order within Group A.** Any order is defensible since all three are independent. A3 is the cheapest unblock and prevents fixture rot, so a natural sequence is A3 → A1 / A2 in parallel (they share the agent-health module, so coordinate or land that piece first under whichever ships first).
3. **Crate boundaries.** Do A1 and A2 ship in the same `coral_node` crate, or do they motivate the workspace split we discussed earlier (`coral_node` core + `coral_decide_llm` + `coral_mcp` extensions)? My read: defer the workspace until A1 lands and we feel actual compile-time pain or a real second consumer.

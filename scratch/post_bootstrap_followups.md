## Post-bootstrap follow-ups

*Status: planning surface. Ideas surfaced by the JAR2-1 bootstrap (PRs #1–#11) that did not make the cut for the bootstrap itself but are now reasonable next moves. Each entry has motivation, scope, dependencies, sizing, and open questions. Read in dependency order — Group A items can ship anytime; Group B is a prerequisite for any real-scale agent; Group C are strategic forks that need their own design rounds before tickets.*

*Read order: `VISION.md` § 4–5, `scratch/agent_runtime.md`, `scratch/minimal_node_backend.md`, then this.*

---

### Status of the bootstrap

PRs #1–#11 deliver the entire spec from `scratch/minimal_node_backend.md`:

- `jarvis_node` crate, single-crate, stable Rust 1.84.
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
- An `LlmDecide` struct holding model config + a client.
- Prompt assembly from `ContextBundle`: render the mandate, recent triggers, recent outputs, and recent evidence into a structured prompt; emit a `Decision` via tool-use / structured output.
- Schema validation: the model's output must parse into `Decision`. On parse failure, retry once with a corrective system message; on second failure, return `Err`.
- Cost + latency accounting per call (rough; real metering is its own ticket).
- A new feature flag or build profile to keep `LlmDecide` optional so the test suite stays free of network calls.

**Choice points.**
- **Vendor abstraction.** Direct Anthropic SDK vs. a model-agnostic abstraction (e.g. our own thin layer, or a crate like `genai`). VISION §4 ("open kernel, sovereign default") wants vendor-substitutable. The minimum-viable answer is a small `ModelClient` trait this ticket defines, with one implementation for Anthropic. A second implementation lands when we need open-weight support.
- **Structured output mechanism.** Tool-use vs. JSON mode vs. constrained generation. Tool-use is the most robust for typed enums today on Anthropic.
- **Caching.** Anthropic prompt caching could halve cost for the always-static parts (mandate, system prompt). Worth wiring in v1.

**Dependencies.** None on the bootstrap. Pulls in a real HTTP client (`reqwest`), an SDK (`anthropic-sdk` or hand-rolled), and possibly `tokio-util`.

**Sizing.** ~2 weeks of focused work for a credible v1, including prompt iteration. Sub-tickets: model client trait + Anthropic impl, prompt template, tool-use Decision schema, retry/validation harness, cost accounting hooks, integration test against a recorded fixture.

**Open questions.**
- Which model is the default for the bootstrap mandate ("the smallest correct decision per tick")? Likely `claude-sonnet-4-6` for a mix of cost and reliability; flag-overridable.
- How do we test without burning real API calls? Recorded-response fixtures (VCR-style) for CI; a small live-test target gated behind an env var for occasional verification.
- When the model emits a `Decision` we cannot satisfy (e.g. CallTool for a tool not registered), do we retry, error, or auto-emit a corrective synthetic Trigger?

---

### A2. Real tools via MCP

**Why.** `EchoTool` proves the dispatch path; agents need real data. Per `VISION.md` §4 ("data flows through MCP") the connector ecosystem is MCP, not a parallel framework.

**Scope.**
- Implement `Tool` for an `McpTool` that wraps an MCP server connection and exposes one or more tools (one MCP server can expose multiple).
- Connection lifecycle: spawn the server (stdio or SSE transport), register handshake, route `Tool::call` through `tools/call` JSON-RPC.
- Wire the `ToolRegistry::register` path so an MCP server's advertised tools show up automatically — possibly a `ToolRegistry::register_mcp_server(...)` helper that introspects via `tools/list`.
- Smoke fixture using a trivial MCP server (`@modelcontextprotocol/server-everything` is a good first target).

**Choice points.**
- **Rust MCP client crate.** `rmcp` exists and is the official Rust SDK; check its maturity. Otherwise hand-rolled (small surface).
- **One MCP server per tool vs. one per agent.** `agent_runtime.md` §11.6 flags MCP traffic multiplexing across siblings as a kernel concern; that's a later ticket. For now, one server per agent is fine.
- **Process supervision.** What happens when an MCP server crashes mid-tool-call? Restart vs. surface as a tool error. Bootstrap-grade: surface as error, don't restart.

**Dependencies.** None on the bootstrap. Composes with A1 — once an LLM is asking for tools, we want real ones to give it.

**Sizing.** ~1 week for the client + registry plumbing + one server smoke. Sub-tickets: MCP client integration, `McpTool` impl, registry registration helper, smoke fixture against `server-everything`.

**Open questions.**
- Authentication / secrets for MCP servers that need them (e.g. a Slack or GitHub MCP). Probably env-var based for the bootstrap, real secret manager later.
- Schema validation: do we trust the MCP server's tool schemas, or run them through a validator?
- Streaming tool results — MCP supports them; our `Tool::call` returns once. Punt to a follow-up unless the first server we try requires it.

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

## Group B — Prerequisite for real-scale agents

### B1. Per-directory `index.jsonl` for `outputs/` and `evidence/`

**Why.** `AgentFs::list_recent_outputs` / `list_recent_evidence` (used by `assemble_context` every tick) currently `read_dir` the entire directory, sort all filenames, take the last N. Documented in the rustdoc on `read_recent_json`. At bootstrap scale (M < 100, N = 8) this is microseconds and irrelevant. A long-lived agent with thousands of outputs and tens of thousands of evidence records will spend real wall-time scanning every wakeup.

**Scope.**
- An append-only `outputs/index.jsonl` (and `evidence/index.jsonl`) recording `{filename, created_at}` per write.
- Update the write path: `persist_output` and `record_evidence` append to the index in the same operation that writes the file. Atomicity: write file first, then append to index (so a crash leaves index lagging, never ahead — easier to repair than dangling index entries).
- Update the read path: `list_recent_outputs` / `list_recent_evidence` tail the index instead of `read_dir`.
- Repair / verification path: a `--verify-index` mode (or a separate binary) that walks the directory and reconstructs/checks the index.

**Choice points.**
- **Index format.** JSONL is the obvious match. Sqlite is overkill here; both `outputs/` and `evidence/` are write-once (no updates, no deletes). JSONL append + tail is fine.
- **What to record per entry.** At minimum `{filename}`. Adding `created_at` lets us answer "recent by time" if filename ordering is ever insufficient (today filenames are time-monotonic ULIDs / sha256, so filename-order = creation-order; that may not always hold).
- **Index sync semantics.** Per-write `write + fsync + index-append + fsync` is durable but slow. Per-write `write + index-append` then occasional `fsync` is the realistic default; a verifier reconciles after a crash.

**Dependencies.** None. Self-contained FS-layer change. Composes with future snapshots/forks (C3) since the index is a natural snapshot artifact.

**Sizing.** ~3–4 days. Sub-tickets: index data type + writer, integrate into `persist_output` / `record_evidence`, integrate into list helpers, verifier.

**Open questions.**
- Should `apply_ops` (which writes under `notes/`) also have an index? Probably no — notes are mutable, not append-only, and the loop never lists them.
- Atomicity story across the two writes (file + index) — does fsync matter at bootstrap stage? Probably not; the index is reconstructable from the directory.

---

## Group C — Strategic forks (need design before tickets)

These are big enough that filing tickets without a design round would lock in answers we should debate first. Each warrants its own `scratch/<topic>.md`.

### C1. Mid-tick state durability — Temporal vs. custom substrate

**Why.** Today: a crash mid-tick loses the in-memory `TriggerQueue`. The FS survives because it's on disk, and `agent_runtime.md` §5 names the FS as the source of truth for working memory. But the queue, the scheduler cursor, and any partially-applied decision are not durable. For a continuously running fleet of millions of subagents (`VISION.md` §7), this is unacceptable — process restarts must resume cleanly, not lose in-flight work.

**Two paths, both serious.**

- **Adopt Temporal as the durability substrate** (this is what `scratch/agent_runtime.md` reaches for). Each agent becomes a long-lived Temporal workflow; signals replace mpsc; activities replace direct tool calls. We get durable execution, replayable history, signal/timer composition, and a worker pool for free. The cost: a Temporal cluster (or Temporal Cloud) becomes operational dependency, and the Rust SDK is alpha-grade.
- **Build a custom substrate.** Persist the trigger queue + scheduler cursor + tick metadata to a sidecar `state.json` (or sqlite) in the agent root, snapshot per-tick. On restart, replay from the last snapshot. Lighter operationally, but we own the durability story and the clock-skew, replay, and idempotency edge cases.

**What this design doc would resolve.**
- The forking decision above.
- If Temporal: minimum production-quality version of the Rust SDK, or contribute upstream first?
- If custom: snapshot cadence (per-tick, per-N-ticks, on-decision-boundary), storage layout, replay semantics for activities that are not idempotent (tool calls especially).
- Either way: how does mandate-update / human-override interact with mid-tick state?

**Dependencies.** This is a precondition for C2 (parent–child topology — child handles must be durable) and C3 (snapshots — durability is the substrate snapshots ride on).

**Sizing.** Design doc is ~1 week. Implementation is months either way and worth a Project, not a parent issue.

---

### C2. Parent–child topology

**Why.** Every node currently runs alone. The whole point of the engine (`VISION.md` §3) is graphs of agents reconciling outputs from children. Bootstrap explicitly punted this (`scratch/minimal_node_backend.md` §0 locked B1 — single node, no graph).

**Scope (rough — needs design).**
- Spawn primitive: `SpawnChild { mandate, ... }` decision variant, plus a runtime that creates the child agent's FS, registers a parent–child edge, and starts the child.
- `ChildOutput` trigger variant the parent receives when a child emits an output upward.
- Reconciliation: parent's Decide sees child outputs as part of `ContextBundle`, can emit `ReconcileChildren { ... }` or its own `EmitOutput`.
- Conflict log: when children disagree, parent's reconciliation decision is recorded as a `conflicts/<id>.json` (FS schema extension; flagged in `scratch/minimal_node_backend.md` §6 as deferred).
- Child lifecycle: parent can retire / replace / fork a child. These are kernel primitives, not application-level (`VISION.md` §4: "the human is in the kernel").

**What this design doc would resolve.**
- Decision enum extensions (`SpawnChild`, `ReconcileChildren`, `RetireChild`, `ReplaceChild`).
- Trigger enum extensions (`ChildOutput`, possibly `ChildRetired`).
- FS schema additions (`children/<id>` index, `conflicts/<id>.json`).
- The reconciliation contract — what exactly does the parent's Decide see? Just child output texts, or output texts plus their evidence trails plus their own intermediate state?
- ID scheme — `agent_runtime.md` §3 sketched `{graph_id}/{node_id}`; needs concrete design.

**Dependencies.** **Hard dependency on C1** — child handles must survive parent restart.

**Sizing.** Multi-month. Almost certainly a Linear Project, not a parent issue.

---

### C3. Snapshot / fork of an agent's FS

**Why.** `VISION.md` §5 says the graph layer is "versioned and time-scrubbable. A snapshot is a complete description of a research process — durable, replicable, forkable, and inclusive of every agent's filesystem." That requires snapshotting a single agent's FS as a primitive — copy-on-write or content-addressed.

**Scope (rough — needs design).**
- A `snapshots/` directory or sibling tree per agent that captures the FS state at a point in time.
- Copy-on-write semantics: `outputs/` and `evidence/` are append-only so snapshotting is cheap (just record the current file list); `notes/` is mutable so requires a real copy.
- Fork primitive: produce a new agent root that starts from a snapshot and diverges. Useful for "what if this child had reconciled differently" replay.
- Time-scrubbable read API: given a snapshot id and a path, return the file as it existed at that snapshot.

**Dependencies.**
- B1 (index) makes snapshotting easy — the index *is* most of the snapshot for append-only directories.
- C1 (durability) — snapshots are useful only if the substrate beneath them is durable.

**Sizing.** Real ticket-able once B1 lands. Rough order: design doc, FS-layer extensions, fork primitive, time-scrub reader, integration with the eventual graph layer.

**Open questions.** Does the snapshot include or exclude `retirement.json`? Does forking a retired agent restart it from before retirement?

---

## Minor cleanups (no tickets needed; folded into the next nearby change)

- **`node-run` trigger feeder** silently `break`s if the queue receiver is gone. Acceptable for a smoke binary; a real long-lived runner will want a typed shutdown channel.
- **`node-run print_tree`** re-reads each subdirectory eagerly with no `--max-entries` flag. Fine for fixtures; pathological agents would want elision (same family of FS-scale problems as B1).
- **Wrong-arg-count exit** in `node-run` uses `std::process::exit(2)` from inside a sync helper, skipping destructor cleanup of the (not-yet-built) tokio runtime. Fine because no resources held; would graduate to a typed error if the binary grew.

---

## Decision needed before filing tickets

For each item above, you'll want to decide:
1. **Linear shape.** Single ticket vs. parent issue with sub-issues vs. Project. My read: A3 is a single ticket; A1, A2, B1 are parent issues; C1, C2, C3 are Projects (each preceded by its own scratch doc).
2. **Order.** Group A in any order. Group B before any real-scale demo. Group C requires C1 first, then C2, then C3.
3. **Crate boundaries.** Do A1 and A2 ship in the same `jarvis_node` crate, or do they motivate the workspace split we discussed earlier (`jarvis_node` core + `jarvis_decide_llm` + `jarvis_mcp` extensions)? My read: defer the workspace until A1 lands and we feel actual compile-time pain or a real second consumer.

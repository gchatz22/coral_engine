# Temporal-backed engine — staged execution plan

*Status: ideation, ready for ticket-filing review. Captures the dependency-ordered breakdown of work to take the engine from "single in-process agent with real LLM + MCP" to "production-grade Temporal-backed graph runtime with parent–child topology, structural DB, snapshots, and operator surfaces." Each stage maps to a Linear shape (single issue / parent issue / Project) and carries enough detail to file tickets directly off the relevant section.*

*Read order: `VISION.md` § 4–5, `scratch/agent_runtime.md` (the Temporal-shaped design — now ratified by the durability decision), `scratch/durability_substrate.md` (the substrate fork — Temporal won), `scratch/post_bootstrap_followups_later.md` (B/C groups — this plan supersedes their phasing), `scratch/graph_yaml_schema.md`, `scratch/graph_tui.md`, then this.*

---

## 1. Goal of this plan

Move from today's state — single-agent `Agent::run` running in-process against real LLM/MCP, durable only by FS — to: **a graph of long-lived agents hosted as Temporal workflows, with a structural DB for topology, parent–child reconciliation, operator surfaces, and snapshot/fork primitives.**

The decision to go directly to Temporal (skipping the custom-substrate stage offered in `durability_substrate.md`) is locked. This plan reflects that. The hybrid `DurableHost`-trait framing from the substrate doc still informs the code-reuse strategy below, but we don't ship a custom impl — we ship `AgentCore` (pure, hermetic) wrapped by `AgentWorkflow` (Temporal-hosted) plus a vestigial `Agent::run` that calls the same core, kept around as the hermetic test driver.

---

## 2. Code-reuse strategy (load-bearing for every stage)

Today's `Agent::run` (in `src/agent.rs`) is ~700 lines of "race triggers vs deadline → drain triggers → assemble context → decide → dispatch decision → record evidence → persist output / apply ops / retire / idle → handle correction → maybe transition health." Every line of this logic is independently valuable; what's wrong is only **how the loop is hosted** (in-memory mpsc queue, sync `tokio::select!`, no durability between ticks).

We factor along that seam. The proposed shape:

```
AgentCore — no async runtime, no mpsc, no signal source. Pure of those host
            concerns; NOT pure of FS/tool side effects — dispatch IS the
            in-process applier, called only by Agent::run.
  - drain_triggers(in: Vec<Trigger>, fs, cfg, prior_correction) -> ContextBundle
  - decide(bundle, &Decide impl) -> Result<Decision>
  - dispatch(&AgentFs, &ToolRegistry, &mut Scheduler, decision) -> DispatchOutcome
    where DispatchOutcome ∈ {
      Continue,                                  // loop continues
      NeedsCorrection(String),                   // stage a correction for next tick
      ToolError { failures: Vec<ToolFailure> },  // K-call partial failure (JAR2-38)
      Retired(RetireReason),                     // terminal
    }
    Body calls fs.persist_output / fs.apply_ops / fs.persist_retirement /
    fs.record_evidence / tools.call / scheduler.set_next_after inline; the
    returned DispatchOutcome is the loop's continuation state machine.

AgentWorkflow — Temporal workflow, hosts the loop:
  - signal handlers route Trigger into workflow state
  - workflow body matches on Decision directly (NOT via AgentCore::dispatch):
      wait_condition(triggers_pending, timeout=next_wake)
      → AgentCore::drain_triggers (pure, in-workflow)
      → activity(assemble_context)
      → activity(decide_next_action)
      → match Decision { CallTools => N × activity(execute_tool), EmitOutput =>
        activity(persist_output), RewriteFs => activity(apply_fs_ops),
        Retire => activity(persist_retirement), Idle => update next_wake }
  - continue_as_new on history threshold
  - small typed carryover

Agent::run — in-process loop (existing), refactored to call AgentCore::dispatch.
  - Stays alive for hermetic tests and the existing node-run/node-run-llm smokes.
  - Source of truth for "how the loop behaves" until AgentWorkflow ships; then becomes the test driver.
```

The seam that survives across hosts is **`Decision` (input, shared)** + **`DispatchOutcome`'s continuation variants (shared semantic intent: Continue / NeedsCorrection / ToolError / Retired)**. `AgentCore::dispatch` itself is the in-process loop's implementation of that semantic; the workflow host has its own implementation that maps `Decision` to activities directly and never calls `AgentCore::dispatch`. `drain_triggers` and `decide` are genuinely pure of FS/tool effects and are shared by both hosts; `dispatch` is host-specific code that happens to live in `jarvis_node` so the in-process loop can be a thin wrapper.

The async runtime concerns and the signal source are properties of the host (`AgentWorkflow` or `Agent::run`), not the core. This gives us a clean cutover when we're ready: route real traffic through `AgentWorkflow`, keep `Agent::run` only for `cargo test`.

**Stage 3 is mostly a refactor of `Agent::run` into `AgentCore` (drain + decide + in-process dispatch) + a new `AgentWorkflow` that orchestrates `Decision` via activities.** The size is not "rewrite the agent loop" — it's "factor the agent loop, share what generalises (drain/decide/Decision/continuation semantics), and let each host own its own side-effect orchestration." That's a key piece of context for sizing.

---

## 2.5 Staged execution — fine-grained durability as a first-class principle

A load-bearing principle the rest of the plan obeys. Calling it out at the top of the plan because it dictates the activity boundaries every stage from 3 onward draws.

**The principle.** Every step in the agent loop that is *expensive enough that re-doing it hurts* or *non-idempotent enough that re-doing it is wrong* runs as its own Temporal activity — and therefore has its own durable boundary in workflow history. A failure between two steps leaves the prior step's outcome in history and resumes from the next; the prior step is not re-executed.

**The boundaries.** Per tick of `AgentWorkflow`, the workflow code runs **deterministic orchestration only** and calls activities for everything else. The activity boundaries:

| Step | Activity | Why durable |
|---|---|---|
| Wait for trigger / deadline | (workflow primitive — `wait_condition` + `timer`) | Workflow state, replayable for free |
| Drain triggers (typed, ordered) | (pure, in-workflow) | Deterministic; lives in workflow state |
| Assemble context (read FS) | `assemble_context` | FS read can be expensive at scale + result is reused; durability avoids re-walking on partial-tick replay |
| **LLM synthesis** | `decide_next_action` | **Most expensive step in the loop. Never re-execute unless the call itself failed.** |
| **One tool call** (out of N in `CallTools`) | `execute_tool` (dynamic — one invocation per call) | External non-idempotent side effects, possibly $$$, possibly slow. Each one is its own boundary. Partial parallel batch survives a worker crash. |
| Persist output (with provenance check) | `persist_output` | FS write + provenance enforcement |
| Apply FS ops (`notes/` writes) | `apply_fs_ops` | FS writes; small but distinct boundary |
| Persist retirement | `persist_retirement` | Terminal FS write; workflow exits after |

**What this means concretely.** A tick that:

1. Wakes on a `ScheduledWake`.
2. Reads context (activity 1).
3. Asks the LLM (activity 2).
4. Gets back `CallTools { calls: [A, B, C, D, E] }`.
5. Dispatches A–E as 5 parallel `execute_tool` activities (activities 3–7).
6. Suppose C fails after retries; A, B, D, E succeeded.
7. Workflow code observes the partial outcome, decides to either (a) stage a correction context describing C's failure for next tick's `decide_next_action` to handle, or (b) treat the remaining evidence as sufficient and proceed to the next decision — *without re-running A, B, D, or E*.

If the **worker process itself crashes** anywhere in 1–7, Temporal replays from history: any completed activity's result is already durable; only in-flight activities re-execute. The LLM call from step 3 is *never* re-executed unless the activity itself reported failure.

**What this principle does NOT mean.**

- Not every micro-operation is an activity. Splitting an LLM call into "build messages / send request / parse response" gives us nothing — the failure modes don't decompose. Splitting a tool call into "validate args / send / record evidence" same. The principle is *calibrated*, not maximal.
- Activity overhead is real (workflow history events, worker round-trip latency). The grain we're picking adds tens of milliseconds per step at the wire level. Worth it for durability; not worth pushing finer until profiling tells us otherwise.
- Within an activity, ordinary Rust error handling applies — Temporal's retry policy reruns the *whole* activity on failure, so activity code should be safe to re-execute (idempotent or naturally restartable). Content-addressed evidence makes most of our activity bodies idempotent for free; `persist_output` is idempotent because evidence IDs are deterministic; `execute_tool` is idempotent as long as the underlying tool is (and when it's not, our retry policy is the place to encode that).

**Replayability implication for the workflow code.** Workflow code (the orchestrator inside `AgentWorkflow`) must be **deterministic across replay** — no `std::time::now()`, no random, no file I/O, no network. All non-determinism lives in activities. This is a Temporal hard rule; we obey it by making the workflow code small and the activities do everything else.

This principle shapes how `AgentWorkflow` is structured — it matches on `Decision` directly and routes each variant to its own activity, so each side effect lands inside a durable boundary. `AgentCore::dispatch` is the in-process host's parallel implementation (calls FS/tools inline, no activities, no durability), kept around for hermetic tests and the existing `node-run`/`node-run-llm` smokes. What's shared across hosts is **`Decision`** (the LLM-shaped action description, the same input on both sides) and **the continuation-state semantics** (Continue / NeedsCorrection / ToolError / Retired — surface as `DispatchOutcome` in-process, as workflow-state mutations + signal-queue updates in the workflow host). The earlier framing in § 2 — "dispatch returns a description; the host maps it to activity calls" — was abandoned at implementation because the workflow host doesn't go through `dispatch` at all and a pure-translator dispatch would have no consumers.

---

## 3. Workspace shape

Single crate today (`jarvis_node`); this plan grows the surface enough that splitting into a workspace becomes worth the cost. Proposed:

```
crates/
  jarvis_node       — types, AgentCore, AgentFs, Decide, Tool, Mandate, Trigger, Decision.
                      No Temporal deps. No structural-DB deps. Library only.
  jarvis_temporal   — AgentWorkflow, activities, Temporal worker binary, signal/update API.
                      Depends on jarvis_node + temporalio-sdk.
  jarvis_graph      — Structural DB (sqlite), graph.yaml parser, `jarvis apply` binary.
                      Depends on jarvis_node (types) + sqlx + serde_yaml.
  jarvis_tui        — TUI binary. Depends on jarvis_node (types), reads via FS today;
                      later may depend on jarvis_temporal/jarvis_graph for live signals.
  jarvis_cli        — Operator CLI (jarvis apply / jarvis run / jarvis tui as subcommands).
                      Thin top-level binary aggregating the others. Optional; could
                      stay as separate binaries inside each crate until pressure justifies.
```

Stage 0 stages the workspace move. After that, each subsequent stage lands in the right crate without further restructuring.

---

## 4. Stage table (high level)

| # | Stage | Linear shape | Effort | Depends on | Critical path? |
|---|---|---|---|---|---|
| 0 | Temporal Rust SDK spike + workspace setup | Parent issue (≈4 sub) | ~3–5 days | — | **yes** |
| 1 | Structural DB (Postgres) + topology model | Parent issue (≈5 sub) | ~1 week | 0 | yes |
| 2 | B1 — FS indexing (`outputs/`, `evidence/`) | Parent issue (≈3 sub) | ~3–4 days | — (independent) | no |
| 2.5 | `AgentStorage` trait + `LocalStorage` impl | Parent issue (≈4 sub) | ~3–4 days | — (independent of 1, 2) | **yes** (gates 3) |
| 3 | `AgentCore` factor + `AgentWorkflow` port | **Project** (≈12 sub) | ~6–8 weeks | 0, 1, 2.5 | **yes** |
| 4 | Graph YAML consumption (single-agent first) | Parent issue (≈4 sub) | ~2 weeks | 1, 3 | partial |
| 5 | C2 — parent–child topology | **Project** (≈10 sub) | ~6–8 weeks | 3 | **yes** |
| 6 | Human-in-kernel surfaces (signal/update API) | Parent issue (≈5 sub) | ~3–4 weeks | 3 | no |
| 7 | TUI phase 0–1 (read-only inspector) | Parent issue (≈6 sub) | ~3 weeks | — (phase 0); 3 (phase 1) | no |
| 8 | C3 — snapshots / fork / time-scrub | Parent issue (≈6 sub) | ~3–4 weeks | 2, 3 | no |

**Critical path:** 0 → (1 + 2.5) → 3 → 5. Stages 2, 6, 7, 8 are parallel tracks; stage 4 mostly parallel after 1 + 3. Stage 2.5 can run in parallel with stage 1 (independent surfaces) but must complete before stage 3 starts.

---

## 5. Stage details

### Stage 0 — Temporal Rust SDK spike + workspace setup

**Goal.** Retire the single biggest risk in the plan (Rust SDK production-readiness) before building on it; stage the workspace split so the rest of the work lands cleanly.

**Sub-tickets.**

- **0.1 — Workspace split.** Move `jarvis_node` into `crates/jarvis_node`. Add `crates/jarvis_temporal` stub (just `lib.rs` + `Cargo.toml`). Workspace `Cargo.toml`. CI updates. No logic changes.
- **0.2 — Temporal Rust SDK smoke.** In `jarvis_temporal`: pull `temporalio-sdk` at a pinned version. Write a trivial "hello workflow" that exercises: workflow definition, activity definition, signal handler, durable timer, continue-as-new, child workflow start (abandoned), `wait_condition`. Run against a local Temporal Server (docker-compose). Document **what works, what's missing, what's unexpected** in a sibling note `scratch/temporal_rust_sdk_smoke.md`. If anything is a blocker, this is the moment to know.
- **0.3 — Local Docker dev environment (Temporal + Postgres + worker scaffold).** A `docker-compose.yml` that brings up: Temporal Server + Temporal UI, Postgres for the structural DB (used by stage 1), and a worker container scaffold (Dockerfile for the Rust worker — built but doesn't yet host real workflow code). Volume mount for per-agent FS at `/agent-fs` in the worker container. README section: how to run the stack, how to reset, how to run the worker natively (`cargo run`) against the containerized services for dev iteration. The full architecture (everything-in-containers) is the production shape; the dev-shortcut shape (containers for backing services, worker native) is what we use day-to-day.
- **0.4 — Operational doc draft.** One page: how production Temporal will be deployed (server topology, persistence backend, namespace — one per deployment per § 8.3, workflow-id scheme — `graphs/<graph_id>/agents/<agent_id>` per § 8.2). Postgres deployment story (single instance dev → managed prod). Per-agent FS volume topology (one volume per deployment containing `graphs/<graph_id>/agents/<agent_id>/...`). Not implementation; scoping for stage 3+.

**Out of scope.** Any production code beyond the smoke. The smoke deletes itself when stage 3 starts.

**Acceptance.** A `cargo run --bin temporal-smoke` invokes a workflow that signals itself, times out, continues-as-new, and exits cleanly against a local Temporal Server. Sibling smoke note records findings. CI runs the workflow against an ephemeral Temporal Server container (or, if that's prohibitive, gates the live test on a feature flag).

**Risk being retired.** If the Rust SDK doesn't support one of the primitives `agent_runtime.md` § 4 depends on (especially continue-as-new, signal handlers, dynamic activities, child workflow start with abandon close), we discover here, not after writing the workflow. The cost of finding out three months in is enormous; finding out in 3 days is cheap.

---

### Stage 1 — Structural DB + topology model

**Goal.** A lightweight DB for the "cold start" question: *what graphs exist, what agents are in each, what edges, what tools, what authored mandates?* This is refinement (1) from the substrate-doc discussion made concrete.

**Engine.** **Postgres**, deployed via Docker (per refinement from the comment round). One container in the dev stack, managed Postgres in prod. The earlier "sqlite first" lean is dropped: with everything else operating in Docker anyway, Postgres adds zero operational complexity, gets us a real DB from day one, supports multi-container read access (worker + future API + TUI live-feed), and avoids a migration cost downstream.

**Sub-tickets.**

- **1.1 — Crate stub + dep choice.** `crates/jarvis_graph` stub. Use `sqlx` with the Postgres feature for compile-time-checked queries and async-tokio fit. Pin version. `DATABASE_URL` env-var convention.
- **1.2 — Schema + migrations.** Tables: `graphs`, `agents`, `edges` (parent→child), `tools` (id, kind, command, args, env-refs), `agent_tools` (many-to-many), `mandates_as_authored` (the YAML-authored mandate; agent's *current* mandate stays in `mandate.json` on disk). Use `sqlx::migrate!` against Postgres. Document the schema in a module doc.
- **1.3 — Rust types + serde.** `Graph`, `AgentRecord`, `Edge`, `ToolRecord` structs with serde. Conversion to/from the existing `Mandate` / `Trigger` types where overlap exists.
- **1.4 — CRUD API.** `GraphStore` trait or concrete struct: `create_graph`, `add_agent`, `add_edge`, `register_tool`, `list_agents_in_graph`, `get_agent`, etc. Async, returns `Result`. Tests use `sqlx::test` against an ephemeral test DB (Postgres in the dev stack — CI either runs Postgres as a service container or gates DB tests behind an env var, per § 8 question 5).
- **1.5 — Integration test fixture.** A test that creates a graph with two agents and an edge, queries them back, verifies edge resolution. Covers the cold-start path stage 4 will use.

**Out of scope.** Reading/writing agent FS state from this DB. The DB has structural state only. Outputs/evidence/notes/claims/health stay on disk.

**Acceptance.** `cargo test -p jarvis_graph` exercises create/read/update of all five entities. Schema migration runs cleanly on a fresh DB and on a DB with prior data.

---

### Stage 2 — B1 — FS indexing (`outputs/`, `evidence/`)

**Goal.** Replace `read_dir` + sort scans in `AgentFs::list_recent_outputs` / `list_recent_evidence` with O(1) tail-of-index reads. Already designed in `scratch/post_bootstrap_followups_later.md` B1 and re-stamped in `scratch/context_assembly_v2.md` § 7.

**Sub-tickets.**

- **2.1 — Append-only index format.** `outputs/index.jsonl` and `evidence/index.jsonl`. Each line `{filename, created_at}`. Rust types + writer.
- **2.2 — Write path.** `persist_output` and `record_evidence` write file *first*, then append to index (so a crash leaves the index lagging, never ahead). Tests for crash recovery (simulate truncated index, verify repair).
- **2.3 — Read path + verifier.** `list_recent_outputs(N)` and `list_recent_evidence(N)` tail the index. New `--verify-index` mode on `node-run` (or a small `verify-fs` binary) that walks the directory and reconstructs/checks the index.

**Out of scope.** Indexing `notes/` (mutable, not append-only — no value), `claims/` (mutable status). Per-claim status indexing is a future move.

**Acceptance.** With 10k outputs + 10k evidence records, `list_recent_outputs(8)` returns in <1ms (microbench). Tests for crash recovery (truncated index file → verify regenerates from directory).

**Why this is parallel-trackable.** Touches `jarvis_node` only; no Temporal, no DB. Could ship in the same week as stage 1.

---

### Stage 2.5 — `AgentStorage` trait + `LocalStorage` impl

**Goal.** Make the per-agent FS a pluggable module so the engine can deploy against local disk (today) or remote object storage (production cloud shape) without refactoring downstream. Land the trait + local impl now; the S3 impl is a follow-up stage (~9) that lands when we actually want to deploy against remote storage.

Detailed design lives in `scratch/agent_storage.md`. Headline: every `AgentFs` method today is already object-shaped (put named blob, get named blob, list prefix). Extracting the trait is mostly a refactor and a small async-trait-ification, not a rethink.

**Why now and not later.** Stage 3's `AgentCore` refactor wants to take `&dyn AgentStorage` so hermetic tests use `MemoryStorage` instead of a real tmpdir. If we land the trait in stage 2.5, the AgentCore refactor in stage 3.1 stays small and review-clean. If we bundle, ticket 3.1 grows and the review surface gets noisy. Doing it as its own focused stage is cheaper than retrofitting later (when we'd have to revisit every consumer of `AgentFs`).

**Sub-tickets.**

- **2.5.1 — `AgentStorage` trait + `MemoryStorage`.** Define the trait per `scratch/agent_storage.md` § 5. Implement `MemoryStorage` (HashMap-backed) for unit tests. Trait-level tests against `MemoryStorage` covering put/get/put_if_absent/get_many/delete/list semantics including the `ListPage` cursor.
- **2.5.2 — `LocalStorage` impl.** Port today's `AgentFs` backend (atomic write-then-rename, `O_EXCL` for `put_if_absent`, `read_dir` + sort for `list`) behind the trait. Tests verify byte-identical behavior to the existing code.
- **2.5.3 — `AgentFs` facade refactor.** Refactor `AgentFs` to hold `Arc<dyn AgentStorage>` + a key prefix `<graph_id>/<agent_id>/`. Every existing method translates to one or a few trait calls (see `agent_storage.md` § 2 mapping table). All callers (`Agent`, tests, binaries) keep their existing surface — no upstream changes. The 71+ existing tests stay green.
- **2.5.4 — Tail-index integration.** Replace `read_recent_json`'s directory-scan with the tail-index pattern from `agent_storage.md` § 7. Updates `outputs/_tail.json` and `evidence/_tail.json` (≤ 64 entries) on every write to those prefixes. `list_recent_outputs(N)` / `list_recent_evidence(N)` become 1 GET when N ≤ 64; fall through to the full LIST path otherwise. Coordinates with stage 2 — if stage 2 lands first, the JSONL-append form gets superseded by the tail object as part of 2.5.4.

**Out of scope.**

- `S3Storage` impl — follow-up stage 9 when we want remote storage.
- Per-worker read-through cache — follow-up stage 9 sub-ticket; not needed at single-host scale.
- Object versioning support in the trait — defer to stage 8 when snapshot shape is concrete.
- Streaming / chunked I/O — defer until an MCP tool returns multi-MB results.
- The `jarvis fs` debug CLI — follow-up stage 9 sub-ticket.

**Acceptance.**

- `cargo test -p jarvis_node` passes — every existing FS test stays green.
- New trait-level tests against `MemoryStorage` cover the full surface.
- Adversarial test: pull `AgentFs` apart, mock storage to simulate transient failures, confirm correct error propagation.
- A `recent_outputs` benchmark shows the tail-index path is O(1) regardless of total output count (assert in a microbench).

**Risk.** Touches `AgentFs`, which is consumed by `Agent::run` and every test. The refactor risk is real but bounded — the trait surface is small, and the refactor is mechanical. Plan for one focused PR per sub-ticket; review ticket 2.5.3 (the facade refactor) carefully because every downstream depends on it.

---

### Stage 3 — `AgentCore` factor + `AgentWorkflow` port (the big one)

**Goal.** Refactor today's `Agent::run` into a reusable `AgentCore` and a new `AgentWorkflow` Temporal workflow that hosts it durably. End-state: every agent in production runs as a Temporal workflow; `Agent::run` survives as the in-process test driver.

This is the load-bearing Project. Filing as a **Linear Project** (per CLAUDE.md large-feature shape), not a parent issue, because the sub-ticket count and review surface are both Project-scale.

**Sub-tickets (~12, ordered by dependency).**

- **3.1 — Extract `AgentCore`.** Pull the pure logic out of `Agent::run` into functions on a new `AgentCore` type (or module). Functions are `pub` and take `&mut` references to the (now trait-backed via stage 2.5) `AgentFs`, tools, decide. Keep `Agent::run` alive — it now calls `AgentCore` instead of doing the work inline. *No behavior change.* Tests stay green. **This is the foundational refactor; review it carefully because every following ticket depends on the seam.** Stage 2.5 unblocks this — `AgentCore` takes the FS facade backed by `Arc<dyn AgentStorage>`, which means `MemoryStorage` covers hermetic tests cleanly.
- **3.2 — `AgentWorkflow` skeleton.** In `jarvis_temporal`: define `AgentWorkflow` workflow type, `AgentInput` (cfg, fs_handle, parent_handle, carryover). Workflow IDs use the URL-shaped scheme `graphs/<graph_id>/agents/<agent_id>` per § 8.2. Empty body that just continues-as-new immediately. Worker binary that registers it. Live test: workflow starts, continues-as-new, terminates.
- **3.3 — Signal handlers.** `external_signal(Trigger)`, `human_override(HumanOp)`, `mandate_update(MandatePatch)`, `retire(String)`, `inspect_state()` (update). Signals push onto an in-workflow `Vec<Trigger>`; updates return a snapshot.
- **3.4 — Workflow loop body.** Race `wait_condition(triggers_pending)` against `timer(next_wake)`; drain triggers via `AgentCore::drain_triggers`; call into activities for the actual work. The workflow code is the *orchestrator* — it sequences activities (assemble → decide → dispatch each step) and on a `CallTools` decision spawns one activity invocation per call in parallel via `tokio::join!` of N futures. No tool calls or LLM yet — just the loop shape, exercised against `MockDecide` via a hermetic test. Workflow code is deterministic (no clocks, no I/O, no randomness — per § 2.5); all side effects live in activities. **SDK constraint** (see [`temporal_rust_sdk_smoke.md`](./temporal_rust_sdk_smoke.md) § 2 row 4 and § 3): the "race" and "parallel join" constructs above must use the SDK's deterministic `temporalio_sdk::workflows::select!` (and equivalent join), not `tokio::select!` / `tokio::join!`, or replay determinism breaks silently.
- **3.5 — `assemble_context` activity.** Wraps `AgentCore::assemble_context` (which already exists). Activity is deterministic given `(fs_snapshot_id, triggers, mandate, correction, policy)`. Hermetic-mode: reads FS directly.
- **3.6 — `decide_next_action` activity.** Wraps existing `LlmDecide`. Vendor selection comes from `cfg`. Test against recorded fixtures (existing fixture set transfers over).
- **3.7 — `execute_tool` dynamic activity (one activity *invocation* per tool call).** Dynamic activity registration so any tool name routes to its handler. Wraps existing `ToolRegistry::call`. Handles MCP. **Critical detail (per the staged-execution principle in § 2.5): a single `CallTools { calls: [A, B, C, ...] }` decision dispatches as N separate activity invocations in parallel from the workflow code, NOT as one bulk activity. This means a partial parallel batch (some calls succeed, some fail) leaves the successful evidence in workflow history; only the failures are retried per Temporal retry policy.** Idempotency via content-addressed evidence (already in place); the activity body is safe to re-execute on retry.

  **Open question — re-decide before sizing this sub-ticket.** [`temporal_rust_sdk_smoke.md`](./temporal_rust_sdk_smoke.md) § 2 row 7 (and the surprises in § 3) record that **the Rust SDK does not support dynamic activity registration today**. The `#[activities]` macro is compile-time static; there is no `unknown_activity_handler`. The plan as written above assumes a primitive the SDK doesn't provide. Two shapes are on the table and the choice is deferred:

    1. **Single dispatcher activity** keyed by `(tool_name, args)` that fans out internally to the real tool. One activity, one boundary per `CallTools` member, partial-batch survival still works. Cost: per-tool retry policy collapses to a single shared policy (the dispatcher's), and per-tool activity-level observability blurs into one activity span.
    2. **Wait on / contribute upstream support** for dynamic activities before stage 3.7 lands. Preserves the original shape but introduces an unknown-scope external dependency on the critical path.

    Revisit with full smoke-doc context when stage 3.7 is sized; do not pre-commit here.
- **3.8 — `persist_output` activity.** Provenance check (every cited evidence-id resolves to a file) lives here. Writes to FS. Stage-2 index write happens here if 2 has shipped.
- **3.9 — `apply_fs_ops` activity.** For `RewriteFs` decisions. Writes under `notes/`. Same path validation as today.
- **3.10 — `persist_retirement` + retirement path.** Writes `retirement.json`, workflow exits cleanly (no continue-as-new).
- **3.11 — Continue-as-new.** History-driven (`workflow_info().history_length` or `history_size` threshold). Carryover struct per `agent_runtime.md` § 9 — explicitly *not* conversation history; just trigger queue + scheduler cursor + child handles + last output id + mid-tick evidence (if applicable).
- **3.12 — Replace `node-run-llm` with workflow-driven smoke.** `jarvis run --workflow` (or a new bin in `jarvis_temporal`) spawns an `AgentWorkflow` against the existing `examples/smoke_llm_mcp/config.json` via the Temporal client. Asserts the same end-state (output emitted with provenance, retirement marker) the existing smoke does. The existing `node-run-llm` stays around against the in-process path until we're confident.

**Out of scope.** Parent–child topology (stage 5). External signal API for non-workflow callers — internal Temporal client calls only for stage 3 (stage 6 exposes the API). Snapshot / fork (stage 8).

**Acceptance.**
- All sub-tickets land with their own tests + a structured summary per DEVELOPMENT.md § 4.
- The existing 71+ tests stay green (the `Agent::run` path still works).
- A new integration test boots an `AgentWorkflow` against the smoke config, asserts a provenance-grounded output lands in the FS, asserts continue-as-new doesn't fire on a short run, asserts retirement exits cleanly.
- One live-LLM smoke against Temporal end-to-end (vendor-gated behind env var, same shape as the existing `node-run-llm` live test).

**Why this is a Project, not a parent issue.** 12+ sub-tickets, ~6–8 weeks of focused work, cross-crate (`jarvis_node` for the refactor, `jarvis_temporal` for everything else), review surface that benefits from a project-level progress bar.

---

### Stage 4 — Graph YAML consumption (single-agent first)

**Goal.** Operator authors `graph.yaml`; runtime brings the graph into existence: parses YAML → validates → writes to structural DB → instantiates `AgentWorkflow` per agent.

Stage 4 ships **single-agent only** because parent–child topology (stage 5) hasn't landed yet. The strawman in `scratch/graph_yaml_schema.md` § 2 is the target shape for stage 4; § 3 (multi-agent) waits for stage 5.

**Sub-tickets.**

- **4.1 — `schemars`-derived schema + parser.** `serde_yaml` parses into the Rust types from `jarvis_node` + `jarvis_graph`. JSON Schema is generated from `schemars` derive (per the graph-yaml-schema doc § 4.8). Ship `graph.schema.json` for editor autocomplete.
- **4.2 — Validation pass.** Refs resolve (`tools: [echo]` refers to an existing tool id). Times parse (`100ms`). Required fields present. Validation errors carry source location (line:col) per `serde_yaml` capabilities.
- **4.3 — `jarvis apply` binary.** `jarvis apply graph.yaml` writes the parsed structure to the structural DB. For single-agent graphs in stage 4: also starts the `AgentWorkflow` via Temporal client. (Multi-agent topology lands in stage 5; this binary's behavior extends then, not now.)
- **4.4 — Integration test.** Round-trip: apply a YAML fixture, assert the structural DB has the expected rows, assert the workflow is running, assert an output lands.

**Out of scope.** Multi-agent topology (stage 5). The "missing from YAML → ?" reconciliation question from § 4.6 of the schema doc — lean "warn-and-leave" for v1, defer the `--prune` flag. Dynamic spawn / sidecar (§ 4.7) — defer to stage 5.

**Acceptance.** A `graph.yaml` describing the existing single-agent smoke parses, applies, runs, and emits the same end-state as `node-run-llm`.

---

### Stage 5 — C2 — parent–child topology

**Goal.** Multi-agent graphs work end-to-end: parent spawns child via decision, child runs its own workflow, child outputs flow upward as triggers, parent reconciles.

Filing as a **Linear Project** (Project size, multi-month, multi-cross-crate).

**Sub-tickets (~10).**

- **5.1 — Decision enum extensions.** `SpawnChild { mandate, parent_id }`, `ReconcileChildren { child_output_ids }`, `RetireChild { ref }`, optionally `ReplaceChild { ref, new_mandate }`.
- **5.2 — Trigger enum extensions.** `ChildOutput { child_id, output_id }`, `ChildRetired { child_id, reason }`. Update ordering (Human > External > ChildOutput > Scheduled — or merge ChildOutput into External? open question for the design doc).
- **5.3 — `spawn_child` activity.** In `AgentWorkflow`: when decision is `SpawnChild`, call activity that (a) creates the child's FS root, (b) writes to structural DB, (c) starts the child workflow via `start_child_workflow` with `parent_close_policy=ABANDON`, (d) returns an `AgentRef` (workflow ID + signal channel). Parent's workflow state registers the child ref.
- **5.4 — Child → parent signal path.** Child's `persist_output` activity additionally signals the parent via `signal_external_workflow`, payload = `ChildOutput { child_id, output_id }`. Parent's signal handler routes to trigger queue. Child does **not** block on parent acknowledgment.
- **5.5 — Parent reconciliation.** When parent's `Decide` returns `ReconcileChildren`, run a `reconcile` activity: reads referenced child outputs from disk (FS access across agent roots — well-defined read-only), produces a typed `ReconciliationResult`, parent emits its own `EmitOutput` referencing the child output IDs as part of its evidence (provenance!).
- **5.6 — Conflict log.** New FS schema: `<agent_root>/conflicts/<id>.json`. Written when parent reconciliation explicitly holds disagreement open or chooses a side over a recorded alternative. Captures the decision, the alternatives, and the timestamp. Inspectable.
- **5.7 — Lifecycle ops.** `RetireChild` / `ReplaceChild` activities: signal child to retire (clean exit), or stop child + spawn replacement with new mandate.
- **5.8 — Multi-agent YAML.** Extend `jarvis apply` to walk the hierarchical YAML and spawn the right shape. Tools-by-reference resolution. Defaults inheritance.
- **5.9 — End-to-end integration test.** A fixture with parent + 2 children. Children emit outputs at different times. Parent reconciles, emits its own output. All provenance trails resolve. Conflict log populated when children disagree (use scripted `MockDecide` to force the disagreement).
- **5.10 — Documentation pass.** Module docs in `jarvis_node` and `jarvis_temporal` updated for multi-agent semantics. Update `agent_runtime.md` § 7 to reflect what shipped vs. what was sketched.

**Out of scope.** Snapshot / fork (stage 8). Cross-graph references. Human-as-reconciler override (stage 6's territory once it lands, but the conflict log records mean stage 6 can build the override surface on top of stage 5's primitives).

**Acceptance.** A multi-agent smoke runs end-to-end. Reviewer can read provenance from a parent's output all the way back to a leaf tool call across 2 levels.

---

### Stage 6 — Human-in-kernel surfaces

**Goal.** Wire `HumanOverride { op }` (and friends — mandate-update, dispute-output, inspect-state, retire) from outside the workflow. Today `HumanOverride` exists as a trigger variant with no caller.

**Sub-tickets.**

- **6.1 — External signal API design.** Short scratch sub-doc: HTTP/gRPC choice? Auth surface? Per-workflow addressability? Out of scope for the first cut: cross-tenant isolation, RBAC. Decide before code.
- **6.2 — Signal/update wiring in `AgentWorkflow`.** Already partially in place from stage 3.3; complete the routing.
- **6.3 — CLI commands.** `jarvis signal <agent_id> --human-override '<json>'`, `jarvis inspect <agent_id>`, `jarvis retire <agent_id> --reason '...'`. Use Temporal client directly.
- **6.4 — Dispute path.** `dispute_output(output_id, reason)` as a Temporal update (sync ack). Parent's workflow records dispute in conflict log; trigger queue gets a `Disputed` entry the next loop iteration reconciles.
- **6.5 — Inspect-state read API.** `inspect_state()` returns a typed snapshot (mandate, last_decision, health, recent_output_ids, child_handles). The TUI's `KernelGraphSource` (stage 7 phase 3) will call this.

**Out of scope.** Web UI. Auth beyond a shared-secret env var. Multi-tenant.

**Acceptance.** `jarvis signal --human-override` against a running agent injects a trigger that the agent's next tick sees. `jarvis inspect` returns the snapshot.

---

### Stage 7 — TUI phase 0–1 (read-only inspector)

**Goal.** Replace `cat`/`jq`/`find` workflows during development with a K9s-style TUI that reads the on-disk FS. Already designed in `scratch/graph_tui.md` — this stage delivers phases 0 and 1 of that plan.

**Sub-tickets (~6).**

- **7.1 — `jarvis_tui` crate scaffold.** Ratatui + crossterm + tokio + notify deps.
- **7.2 — `GraphSource` trait + `FsGraphSource`.** Trait as designed in graph_tui.md § 4. FS impl reads under `<graph_root>/`.
- **7.3 — `Agents` + `AgentDetail` screens.** Degenerate one-row tree against single-agent graphs today; ready for multi-agent post-stage-5.
- **7.4 — Drill-down screens.** `Outputs`, `OutputDetail`, `Evidence`, `EvidenceDetail`, `Notes`, `Claims`, `Health`.
- **7.5 — Live tail.** Wire `notify` for FS changes; redraw on event. Polling fallback for environments where inotify is unavailable.
- **7.6 — Phase 1 — `Decisions` screen.** Depends on a decision-log primitive that doesn't exist today. Open question: write `decisions/<tick>.jsonl` in `AgentCore::dispatch`? Becomes a follow-up if it slips this stage.

**Out of scope.** Phase 2 (multi-agent tree view), phase 3 (`KernelGraphSource`), phase 4 (writes). All deferred.

**Acceptance.** Operator can browse a running smoke graph, drill from `Agents` → `AgentDetail` → `Outputs` → `OutputDetail` → `EvidenceDetail` without leaving the TUI. Live changes redraw within 200ms.

**Why this is independent.** Reads the FS schema that already exists. Doesn't touch Temporal, doesn't touch the DB. Could ship before stage 3 in parallel — visible operator value while the kernel work happens behind it.

---

### Stage 8 — C3 — snapshots / fork / time-scrub

**Goal.** Snapshot a per-agent FS at a point in time; fork from a snapshot to produce a divergent agent; time-scrub via a read API. Pre-designed in `scratch/post_bootstrap_followups_later.md` C3.

**Sub-tickets (~6).**

- **8.1 — Snapshot primitive.** Cheap on append-only dirs (`outputs/`, `evidence/`) — snapshot is the current B1 index file. Real copy required for mutable dirs (`notes/`, `claims/`, `mandate.json`, `health.json`).
- **8.2 — Snapshot storage layout.** `<agent_root>/snapshots/<id>/` with copied mutable dirs and a manifest listing append-only file lists at snapshot time.
- **8.3 — Time-scrub read API.** `AgentFs::open_at(snapshot_id) -> ReadOnlyFsHandle` that resolves paths against the snapshot's view.
- **8.4 — Fork primitive.** Produce a new agent root from a snapshot. Mutable dirs copied, append-only dirs hard-linked or copy-on-write where the OS supports it. Forked agent starts fresh in Temporal with a new workflow ID; provenance trail back to source snapshot recorded.
- **8.5 — Workflow-level snapshot tie-in.** Snapshotting an agent should also capture a Temporal-side cursor (workflow ID + history point) so the snapshot is reproducible. Open design question: do we snapshot Temporal history as well, or rely on Temporal's own visibility?
- **8.6 — Integration test.** Snapshot a mid-life agent, fork it, run the fork to a different output, verify both lineages co-exist and provenance trails resolve correctly.

**Out of scope.** Cross-agent snapshots (whole-graph snapshots). UI for snapshot management.

**Acceptance.** Snapshot/fork/scrub work for a single agent end-to-end.

---

## 6. Parallelism map

Visualizing what runs in parallel after stage 0 lands:

```
0 → 1   → 3 → 5 → 8
    2.5 ↗  ├→ 4
            └→ 6
2 (anytime, independent of 0/1/2.5)
7 phase 0 (anytime, independent); 7 phase 1+ (after 3)
```

(1 and 2.5 in parallel, both gate 3. 2 fully independent.)

Realistic concurrency for a single-maintainer + agent-driven workflow:

- **Weeks 1–2:** Stage 0 (sequential). Then stages 1 + 2 + 2.5 + 7-phase-0 in parallel (4 parallel agents) — 1, 2, and 2.5 are independent of each other; 7-phase-0 also independent.
- **Weeks 3–10:** Stage 3 (Project). Stage 7-phase-0 ships during this; stage 4 starts in the back half once stage 3 is far enough along to instantiate workflows.
- **Weeks 11–18:** Stage 5 (Project). Stage 6 and stage 8 start in parallel.
- **Weeks 19+:** Polish, observability, performance.

These are estimates with normal uncertainty. The critical path (0 → 1 → 3 → 5) is the schedule.

---

## 7. Linear shape per stage

Per CLAUDE.md feature-workflow rules:

| Stage | Linear primitive | Reason |
|---|---|---|
| 0 | Parent issue | 4 sub-tickets, one focused week |
| 1 | Parent issue | 5 sub-tickets, one week |
| 2 | Parent issue | 3 sub-tickets, days |
| 2.5 | Parent issue | 4 sub-tickets, days; foundational for stage 3 |
| 3 | **Project** | 12+ sub-tickets, 6–8 weeks, cross-crate |
| 4 | Parent issue | 4 sub-tickets, 2 weeks |
| 5 | **Project** | 10 sub-tickets, 6–8 weeks, cross-crate |
| 6 | Parent issue | 5 sub-tickets, 3–4 weeks |
| 7 | Parent issue | 6 sub-tickets, 3 weeks (phase 0+1) |
| 8 | Parent issue | 6 sub-tickets, 3–4 weeks |

Sub-issues of Projects 3 and 5 that are themselves multi-step (e.g. 3.2 `AgentWorkflow` skeleton has ~3 implicit sub-steps) become parent issues *inside* the Project, per CLAUDE.md's "Projects and parent/sub-issues compose."

---

## 8. Decisions (resolved at plan review)

The seven questions raised during plan review have been resolved. Each decision and its rationale, recorded for the implementation tickets to reference.

1. **Workspace split timing.** Lands in stage 0.1. *Why:* cheap when done early, painful when done late. Costs us one slightly bigger early ticket; saves us a major restructure mid-flight through any later stage.

2. **Workflow ID scheme.** **URL-shaped:** `graphs/<graph_id>/agents/<agent_id>`. *Why:* mirrors what the eventual HTTP API will look like (`GET /api/v1/graphs/<id>/agents/<id>/...`), pluralized REST resource conventions, no leading slash (Temporal IDs don't carry one), and stays flat within a graph — parent-child topology lives in the structural DB, not in the workflow ID, so child IDs don't bloat when graphs deepen and reparenting doesn't rewrite IDs. (`agent_runtime.md` § 3 sketched the simpler `{graph_id}/{node_id}`; this decision supersedes it. The reasoning the simple form gave — operator-readable, deterministic, no separate registry — survives in the URL form.)

3. **Temporal namespace strategy.** One Temporal namespace per deployment. *Why:* simplest operational model; namespace-per-tenant or namespace-per-graph adds management overhead with no v1 benefit. Future multi-tenant deployments can migrate to namespace-per-tenant when load justifies it; the workflow ID scheme above is namespace-independent so the migration is non-breaking.

4. **Vendor / model selection in `cfg`.** Three-layer resolution: structural DB authors the operator's choice (`tools` and `agents` tables carry vendor/model hints), per-agent FS can override (`mandate.json` may carry `model_routing` overrides set by `mandate_update` signals or human overrides), and `cfg.model_routing` carries the resolved choice at workflow start (the result of "what does the DB say + what does the FS override say"). *Why:* keeps the operator authoring story declarative (DB is the source of truth), supports per-agent customization (override layer), and the workflow doesn't re-derive routing on every tick (resolved at start, in-flight changes come via signals).

5. **CI strategy.** Mixed: hermetic-by-default for fast feedback + a lightweight live job exercising Temporal + Postgres end-to-end. Concretely:
   - **Hermetic tests** (every PR, fast): `WorkflowEnvironment` for Temporal workflow tests, `sqlx::test` with ephemeral test DB for structural-DB tests, `MemoryStorage` for storage-layer tests. These run in CI in tens of seconds and cover the bulk of correctness.
   - **Live smoke job** (every PR, slower): GitHub Actions service containers for Postgres + Temporal Server. Runs one end-to-end smoke per relevant stage that exercises the real wire protocols — e.g. spin up a workflow, send a signal, write to Postgres, assert outcomes. Catches "the test framework lies" issues that hermetic tests structurally can't.
   - **Live vendor smoke** (env-gated, manual): occasional verification against real LLM vendors (Anthropic, Cohere) when prompt or schema changes warrant. Not on every PR.
   
   *Why:* hermetic gives fast feedback loops developers actually use; the live smoke gives us a backstop against bugs that only show up against real Temporal/Postgres. Vendor-live stays manual because it's the one job that costs real money per run.

   **SDK constraint (per [`temporal_rust_sdk_smoke.md`](./temporal_rust_sdk_smoke.md) § 2 / § 3):** the Rust SDK does **not** ship a `WorkflowEnvironment` analogous to Python/Go/Java. The "hermetic Temporal workflow tests" bullet above currently has no hermetic mechanism. Until that gap closes, stage 3+ workflow tests are env-gated live tests against a real Temporal Server (the JAR2-42 Docker compose stack or `temporal server start-dev`). Re-decide when stage 3 is sized whether to (a) live with the live-test floor for workflow code, (b) hand-roll a minimal in-process test harness against `temporalio-sdk-core`, or (c) wait on an upstream contribution. The decision shifts the CI shape from "hermetic by default" to "live-by-necessity for workflow tests."

6. **Decision log artifact.** Piggyback onto stage 3.12. *Why:* the workflow is the natural single writer of decisions (in-process `Agent::run` doesn't survive long-term anyway). Writing `decisions/<tick>.jsonl` from `AgentCore::dispatch` means both the workflow and the test-driver `Agent::run` produce it consistently. Spec is small enough (one append per tick of a typed `DecisionLogEntry`) that it doesn't justify its own stage. TUI phase 1 (stage 7.6) reads it.

7. **Stage 4 ordering.** Stage 4 starts after stage 1 with a degenerate `jarvis apply` mode that only writes the structural DB (and prints "would instantiate workflow X" without doing so). The "actually start the workflow via Temporal client" sub-ticket within stage 4 is gated on stage 3 landing. *Why:* the DB-writing part of stage 4 is independently useful (operators can author and validate graphs before the runtime is ready); coupling the whole stage to stage 3 wastes parallelism.

---

## 9. What this plan deliberately does not address

- **Observability + audit at scale.** `VISION.md` § 5 calls for per-claim provenance graphs, per-node calibration, conflict-log replay. We get the *primitives* (provenance enforcement, conflict log) from stages 3 + 5; the *surfaces* (calibration UIs, audit dashboards) are post-stage-8 work.

- **MCP traffic multiplexing across siblings.** Flagged in `agent_runtime.md` § 11.6. Becomes relevant when sibling agents start hammering the same MCP server. Defer until stage 5 produces real cross-agent traffic to design against.

- **Cost accounting + model routing at scale.** Per-agent cost meters land naturally as activities log token counts (already partially in place via `CallStats`); per-graph and per-tenant aggregation is a future move.

- **Sandboxed execution layer.** VISION § 5 ("execution and tool layer") has sandboxed code interpreters / REPLs / browsers as a separate substrate from data fetching. Out of scope here; MCP suffices for the data side.

- **Application API (the layer above the engine).** Stays an MCP-style read API for now; productionizing it (stable versioned contract, REST/gRPC, language-agnostic clients) is a post-stage-8 effort.

These are flagged so we don't pretend the plan covers them; each will get its own design round when it's the next bottleneck.

---

## 10. References

- `VISION.md` — what the engine is.
- `DEVELOPMENT.md` — rules every stage's tickets obey.
- `scratch/agent_runtime.md` — the Temporal-shaped design; stage 3 implements its § 4–9.
- `scratch/durability_substrate.md` — the substrate fork (Temporal won).
- `scratch/post_bootstrap_followups.md` (Group A) — already shipped.
- `scratch/post_bootstrap_followups_later.md` (B/C) — superseded by this plan; B1=stage 2, C1≈stage 3, C2=stage 5, C3=stage 8.
- `scratch/agent_storage.md` — pluggable per-agent FS design; stage 2.5 implements the trait + local impl; future stage 9 implements `S3Storage`.
- `scratch/temporal_rust_sdk_smoke.md` — per-primitive verdict + surprises from JAR2-41's SDK smoke. Stage 3 sub-tickets 3.4 and 3.7 and the § 8 CI-strategy decision reference findings from it; re-read before sizing stage 3.
- `scratch/graph_yaml_schema.md` — stage 4 implements its § 2–3.
- `scratch/graph_tui.md` — stage 7 implements its phase 0–1.
- `scratch/context_assembly_v2.md` — already partially shipped; phase 2+3 may slip in around stage 3.
- `scratch/claim_seed_persistence.md` — shipped.
- Temporal Rust SDK: <https://docs.temporal.io/develop/rust/>, <https://github.com/temporalio/sdk-core/tree/master/crates/sdk>.

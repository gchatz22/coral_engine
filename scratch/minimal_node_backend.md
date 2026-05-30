## Minimal Rust backend for a single node — design

*Status: ideation, scope narrowed and agreed with maintainer. Sibling to `scratch/agent_runtime.md`. This doc specializes that design down to "the smallest Rust thing that runs one node end-to-end and is the seed of the engine." It does not replace `agent_runtime.md` — it specializes some of its open questions and explicitly defers others.*

*Read order: `VISION.md` § 4–5, then `scratch/agent_runtime.md`, then this.*

---

### 0. Scope decisions (locked)

- **Substrate: from-scratch, in-process Rust node.** Pure Rust + tokio. No Temporal yet. Iterate on the contracts (trigger taxonomy, decision enum, evidence shape, FS layout) in plain Rust before committing to a durability substrate. Temporal becomes a later ticket once those contracts stabilize. Until then: crash = lose in-memory trigger queue; the FS survives because it's on disk, and `agent_runtime.md` § 5 already names the FS as the source of truth for working memory.
- **Topology: standalone node, no graph.** One agent. No parent. No children. No spawn primitive — not even at the type level. We will not pay the cost of designing parent/child contracts under "minimal" pressure when `agent_runtime.md` § 11 still has them open. Adding spawn is a clean follow-up once the single-node loop is solid.

These two together define what "minimal" means in this doc. Anything not derivable from them is out of scope (see § 1).

---

### 1. Scope

**In scope.**

- A `node` library crate that exposes one runnable `Agent` primitive.
- A run loop matching `agent_runtime.md` § 4 in shape: race signals against deadlines, drain triggers, decide, act, repeat.
- Typed `Trigger`, `Decision`, `Mandate`, `Output`, `Evidence` with serde.
- A `Decide` trait so the LLM call is mockable; a `MockDecide` for tests.
- A `PerAgentFs` backed by a directory on disk, with content-addressed evidence records.
- A trivial `Scheduler` (`next_deadline = now + cfg.idle_period` unless triggers pending). The kernel-grade scheduler is a separate doc.
- Tool dispatch as an `async fn(name, args) -> Result<EvidenceRecord>` trait. One built-in tool: `echo`, for tests.
- A binary `node-run` that boots one agent against a config file and a JSONL trigger source, for hand-driven smoke tests.
- Tests covering: loop wakes on signal, loop wakes on deadline, decision producing `EmitOutput` writes evidence-linked output to FS, decision producing `Retire` exits cleanly, `EmitOutput` with empty/invalid evidence fails.

**Explicitly out of scope (deferred to follow-up tickets).**

- Real LLM and MCP integration. `Decide` is mockable; a real adapter is its own ticket.
- Parent–child topology, child spawn, `ChildOutput` triggers, reconciliation, conflict log. Not even type stubs.
- Durable execution / crash recovery. Temporal-equivalent guarantees come with the durability ticket.
- Continue-as-new equivalent. Not relevant in-process.
- Cost accounting, model routing, observability beyond `tracing` spans.
- Multi-agent scheduler at scale (`agent_runtime.md` § 11.5).
- MCP traffic multiplexing.
- Human-in-kernel surfaces beyond a single `HumanOverride` trigger variant carrying an opaque op. No UI, no override-resolution semantics.

---

### 2. Crate layout

```
Cargo.toml                 # bin "node-run" + lib "jarvis_node"
src/
  lib.rs                   # re-exports
  agent.rs                 # Agent struct, run loop
  mandate.rs               # Mandate, Output
  trigger.rs               # Trigger enum, TriggerQueue
  decision.rs              # Decision enum, Decide trait, MockDecide
  fs.rs                    # PerAgentFs (directory-backed)
  evidence.rs              # EvidenceRecord, EvidenceId
  scheduler.rs             # tiny scheduler stub
  tools.rs                 # Tool trait + ToolRegistry + echo tool
  bin/node_run.rs          # smoke-test binary
tests/
  loop_smoke.rs            # signal-vs-deadline race, retire, output, evidence enforcement
```

Single crate. No workspace yet. Avoid splitting into sub-crates until we have at least two consumers.

---

### 3. Types (sketch)

```rust
pub struct Mandate {
    pub text: String,
    pub idle_period: Duration,
    pub max_ticks: Option<u64>,
}

pub enum Trigger {
    ScheduledWake,
    External { kind: String, payload: serde_json::Value },
    HumanOverride { op: HumanOp },
}

pub enum Decision {
    CallTool { name: String, args: serde_json::Value, claim_seed: ClaimSeed },
    EmitOutput { content: String, evidence: Vec<EvidenceId> },
    RewriteFs { ops: Vec<FsOp> },
    Idle { next_after: Duration },
    Retire { reason: String },
}

#[async_trait]
pub trait Decide {
    async fn decide(&self, ctx: ContextBundle) -> Result<Decision>;
}

pub struct Agent<D: Decide, T: ToolRegistry> {
    cfg: Mandate,
    fs: PerAgentFs,
    triggers: TriggerQueue,    // mpsc receiver under the hood
    decide: D,
    tools: T,
    scheduler: Scheduler,
}

impl<D: Decide, T: ToolRegistry> Agent<D, T> {
    pub async fn run(self) -> Result<RetireReason> { /* loop in §4 */ }
    pub fn signal(&self) -> SignalSink { /* clone of mpsc sender */ }
}
```

Types are sketches; final shape lands in the implementation tickets. `serde_json::Value` for tool args/payloads keeps the surface tiny in the bootstrap; we can tighten later.

---

### 4. Loop (Rust shape of `agent_runtime.md` § 4)

```rust
loop {
    let next_wake = scheduler.next_deadline();
    tokio::select! {
        _ = triggers.wait_nonempty() => {}
        _ = tokio::time::sleep_until(next_wake) => {
            triggers.push(Trigger::ScheduledWake);
        }
    }

    let drained = triggers.drain_ordered();
    let bundle  = assemble_context(&fs, &drained, &cfg).await?;
    let decision = decide.decide(bundle).await?;

    match decision {
        CallTool { .. }    => { let ev = tools.call(...).await?; fs.record_evidence(ev)?; }
        EmitOutput { content, evidence } => {
            ensure!(!evidence.is_empty(), "provenance: output requires evidence");
            for id in &evidence { fs.evidence_must_exist(id)?; }
            fs.persist_output(&content, &evidence)?;
        }
        RewriteFs { ops }  => fs.apply_ops(ops)?,
        Idle { next_after } => scheduler.set_next_after(next_after),
        Retire { reason }  => { fs.persist_retirement(&reason)?; return Ok(reason); }
    }
}
```

`assemble_context` is a plain async fn for now (not a trait). It reads from FS and packages a `ContextBundle`. When real LLMs land, it becomes a richer activity; for the bootstrap a pure function is enough.

Trigger ordering rule: human > external > scheduled, FIFO within each class.

---

### 5. Per-agent filesystem (bootstrap shape)

The full FS schema is a separate doc (`agent_runtime.md` § 11.4). For the bootstrap:

```
<root>/
  mandate.json                 # current mandate
  outputs/<ulid>.json          # one per EmitOutput; references evidence ids
  evidence/<sha256>.json       # one per tool call result; content-addressed
  notes/                       # free-form scratch (RewriteFs writes here)
  retirement.json              # written on Retire, agent does not start back up
```

Content-addressing evidence by sha256 of the canonical JSON gives us dedup for free and makes the "every claim traces to evidence" invariant cheap to enforce — `EmitOutput` validates that every referenced id resolves to a file.

This is a deliberate concrete decision against `agent_runtime.md` § 11.4 staying open: the bootstrap can't build without a layout, so we pick the smallest one that supports provenance now and revise when the FS doc lands.

---

### 6. What this forces and what it punts on

Mapping to `agent_runtime.md` § 11:

| Open question | This doc forces | This doc punts |
|---|---|---|
| 1. Trigger taxonomy/ordering | Minimal taxonomy (ScheduledWake, External, HumanOverride). Ordering: human > external > scheduled, FIFO within. | ChildOutput, SiblingBatch, MandateUpdate semantics. |
| 2. Continue-as-new carryover | N/A in-process. | Whole question, until durability ticket. |
| 3. Provenance contract | EmitOutput rejects empty/unresolvable evidence; evidence is content-addressed JSON in FS. | Audit reconstruction tooling, dispute propagation. |
| 4. Per-agent FS schema | Concrete bootstrap layout in § 5. | Versioning, snapshots, forks. |
| 5. Scheduler at scale | Stub: per-agent `next_deadline`. | Cross-agent scheduling (separate kernel doc). |
| 6. MCP multiplexing | Tool trait shaped to allow batching later (`name`-keyed dispatch supports adding `call_batch`). | The multiplexer itself. |
| 7. Conflict log | — | Whole question (no parent/child). |
| 8. Cost accounting | `tracing` spans only; no metrics. | Budget enforcement. |

---

### 7. Verification plan

- `cargo build` clean, `cargo clippy -- -D warnings` clean, `cargo fmt --check` clean.
- `cargo test` covers:
  - Loop wakes on injected signal before deadline.
  - Loop wakes on deadline if no signal.
  - `MockDecide` returning `EmitOutput` with valid evidence → file appears in `outputs/`.
  - `EmitOutput` with empty evidence → loop returns provenance error, agent does not exit.
  - `EmitOutput` referencing nonexistent evidence id → same.
  - `Retire` exits cleanly; `retirement.json` written.
  - `RewriteFs` op writes the expected file under `notes/`.
- `node-run` boots an agent against a config + JSONL trigger file and prints the FS state; this is for hand-driven inspection, not asserted in tests.

---

### 8. Proposed GitHub-issue breakdown (after this doc is approved)

Medium feature → parent issue with sub-issues, no Project board. Ordered by dependency:

1. **Bootstrap crate** — `Cargo.toml`, lib + bin, CI lint job. (Trivial; no logic.)
2. **Core types** — `Mandate`, `Trigger`, `Decision`, `Evidence`, `Output`. Serde + tests.
3. **PerAgentFs** — directory-backed, content-addressed evidence, output persistence with provenance check.
4. **TriggerQueue + Scheduler stub** — mpsc-backed queue with the ordering rule from § 4.
5. **Decide trait + MockDecide** — trait surface, mock for tests, `assemble_context` fn.
6. **Tool trait + echo tool** — registry, dynamic dispatch by name.
7. **Agent run loop** — wires everything; this is the integration ticket.
8. **`node-run` binary + smoke fixture** — JSONL trigger source, hand-driven smoke.

Each ticket: rules 1–4 from `DEVELOPMENT.md`, runs in one `cargo test`. Tickets 2–6 are independently reviewable; ticket 7 is the integration; ticket 8 is the demo surface.

---

### 9. Smaller calls still open

Defaults proposed; flag if you want to override before I file tickets.

1. **Crate name** — `jarvis_node`. (Picks the import path forever.)
2. **MSRV / toolchain** — pin stable 1.84.
3. **Async runtime** — tokio.
4. **Error type** — `anyhow` for the bootstrap, switch to `thiserror`-typed errors at module boundaries later.

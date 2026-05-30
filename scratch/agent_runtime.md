# Agent Runtime — High-Level Design (Temporal-based)

*Status: ideation. Not yet decomposed into GitHub issues. Scope: the "Agent runtime" layer from VISION.md § 5 — the primitive that runs a single node in the graph. Adjacent layers (graph layer, per-agent FS, data layer, observability) are referenced where they constrain this layer but are out of scope for this doc.*

---

## 1. Goal of this layer

Run a single node in the research graph as a long-lived, durable, signal-driven process that:

- holds a narrow mandate that may evolve over time,
- wakes on schedule **or** on external signal, drains pending triggers, and decides what to do next,
- calls tools (data fetchers via MCP, sandboxed execution, internal services) as durable activities,
- reads and writes its own per-agent filesystem as working memory across wakeups,
- emits outputs (memos, flags, numbers, recommendations) with provenance attached by construction,
- can spawn children, receive their outputs, reconcile, and emit upward,
- supports first-class human override, mandate change, dispute, and retirement.

This is the "agent" primitive. The graph layer composes them; the kernel schedules them; the data layer feeds them. The runtime is the unit.

---

## 2. The big picture

**Each agent in the graph = one long-lived Temporal workflow.** A graph of N agents is N workflow instances of a single workflow type, connected by parent → child spawn relationships and signal channels. Temporal gives us, for free:

- durable execution: workflow state survives worker crashes, restarts, deploys;
- replayable history for debugging and audit;
- timers and signals that compose cleanly with a "wait for one of: signal, deadline" pattern;
- a worker pool for activity execution, with retries, timeouts, and rate-limiting;
- versioning primitives for evolving workflow logic over time.

What Temporal does **not** give us, that we have to build:

- the per-agent filesystem (durable, versioned, inspectable working memory — VISION § 4);
- provenance contracts (VISION § 4: "every claim traces to evidence");
- multi-agent topology semantics (parent reconciliation, conflict logs);
- the scheduler that decides when each of millions of workflows should next be woken;
- MCP traffic multiplexing across siblings (VISION § 7);
- human-as-kernel-primitive surfaces (override, dispute, re-decompose).

So the agent runtime is **Temporal as durable substrate + a deliberate set of contracts on top**, not a thin wrapper around Temporal.

---

## 3. The workflow contract

**One workflow type, many instances.**

```
AgentWorkflow(input: AgentInput) -> never-returns (continues-as-new indefinitely)

AgentInput {
  cfg:            AgentConfig          # mandate, role, model routing hints, tool whitelist, scheduler hints
  fs_handle:      FsHandle             # pointer to the per-agent filesystem root in external storage
  parent_handle:  Option<AgentRef>     # parent workflow id + signal channel, if this node has a parent
  carryover:      Option<CarryState>   # set on continue-as-new; None on first instantiation
}
```

The mandate is **input + mutable state**, not a workflow-type discriminator. Adding a new "kind" of agent should require **zero** new workflow types — only a new config, new tool bindings, possibly a new prompt template loaded at activity time. This is decisive for versioning: one workflow type is tractable to evolve safely; N workflow types per vertical is not.

A node's identity is its workflow ID. The ID scheme is `{graph_id}/{node_id}` so every running workflow is addressable by the graph layer without a separate registry.

---

## 4. The agent loop

```
loop:
  next_wake = scheduler.next_deadline(state)
  await workflow.wait_condition(state.has_triggers(), timeout=next_wake)

  triggers = state.drain_triggers()                          # typed, ordered
  context  = activity.assemble_context(fs, triggers, cfg)    # see § 6
  decision = activity.decide_next_action(context)            # the LLM step

  match decision:
    CallTool(name, args, claim_seed):
        result = activity.execute_tool(name, args, dedup_key)
        state.record_evidence(claim_seed, result.id)

    SpawnChild(child_cfg):
        ref = activity.spawn_child(child_cfg, parent=self)
        state.register_child(ref)

    EmitOutput(content, evidence_ids):
        out_id = activity.persist_output(content, evidence_ids, fs)
        if cfg.parent: signal_external(cfg.parent, ChildOutput(self.id, out_id))

    RewriteFs(ops):
        activity.apply_fs_ops(fs, ops)

    ReconcileChildren(child_output_ids):
        result = activity.reconcile(child_output_ids, fs)
        state.apply_reconciliation(result)

    Idle(next_hint):
        state.scheduler.set(next_hint)

    Retire(reason):
        activity.persist_retirement(self, reason)
        return                                                # workflow exits cleanly

  if workflow.history_size_or_length_threshold():
      continue_as_new(cfg, fs, state.compacted())             # see § 9
```

Properties of this loop, called out because they matter:

- **One generic workflow shape.** No per-domain specialization at this layer.
- **Race signals against scheduled wakes.** Idle agents cost only a Temporal timer; active agents wake immediately on relevant signal.
- **Triggers are typed and ordered.** Signals push typed triggers onto state; the loop drains them deterministically per tick. (The taxonomy and ordering rules are TBD — see § 11.)
- **Two LLM activities per tick, not one.** `assemble_context` and `decide_next_action` are split deliberately. See § 6.
- **No prompt assembly inside the workflow.** Anything that touches model APIs, MCP, or non-deterministic Python lives in activities. The workflow is pure orchestration.
- **`continue_as_new` is mandatory, not optional**, and triggered by Temporal's history limits, not by turn count.

---

## 5. State: what lives where

The single most load-bearing decision in this design.

| Where | Holds | Lifetime |
|---|---|---|
| Workflow state (in-memory, replayable from history) | `cfg` (current mandate), pending trigger queue, child handle list, scheduler cursor, last output id, fs root pointer, evidence-attribution scratch for the current tick | Tick-to-tick within one workflow run |
| `continue_as_new` carryover | A small, typed, deterministically-rebuildable subset of workflow state — *not* conversation history, *not* scratchpad, *not* tool results | Across continue-as-new boundaries |
| Per-agent filesystem (external durable storage) | All durable working memory: notes, distilled summaries, prior outputs, tool-call evidence records, conflict logs, reasoning traces | The full life of the agent (versioned, snapshotable, forkable) |
| Temporal event history | Every signal received, every activity invoked and its result, every state transition the workflow took | Until `continue_as_new` or workflow exit |

**Scratchpad is not workflow state.** Scratchpad is a view onto the filesystem. The LLM never sees workflow state directly — it sees `ContextBundle`s assembled from the FS by an activity.

This contradicts the reference repo we surveyed (`temporal-community/temporal-ai-agent`), which puts conversation history in workflow state and continues-as-new with a 2-sentence LLM summary. We reject that design wholesale: it loses provenance at every continue-as-new boundary and bloats workflow history during normal operation.

---

## 6. LLM activities

**Two activities per tick, not one:**

```
assemble_context(fs_handle, triggers, mandate) -> ContextBundle
    Reads from FS, applies mandate-specific selection/distillation,
    returns a sized, structured prompt context. Deterministic given inputs +
    FS snapshot. Caching key includes (fs_snapshot_id, trigger_set_hash).

decide_next_action(ContextBundle) -> Decision
    Pure LLM call. Input is the bundle, output is a typed Decision.
    Model routing happens here: cfg's hints + the bundle's complexity
    determine which model is invoked. Uses LiteLLM-style abstraction
    so model vendor is swappable.
```

Why split:

1. **Determinism in the workflow.** Splitting forces all prompt-construction code into activity-land, where it can use `litellm`, `mcp`, `jinja2`, file IO, etc. The workflow module never imports any of those. (The reference repo uses `workflow.unsafe.imports_passed_through()` to dodge the sandbox — we don't have to.)
2. **Caching across siblings.** Two siblings of the same parent often need the same context. Splitting lets us memoize `assemble_context` results outside the workflow.
3. **Model routing.** Different mandates → different models. Different ticks of the *same* mandate may also call for different models (cheap routine wake vs. high-stakes reconciliation). The decide activity owns this routing.

Tool calls are individual activities. We adopt the dynamic-activity dispatch pattern from the reference repo (`@activity.defn(dynamic=True)` + tool name as activity type) so a single registration covers all tools and tool name → handler routing happens at the worker.

---

## 7. Parent–child topology

VISION § 3: parent receives children outputs, reconciles, emits its own output upward. Children spawn grandchildren as needed. The graph fans out to whatever depth and width the work requires.

Stage 5 (Linear Project — `scratch/temporal_staged_plan.md` § 5; sub-tickets JAR2-78..JAR2-87) shipped this. The original sketch in this section is replaced by the section below; the canonical references for what's on disk are the module docs of `crates/coral_node/src/{decision,trigger,conflict}.rs` and `crates/coral_temporal/src/workflow.rs`, plus the Stage 5 Project page in Linear (16 baked-in decisions).

### 7.1 Detached children + the SDK two-step signal chain

Children are detached child workflows + cross-workflow signals; the parent does **not** await child completion. Concretely:

- The parent's `Decision::SpawnChild { agent_name, mandate }` arm (JAR2-78) routes to the `register_child_in_structural_db` activity (JAR2-80, Stage 5.3 — writes the `agents` + `edges` rows, mints the child's `AgentId`), then to the SDK workflow command `ctx.child_workflow(AgentWorkflow::run, child_input, opts)` with `ParentClosePolicy::Abandon` (per Stage 5 Project decision 5). The started child handle is dropped without `.result().await`. Per Stage 5 Project decision 6 the child workflow id is the flat `graphs/<parent_gid>/agents/<child_aid>` form (topology lives in the structural DB's `edges` table, not in the id string).
- The child's `Decision::EmitOutput` arm runs the existing `persist_output` activity (writes the child's own `outputs/<id>.json`), then — if `input.parent_handle.is_some()` — fires a `Trigger::ChildOutput { child_ref, agent_name, output_id }` at the parent via the SDK's two-step external-workflow signal chain (JAR2-81, Stage 5.4):
  ```rust
  ctx.external_workflow(parent.workflow_id.clone(), None)
     .signal(AgentWorkflow::external_signal, trigger)
     .await
  ```
  There is no single `ctx.signal_external_workflow(workflow_id, signal_name, payload)` method in the Rust SDK at v0.4.0 — see `scratch/temporal_rust_sdk_smoke.md` § 3.10 for the API divergence finding. The signal name is bound at compile time via the `#[signal]`-macro-generated marker, so `ParentRef.signal` is informational at v1; the dispatch target is always `AgentWorkflow::external_signal` because the only signal recipient is another `AgentWorkflow` instance.
- Per Stage 5 Project decision 10, signal failures (parent retired, transient server error) are **logged and swallowed**. The child's data is durable on its own FS regardless; the child does not block on parent acknowledgment, and there is no `Trigger::ParentUnreachable` correction path at v1.

### 7.2 Parent ingest path — same handler as everything else

The parent receives `Trigger::ChildOutput` and `Trigger::ChildRetired` through the existing `AgentWorkflow::external_signal` handler (the same one that takes operator-driven `Trigger::External` payloads). There is no dedicated cross-agent signal arm. The handler pushes the typed `Trigger` onto `pending_triggers`; the loop drains it on the next wake and `assemble_context` surfaces it in the `ContextBundle`.

The trigger taxonomy tightened with Stage 5: `TriggerQueue::drain_ordered` enforces `Human > External > ChildOutput/ChildRetired > Scheduled`, FIFO within each class (JAR2-79, Stage 5.2). `ChildOutput` and `ChildRetired` share a priority class because both represent "a child told the parent something actionable"; operator-driven signals (`Human`, `External`) always preempt cross-agent traffic; cross-agent traffic always preempts idle timers.

### 7.3 Reconciliation = synthetic evidence (the load-bearing move)

When the parent's LLM decides to fold N child outputs back into its own context, it emits

```rust
Decision::ReconcileChildren {
    sources:  Vec<ReconcileSource>,         // 1+ child outputs to fold in
    conflict: Option<ConflictRecordIntent>, // Some iff the LLM observed disagreement
}
```

The `reconcile_children` activity (JAR2-82, Stage 5.5) does three things:

1. For each source, open the child's per-agent FS read-only via `AgentFs::open_for_agent(storage, parent_graph_id, child_agent_id)` (a thin `attach` wrapper — no mandate read, no tail reconcile — appropriate for point lookups across agent roots) and `read_output(output_id)` the cited `Output`.
2. Write one **synthetic** `EvidenceRecord` per source into the **parent's** `evidence/` directory. The synthetic record's `tool` discriminator is the fixed string `"reconcile"`; its `args` capture the child's `AgentRef` + cited `OutputId`; its `result` carries the child output verbatim. The record gets a normal content-addressed `EvidenceId` like any other piece of evidence on the parent's FS.
3. If `conflict.is_some()`, write one `ConflictRecord` to the parent's `<agent_root>/conflicts/<id>.json` (per § 7.4 below).

This is the conceptual move the original § 7 sketch did not capture. The parent's next tick picks the synthetic evidence records up through the existing `list_recent_evidence` window in `assemble_context`. The LLM cites them on its next `Decision::EmitOutput { content, evidence }` exactly like any other evidence id. **The existing provenance check in `AgentFs::persist_output` — every cited evidence id must resolve to a file under `<agent_root>/evidence/` — keeps working unchanged.** Cross-agent provenance becomes a normal evidence trail; no new contract, no new workflow-state slot (`staged_reconciliation` or similar), no special-case branch in the prompt renderer. A reviewer reading the parent's emitted output can follow `output → evidence → "this came from <child_agent_id>'s output_id X" → open the child's FS → output → leaf tool call` without ambiguity across two agent FS roots.

Stage 5 Project decision 4 baked in why the variant carries claim summaries inline: the LLM is the only thing with enough context to summarize what a child claimed, so the activity persists what's in the decision (claim text, chosen-alternative index, reasoning) verbatim rather than asking the activity to introspect arbitrary JSON output bodies.

### 7.4 Conflict log primitive

`<agent_root>/conflicts/<id>.json`. Written by the `reconcile_children` activity when `Decision::ReconcileChildren.conflict.is_some()` (JAR2-83, Stage 5.6). Shape on disk:

```text
ConflictRecord {
    id,              // ConflictId — sha256 hex over (alternatives, resolution)
    timestamp,       // when minted; NOT part of the id
    kind,            // HeldOpen | Resolved — derived from resolution.is_some()
    alternatives,    // >= 2 — validated by AgentFs::write_conflict
    resolution,      // None iff HeldOpen
}
```

Content-addressed over `(alternatives, resolution)` only — `timestamp` is excluded for retry idempotency (a retried activity PUTs byte-identical bytes under the same key; `put_if_absent` dedupes cleanly). `kind` is derived from `resolution.is_some()` (single source of truth — the LLM doesn't author it on `ConflictRecordIntent`; the writer derives it). `HeldOpen` is the recorded-disagreement-without-a-winner case (audit primitive for Stage 6's human-as-reconciler override); `Resolved` carries the parent's chosen alternative index + reasoning.

Per Stage 5 Project decision 14, no append-only index: `conflicts/` is bounded (dozens per agent over its lifetime, vs. the millions `outputs/` will see) and `AgentFs::list_conflicts` walks the directory; the cost is fine at v1 scale.

### 7.5 `RetireChild` / `ReplaceChild`

`Decision::RetireChild { child_ref, reason }` (JAR2-78, executed JAR2-84 Stage 5.7) fires `AgentWorkflow::retire` at the child via the same SDK two-step external-workflow chain JAR2-81 uses in reverse. Same best-effort failure semantics (log + continue); the parent drops the child from its `child_handles` workflow-state field regardless of signal outcome (the intent — "this child is gone from the parent's model" — is the load-bearing state mutation).

`Decision::ReplaceChild { child_ref, new_mandate }` is **retire + fresh spawn**, not an in-place mandate swap. The replacement gets a fresh `AgentId` + workflow id + `edges` row; the old `edges` row stays (audit trail; no `retired_at` column at v1). Per Stage 5 Project decision 6, the flat workflow-id scheme means "replace" is structurally retire + spawn from the kernel's point of view — ids do not encode topology so there is nothing to rename. The replacement's deterministic name is `replacement-of-<old_agent_id>` (no DB lookup needed inside the workflow body; the kernel doesn't promise human-meaningful names for runtime spawns).

### 7.6 `ParentClosePolicy::Abandon` matches VISION's framing

Per Stage 5 Project decision 5, every child is spawned with `ParentClosePolicy::Abandon`. Children survive every parent boundary: continue-as-new, worker restart, **parent retirement**. The only kill path is `Decision::RetireChild`. This matches VISION's "continuous, never-ending agents" framing — a parent CAN or restart is not a lifecycle event children should observe; if a parent wants a child gone it has to say so explicitly.

### 7.7 Hermetic-in-process limitation

Per Stage 5 Project decision 11, `AgentCore::dispatch`'s in-process behavior for the 4 new `Decision` variants is `unimplemented!()`. The in-process loop stays single-agent forever; `Agent::run` is the hermetic single-agent test driver and Stage 5 does NOT aim to make it parent-aware. Every multi-agent test in the Stage 5 sub-tickets (5.5, 5.6, 5.7, 5.9) is `TEMPORAL_LIVE_TEST=1` gated against a real Temporal dev server. There is no hermetic in-process multi-agent path. Hermetic-mode coverage of single-agent semantics (`AgentCore` + `MockDecide` + `MemoryStorage`) is unaffected and remains the fast-feedback test loop.

---

## 8. Human-in-the-kernel

Human override is a kernel primitive (VISION § 4), not an application bolt-on. The runtime exposes it as **typed signals and updates** the workflow's signal handlers route to the trigger queue:

| Surface | Type | Purpose |
|---|---|---|
| `human_override(op)` | Signal | Edit FS, replace mandate, force re-decompose |
| `mandate_update(patch)` | Signal | Targeted mandate edit (narrow case of `human_override`) |
| `dispute_output(output_id, reason)` | Update (sync ack) | Flag a prior output as wrong; parent must re-reconcile |
| `retire(reason)` | Signal | End the agent's life cleanly |
| `inspect_state()` | Update (sync) | Read-only snapshot for the UI/audit surface |

Human ops land on the trigger queue alongside other triggers and are drained by the same loop. They're not special-cased into a side channel — they get the same provenance and audit treatment as agent-driven decisions. This matters for the audit story: a human's edit is visible in the same conflict log as a parent's reconciliation.

---

## 9. Long-running discipline

A workflow that lives for months will hit Temporal's history limits (~50k events / 50MB) repeatedly. The runtime continues-as-new proactively, **driven by `workflow.info().history_length` and history bytes, not by turn count.**

The carryover state is small and typed:

```
CarryState {
  fs_root_id, last_compaction_cursor,
  pending_triggers: deque<Trigger>,
  child_handles: Vec<AgentRef>,
  scheduler_state, last_output_id,
  current_tick_evidence: Vec<EvidenceId>,    // only if mid-tick at boundary
}
```

Notably **not** in carryover: prior tool results, conversation history, scratchpad. Those live in the filesystem and survive trivially because the FS is external. After a continue-as-new the new workflow run reads from the same FS root and has all the history it needs, structurally — without an LLM-summary lossy compression step.

The exact carryover schema and continue-as-new cadence are **open** — see § 11.

---

## 10. Provenance (placeholder — open)

VISION § 4: "every claim traces to evidence." The runtime contract that enforces this is not yet drafted. The shape is: `persist_output` activity takes typed `evidence_ids`, refuses to persist with empty or unresolvable ones, and the FS holds the canonical evidence records (one per `execute_tool` invocation).

This is the next thing to drill on. See § 11.

---

## 11. Open questions / not yet decided

These ripple back into the runtime and need their own design rounds:

1. ~~**Trigger taxonomy and ordering.**~~ **LOCKED for the parent-child cross-agent variants** (Stage 5 — JAR2-79, Stage 5.2). The variants are `Scheduled` / `External { kind, payload }` / `HumanOverride { op }` / `ChildOutput { child_ref, agent_name, output_id }` / `ChildRetired { child_ref, agent_name, reason }`. Drain order: `Human > External > ChildOutput/ChildRetired > Scheduled` (FIFO within each class). `ChildOutput` and `ChildRetired` share a priority class. Sibling-batch / dispute-specific trigger types remain open for their respective stages; the mandate-update mid-tick question is still open and lives under Stage 6.
2. **Continue-as-new carryover schema.** Exact bytes carried, exact threshold logic, behavior when continue-as-new fires mid-decision. (Stage 3.11 / JAR2-67 shipped the v1 schema — load-bearing closure of this question for the single-agent path is on disk in `crates/coral_temporal/src/workflow.rs::Carryover`; the threshold logic uses `ctx.continue_as_new_suggested()` per the SDK gotcha called out there. The "mid-decision CAN" sub-question remains open for the long-running smoke that has the wall-clock budget to observe it.)
3. **Provenance contract.** What `evidence_ids` look like, where evidence records live (FS layout), what the auditor's reconstruction path is, how disputes propagate. (Single-agent provenance shipped via `AgentFs::persist_output`'s evidence-presence check; **cross-agent provenance** is now also closed by Stage 5's synthetic-evidence pattern — see § 7.3 above. Disputes specifically remain Stage 6 territory.)
4. **Per-agent filesystem schema.** Out of scope for the runtime layer per se, but the runtime's activity contracts depend on it. Needs a sibling design doc.
5. **Scheduler at scale.** "Millions of subagents continuously" (VISION § 7) is not solved by Temporal timers alone. The scheduler is a separate kernel concern that decides which workflows wake when. Needs its own doc.
6. **MCP traffic multiplexing.** Sibling batching of MCP calls is cross-workflow and lives outside the agent runtime — but the `execute_tool` activity contract has to be shaped so multiplexing can be added without rewriting agents.
7. ~~**Conflict log.**~~ **LOCKED at the v1 shape** (Stage 5 — JAR2-83, Stage 5.6). `<agent_root>/conflicts/<id>.json` where `id` is `sha256_hex((alternatives, resolution))`. `kind` ∈ `{HeldOpen, Resolved}` is derived from `resolution.is_some()`; `timestamp` is excluded from the content-address hash for retry idempotency. One file per disagreement, no append-only index at v1 (`conflicts/` is bounded; the `AgentFs::list_conflicts` directory scan is the only reader). See § 7.4 for the rationale. **Human-as-reconciler override** — the surface that *reads* this log to let a human break ties or revisit a held-open conflict — stays open and is Stage 6 territory; the conflict-log records *are* the primitive Stage 6 will build that override against.
8. **Cost accounting.** Per-agent, per-graph, per-tenant. Where are the hooks?

---

## 12. References

- `VISION.md` — overall product/architecture vision; § 4 (principles) and § 5 (the engine, concretely) are most relevant.
- `temporal-community/temporal-ai-agent` — surveyed as a reference for Temporal mechanics. **Adopt:** generic workflow type, signal-driven loop, dynamic tool activity, LiteLLM, MCP client manager. **Reject:** all-state-in-workflow, continue-as-new with LLM summary, prompt assembly inside the workflow, validation as extra LLM call, single-tenant workflow IDs, `change_goal` as LLM-emitted tool result.

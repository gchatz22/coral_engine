# Agent Runtime — High-Level Design (Temporal-based)

*Status: ideation. Not yet decomposed into Linear tickets. Scope: the "Agent runtime" layer from VISION.md § 5 — the primitive that runs a single node in the graph. Adjacent layers (graph layer, per-agent FS, data layer, observability) are referenced where they constrain this layer but are out of scope for this doc.*

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

**Children are detached child workflows + signal channels, not awaited child workflows.**

- Parent spawns child via `start_child_workflow(AgentWorkflow, child_cfg, parent_close_policy=ABANDON)`.
- Parent does **not** block on child. Parent's loop continues; it sees child output as a `ChildOutput` trigger when the child signals it.
- Child's `cfg.parent` carries a typed `AgentRef` it uses to `signal_external_workflow` when it has output to deliver.
- Parent's reconciliation is a `Decision` variant (`ReconcileChildren`) the LLM emits when its triggers indicate enough children have spoken.

Why detached over awaited: an awaited model would force the parent to block until the child completes, which is wrong shape for "continuous, never-ending" agents that may run for months. Detached + signals matches VISION's mental model: agents don't end, they idle.

Lifecycle ops on children (retire, fork, replace) are kernel-level operations the parent agent can request via dedicated activities (`retire_child(ref)`, etc.). Temporal's parent-close semantics aren't doing this work for us.

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

1. **Trigger taxonomy and ordering.** What types of triggers exist (external event, child output, human override, mandate update, scheduled wake, sibling batch invitation, ...), what are the priority/ordering rules, what happens if a mandate-update arrives mid-tick?
2. **Continue-as-new carryover schema.** Exact bytes carried, exact threshold logic, behavior when continue-as-new fires mid-decision.
3. **Provenance contract.** What `evidence_ids` look like, where evidence records live (FS layout), what the auditor's reconstruction path is, how disputes propagate.
4. **Per-agent filesystem schema.** Out of scope for the runtime layer per se, but the runtime's activity contracts depend on it. Needs a sibling design doc.
5. **Scheduler at scale.** "Millions of subagents continuously" (VISION § 7) is not solved by Temporal timers alone. The scheduler is a separate kernel concern that decides which workflows wake when. Needs its own doc.
6. **MCP traffic multiplexing.** Sibling batching of MCP calls is cross-workflow and lives outside the agent runtime — but the `execute_tool` activity contract has to be shaped so multiplexing can be added without rewriting agents.
7. **Conflict log.** When a parent reconciles disagreeing children and "holds the disagreement open" (VISION § 4), where does that record live and how is it structured?
8. **Cost accounting.** Per-agent, per-graph, per-tenant. Where are the hooks?

---

## 12. References

- `VISION.md` — overall product/architecture vision; § 4 (principles) and § 5 (the engine, concretely) are most relevant.
- `temporal-community/temporal-ai-agent` — surveyed as a reference for Temporal mechanics. **Adopt:** generic workflow type, signal-driven loop, dynamic tool activity, LiteLLM, MCP client manager. **Reject:** all-state-in-workflow, continue-as-new with LLM summary, prompt assembly inside the workflow, validation as extra LLM call, single-tenant workflow IDs, `change_goal` as LLM-emitted tool result.

## Post-bootstrap follow-ups — deferred (B & C)

*Status: planning surface, deferred. Originally part of `scratch/post_bootstrap_followups.md`; split out so Group A can be worked in isolation. Group B is a prerequisite for any real-scale agent; Group C are strategic forks that need their own design rounds before tickets. Pick this back up once Group A is in flight or shipped.*

*Read order: `VISION.md` § 4–5, `scratch/agent_runtime.md`, `scratch/minimal_node_backend.md`, `scratch/post_bootstrap_followups.md` (Group A), then this.*

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

## Decision needed before filing tickets

For each item above, you'll want to decide:
1. **Linear shape.** Single ticket vs. parent issue with sub-issues vs. Project. My read: B1 is a parent issue; C1, C2, C3 are Projects (each preceded by its own scratch doc).
2. **Order.** Group B before any real-scale demo. Group C requires C1 first, then C2, then C3.

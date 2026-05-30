# Graph TUI — K9s-style terminal inspector for a Coral graph

*Status: ideation. Sketch of an inspector / navigator TUI for the Coral graph,
modeled on K9s' navigation idiom. Read order: `VISION.md` §3–5,
`scratch/agent_runtime.md`, `scratch/graph_yaml_schema.md`, then this. Not yet
decomposed into GitHub issues.*

---

## 0. Goal

Give an operator a fast, keyboard-driven terminal UI that **inspects a running
(or retired) Coral graph** the way `k9s` inspects a Kubernetes cluster: list
the agents, drill into one, see its live state, scroll its filesystem, read
the evidence behind any claim, replay decisions over time, watch health and
cost. One process; one keyboard; everything an operator wants to know about a
graph one or two keys away.

The deeper bet: **state-as-files** (`VISION.md` § 4) means the graph is
already inspectable without inventing a kernel API. The TUI is mostly a
viewer over `<graph_root>/`. That makes a useful v1 reachable today even
though the runtime is still single-agent.

---

## 1. Scope

**In scope (v1 — read-only inspection over the existing FS schema):**

- Read the on-disk state under a graph root and render it as navigable views.
- Tail running agents live (file-watch the FS, redraw on change).
- Drill from an agent list → an agent's detail → its outputs → the evidence
  behind a single output → the raw tool-call record.
- Inspect notes, claims, recent triggers, recent decisions (once a decision
  log exists), health state, and per-tick model-call stats (`CallStats` /
  `TickTotals` from JAR2-33).
- Work against a single retired graph from disk (post-mortem mode), the same
  binary, the same keys.

**Out of scope (deferred, called out so future-us doesn't re-litigate):**

- **Write operations.** Inject a trigger, edit a mandate, force-retire an
  agent, dispute an output. `Trigger::HumanOverride { op: HumanOp }` exists in
  the enum but is not routed anywhere yet (`scratch/agent_runtime.md` § 8
  flags this as the human-in-the-kernel surface). Writes wait for that wiring
  and a kernel-side admin API; see § 11 phasing.
- **Topological graph rendering.** K9s is tabular; the user asked for "K9s
  style," not a node-link graph view. A 2-D visualization of edges is a
  different doc and a different rendering problem (`taffy` / Sugiyama layout
  / `tui-graph`-grade work). The TUI here uses indentation + a tree widget,
  not a canvas.
- **Cross-graph dashboards.** One graph at a time. Multi-graph orchestration
  (a "fleet view" across thousands of graphs) is its own product surface.
- **Authoring.** Editing `graph.yaml` (`scratch/graph_yaml_schema.md`) is the
  job of `$EDITOR`, not the TUI. Possibly an "open in `$EDITOR`" shortcut from
  a node detail view, but that's all.
- **Audit charts / calibration metrics.** VISION § 5 calls for per-node
  calibration and per-claim provenance graphs. Worth its own viewer; not
  this one.

---

## 2. The K9s analogy, in 30 seconds

K9s exposes a Kubernetes cluster through a single TUI built around four
ideas:

1. **Resource lists** as the primary surface — pods, deployments, services,
   etc. — each rendered as a sortable, filterable table.
2. **Modal command palette** (`:`) to jump between resource kinds; vim-style
   keys (`/` filter, `j/k` move, `Enter` drill, `Esc` back) inside each list.
3. **Drill-down detail** views with describe / logs / shell / events stacked
   behind keys (`d`, `l`, `s`, `e`).
4. **Live tailing.** Whatever you're looking at re-renders as the world
   changes. A pod that crashes flips red without the operator hitting refresh.

We adopt all four. We change *what* a "resource" is (agent, output,
evidence, claim, …) and *how* we tail (file-watch + future kernel
subscription, not a single watch API).

---

## 3. Mapping K9s primitives to Coral primitives

| K9s concept       | Coral equivalent                          | Notes |
|-------------------|--------------------------------------------|-------|
| Cluster           | Graph (root path on disk)                  | One graph per TUI session (v1) |
| Namespace         | Subtree / parent agent                     | Filter scope inside one graph |
| Pod               | Agent (a long-lived node)                  | Crate is `coral_node`; "node" already means "graph node = agent" — avoid the k8s "node = host" overload. We use "agent" consistently |
| `describe pod`    | Agent detail view                          | Mandate, status, last tick, last decision, child handles |
| `logs`            | Decision timeline + tick spans             | Once a per-tick log exists; bootstrap renders what's in `tracing` plus the FS-derived history |
| `events`          | Trigger queue history                      | Per-agent, ordered |
| `exec` / shell    | "Open notes in `$EDITOR`" + future writes  | v1: read-only. Writes phase 2 |
| Resource kinds    | Agent · Output · Evidence · Claim · Note · Trigger · Decision · Health · CallStats |
| Watch API         | `notify` FS watcher (v1); kernel subscribe (later) | See § 4 |

The set of resource kinds is closed and small. Each kind has one list view
and one detail view; navigation between them is the entire UX.

---

## 4. The data plane (load-bearing)

**This is the section to get right.** Everything else falls out of it.

Today the only way to read agent state is the on-disk FS schema
(`src/fs.rs` doc comment):

```
<root>/
  mandate.json
  outputs/<ulid>.json
  evidence/<sha256>.json
  notes/<...>
  claims/<slug>.json
  health.json
  health/<ts>.json
  retirement.json
```

Tomorrow the kernel will likely expose a typed read/subscribe API (a "graph
read-side" service, akin to k8s' watch endpoint). The TUI should not pick a
side; it should hide both behind one trait and let the implementation evolve.

**Strawman:**

```rust
trait GraphSource {
    fn list_agents(&self) -> Result<Vec<AgentSummary>>;
    fn agent(&self, id: AgentId) -> Result<AgentSnapshot>;
    fn outputs(&self, id: AgentId, window: Window) -> Result<Vec<Output>>;
    fn evidence(&self, id: AgentId, ev: EvidenceId) -> Result<EvidenceRecord>;
    // ... one method per resource kind
    fn subscribe(&self, scope: Scope) -> impl Stream<Item = Event>;
}
```

Two implementations to start:

- **`FsGraphSource`** — reads the directory tree. `subscribe` wraps the
  `notify` crate to fire `Event::AgentChanged(id)` / `Event::OutputAppended`
  events on file changes. Polling fallback every N seconds for filesystems
  where `notify` is unreliable (network shares, some CI sandboxes).
- **`KernelGraphSource`** — RPC into the future kernel admin endpoint. Same
  trait, lower latency, scales past where FS-walking stops being free.

The TUI talks only to the trait. Switching is a config flag.

**Why this matters for the rest of the design:**

- **Latency floor.** FS polling gives best-case ~100 ms. Good enough for an
  operator skimming, not good enough for "render every tick of a hot agent."
  `notify` is faster but loses events on bursts. The kernel-backed source is
  the only one that holds up at full agent speed.
- **Scale ceiling.** Walking a directory of 10 k outputs is microseconds;
  walking 1 M is not. The trait has to allow pagination (`Window`) and
  indexed lookup, not assume "load everything."
- **Time scrubbing.** `VISION.md` § 5 says the graph layer is
  "time-scrubbable." `FsGraphSource` can only see *current* contents (the FS
  bootstrap has no snapshots — see `src/fs.rs` § "What this layout
  deliberately does not include yet"). The kernel-backed source is what
  unlocks scrubbing later. The TUI's time-cursor primitive (§ 6) is designed
  for both ("on FS source, the cursor is pinned to live").

---

## 5. Scale — the K9s shape doesn't survive a million agents

K9s is comfortable around 1 k pods, sluggish past 10 k. `VISION.md` § 7
targets *millions of subagents continuously*. A flat resource list is the
wrong primitive at that scale; the doc owes an honest answer.

The stance for this TUI:

- **The default scope is one graph**, not the whole estate. Even at a million
  agents per estate, a single research graph is typically tens to thousands
  of agents. K9s-shape works fine there.
- **Default view of a graph is the tree**, not a flat list. Subtree-rooted
  navigation. Expand-collapse like a file explorer. A graph with 10 k agents
  collapses to ~10 visible rows at the root level.
- **Filter is first-class.** `/` opens a fuzzy filter over the *expanded*
  rows; `\` toggles "search the whole subtree." `:agents` (the command
  palette) flips to a flat virtualized list with the same filter.
- **Lists are virtualized.** Render only what's on screen. The
  `GraphSource::subscribe` events let us patch one row instead of redrawing
  the whole list.
- **Aggregation rolls up the tree.** A subtree row shows summary stats
  (number of children, max depth, % unhealthy, total cost-to-date) so the
  operator can spot hotspots without expanding everything. K9s-style top
  bars but per node.

What we are *not* trying to do: render a million agents at 60 fps. The point
is to make the *operator's* navigation O(log N) in graph size, not the
render. If you want to look at one specific agent in a graph of a million,
the command palette (`:agent <id>`) takes you there directly without ever
materializing the surrounding list.

---

## 6. View catalog (the "screens")

Each screen is a `Widget` that owns a `Scope` (which agent / subtree / time
range it shows). Navigation is mostly "open another screen with a tighter
scope."

| Screen           | Shows                                                         | From → To navigation |
|------------------|---------------------------------------------------------------|----------------------|
| `Agents`         | Tree of agents in the current graph; status + last tick + idle countdown | `Enter` → `AgentDetail` |
| `AgentDetail`    | Mandate, retirement, health, last decision, child count       | `o` outputs, `e` evidence, `n` notes, `c` claims, `t` triggers, `h` health, `s` stats |
| `Outputs`        | List of outputs for an agent (newest first, paged)            | `Enter` → `OutputDetail` |
| `OutputDetail`   | One output: content + linked evidence list                    | `Enter` on an evidence row → `EvidenceDetail` |
| `Evidence`       | Recent evidence records for an agent                          | `Enter` → `EvidenceDetail` |
| `EvidenceDetail` | Raw tool-call record (tool, args, result, ts, id)             | `r` recompute hash to verify provenance |
| `Notes`          | File browser over `<agent>/notes/`                            | `Enter` → `NoteView` |
| `NoteView`       | Read-only file viewer with syntax-aware rendering for `.md`/`.json` | `o` open in `$EDITOR` (read-only flag in v1) |
| `Claims`         | List of `claims/<slug>.json`                                  | `Enter` → `ClaimDetail`; `o` show outputs that share this claim |
| `Triggers`       | Trigger history for the agent (drained queue, last N)         | typed badge per kind (Scheduled / External / HumanOverride) |
| `Decisions`      | Per-tick timeline: `CallTool`/`EmitOutput`/`RewriteFs`/`Idle`/`Retire` | needs a decision-log primitive; today reconstruct from FS deltas (outputs appearing, claims minted, retirement marker) |
| `Health`         | Current state (Healthy/Unhealthy) + archive of past incidents | `Enter` on an archive entry → incident detail |
| `Stats`          | Last-tick `CallStats` per call + `TickTotals`                 | aggregates across recent ticks |

The right-hand column should make clear: every screen except `Decisions` is
implementable today against the existing FS schema. `Decisions` waits on a
log primitive (post-bootstrap follow-up).

**Cross-cutting affordances on every screen:**

- A status bar at the bottom: graph name · scope · live/scrubbing · keybinds.
- A time cursor: `[`/`]` to step backwards/forwards through events (today
  this only moves over the discrete events the source emits; with the
  kernel source it scrubs continuously).
- A `?` overlay with keybindings.

---

## 7. Strawman screen sketches

### `Agents` (default landing screen)

```
┌ coral · graph: fda-monitor · live ────────────────────────────────────┐
│ NAME                       STATE     LAST TICK  IDLE   OUTPUTS  CHLD   │
│ ▾ root                     Healthy   2s ago     4h     12       3      │
│   ▾ drug-alpha             Healthy   12s ago    1h     34       0      │
│     · drug-alpha           Healthy   12s ago    1h     34       0      │
│   · drug-beta              Unhealthy 3m ago     1h     8        0      │
│   ▾ competitive-landscape  Healthy   45s ago    12h    2        2      │
│     · competitor-a         Healthy   45s ago    1h     5        0      │
│     · competitor-b         Healthy   1m ago     1h     6        0      │
│                                                                        │
│ /filter  :command  Enter open  Esc back  q quit          ?  help       │
└────────────────────────────────────────────────────────────────────────┘
```

### `AgentDetail`

```
┌ agent: drug-alpha · Healthy · since 2026-05-19T08:14Z ─────────────────┐
│ Mandate:                                                               │
│   "Watch the FDA docket for Drug Alpha. Surface any docket event       │
│    within 24h with a structured summary referencing the primary        │
│    filing."                                                            │
│   idle_period: 1h  ·  max_ticks: ∞                                     │
│   retry_policy: default (3 / 50ms)                                     │
│                                                                        │
│ Last tick:                                                             │
│   2s ago · trigger: ScheduledWake · decision: Idle(next_after=1h)      │
│   2 calls · 1.4k in / 380 out tok · 920 ms                             │
│                                                                        │
│ Counters:                                                              │
│   outputs: 34  ·  evidence: 121  ·  notes: 8  ·  claims: 5             │
│                                                                        │
│ o outputs  e evidence  n notes  c claims  t triggers  h health  s stats│
└────────────────────────────────────────────────────────────────────────┘
```

### `OutputDetail` (the provenance drill-down)

```
┌ output 01HXY8Z…GQR · 2026-05-19T10:02Z ────────────────────────────────┐
│ "Phase 2 readout for Drug Alpha published 2026-05-19; meets primary    │
│  endpoint. See evidence below."                                        │
│                                                                        │
│ Evidence:                                                              │
│   1. 1d6a153a… · echo(args={...}) → result                             │
│   2. 5fab21e8… · fda-feed.fetch(docket=...) → ...                      │
│                                                                        │
│ Enter inspect evidence  o open in editor  Esc back                     │
└────────────────────────────────────────────────────────────────────────┘
```

These sketches deliberately use the existing on-disk field names (ULIDs for
outputs, sha256 for evidence, `idle_period`/`max_ticks` for mandate) so we
know the renderer maps onto types that already exist.

---

## 8. Navigation model

- **Vim keys** in lists: `j/k` row, `gg/G` top/bottom, `Ctrl-d/u` page.
- **Command palette** (`:`) switches resource kinds globally: `:agents`,
  `:outputs root`, `:evidence drug-beta`, `:claims`, `:health drug-beta`.
- **Search** (`/`) is a fuzzy filter over the current list.
- **Back stack** (`Esc`): every drill pushes onto a stack; `Esc` pops.
  Mirrors K9s exactly. No menus, no breadcrumbs needed if the stack works.
- **Live indicator** in the status bar shows whether the view is tailing or
  scrubbed.
- **`:graph <path>`** loads a different graph root (post-mortem view of a
  retired graph from disk).
- **`y`** yanks the current view's primary identifier (ULID, sha256, agent
  id) to the clipboard for pasting into other tools.

---

## 9. Tech choices

- **`ratatui` + `crossterm`** — the de facto Rust TUI stack. Mature, well
  documented, big enough widget ecosystem (`tui-tree-widget`,
  `tui-textarea`, `tui-input`) that we don't have to roll our own.
- **`notify`** for FS watching, with a polling fallback (`PollWatcher`)
  for environments where inotify/kqueue isn't available.
- **`tokio`** for async I/O and the event loop. `tokio::select!` over
  (`crossterm::EventStream`, `notify` events, `GraphSource::subscribe`,
  periodic redraw timer).
- **`serde_json`** + existing types from `coral_node`. The TUI links the
  library crate; no duplicate schema.
- **`tui-logger`** behind a flag for live `tracing` output, useful while
  developing.

The TUI is a separate binary that depends on the `coral_node` library
crate. No new public API beyond what `GraphSource` requires; everything
else is already `pub` on the existing types.

---

## 10. Implementation shape

Three layers:

```
+----------------------------+
|       UI screens           |   ratatui widgets, navigation state
+----------------------------+
|     Cache + event loop     |   in-memory snapshot, diff-on-event,
|                            |   debounce, virtualization
+----------------------------+
|       GraphSource          |   trait; FsGraphSource (v1),
|                            |   KernelGraphSource (later)
+----------------------------+
```

Single Tokio task drives the loop:

```rust
loop {
    tokio::select! {
        ev = input.next_event() => screen.handle_key(ev),
        ev = source_events.next() => cache.apply(ev),
        _ = redraw_tick.tick() => terminal.draw(|f| screen.render(f, &cache)),
    }
}
```

State is the cache (a snapshot of what's currently loaded) plus the
navigation stack. Both are owned by the event loop; screens are pure
renderers over `&Cache`.

---

## 11. Phasing

| Phase | Depends on                              | Scope |
|-------|------------------------------------------|-------|
| 0     | None (works today)                       | `FsGraphSource` over a single-agent root. `Agents` (degenerate, one row), `AgentDetail`, `Outputs`, `OutputDetail`, `Evidence`, `EvidenceDetail`, `Notes`, `Claims`, `Health`, `Stats`. Live tailing via `notify`. |
| 1     | Decision log primitive (post-bootstrap)  | `Decisions` screen + trigger-history retention |
| 2     | Parent–child topology (`scratch/post_bootstrap_followups.md` Group C2) | Tree view in `Agents`; subtree filters; child-output flows in `AgentDetail` |
| 3     | Kernel read API                          | `KernelGraphSource`; pagination; time-scrub cursor on real history |
| 4     | Human-as-kernel-primitive writes (`scratch/agent_runtime.md` § 8) | `r` retire, `m` mandate-edit, `i` inject trigger, `d` dispute output. Confirm-modal pattern, dry-run / `--allow-write` flag |
| 5     | Multi-graph fleet view                   | Out of this doc; sibling spec |

Phase 0 is the smallest valuable thing: against a *single-agent* graph (the
only thing the runtime supports today) the TUI already replaces a dozen
`cat`/`jq`/`find` calls during development. That alone is worth the build.

---

## 12. Design decisions worth resolving before code

Each is a real fork. Surface here so the build doesn't pick by accident.

### 12.1. One binary or a subcommand?

```
# Option A — subcommand on the existing CLI
coral tui <graph-root>

# Option B — separate binary (k9s-style)
jr <graph-root>
```

(A) keeps the surface area concentrated; one binary to install. (B) is
shorter to type, gives the tool an identity, decouples the release cadence.

**Lean:** start with (A). Move to (B) only if usage shows operators want
the tool independent of the rest of the CLI.

### 12.2. Live by default or scrubbed by default?

If the operator opens an active graph: do we start tailing or do we pin to
"now" and require an explicit `[`/`]` to move? Tailing-by-default matches
K9s. Pinning matches forensic tooling.

**Lean:** tailing for running graphs, pinned-to-retired for graphs whose
`retirement.json` is present. Detected at open.

### 12.3. Cache freshness model

Two reasonable answers:

- **Pull-on-demand.** Cache only what's on screen + a small look-ahead.
  Re-read on view switch. Cheap on memory, slower on navigation.
- **Eager full-graph cache.** Load everything for the current graph at
  start, patch on events. Snappy nav, expensive on large graphs.

**Lean:** pull-on-demand with an LRU per resource kind. Eager cache is
fine for the single-agent bootstrap (it's tiny) but doesn't scale to
phase 2 (multi-agent) without rework — better to bake the right shape in
day one.

### 12.4. How do we render unhealthy agents?

K9s flips pods red on failure. We have a finer distinction: `Unhealthy`
in the agent sense (`health.json`) is not the same as crashed (process
gone) or retired (`retirement.json` present).

**Lean:** three glyphs/colors in the agent list: green Healthy, yellow
Unhealthy (running but failing), gray Retired. Crashes inferred from
"process not running and no retirement.json" — only meaningful once we
have a process supervisor; flag as `?` until then.

### 12.5. Trigger injection — modal confirm or freeform JSON?

When phase 4 lands, the operator wants to inject a `HumanOverride { op:
... }`. The op payload is opaque JSON today. Options:

- **Freeform JSON editor** (open `$EDITOR` with a JSON template).
- **Structured form** (typed fields once the op shape is real).

**Lean:** freeform JSON until the override surface has a real schema;
then auto-generate the form from `schemars`. Same pattern as graph YAML.

### 12.6. Multi-graph in one session

Tabs across graphs, or one graph per process? Tabs match k9s context
switching. One graph per process is simpler and lets the operator put two
graphs side by side using their terminal multiplexer (tmux, kitty splits,
zellij).

**Lean:** one graph per process. Punt tabs to phase 5.

---

## 13. Open questions

1. **Decision log shape.** Phase 1 needs a per-tick log somewhere on
   disk (a `decisions/<tick>.json` directory, or `decisions.jsonl`).
   Where it lives, how it interacts with `continue_as_new` (when that
   exists), and what its retention story is — all open.
2. **Cost / accounting view.** `Stats` shows `CallStats` and
   `TickTotals` today. The bigger picture — per-graph dollar cost,
   budgeted spend, per-mandate cost-to-output ratio — needs a cost
   accounting primitive that doesn't exist yet (`scratch/agent_runtime.md`
   § 11). Worth a separate screen once it does.
3. **Cross-process safety.** The TUI opens an agent's FS read-only.
   Concurrent writers (the running agent) write atomically per-file but
   the directory walk is not consistent. Acceptable for an inspector;
   noted so we don't claim more than we have.
4. **Theming.** Defer until phase 0 stabilizes. Probably a TOML at
   `~/.config/coral/tui.toml`; copy K9s' shape.
5. **Remote graphs.** Phase 3's kernel source would let the TUI inspect
   graphs running on another machine. Auth / transport / TLS belong with
   the kernel API spec, not here.
6. **A graph viewer.** Out of scope but inevitable. The tree view in
   `Agents` is enough until somebody actually wants to *see* the edges.
   That tool will be its own doc — likely a web UI rather than another
   TUI mode.
7. **Naming.** "Coral TUI" / `coral tui` / `jr` — see § 12.1. Worth
   picking before code lands, not after.

---

## 14. What this doc is not

- Not a finished spec. The strawman is one stake; the design decisions in
  § 12 will move it.
- Not a justification for building any of this *now*. Phase 0 is cheap and
  pays back during the rest of the bootstrap (replaces `cat`/`jq`/`find`
  loops); phase 2 onward depends on parent–child topology landing.
- Not the only operator surface we'll need. A future web UI will share the
  same `GraphSource` trait. The TUI is the local-first, low-friction
  surface; the web UI is the remote / multi-user surface. Both viewers,
  one read model.

---

## 15. Next concrete step

If this lands the right way, the smallest valuable ticket is **phase 0 §11
row 1**: a single new binary that opens a graph root, watches the FS, and
renders `Agents` (degenerate, one row) + `AgentDetail` + `Outputs` +
`OutputDetail` + `Evidence`. Five screens, the `GraphSource` trait with one
FS-backed impl, vim-style navigation, `notify`-driven redraw. Enough to
replace what we currently do with `cat`/`jq`. Everything past that waits on
real runtime evolution.

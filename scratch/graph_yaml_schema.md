## Graph-as-YAML — strawman schema

*Status: ideation. Strawman for a declarative YAML format that expresses an entire research graph — agents, mandates, tools, and parent–child edges — as a single artifact. Sibling to `scratch/post_bootstrap_followups.md` (Group C2 — parent/child topology). The format only matters once we have the runtime to consume it; this doc gets the design right before the runtime catches up.*

*Read order: `VISION.md` § 3–5, `scratch/agent_runtime.md`, `scratch/minimal_node_backend.md`, then this.*

---

### 0. Goal

A single YAML file that an operator can write, review, fork, and check into git, expressing **the structure and intent of a research graph**. The runtime reads it and brings the graph into existence.

Concretely, the YAML answers:

- What agents exist, what is each one's mandate, how often does it wake?
- What tools (echo, MCP servers, sandboxed runtimes) is each agent allowed to use?
- What is the parent–child shape of the graph?
- Where does the graph start (initial triggers / seeds)?
- What human-authored constraints does the operator impose (cost ceilings, retirement conditions, escalation rules)?

This is the "the graph is the program" framing from `VISION.md` § 4 made concrete.

---

### 1. Scope

**In scope (the YAML expresses):**

- Initial graph topology — agents and edges.
- Per-agent configuration — mandate text, idle period, max ticks, tool whitelist.
- Tool registrations — named, referenced by agents.
- Initial seed triggers — what kicks the graph off.
- Operator constraints (rough first cut) — cost budgets, retirement conditions, conflict-resolution policy hints.

**Out of scope (lives elsewhere):**

- **Runtime state.** Outputs, evidence, notes, retirement markers — all in the per-agent FS. The YAML is structural, not stateful. Same wall as Kubernetes manifests vs. cluster state.
- **Runtime mutations.** Mandate edits by human override, dynamic spawns from agent decisions, retirements driven by `Decide`. These happen via the runtime API and may diverge from the YAML until a human reconciles. Same model as Terraform: file is the source of truth, but the live system can drift.
- **Per-agent secrets.** API keys, credentials. Reference by env-var name in the YAML; the runtime resolves at instantiation.
- **Code.** Agents don't ship code. The YAML wires together fixed primitives (Decide adapters, Tool implementations); writing new behaviour means a new tool or a new Decide adapter, both Rust changes.

---

### 2. Strawman schema (single agent)

The smoke fixture (`examples/smoke/`) is the smallest interesting case — one agent, one tool, scripted decisions, one trigger. Today it's three loose files (`config.json`, `decisions.jsonl`, `triggers.jsonl`); the YAML collapses to one:

```yaml
apiVersion: jarvis.engine/v1alpha1
kind: Graph
metadata:
  name: smoke
  description: |
    Smoke fixture for the bootstrap node-run binary.
    Wakes on a kickoff trigger, calls echo, persists provenance,
    retires via max_ticks.

tools:
  - id: echo
    kind: builtin
    builtin: echo

agents:
  - id: root
    mandate:
      text: "smoke test"
      idle_period: 100ms
      max_ticks: 3
    tools: [echo]

# Scripted decisions live under `seed.scripted_decisions` and are only
# meaningful with a MockDecide-backed runtime — they vanish the moment
# a real Decide adapter is wired (Group A1 in scratch/post_bootstrap_followups.md).
seed:
  triggers:
    - agent: root
      at: start
      external:
        kind: kickoff
        payload: {}
  scripted_decisions:
    root:
      - call_tool:
          name: echo
          args: { hello: smoke }
          claim_seed: seed-1
      - emit_output:
          content: "smoke test passed"
          evidence:
            - "1d6a153a69d110156ca44ed281f859ca09d9875747e3ed16b9964c52632fd96e"
      - idle:
          next_after: 50ms
```

Compared to the current three-file form, this gives one source of truth. Worth doing as part of the real-`Decide` ticket where we're reshaping `node-run` anyway.

---

### 3. Strawman schema (multi-agent graph)

The same format scales to a graph. Hierarchical nesting under `children:` keeps shallow graphs readable; we may need a flat form with explicit `parent:` references later for machine-generated graphs.

```yaml
apiVersion: jarvis.engine/v1alpha1
kind: Graph
metadata:
  name: fda-monitor
  description: "Continuous watch on FDA decisions for biotech X"

defaults:
  # Applied to every agent unless overridden inline.
  idle_period: 1h
  max_ticks: null  # run until Retire

tools:
  - id: web-search
    kind: mcp
    mcp:
      command: "mcp-web-search"
      env:
        SEARCH_API_KEY:
          from_env: WEB_SEARCH_KEY
  - id: fda-feed
    kind: mcp
    mcp:
      command: "mcp-fda"
      args: ["--rate-limit", "10/min"]
  - id: fs-read
    kind: mcp
    mcp:
      command: "mcp-filesystem"
      args: ["--allowed-dirs", "/data/fda"]

agents:
  - id: root
    mandate:
      text: |
        Monitor the FDA for material decisions affecting biotech X.
        Decompose by drug pipeline; reconcile children's outputs into
        a current memo on regulatory state of play.
      idle_period: 4h
    tools: [web-search, fs-read]
    children:

      - id: drug-alpha
        mandate:
          text: |
            Watch the FDA docket for Drug Alpha. Surface any docket
            event within 24h with a structured summary referencing the
            primary filing.
        tools: [fda-feed, web-search]

      - id: drug-beta
        mandate:
          text: |
            Watch the FDA docket for Drug Beta. Same shape as the
            Alpha watcher.
        tools: [fda-feed, web-search]

      - id: competitive-landscape
        mandate:
          text: |
            Track competing biotechs' FDA filings affecting X's
            commercial outlook. Reconcile findings from sub-watchers.
          idle_period: 12h
        tools: [web-search]
        children:
          - id: competitor-a
            mandate:
              text: "Watch Competitor A's FDA filings."
            tools: [fda-feed]
          - id: competitor-b
            mandate:
              text: "Watch Competitor B's FDA filings."
            tools: [fda-feed]

seed:
  triggers:
    - agent: root
      at: start
      external:
        kind: kickoff
        payload: {}

# Optional operator-level constraints. Exact shape TBD.
policy:
  cost_budget:
    daily_usd: 50
    per_agent_daily_usd: 5
  on_budget_exhausted: pause   # pause | retire | escalate
  conflict_resolution:
    default: parent_decides     # parent_decides | hold_open | escalate_human
```

A few things this surfaces:

- **`defaults:`** keeps boilerplate down; per-agent `idle_period` overrides.
- **Tools defined once, referenced by id.** Avoids duplicate MCP server spec across siblings.
- **Hierarchical children** — readable when the graph is shallow. Past 3-4 levels we'd want a flat form too.
- **`policy:`** is a placeholder; the real shape lands when cost accounting (Group A follow-up) and conflict resolution (Group C2) have concrete primitives.

---

### 4. Design decisions worth resolving before code

Things the strawman picks; each is a real fork, surface before coding.

#### 4.1. Hierarchical vs. flat agent list

```yaml
# Option A — hierarchical (strawman uses this)
agents:
  - id: root
    children:
      - id: child-1
      - id: child-2

# Option B — flat with explicit parent
agents:
  - id: root
  - id: child-1
    parent: root
  - id: child-2
    parent: root
```

**Hierarchical (A)** is more readable for human-authored shallow graphs and matches how the relationship works in the runtime. **Flat (B)** is better for machine-generated graphs, cross-cutting references, and refactoring (move a subtree by editing one `parent:`).

**Lean:** support both, with the parser canonicalizing to flat internally. Authors choose. K8s does this with manifest references; Terraform does this with module composition.

#### 4.2. apiVersion + kind, or just version?

```yaml
# Option A — k8s-style
apiVersion: jarvis.engine/v1alpha1
kind: Graph

# Option B — simple
version: 1
```

(A) is overkill for a single artifact type but pays off the moment we have `kind: ToolBundle`, `kind: AgentTemplate`, etc. that can be referenced from a `Graph`. (B) is cleaner today but boxes us in.

**Lean:** start with (A). The cost is one extra line per file; the option value is large.

#### 4.3. Tool reference by id vs. inline definition

The strawman defines tools at the top and references them by id (`tools: [echo, web-search]`). Alternative: inline tool definitions per agent. Inline duplicates; references are cleaner.

**Lean:** by id. Inline is allowed for one-off tools nobody else uses, but reference is preferred. Same pattern as ConfigMap references in k8s.

#### 4.4. Time format

```yaml
# Option A — humanized (strawman uses this)
idle_period: 100ms
idle_period: 5m
idle_period: 1h

# Option B — machine
idle_period_ms: 100
idle_period_ms: 300000
```

Humanized is significantly nicer for human authors. The parser uses a duration crate (`humantime` or similar). Machine form is only better if the YAML is machine-generated; if so, the machine can write `100ms` just as easily.

**Lean:** humanized.

#### 4.5. Mandate text — inline string vs. file reference

For long mandates, inline YAML (`|` block scalar) starts to dominate the file. Alternative: `mandate: { from_file: ./mandates/root.md }`.

**Lean:** support both. Inline for short mandates and prototypes; file reference once mandates grow past ~20 lines or want their own version control / review path.

#### 4.6. Reconciliation semantics — what does "apply this YAML" mean?

This is the Terraform question. When the operator runs `jarvis apply graph.yaml` against an existing graph:

- **Agents in YAML, missing in runtime:** spawn them.
- **Agents in YAML, present in runtime with different mandate:** edit the mandate (treat as a `MandateUpdate` trigger).
- **Agents in runtime, missing from YAML:** what?

The "missing from YAML" case is the loaded one. Options:
- **Auto-retire** — pure declarative, but destructive. Easy to lose a child by accidentally removing a line.
- **Warn-and-leave** — never retire on apply; humans must explicitly retire.
- **Diff-then-confirm** — show the operator what would happen, require approval.

**Lean:** warn-and-leave for v1 (safe default). Add an explicit `--prune` flag for the diff-then-confirm path once humans trust the apply loop.

#### 4.7. Dynamic spawn vs. static graph

When an agent's `Decide` emits `SpawnChild { mandate, ... }` at runtime (Group C2 follow-up), is the new child written back into the YAML?

Options:
- **No** — YAML is human-authored only. Runtime-spawned children exist in the graph but not in any file. Diverges hard from the apply model.
- **Yes** — runtime appends to the YAML when spawning, so the file stays canonical. Requires writing YAML programmatically without losing comments / structure (hard in YAML, easier in JSON).
- **Hybrid** — runtime keeps a `runtime_spawned.yaml` sidecar that the operator can fold back into the canonical file when convenient.

**Lean:** hybrid. Canonical file is human-authored; sidecar tracks runtime additions; operator periodically reviews + folds.

#### 4.8. Schema validation

The runtime should validate the YAML before instantiating anything (typed errors with line numbers > runtime crashes mid-spawn). Options:
- **JSON Schema** — generate from Rust types via `schemars`; ship as `.schema.json`; editors get autocomplete.
- **Custom validator** — hand-rolled. More work, no editor support.

**Lean:** JSON Schema via `schemars` derive. Free editor support, free CI validation, costs us one dep.

---

### 5. Format choice — YAML vs. alternatives

Quick rationale for YAML over the obvious alternatives, since it'll come up:

| Format | Pros | Cons |
|---|---|---|
| **YAML** | Human-readable, multi-line strings, comments, anchors for reuse, ubiquitous in this niche | Indentation traps, type coercion gotchas (`yes` → `true`), spec is huge |
| **JSON** | Simpler, faster parsers, no indentation drama | No comments, miserable for multi-line mandates |
| **TOML** | Comments, simpler than YAML, no indentation | Awkward for nested hierarchies (children's children's children) |
| **RON** | Rust-native, exact mapping to types | Niche; ops folks won't know it |
| **HCL (Terraform)** | Designed for declarative infra, expressions, modules | Tool ecosystem isn't ours; learning cost |
| **Custom DSL** | Express exactly what we need | Build, parse, document, support |

**YAML wins** because: comments + multi-line strings (mandates) + indentation matches the hierarchy + every operator already knows it. The cons are real but well-trodden — the Rust ecosystem has `serde_yaml` (or `serde_yml`, the maintained fork).

**Stay aware:** the YAML 1.2 spec is large and `serde_yaml` only implements a useful subset. Document the subset we accept; reject anchors / merge keys if they cause more confusion than they save.

---

### 6. Phasing — when each step actually happens

Sequencing the schema's evolution against the bootstrap follow-ups in `scratch/post_bootstrap_followups.md`:

| Step | Trigger | Scope |
|---|---|---|
| 1. Single-agent YAML for `node-run` | Group A1 (real Decide adapter) | Replaces the three-file fixture. One agent, one tool, no scripted decisions (real Decide takes over). |
| 2. Multi-agent YAML | Group C2 (parent–child topology) | Hierarchical agents, tool references, defaults. No `policy:`. |
| 3. Apply / reconcile loop | After C2 stabilizes | `jarvis apply graph.yaml`, the warn-and-leave reconciliation. |
| 4. Dynamic-spawn integration | After C2 + apply loop | Hybrid sidecar approach from § 4.7. |
| 5. Policy block | After cost accounting + conflict log primitives exist | The `policy:` block fills in. |
| 6. JSON Schema export + editor support | Anytime after step 1 | `schemars` derive, ship `graph.schema.json`. |

Steps 1 and 6 are each a single ticket. Steps 2–5 are each a parent issue or small Project.

---

### 7. Open questions

1. **Versioning the YAML format.** `apiVersion: v1alpha1` says "expect breaking changes." When do we cut `v1`?
2. **Multi-document YAML** (`---` separators) — useful for splitting a big graph across files. Do we support it day one or never?
3. **Templating / inheritance.** Agent templates (`AgentTemplate` resource referenced by id) so `drug-alpha` and `drug-beta` share a shape with only the drug name varying. Worth it once we have ≥3 nearly-identical sibling agents.
4. **Imports / includes.** `!include other-graph.yaml` so a graph can compose subgraphs. Powerful, but every templating system in YAML is a small landmine.
5. **Tool-level config secrets.** `from_env: WEB_SEARCH_KEY` works for env-vars; what about secret managers (vault, AWS SM)? Probably out of scope until someone deploys for real.
6. **Mandate localization / i18n.** A graph for a non-English operator. Probably out of scope; mandates can be in any language as long as the model handles it.
7. **Hot reload.** Does `jarvis apply` reload a running graph in place, or does it require restart? Lean: in-place (else what's the point).

---

### 8. What this doc is not

- Not a finished spec. The strawman is one stake in the ground; the design decisions in § 4 will move it.
- Not a justification for building any of this now. Steps 2+ depend on Group C2 (parent–child topology), which itself depends on Group C1 (durability substrate) — both are months out per the bootstrap follow-ups doc.
- Not the only artifact format we'll need. The runtime will also need a typed wire format for the graph state (probably proto/CBOR) and possibly a UI-friendly JSON projection for the eventual graph viewer. YAML is the **operator-facing surface**, not the dominant on-wire format.

---

### 9. Next concrete step

If this design lands the right way, the smallest valuable ticket is **step 1 from § 6**: collapse `examples/smoke/` into one YAML, parsed into the existing types via `serde_yaml`. That happens alongside Group A1 (real Decide) and is the cheapest way to validate the schema against the runtime we already have. Multi-agent (step 2) waits for Group C2.

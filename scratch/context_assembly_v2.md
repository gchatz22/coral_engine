# Context assembly v2 — warm cache + tool-driven retrieval split

*Status: design only. No production-code changes accompany this doc.
Successor design for JAR2-6's `assemble_context` / `ContextBundle`,
tracked in JAR2-10. Concrete strawman types appear inline as a
communication aid; they are not contracts. The v1 implementation ticket
filed alongside this doc owns the final shapes.*

*Read order: `VISION.md` § 4–5, `scratch/agent_runtime.md` § 6 ("LLM
activities") and § 11, `scratch/post_bootstrap_followups.md` (Group A1
context anchoring) and `scratch/post_bootstrap_followups_later.md` (B1
index, C2 parent–child), `scratch/claim_seed_persistence.md` (the
"surface `claims/` to the bundle?" question is forwarded here),
`scratch/graph_yaml_schema.md` (only the bits about per-mandate
policy), then `src/decision.rs` (current `assemble_context` /
`ContextBundle` / `CorrectionContext`), `src/fs.rs` (`AgentFs` schema —
`outputs/`, `evidence/`, `notes/`, `claims/`, `mandate.json`),
`src/mandate.rs` (where a `ContextPolicy` field would live), and
`src/agent.rs` (where `assemble_context` is called).*

---

## 1. Goal of this layer

`assemble_context` is the activity that turns a snapshot of (FS state,
drained triggers, current mandate, pending correction) into the
`ContextBundle` a `Decide` impl reads each tick. JAR2-6 shipped a
fixed `RECENT_WINDOW = 8` slice as bootstrap scaffolding. This doc
sketches the v2 shape `agent_runtime.md` § 6 and `VISION.md` § 4–5 call
for — "mandate-specific selection/distillation" over the agent's full
durable working memory — and stakes out the path from here to there in
small phases.

The v2 design is **not** "delete the warm cache and give the agent
tools to pull whatever it wants." That's a one-axis solution to a
two-axis problem; the right shape is a deliberate split between what
the runtime hands over unconditionally each tick (cheap, deterministic,
mandate-shaped) and what the agent reaches for on demand (expressive,
non-deterministic, lifetime-spanning).

---

## 2. The split, concretely

| Kind of context need | Warm cache | Tool call |
|---|---|---|
| Standing instruction (mandate) | yes (anchor every tick) | no — never makes sense |
| Triggers that woke this tick | yes (anchors what the tick is about) | no |
| Correction context (`CorrectionContext` from prior tick) | yes (already there — see § 4) | no |
| Last N outputs (so the agent knows what it just said) | yes (small N) | also surfaceable via `read_file` |
| Tail of the conflict log | **reserved slot** (no conflict log yet — lands with C2) | also surfaceable once C2 lands |
| Open claims (`claims/`) — JAR2-28 forwarded this here | yes (small list, "is this a new claim?" every tick) | also via `list_dir` / `read_file` |
| Specific historical output by id | no | `read_file outputs/<ulid>.json` |
| Evidence supporting a specific claim | no | `read_file evidence/<sha>.json` |
| Free-form scratchpad (`notes/`) | no | `list_dir notes/` + `read_file` |
| Older outputs the warm window dropped | no | tool call (with future semantic search as the scalable path) |
| Mandate history (once mandate edits land) | no | tool call |

**Rule of thumb.** *Needed almost every tick to keep the agent
oriented* → warm cache. *Needed conditionally when the agent is
reasoning about a specific past episode* → tool call. The warm cache
is anchors; the tools are the rest of the working memory.

**Why not zero warm cache.** Re-discovering "what was I doing,
what claims are open, what did I just emit" on every tick via tool
calls multiplies model roundtrips for context the agent needs anyway.
The cost is paid on every wakeup; the savings (more flexibility) are
realized only on the subset of ticks that need lifetime memory. The
existing `correction` field already proves this — staging it as warm
state rather than asking the model to fetch it back from disk is the
same calculus.

**Why not infinite warm cache.** Inverse of the above. As the FS
grows, packing more and more state into the warm cache hits the model
context window first and the prompt-construction time second. Once the
FS exceeds the warm-cache budget, the only scalable answer is for the
agent to choose what to load.

The split is the answer to both pressures at once.

---

## 3. Warm cache shape

Strawman `ContextBundle` v2 — additive against today's struct, not a
rewrite. (Types are illustrative; the impl ticket owns the final
names.)

```rust
pub struct ContextBundle {
    // ---- already present in v1 (`src/decision.rs`); reaffirmed here. ----
    pub mandate: Mandate,
    pub triggers: Vec<Trigger>,
    pub recent_outputs: Vec<Output>,        // sized by ContextPolicy (§ 6)
    pub recent_evidence: Vec<EvidenceRecord>, // sized by ContextPolicy (§ 6)
    pub correction: Option<CorrectionContext>,

    // ---- new in v2 ----

    /// Open claims (`claims/<slug>.json` with `status == Open`), capped
    /// by `ContextPolicy::open_claims_max`. The `claim_seed_persistence`
    /// convention asks the agent to consult `claims/` before minting a
    /// new seed; surfacing the open set in the warm cache makes that the
    /// default, with `list_claims` available for the unusual case where
    /// the model wants a broader view (resolved/abandoned as well).
    pub open_claims: Vec<Claim>,

    // Reserved slot — `conflict_log_tail: Vec<ConflictEntry>` is added
    // alongside C2 (parent–child topology), when the log itself starts
    // existing. Adding the field now would force a placeholder
    // `ConflictEntry` with no users, which is the "extension point for
    // hypothetical future needs" DEVELOPMENT.md § 2 rejects. The §2
    // table reserves the slot in the design without reserving it in code.
}
```

**Explicitly NOT in the warm cache:**

- The contents of `notes/`. Free-form scratchpad, mutable, no provenance
  requirement — the agent reaches for it conditionally via `list_dir`
  and `read_file` (§ 5). Pulling it warm would route every doodle through
  the model on every tick.
- The full `outputs/` / `evidence/` history. Recent slice only; the rest
  is tool-call territory.
- Resolved / abandoned claims. The agent rarely needs them; tool call
  is the right shape.
- Mandate history. Doesn't exist yet (deferred per `fs.rs` docstring),
  and when it lands it's exactly the "specific historical episode"
  shape that belongs behind a tool.

**Per-mandate tuning surface** — see § 6.

---

## 4. The pieces v2 inherits from v1

These are already shipped and load-bearing for the design; they are
*not* re-litigated here:

- **`mandate`, `triggers`, `correction`** — anchored by the existing run
  loop in `src/agent.rs`. v2 does not move them. `correction` in
  particular is agent-internal continuation state (see `agent.rs`
  module doc), and the rationale for keeping it off the trigger queue
  is exactly the rationale for keeping it on the warm bundle: it's
  needed *every* tick that follows an unsatisfiable decision.
- **The `Decide` trait surface.** `decide(ctx: ContextBundle) ->
  Decision`. v2 adds fields to `ContextBundle` and routes some context
  through tools instead, but the signature stays.

---

## 5. Self-directed retrieval surface

The agent's own filesystem is the retrieval substrate. The runtime
exposes it through normal `Tool` implementations registered in
`ToolRegistry`, so they compose with the existing apply-time correction
loop, evidence recording, and retry policy with no new infrastructure.

**Tools v2 introduces (sketch):**

```text
list_dir { path: String }
    -> { entries: [{ name, kind: "file" | "dir", size_bytes? }] }

read_file { path: String, max_bytes?: u64 }
    -> { content: String, truncated: bool, size_bytes: u64 }

# (v3+) Semantic search over the agent's FS. External MCP server,
# bound per-agent. See § 7.
semantic_search { query: String, top_k?: u32, scope?: [String] }
    -> { hits: [{ path, score, snippet }] }
```

These are MCP-flavored tools, not new kernel primitives — they're
implementations of `crate::tools::Tool` registered through
`ToolRegistry::register`. The internal-vs-external choice for the basic
two is discussed in § 7; today either path works.

**Read-scope rule (load-bearing).** `apply_ops`'s
`resolve_notes_path` confines *writes* to `<root>/notes/`. *Reads*
need a wider scope but still root-confined. The retrieval tools accept
paths under exactly these prefixes:

```text
mandate.json
outputs/
evidence/
notes/
claims/
retirement.json   (read-only; mostly useful to a future audit tool)
health/           (read-only; once health archives land)
```

Any other prefix, any `..`, any absolute path, any Windows prefix, any
symlink target outside the agent root → reject with the same
`PathTraversal` / `PathOutsideRoot` error family `resolve_notes_path`
uses today. The agent reads from its own FS root and nothing else.
**No cross-agent reads** — out of scope, explicitly (matches VISION's
"per-agent filesystem" wall and the ticket's out-of-scope clause).

**Evidence recording.** Every retrieval tool call goes through
`ToolRegistry::call` and produces an `EvidenceRecord` like any other
tool. This is what makes the non-determinism of tool-driven retrieval
auditable — the evidence trail *is* the record of what the agent
looked at. § 9 returns to this.

**Health / retry.** Retrieval tools inherit `RetryPolicy` / health
wiring from JAR2-25, same as any other tool. A flaky `read_file` (FS
I/O hiccup) counts toward the tool-call budget the way an MCP tool
hiccup does today.

---

## 6. Per-mandate policy

Today `Mandate` is `{ text, idle_period, max_ticks }` — plain serde
data with no extension point. v2 adds **one field**:

```rust
pub struct Mandate {
    pub text: String,
    pub idle_period: Duration,
    pub max_ticks: Option<u64>,

    /// Tuning knobs that shape warm-cache assembly for this mandate.
    /// Defaults match v1 behavior (`RECENT_WINDOW = 8`, no open-claims
    /// cap), so existing graphs round-trip unchanged.
    #[serde(default)]
    pub context_policy: ContextPolicy,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ContextPolicy {
    #[serde(default = "default_recent_outputs")]   // 8
    pub recent_outputs: usize,
    #[serde(default = "default_recent_evidence")]  // 8
    pub recent_evidence: usize,
    #[serde(default = "default_open_claims_max")]  // e.g. 32
    pub open_claims_max: usize,
    #[serde(default)]                              // 0 today; > 0 once log exists
    pub conflict_log_tail: usize,
}
```

**Field, not trait.** A trait would have been the obvious "extension
point" reflex, but `Mandate` is serde-round-tripped through
`mandate.json` and the YAML graph schema (`graph_yaml_schema.md`).
Traits don't serialize. A field on `Mandate` carrying a typed struct
does — and gives the YAML schema a flat, validatable place to expose
the knobs. The trait option is dead-on-arrival given the existing
shape, not just less convenient.

**Future enum variants** (e.g. `ContextPolicy::Custom(CustomPolicy)`,
`ContextPolicy::SiblingBatch { ... }`) can be layered in if a real
mandate wants more than dial-twiddling, without touching the bundle
contract. Defer until we have a real second policy.

**Where the precise default sizes come from.** Empirical. The ticket
explicitly authorizes "say so and propose a small spike" rather than
bluff a precision-tuned default — see § 12, which scopes a measurement
spike into the v1 implementation ticket.

---

## 7. FS indexing

`AgentFs::read_recent_json` scans an entire directory, sorts every
filename, takes the last N. Documented in `fs.rs` as fine at bootstrap
scale (M < 100) and explicitly flagged as a real problem once a
long-lived agent accumulates thousands of outputs / tens of thousands of
evidence records. `post_bootstrap_followups_later.md` **B1** already
specifies the smallest indexing primitive that pays off:

- `outputs/index.jsonl` — append `{filename, created_at}` on every
  `persist_output`.
- `evidence/index.jsonl` — append `{filename, created_at}` on every
  `record_evidence`.
- Read path tails the index instead of `read_dir`.
- Verifier rebuilds the index from the directory on demand (crash
  recovery / drift).

**Reuse B1; do not invent a parallel scheme.** v2's warm-cache reads
(`recent_outputs`, `recent_evidence`) become `tail_index(N)` instead of
`read_recent_json(N)` once B1 lands. The retrieval tools (`list_dir`,
`read_file`) don't need the index — they already address by path —
but `semantic_search` (v3) builds on top of the index by reading the
`created_at` stamps as part of its scoring metadata.

**Why JSONL, not sqlite or an embedding store.** JSONL composes with
the append-only properties of `outputs/` and `evidence/`; the directory
*is* the source of truth and the index is a derived artifact that can
always be rebuilt. Sqlite would introduce a second source of truth and
a schema migration path with no payoff at this scale. An embedding
store is the v3 story (semantic search), not the v1 indexing primitive
— it indexes content, the JSONL index indexes existence + time.

**Per-write atomicity** — same answer as B1: write the file first,
then append to the index. A crash leaves the index lagging (easy to
verify and repair), never ahead (which would dangle).

---

## 8. Time vs. filename ordering

Outputs are ULID-named (`<crockford-base32>.json`); filename order ≈
creation order. Evidence is sha256-named; filename order has no
temporal meaning. Today's `read_recent_json` sorts by filename, which
is correct for outputs by accident and arbitrary-but-deterministic for
evidence.

**Decision: stamp `created_at` on every record (already done — see
`EvidenceRecord` and `Output`) and index by it.** Concretely: the B1
index entries carry `created_at`; the warm-cache reads
(`recent_outputs`, `recent_evidence`) become "tail the index by
`created_at` descending, take N, return ascending." Outputs end up in
the same order as today (since ULID-time ≈ wall-time); evidence ends up
in *true* recency order rather than lexical-by-hash order, which is the
behavior the field name has always implied.

**Why not "sort on read, no index."** `read_dir` already costs O(M);
sorting by `created_at` would require reading every file (since the
timestamp lives inside the JSON, not in the name). Indexing on write is
the cheap fix and is the same fix B1 already calls for.

**ULID-only fallback.** If we ship the warm-cache policy field before
B1 (the suggested v1 phasing — see § 12), the v1 implementation can
still sort outputs by ULID (correct by construction) and *defer*
evidence-by-time until B1 lands. v1 docstring marks the evidence path
as "filename-order, will become true-recency under B1."

---

## 9. Determinism / replay

Today `assemble_context` is deterministic in `(fs_snapshot, triggers,
mandate, correction)`. v2 splits the picture:

**Deterministic, unchanged in shape:** the warm cache itself. Given
the same FS snapshot, triggers, mandate, correction, and
`ContextPolicy`, `assemble_context` returns the same `ContextBundle`.
The B1 index doesn't change this — it just makes the read cheap.

**Non-deterministic, but already audited:** what the agent retrieves
via tools during `decide`. Two ticks with identical warm caches can
produce different retrieval traces because the model decides what to
fetch. *That's the entire point of tool-driven retrieval.*

**Replay artifact:** every retrieval tool call goes through
`ToolRegistry::call` and writes an `EvidenceRecord`. The evidence
trail *is* the replay artifact for context selection. To reconstruct
what the model saw on tick T, an auditor reads:

1. The warm `ContextBundle` (re-assemble from the FS at tick T's
   snapshot — deterministic).
2. The evidence records emitted during tick T whose tool was
   `read_file` / `list_dir` / `semantic_search` — these are the model's
   self-directed lookups, in the order it issued them.

The contract is already there. We don't need a parallel "context
trace" sidecar; the existing provenance trail covers it because
retrieval *is* tool use.

**One caveat.** A retrieval tool's `EvidenceRecord` is keyed on
`(name, args, result)`, so two identical `read_file` calls within a
tick collapse to one record (dedup). That's correct for provenance
(the same fact has the same id) but loses the *count* of how many
times the model asked. Acceptable; if we ever need it, we add a
per-tick retrieval log as a separate, throwaway artifact under
`notes/`. Out of scope here.

---

## 10. Caching across siblings

`agent_runtime.md` § 6 calls out memoizing `assemble_context` across
siblings of the same parent. The split makes this two questions, not
one:

**Warm cache is memoizable across siblings.** The cache key is `(
fs_snapshot_id, triggers, mandate, correction, context_policy)`. Two
siblings with identical state would compute identical bundles, so a
process-local cache keyed on that tuple is a free latency win. In
practice siblings often differ on `mandate` and `correction`, so the
hit rate is bounded — but the warm cache is small and assembling it
is cheap, so the cache is a v5 nicety, not a v1 must-have.

**Tool-driven retrieval is per-agent.** The retrieval tools read each
agent's *own* FS root. Cross-sibling memoization doesn't apply —
sibling A's `read_file outputs/01H...json` returns a file in sibling
A's directory; sibling B has its own (different) `outputs/`. The
right cache shape there is the per-tool result cache (idempotent calls
within a tick), which `ToolRegistry::call` already gets via
content-addressed evidence. No new layer needed.

**Where multi-agent caching *does* show up** is the MCP traffic
multiplexing question in `agent_runtime.md` § 11.6 — sibling agents
hitting the same MCP server with the same args. That's a different
cache (cross-agent, on outbound MCP traffic) and is out of scope here.

---

## 11. What this forces vs. punts

**Forces** — these have to land for the v1 implementation ticket to be
coherent:

- `ContextPolicy` field on `Mandate` with serde defaults that preserve
  today's behavior (round-trip safety for existing `mandate.json`).
- `assemble_context` reads window sizes from `cfg.context_policy`
  instead of the `RECENT_WINDOW` const.
- `open_claims` populated from `AgentFs::list_claims().filter(status =
  Open).take(open_claims_max)`. Tracker for the rule-of-thumb check —
  see the spike in § 12.
- Doc updates: `decision.rs` module doc and the JAR2-10 retirement of
  `RECENT_WINDOW`.

**Punts** (carries forward to follow-up tickets — `agent_runtime.md`
§ 11 stays the canonical open-question list):

- Conflict log surface and the `conflict_log_tail` content. C2.
- Retrieval tool implementations (`read_file`, `list_dir`). v2 phase
  (after v1).
- Semantic search. v3 phase.
- B1 index integration. Sibling track to v2/v3; can land in either
  order.
- Sibling memoization of the warm cache. v4/v5 phase, after multi-agent
  topology exists.
- Mandate-history tool. Lands with mandate-edit semantics; not v2.

---

## 12. Phasing — what lands first

Sequenced from cheapest to most speculative. Each phase is independently
shippable; the v1 follow-up ticket scopes phase 1 only and stays small.

| Phase | Scope | Depends on |
|---|---|---|
| **v1** | `ContextPolicy` field on `Mandate`; `assemble_context` reads from it; default values match today; `open_claims` field on `ContextBundle` populated from `list_claims`. Small measurement spike (next paragraph). | — |
| **v2** | `read_file` and `list_dir` Tool impls + read-scope rule (§ 5). Registered into `ToolRegistry` by `Agent::new`. | v1 |
| **v3** | Wire B1 (`outputs/index.jsonl`, `evidence/index.jsonl`); warm-cache reads tail the index; evidence order becomes true-recency. | independent of v2; can land in parallel |
| **v4** | Semantic search. External MCP server, bound per-agent; engine ships a default impl spec but not the embedding choice. | v2 + v3 |
| **v5** | Sibling memoization of the warm cache. | C2 (parent–child) — no siblings exist before then. |

**Measurement spike inside v1.** The ticket flagged this and the
guardrail is real: we don't have empirical data yet on what context
the LLM actually uses, model latency, or the cost of an extra tool
roundtrip. The cut-line between warm and tool is therefore an
educated guess until measured. v1 includes a small, time-boxed spike:

1. Run the existing recorded-fixture integration tests
   (`tests/loop_smoke.rs` + the JAR2-21 fixtures) with bundle field
   counts logged.
2. Add a one-shot harness that emits the warm cache only and one that
   emits warm cache + a synthetic prior output via `read_file`, and
   compare model behavior on a representative mandate.
3. Write the findings into a brief sibling scratch note
   (`scratch/context_assembly_v1_measurements.md`) — *not* into this
   doc — and use them to set the default `recent_outputs` /
   `open_claims_max` values.

If the spike surfaces a finding that should reshape v2+, file it as a
follow-up; don't expand v1.

---

## 13. Verification plan for the v1 implementation ticket

The v1 implementation lands without an LLM in the loop — every test
runs against `MockDecide`. Concretely:

- **Unit (in `mandate.rs`).** `ContextPolicy` round-trips through
  serde with defaults; an old `mandate.json` missing `context_policy`
  parses (default applied).
- **Unit (in `decision.rs`).** `assemble_context` respects
  `cfg.context_policy.recent_outputs` and `recent_evidence` (window
  scales with the field, not the dead `RECENT_WINDOW` const).
- **Unit (in `decision.rs`).** `assemble_context` populates
  `open_claims` from `AgentFs::list_claims` filtered to `status ==
  Open`, capped by `open_claims_max`; resolved/abandoned claims are
  excluded.
- **Integration (`tests/loop_smoke.rs`).** Existing smoke runs through
  unchanged because the default `ContextPolicy` reproduces today's
  behavior — the v1 diff is additive.
- **Integration.** A new smoke that sets `recent_outputs = 2`,
  emits 5 outputs via a scripted `MockDecide`, and asserts the bundle
  on the next tick carries 2 outputs (not 8).
- **Determinism.** Calling `assemble_context` twice against the same
  FS snapshot produces equal bundles — the property the v1 test
  in `decision.rs` already asserts. Re-asserted under the new policy
  fields.

Tool-surface tests (`read_file`, `list_dir`) ship with their own
ticket in v2 and live alongside the existing `tools.rs` test pattern.

---

## 14. References

- `VISION.md` § 4 (every agent has a filesystem; data flows through
  MCP), § 5 (per-agent filesystem; data layer).
- `scratch/agent_runtime.md` § 6 ("LLM activities" — the two-activity
  split + sibling memoization callout), § 11 (canonical open-question
  list).
- `scratch/post_bootstrap_followups.md` (Group A1 context anchoring;
  retrieval shape).
- `scratch/post_bootstrap_followups_later.md` **B1** (the index this
  doc reuses), C2 (parent–child topology — gates the conflict log and
  sibling memoization).
- `scratch/claim_seed_persistence.md` ("Surfacing `claims/` in
  `ContextBundle`. JAR2-10's territory." — answered in § 3 and § 11
  here).
- `scratch/graph_yaml_schema.md` § 4 (per-agent `context_policy`
  surfaces as a flat block in the YAML once it lands).
- `src/decision.rs` (`ContextBundle`, `assemble_context`,
  `RECENT_WINDOW`, `CorrectionContext`), `src/fs.rs` (`AgentFs`
  schema; the scaling caveat on `read_recent_json`), `src/mandate.rs`
  (where `context_policy` will live), `src/agent.rs` (where
  `assemble_context` is called and how `correction` is threaded),
  `src/tools.rs` (the `Tool` trait the retrieval tools implement).

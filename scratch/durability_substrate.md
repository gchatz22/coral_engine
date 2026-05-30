# Durability substrate — Temporal vs. DBOS vs. Restate vs. custom

*Status: ideation. Discussion piece on the C1 fork that `scratch/post_bootstrap_followups_later.md` flagged and `scratch/agent_runtime.md` answered prematurely (it assumed Temporal). Goal: put every serious candidate honestly on the table, identify the load-bearing constraints, recommend a path. Output of this doc is one decision: which substrate the C1 implementation Project is built against.*

*Read order: `VISION.md` § 4 ("open kernel, sovereign default"), § 5 (kernel concerns), `scratch/agent_runtime.md` (the Temporal-assumed design, especially § 2, § 4–5, § 9), `scratch/post_bootstrap_followups_later.md` C1, then this. The agent runtime design is mostly substrate-independent — what changes between candidates is the host the loop runs inside, not the loop's shape.*

---

## 1. What we actually need durability for

Naming this concretely so we don't over-scope. Today's agent already has these durable-on-disk via the per-agent FS:

- The mandate (`mandate.json`).
- Emitted outputs and their evidence (`outputs/<ulid>.json`, `evidence/<sha256>.json`).
- Notes (`notes/`), claims (`claims/`), health (`health.json` + archive), retirement marker.

What is **not** durable today:

| State | Where it lives now | What goes wrong on crash |
|---|---|---|
| Trigger queue | In-memory mpsc inside `Agent` | Any unprocessed external/human/scheduled trigger is lost on restart |
| Scheduler cursor (`next_deadline`) | In-memory `Scheduler` | Agent doesn't know when next to wake; restart resets cadence |
| In-flight tick state | Stack-local variables inside `Agent::run` | A tick interrupted between `decide()` and the write of its result is lost; on restart, either replays from scratch (re-asking the LLM) or skips work |
| Tool-call idempotency | Evidence written content-addressed, but no per-tool-call dedup key | Replaying a tick could double-call an external tool that isn't idempotent |
| Child workflow handles | Doesn't exist yet (single agent only) | When C2 lands: parent restart must not orphan children |

This is the durability surface. Notably absent: **agent working memory and outputs/evidence don't need a new substrate** — they're already on disk and survive crashes. The substrate question is exclusively about *execution state*. This matters because it shrinks the problem from "rebuild storage" to "durably orchestrate ticks against existing storage."

---

## 2. The three state layers, named explicitly

Now that the lightweight-DB-for-topology call from refinement (1) is settled, the architecture has three storage concerns that should never be conflated:

| Layer | What's there | Backed by | Lifetime |
|---|---|---|---|
| **Structural** | Graphs, agents that exist, parent–child edges, tool registrations, operator-edited mandates as authored, cost-budget config | Lightweight DB (sqlite or Postgres) — decision later, but it's a small relational store | Outlives any single agent run |
| **Working memory** | Mandate (current), outputs, evidence, notes, claims, health, retirement | Per-agent FS on disk | Life of the agent |
| **Execution** | Trigger queue, scheduler cursor, in-flight tick, child handles, idempotency keys | **This doc decides what** | Tick to tick, surviving process restart |

These should not bleed into each other. Outputs do not live in the DB. Trigger queue does not live in the FS as a regular file (a JSONL spool is fine; queue *semantics* — ordering, drain, dedup — are not). The lightweight DB does not store evidence records. Discipline here keeps the kernel small.

This doc is exclusively about layer 3.

---

## 3. Evaluation dimensions

Five axes, weighted by what `VISION.md` and `DEVELOPMENT.md` force on us:

1. **Rust story.** `DEVELOPMENT.md` § 1: stable Rust, no exceptions. If a candidate's Rust SDK is alpha-grade or missing, we either contribute upstream, build a thin wrapper around a non-Rust SDK over IPC, or pick differently. This dimension is load-bearing, not preference.
2. **Operational footprint (sovereign deploy).** `VISION.md` § 4: must be deployable on user compute. A substrate that requires a cluster of services has a real adoption tax. One a hospital or hedge fund can boot with `docker compose up` is much easier to land in serious deployments.
3. **Programming-model fit.** `scratch/agent_runtime.md` § 4 sketches the loop: race signals against a timer, drain ordered triggers, call activities (LLM, tools, FS ops), persist a decision, continue-as-new on history bounds. Some substrates want this shape natively; others would force us to fight the framework.
4. **Maturity / bus factor.** How many production users does the substrate have? How many engineers? What happens if the company behind it pivots? Temporal is the safe bet here; everyone else is younger.
5. **Reversibility.** If we pick wrong, how expensive is the swap? Hiding the substrate behind a `DurableHost` trait at the right seam makes a future migration cost weeks, not months. If we can't draw that seam cleanly against a candidate, that's a red flag.

---

## 4. Candidates

### 4.1 Temporal

**What it gives us.** Durable workflow execution with replayable event history; signals/timers/queries as primitives; activity retries with exponential backoff; worker pools with rate limiting; first-class versioning for evolving workflow code; continue-as-new for unbounded-runtime workflows. The agent_runtime.md design maps almost one-to-one onto Temporal's primitives, which is why that doc assumed it.

**Rust story.** Materially better than I credited in the first pass of this doc, and worth correcting in the record. Temporal now ships **first-party Rust SDK documentation** on docs.temporal.io alongside Go/Java/Python/TypeScript, with a dedicated `develop/rust/` section covering workflows (basics, child workflows, continue-as-new, message passing, cancellation, timers, timeouts), activities (basics, execution, timeouts), workers and worker processes, the Temporal client, and **Temporal Nexus** (the newer cross-namespace primitive). The SDK lives at [`temporalio/sdk-core/crates/sdk`](https://github.com/temporalio/sdk-core/tree/master/crates/sdk) — i.e. shipped from the same repo as `sdk-core` itself, which is a much closer relationship than the historical community-wrapper picture suggested. API docs are at [docs.rs/temporalio-sdk](https://docs.rs/temporalio-sdk/latest/temporalio_sdk/).

What the documentation page does **not** claim: GA status, a 1.0 version, or absence of "preview"/"alpha" labelling. Feature parity with the older SDKs (Go/Java) is also not explicitly asserted. The docs surface is much wider than I'd have expected for an alpha, but the production-readiness signal is still missing in writing. Before committing the C1 Project to Temporal, the verification spike (§ 8 decision 3) should pin: published crate version on crates.io, any explicit "not for production" notes in the repo README, and whether any user runs it in production publicly. Cheap to retire that uncertainty before months of work.

If the Rust SDK isn't where we need it:
- **Contribute upstream.** Realistic if we're committed; ties our delivery cadence to upstream.
- **Sidecar another SDK over IPC.** Spawn a Java/Go worker process, talk to it via local socket. Ugly, but ships now. Sets fire to "small kernel."
- **Write our own SDK against `sdk-core`.** Probably less work than it sounds because `sdk-core` is Rust, but it's still building the SDK that the Temporal team builds.

**Operational footprint.** Heavy. Temporal Server is OSS but ships as multiple services (frontend, history, matching, worker) backed by Postgres/MySQL/Cassandra and (optionally) Elasticsearch for visibility. Self-host is `docker compose up` for a dev setup but a real deployment is a real cluster. Temporal Cloud sidesteps this but is a hosted dep — fine for some users, blocking for the "sovereign deploy" cases (hospitals, agencies) that VISION cares most about.

**Programming-model fit.** Excellent in the abstract — `agent_runtime.md` § 4 reads like a Temporal workflow. The bolts that worry me:
- Activities that fetch from LLMs/MCP need careful idempotency or non-retryable error tagging; we already understand how to do this.
- The "two LLM activities per tick" pattern (assemble + decide) maps cleanly.
- Workflow code must be deterministic; tool code goes in activities. Our existing module layout already supports this split — `Decide` and `Tool` are the activity boundaries.

**Maturity / bus factor.** Highest of any candidate. Temporal Inc. is well-funded, the project predates the company, and the OSS Server has a real ecosystem.

**Reversibility.** Once the workflow code is written, swapping is hard — workflows-as-code is *the* Temporal idiom, and it leaks into our run loop. But if we put the run loop behind a `DurableHost` trait and keep workflows thin (just call into our existing `Agent::run`), the seam is plausible.

**When this is right.** If we believe (a) Rust SDK gets to production-quality on the timeline we care about, *or* (b) we're willing to own a meaningful chunk of upstream Rust SDK work as part of the project. If neither, we're knowingly building on alpha.

### 4.2 DBOS (dbos.dev)

**What it gives us.** A radically simpler model: durable workflows as ordinary functions, with each step checkpointed to a row in **your existing Postgres**. No separate cluster, no separate event store — Postgres is the substrate. Workflow recovery on restart replays from checkpoints. Idempotency via per-step UUIDs. The pitch is "you already have Postgres; you don't need Temporal."

**Rust story.** Confirmed: DBOS Transact (the OSS framework) is **Python and TypeScript only** — no first-party Rust SDK and no announced plan for one. Implications:

- **Reimplement the Postgres protocol in Rust.** The protocol (checkpoint each step with a workflow_id + step_id) is small. We could implement it ourselves and benefit from the same Postgres tables / recovery semantics. This is essentially building "custom substrate" inspired by DBOS, not adopting DBOS.
- **Polyglot deployment.** Ship a Python worker that hosts the DBOS workflow, RPC into Rust for LLM / FS / MCP work. This violates the single-language principle hard and adds a Python runtime to "sovereign deploy."

**Operational footprint.** Smallest of the three external-substrate candidates. One Postgres instance. That Postgres is also the lightweight-DB we already need for the structural layer (refinement 1). The infra-collapse is real and attractive.

**Programming-model fit.** Closer to "decorate ordinary functions" than "write workflow code against a foreign runtime." Continue-as-new isn't a primitive in the same sense; workflows are expected to be bounded. For unbounded agent loops we'd structure as "one workflow per tick, kicked by a scheduler" rather than "one workflow per agent's lifetime." That's a real architectural delta from the agent_runtime.md sketch but it's not obviously worse — arguably it's *simpler*, because tick-as-workflow gives us a natural unit of replay and no continue-as-new dance.

**Maturity / bus factor.** Smaller than Temporal by a couple of orders of magnitude. The team includes Postgres luminaries (Stonebraker) and the framework is well-architected, but the user base is much smaller.

**Reversibility.** The "DBOS-inspired custom" path (reimplement the checkpointing protocol in Rust against Postgres) is actually highly reversible — it's just our own code talking to Postgres. The polyglot path is not.

**When this is right.** If we want one infra dep (Postgres) doing double duty (structural state + execution state) and we're willing to either run Python alongside Rust or implement the protocol ourselves. The "implement the protocol ourselves" option starts to look a lot like § 4.4.

### 4.3 Restate (restate.dev)

**What it gives us.** Durable execution as a service, designed for "fearless distributed applications." Each invocation gets a deterministic journal that survives restarts; signals (called "awakeables"), durable state per-key, durable promises. The Restate Server is **written in Rust**, and the official Rust SDK is first-party — not the second-class citizen Temporal's Rust SDK is.

**Rust story.** Confirmed by the published crate. `restate-sdk` is at **v0.10.0** on crates.io with first-party documentation on docs.rs, covering essentially every primitive we'd use: **Services** ("a collection of durable handlers"), **Virtual Objects** (stateful entities where "at most one handler can run at a time per object"), **Workflows** (handlers that "execute exactly once per workflow instance"), durable RPC and messaging between services (optionally delayed), a key-value state store, **Awakeables** ("Durable Futures to wait for events and the completion of external tasks" — this is the signal primitive), durable **Timers** Restate "durably tracks across failures", **journaling** of results to skip re-execution on retries, error handling with retries + terminal errors, and a `#[shared]` attribute for concurrent handlers on Virtual Objects/Workflows.

What's not asserted on the docs.rs page: a 1.0 stability claim, an MSRV, or a production-readiness statement. The version (0.10.0) places this in "active 0.x development" — past prototype, pre-1.0. The docs.rs metric of 68.22% coverage is consistent with that — a real working SDK whose corners aren't fully papered. Worth a closer look at the repo README and changelog for any explicit warnings, but the publicly stated feature surface is enough to confirm this is the candidate I described as the dark horse.

**Operational footprint.** Lighter than Temporal — one Restate Server binary plus an embedded RocksDB-style state store, or Postgres for the production deployment. Doesn't require a multi-service cluster. Closer to "single binary with a sidecar" than "deploy a distributed system."

**Programming-model fit.** Similar to Temporal at the conceptual level (workflows, activities, signals, durable timers, replay-on-restart) but the API surface is closer to "regular async Rust functions with `#[restate::handler]` decorators." Continue-as-new exists in the form of long-running "virtual objects" that hold durable per-key state.

**Maturity / bus factor.** Young (≈2023 product), small but credible founding team (ex-Apache Flink, ex-Confluent). Real production users. The bus-factor concern is real — if Restate pivots, we'd need to migrate. Mitigated by their open-source approach.

**Reversibility.** Same seam as Temporal — workflows-as-code is the idiom, hide behind a `DurableHost` trait, swap is weeks not months.

**When this is right.** If the Rust SDK survives a closer look, this is the path of least resistance for a Rust shop. It pays the smallest tax on language, gives us 80% of Temporal's primitives, and the operational story is one binary + one persistent volume.

### 4.4 Custom in-crate (sqlite-journaled run loop)

**What it gives us.** Stay in `coral_node`. No external dependency. Concretely: persist the trigger queue and scheduler cursor to sqlite (or to JSONL files, but sqlite gives us atomic transactions for free) at every tick boundary. Journal a small typed record per tick: `{tick_id, drained_triggers, decision, applied_at, evidence_ids}`. On restart, read the last tick's journal entry; if the previous tick's decision was applied (output written / evidence recorded / scheduler updated), advance; if not, re-execute the decision idempotently (replay-safe because tool calls dedup on content-addressed evidence).

**Operational footprint.** Zero. A sqlite file inside `<agent_root>/journal.db`. Sovereign deploy = `cargo run`.

**Programming-model fit.** Perfect — we own the loop shape.

**Maturity / bus factor.** Sqlite is bombproof. Our code on top: only as good as we write it. We will get the replay edge cases wrong at least twice. The honest scope is **months** of building durable-execution primitives badly that Temporal/Restate/DBOS already build well.

**Reversibility.** Highest. The whole substrate is our code; we can replace it with Temporal/Restate later by porting the journal semantics into their primitives. The `DurableHost` trait is trivially extractable because we wrote the only impl.

**The hybrid framing.** Start custom — small sqlite journal, in-process — to get the single-host single-agent durability story landed in weeks rather than months. Hide the substrate behind a `DurableHost` trait from day one. When (or if) we hit the scale ceiling where in-process durability isn't enough — multiple workers per agent, geographic distribution, replay-as-debug-tool — swap to Restate or Temporal behind the trait. This is the option that doesn't paint us into a corner either way.

**When this is right.** If we believe (a) the single-host bound is fine for the next 6–12 months of users, *and* (b) we want to ship visible progress against the durable run-loop in weeks not months. The risk is "10 months in we wish we'd just adopted Restate/Temporal because the edge cases of distributed durability are hard." Trait-behind-the-substrate mitigates this materially.

---

## 5. Comparison

| Dimension | Temporal | DBOS | Restate | Custom |
|---|---|---|---|---|
| Rust SDK quality | First-party SDK from the `sdk-core` repo, full docs surface (workflows + activities + Nexus); no explicit GA/version claim — verify production-readiness | None first-party, no announced plan (confirmed) | First-party `restate-sdk` v0.10.0, full primitive surface (services, virtual objects, workflows, awakeables, timers, durable RPC, retries); 0.x | N/A — we own it |
| Sovereign deploy footprint | Heavy (multi-service cluster) | Light (one Postgres) | Medium (one binary + state store) | Trivial (sqlite file) |
| Programming-model fit for our loop | Excellent | Tick-as-workflow restructuring needed | Excellent | Perfect (we shape it) |
| Maturity / bus factor | Highest | Smaller | Smallest of externals | All on us |
| Reversibility | Moderate (trait seam) | High if "DBOS-inspired", low if polyglot | Moderate (trait seam) | Highest |
| Time to first durable tick in tree | Months (SDK work first) | Months (Rust client first or Python intro) | Weeks–months (verify SDK) | Weeks |
| Years-out-distributed-fleet fit | Highest | Medium | High | Lowest (would migrate) |

---

## 6. Recommendation (speaking up)

**Lean: custom-sqlite in-crate now, behind a `DurableHost` trait, with Restate as the first external candidate to swap in once we have load that justifies it.**

Reasoning, in order:

1. **The Rust constraint is load-bearing.** `DEVELOPMENT.md` § 1 isn't a preference — it's a rule. Temporal's alpha-grade Rust SDK and DBOS's missing one are both real concerns. Restate's first-party Rust SDK is the only "external substrate that doesn't make us reach for a non-Rust SDK" option, and even that needs verification.

2. **Custom is the fastest path to "durable execution exists in the repo."** Persisting the trigger queue, scheduler cursor, and per-tick journal to sqlite is on the order of weeks. Tool-call idempotency we already get for free via content-addressed evidence. The "we'll get edge cases wrong" risk is real but bounded — we are running single-host single-agent for the entire foreseeable horizon. Distributed-workflow edge cases (cross-node consensus, geographic failover) aren't on the path until we have a multi-host story, and we're nowhere near that.

3. **The trait seam is the insurance policy.** A `DurableHost` trait — start, signal, wait_for_trigger_or_deadline, drain_triggers, persist_decision, replay_pending — drawn around the run loop costs us a half-day and makes future migration *months* of focused work rather than a rewrite. Don't merge code that talks to sqlite directly outside one module. The shape of the trait is the real design artifact this doc owes; sketching it inline below.

4. **Restate is the pre-staged swap target.** When we outgrow custom, Restate maps onto our trait most cleanly because (a) Rust SDK exists, (b) operational footprint stays reasonable, (c) primitives match. Pre-write the migration spike note now so future-us doesn't re-litigate the field.

5. **Temporal stays an option for far-future-us** if Restate doesn't materialize or if we end up wanting the deepest ecosystem. Hopefully by then the Rust SDK is solid. If it isn't, we'd either fund upstream or pick Restate.

**Edit after the Rust-SDK fact-check.** Pulling the published Temporal and Restate Rust docs (added to § 9) shifted the picture less than I expected on the recommendation but more than I expected on the alternatives. Both externals have *more credible* Rust stories than my first pass credited — Temporal ships first-party Rust docs from the `sdk-core` repo covering the full primitive surface including Nexus; Restate is at v0.10.0 with awakeables/timers/virtual-objects/workflows all documented. Neither says "GA, 1.0, production-ready" in writing. Net effect on the recommendation:

- **The case for "skip custom, start on Restate (or Temporal) directly"** is now more defensible than I'd have said before fetching the docs. If the maintainer's read is "we're going to end up on an external substrate anyway, and the Rust SDKs are real, so paying the migration tax once now is cheaper than paying it twice (custom → external)" — that's a coherent position and the fetched docs support it.
- **The case for "custom-with-trait-seam first"** still rests on (a) zero infra dep on day one, (b) weeks rather than months to first durable tick, (c) the trait seam costing us approximately nothing and making the future migration tractable regardless of which external we pick. The fetched docs don't move any of those three.
- **The verification spike from § 8 decision 3 is now the most valuable single artifact** — not less. The published doc pages don't carry production-readiness statements; the answer is in repo READMEs, changelogs, public deployments. A day of digging there could justify skipping the custom stage entirely.

6. **DBOS doesn't make the cut on Rust grounds**, but the "Postgres-as-substrate" *idea* is excellent — and we'd benefit from that anyway because refinement (1) says structural state lives in a lightweight DB. If we end up using Postgres for structural state, the custom-sqlite path naturally graduates to custom-Postgres, and we end up with a DBOS-inspired protocol in our own code at no extra cost. This is a happy accident of the three-state-layer architecture.

### `DurableHost` trait sketch

The shape that has to land regardless of which option we pick. This is what makes the choice reversible. Surface only — final names land in the implementation ticket.

```rust
#[async_trait]
pub trait DurableHost {
    /// Resume an agent from durable state, or start fresh if none.
    async fn open(&self, agent_root: &Path) -> Result<HostHandle>;

    /// Block until a trigger arrives or the deadline passes.
    /// Returns drained triggers in priority order (Human > External > Scheduled).
    async fn wait_for_work(&self, h: &HostHandle, deadline: Instant)
        -> Result<Vec<Trigger>>;

    /// Persist the result of a decided tick atomically: trigger drain consumed,
    /// scheduler cursor advanced, decision recorded. Either all or none.
    async fn commit_tick(&self, h: &HostHandle, tick: TickRecord) -> Result<()>;

    /// Externally injected signals (kernel API, future TUI writes).
    async fn signal(&self, agent_id: AgentId, t: Trigger) -> Result<()>;
}
```

Two impls to start:

- `SqliteDurableHost` — in-process, sqlite file per agent root.
- `MemoryDurableHost` — for tests; today's behavior, no durability. Stays around forever as the testing impl.

A third (`RestateDurableHost`) becomes a follow-up Project when we feel the need.

---

## 7. What this recommendation forces vs. punts

**Forces:**

- The `DurableHost` trait is written before any sqlite code; both impls are scaffolded in the same ticket.
- The structural-state DB (refinement 1) is sqlite from the same crate, separate file, separate module. Postgres is a future migration if/when multi-host arrives. Keeps the "one infra dep" story for users to "one sqlite file per agent root + one for the graph."
- Tool-call idempotency stays content-addressed; no per-call dedup key needed beyond what evidence-id already gives.
- Continue-as-new equivalent: not needed in-process — the in-memory state of `Agent::run` is bounded by tick size and we persist at every tick boundary. The whole class of "history length forces replay" concerns evaporates when there is no foreign workflow runtime to replay through.

**Punts (deliberately):**

- Distributed durability (multiple worker processes per agent, fan-out across machines). Out of scope until we have load that proves it's needed.
- Restate (or Temporal) migration. Becomes a Project when we know we need it; trait seam makes the future Project tractable.
- Continue-as-new equivalent at the Restate/Temporal layer (if we migrate). Their concern, not ours, when we get there.

---

## 8. Decisions needed before C1 implementation tickets

1. **Substrate choice.** Custom (lean), Restate, Temporal, DBOS-protocol-in-Rust, or "verify Restate Rust SDK before deciding." My recommendation is custom-with-trait-seam; happy to be argued out of it.
2. **DB engine for structural state.** Sqlite vs. Postgres. Sqlite is simpler for single-host; Postgres opens the door to multi-host structural reads later. Lean sqlite; revisit when we have a multi-host concern.
3. **Verification step before committing.** A short spike to either (a) verify Restate Rust SDK is what I think it is, (b) verify Temporal's Rust SDK status, or (c) confirm DBOS has no first-party Rust path — pick one to retire the biggest remaining uncertainty. Probably (a) since it's the swap target.
4. **Scope of C1's first ticket.** Just `DurableHost` trait + `SqliteDurableHost` + tests? Or also wire it into `Agent::run` end-to-end in the same ticket? Lean: the wiring is one ticket later, because the trait + impl is a coherent reviewable unit and the run-loop integration touches the agent code we don't want to revisit in the same PR.

---

## 9. References

- `VISION.md` § 4 ("open kernel, sovereign default"), § 5 (kernel concerns).
- `DEVELOPMENT.md` § 1 (Rust, no exceptions), § 2 (smallest correct diff — the trait seam is the cost we pay for reversibility).
- `scratch/agent_runtime.md` — the Temporal-assumed design; § 4 (the loop), § 5 (state placement), § 9 (continue-as-new).
- `scratch/post_bootstrap_followups_later.md` C1 (this is the doc C1 asked for), C2 (parent–child topology — depends on whatever this decides), C3 (snapshots — depends on durable substrate).
- Temporal Rust SDK docs: <https://docs.temporal.io/develop/rust/> (covers workflows, activities, workers, client, Nexus); repo: <https://github.com/temporalio/sdk-core/tree/master/crates/sdk>; API docs: <https://docs.rs/temporalio-sdk/latest/temporalio_sdk/>.
- Restate Rust SDK docs: <https://docs.rs/restate-sdk/latest/restate_sdk/> (v0.10.0; services, virtual objects, workflows, awakeables, timers, durable RPC, retries).
- DBOS Transact language coverage: Python + TypeScript only — no first-party Rust SDK (confirmed).

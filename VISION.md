# Vision

*An open, forkable substrate for continuously running autonomous research — a graph of subagents that read the world, reason about it, and keep a current model of any topic alive forever after.*

---

## 1. The Big Idea

Research today is treated as a query: someone asks a question, a process runs, an answer comes back, the process exits. This was the right shape when the constraint was retrieval — when the work was finding the relevant document, the relevant data point, the relevant precedent. **Retrieval is no longer the constraint.** Reasoning at scale is the constraint, and reasoning at scale doesn't fit inside a query.

The Jarvis Engine reframes research as a **continuously running process**: a graph of autonomous subagents, each with its own narrow mandate, its own tools, and its own state, that wake up when reality changes, do their work, hand their outputs to their parents, and contribute to a model of the topic that is never finished. The graph is the program. The graph is the state of the research. The graph is always live.

The engine is **domain-agnostic infrastructure**. A graph can be a public-equity thesis, a clinical trial pipeline, a piece of contested geopolitics, a piece of code under continuous review, a watch on a port nobody reads. The substrate is the same. Each subagent's mandate is narrow; stack enough of them and you have a model of any slice of the world that updates in real time and traces every claim to its evidence.

The Jarvis Engine is **open source by default and forkable by design**. We are not building a vertical product with a single hosted UI — we are building **the OS for autonomous research, in the spirit of Ubuntu**: a small, well-defined kernel; an opinionated default distribution; a connector and tool ecosystem deep enough to be useful; and a clean contract that a thousand applications can be built against. The substrate is the commons. The applications are where value reaches users.

---

## 2. The Problem

Every team that has tried to build serious autonomous-research tooling has hit the same four walls.

**Episodic, not continuous.** Today's agent frameworks were built around the request/response shape of LLM APIs. An agent "starts", does a thing, and exits. There is no notion of a long-lived process that survives across days, wakes when the world changes, and rejoins a collaborating team where it left off. State doesn't survive. Context doesn't accumulate. The world's ongoing motion is invisible to a process that only runs when summoned.

**Singular, not collaborative.** "Multi-agent" in 2026 mostly means: one orchestrator dispatches a few task-runners, they share a chat thread, they exit. There is no first-class notion of a graph of agents — each with its own durable state, its own narrow mandate, its own evidence base — communicating via structured signal over time. The dominant abstraction is the conversation. The right abstraction is the **society**.

**Ad hoc, not infrastructure.** Every team rebuilds the same plumbing — task queue, durable state, source connectors, evaluation harness, scheduling, observability, cost accounting, model routing, sandboxed tool execution, conflict resolution — from scratch, badly, and then ships a thin domain layer on top. The substrate is where the leverage is, and the substrate doesn't exist.

**Built for chat, not research.** The dominant UX of the agent era is the chat box. Real research is not a conversation — it is a long, branching, contradictory, self-correcting process whose unit of work is closer to a paper trail than a turn-taking dialogue. Tooling shaped like chat will lose to tooling shaped like the work.

The Jarvis Engine treats these four walls as design constraints. The substrate has to be continuous, collaborative, infrastructural, and shaped like research.

---

## 3. The Vision

A user — or another piece of software — instantiates a graph. The root is a goal: a thesis, a question, a watch, a topic. The root is itself an autonomous agent; its first act is to decompose its mandate and spawn children. Those children decompose theirs and spawn grandchildren. The graph fans out to whatever depth and width the work requires — every node an agent, every edge a parent–child relationship, no fixed shape imposed from above.

A subagent monitoring an industrial commodity is parsing futures curves and inventory reports. A subagent monitoring an FDA docket is polling an obscure regulatory feed and re-reading every adjacent submission. A subagent watching a piece of source code is running tests against every upstream dependency change. A subagent watching a regional conflict is correlating satellite imagery, bills of lading, and OSINT social signal. **Every node has the same shape**: a mandate, a set of tools (MCP-backed data fetchers and sandboxed execution environments), a private filesystem it reads and writes as continuous working state, and an output its mandate defines — a memo, a flag, a number, a recommendation, whatever the mandate calls for.

Outputs flow to parents. A parent's mandate is to receive its children's outputs, reconcile them, and produce its own output given its own mandate — the same shape, one level up. When children disagree, the parent owns the resolution: it picks a side or holds the disagreement open, and logs the call in a record the user can review retroactively. The root's output — the model of the topic the user reads — is **always current**, **always sourced**, **always inspectable**. A user who has not opened the platform in a week sees the present state of the world relative to their question — not last week's snapshot. When something material moves at any node, the change ripples up; the affected output is rewritten and surfaced; whoever is subscribed is paged on the conditions they armed.

The engine handles the grind. **It runs millions of subagents.** It schedules them, wakes them, retires them, captures their reasoning, and exposes the whole live state to whatever application is consuming it. The application is responsible for the experience. The engine is responsible for the substrate.

This is what the engine looks like in production: a country of geniuses, each one watching their corner of reality, all communicating in structured signal, all answerable to the same underlying question, all running at all times, at a scale where no single human team could.

---

## 4. Core Principles

**The graph is the program.** The unit of computation is not a request, not a function, not a session. It is a graph of agents with durable state. Programs in this paradigm are written *as graphs* — by humans in a UI, or by other agents instantiating subgraphs. The graph has unlimited depth and width; structure follows the work, not a fixed taxonomy.

**Continuous, not episodic.** Agents don't end. They idle, they wake, they run, they idle again. The runtime is built around long-lived processes, not request handlers.

**Atomic monitorability.** Every agent has a single, narrow mandate. If a mandate can't be answered with confidence, the agent decomposes it and spawns children. The graph is fractal; depth is cheap.

**Every agent has a filesystem.** An agent's state is not a hidden context window — it is a private, durable filesystem the agent reads and writes throughout its life. Notes, code, scratch, distilled memories, prior reasoning, partial drafts, anything that supports continuity. Agents are programs that mutate their own working directories; the filesystem is what makes their state survive across wakeups, inspectable to humans, and forkable.

**Data flows through MCP.** Every tool that fetches data from the outside world is an MCP server. We do not maintain a parallel connector framework. The MCP ecosystem is our connector ecosystem; we adopt the standard, contribute back, and stay out of the business of inventing one.

**Provenance by construction.** Every claim in any output traces to a node. Every node's output traces to evidence. Every piece of evidence is timestamped, sourced, and inspectable. There is no path through the engine that produces a claim without a trail.

**Conflicts are resolved by parents and audited by humans.** When two children of a node draw conflicting conclusions, the parent owns the resolution. The parent picks a side or holds the disagreement open, and writes the decision into a conflict log. The log is retroactively reviewable; a human can override the parent's resolution at any time, and the override propagates.

**The human is in the kernel, not the application.** The graph is collaborative between the agent society and a human architect. Override, injection, dispute, and re-decomposition are kernel primitives — available at every node, not bolted on by individual applications. The substrate is opinionated about leaving the human's voice intact across every refresh.

**Open kernel, sovereign default.** The kernel is small and forkable. MCP servers, agent kinds, schedulers, and execution environments are extension points, not core code. The engine is built to be deployed and operated by its users, with their data, on their compute. Hosted offerings exist as conveniences, not as gatekeepers.

---

## 5. The Engine, Concretely

The engine is composed of eight loosely coupled layers. Most of the surface area is extension; the kernel is intentionally small.

**Kernel.** The core runtime: process model for long-lived agents, scheduling, durable state, message bus, lifecycle management, fault tolerance. The kernel does not know what an "investment thesis" is or what a "molecule" is. It knows about graphs, agents, mandates, ticks, messages, files, and parent–child relationships.

**Graph layer.** The data model: nodes, edges, mandates, outputs, source trails. Versioned and time-scrubbable. A snapshot is a complete description of a research process — durable, replicable, forkable, and inclusive of every agent's filesystem. Human operations on the graph — override, injection, dispute, re-decomposition — are first-class mutations alongside agent-driven changes.

**Agent runtime.** The primitive for running a node. Handles mandate execution, model routing (which model for which mandate), prompt assembly, tool dispatch, retry and reflection logic, cost accounting, and per-agent evaluation hooks. An agent's job is to operate over its mandate and its filesystem, call its tools, and produce an output.

**Per-agent filesystem.** Every agent has a private filesystem — its working memory across wakeups. Notes, code, scratch files, distilled memories, partial drafts. The filesystem is durable, versioned, inspectable by humans, and survives the entire life of the agent. Snapshots include it; forks copy it; humans can read and edit it directly. State as files, not as hidden context.

**Data layer (MCP-native).** The interface to the world is MCP. News feeds, filings, market data, scientific journals, government feeds, satellite imagery, sensor streams, code repos, web search — every data-fetching tool an agent uses is exposed as an MCP server. The engine adds rate-limiting, deduplication, caching, and auth across MCP traffic at scale. The connector ecosystem is the MCP ecosystem; we contribute upstream rather than fork.

**Execution and tool layer.** The substrate for action and computation, separate from data fetching. Sandboxed code interpreters, REPLs, simulators, headless browsers, statistical environments, retrieval indices, structured query engines. Agents reach for these the way humans reach for a calculator or a notebook; the engine ensures they are sandboxed, accounted for, and fast to start.

**Observability and audit.** Every action, every claim, every state transition is captured. The engine ships with an observability surface designed for *correctness in research*, not just system health: per-node calibration metrics, per-claim provenance graphs, per-agent track record over time, and full conflict-log replay.

**Application API.** The contract that applications build against — stable, versioned, language-agnostic. An application is a UI plus a curated set of MCP servers and node templates on top of the same kernel.

---

## 6. The Applications Above

The engine is the substrate; the applications are the verticals where the value reaches a user. We design the engine so that any of these is implementable as a thin layer on top.

**Capital markets research.** The original motivating application: a fund's analysts and PMs work inside a graph for every name, every macro thesis, every sector.

**OSINT and national-security analysis.** Governments, think tanks, and intelligence services run graphs over geopolitical situations, conflict zones, foreign technology programs, and supply-chain risks. The substrate's capacity for continuous monitoring against an enormous and heterogeneous source base is the core value.

**Medical and scientific research.** Living graphs over disease areas, drug pipelines, mechanism hypotheses, and trial readouts. One subagent runs a literature watch over a single mechanism; another re-analyses a trial as new data drops; another runs simulations in a code sandbox and hands its results to its parent.

**Legal, regulatory, and policy monitoring.** A graph over a legislative or litigation domain, with subagents tracking statutes, dockets, comment periods, and case law. The root output is a continuously current memo on the regulatory state of play.

**Software and supply-chain surveillance.** A graph over a codebase or a dependency tree, with subagents watching upstream changes, security advisories, performance regressions, and contributor activity. The root output is the live health of a system.

These are not separate products. They are dialects of one substrate. Each chooses its MCP servers, node templates, conflict-resolution policies, and UI. The kernel is shared; investment in performance, durability, and reliability compounds across all of them.

---

## 7. The Performance Frontier

A platform that runs ten subagents per graph is a demo. A platform that runs ten thousand is a research tool. **A platform that runs millions, continuously, durably, at controllable cost — that is infrastructure.** The engineering ambition of the Jarvis Engine is to operate at that frontier and drag the rest of the field forward.

The hard problems are not novel in isolation; they are novel in combination at this scale.

- **Scheduling.** Most subagents are idle most of the time. The engine wakes the right agents at the right cadence, prioritizes across millions of pending ticks, and respects cost budgets that span a single graph or an entire tenant's estate.
- **Inference economics.** Inference is the dominant runtime cost. The engine caches aggressively, batches across siblings, distills hot paths, and routes each mandate to the cheapest model that meets the bar.
- **State durability and traffic multiplexing.** Millions of agents with rich, versioned, time-scrubbable filesystems demand storage that is fast on the hot path and cheap on the cold path. MCP traffic must be deduplicated and routed so that thousands of agents reading the same source impose one fetch, not thousands.

These problems do not get solved by stacking more frameworks on top of each other. They get solved by treating the engine as a serious systems project — written in languages that respect performance, with profiling and tracing baked in, with a kernel small enough to be optimized end-to-end. The Jarvis Engine is an invitation to performance engineers to do their best work on a workload that did not exist five years ago.

---

## 8. Why Open Source

The substrate that becomes the standard wins. Linux for servers. Postgres for transactional data. Kubernetes for orchestration. Each won because it was open, forkable, and good enough early enough that an ecosystem chose it before alternatives could ossify.

Autonomous research is at exactly that moment. Either an open substrate emerges and wins — or every fund, every government, every hospital, every research org builds its own closed one, badly, in parallel, for a decade. The cost of the second outcome is enormous, both in duplicated effort and in lost interoperability.

**Open source is also the trust layer.** A government will not run national-security analysis on a hosted black box. A hospital will not run patient-relevant research on a vendor's opaque cloud. A hedge fund will not let its mental model live on someone else's servers. The serious users of this technology will demand the right to fork, deploy, audit, and modify. We give them that, by default, from day one.

**Open source is how the connector ecosystem gets built.** Every source the engine connects to is somebody's specialty. The N×M problem of connectors is unsolvable by any single team and trivially solvable by a community where each contributor cares about a sliver. Postgres did not write every extension; the ecosystem did.

The model is **Ubuntu**, not just Linux: an opinionated distribution of an open kernel, with a serious upstream commitment, a clean default experience, and commercial offerings around — but never gating — the core. The kernel is the commons. The ecosystem is the moat.

---

## 9. What Makes This Different

**Versus agent frameworks (LangChain, LlamaIndex, Crew, AutoGen).** Those are libraries for assembling one agent inside an application. The Jarvis Engine is an OS for running millions of them, durably, indefinitely, with shared infrastructure for scheduling, state, observability, and cost. Different layer, different problem.

**Versus vendor agent platforms (OpenAI Assistants, Anthropic Agent SDK, vendor "agent clouds").** Those are hosted, opaque, episodic, and tied to a single model vendor. The engine is open, transparent, continuous, and model-agnostic. We treat model vendors as substitutable suppliers, not as the substrate.

**Versus single-application vertical AI tools.** Specialized research copilots in finance, medicine, and law are domain-specific products that internally reinvent the substrate. We are the substrate they should have been built on. As they realize this, they become applications on top.

**Versus quant systems and HFT infrastructure.** Those are deterministic compute graphs over numerical data. The engine is a generative reasoning graph over heterogeneous evidence. The two are complements: numerical signal feeds the engine; the engine produces structured views the numerical systems can act on.

**Versus building it yourself.** Every serious research org will be tempted to build a smaller, shallower version of this internally. They will build it inward-facing, single-tenant, and uneven — and then maintain it forever. The defensibility for an open substrate is that it gets better than any internal effort can, because every external user contributes back. The internal version's first day is its best day; the open substrate's first day is its worst.

---

## 10. Why Now

Five things changed in the last 24 months.

**Reasoning models that decompose.** Turning a sentence into a 200-node graph used to be a human-only task. It is no longer.

**Agentic tool use that holds, on a standard.** Long chains of tool use across many steps now hold together long enough to be put in production — and with MCP emerging as the de facto way for agents to talk to tools and data, the connector ecosystem has a substrate to grow on instead of fragmenting across vendor frameworks.

**Inference cost curves.** Continuous monitoring of millions of agents was financially absurd in 2023. It is becoming affordable. The cost curve continues to bend in the direction of *thinking continuously* over *thinking on demand*.

**Open weights at production quality.** The engine cannot be vendor-locked. It needs to run on a sovereign deployment with whatever model that user trusts. Open-weight models at frontier quality are the unlock that makes this realistic.

**Demand from every direction.** Funds, governments, hospitals, research labs, and operators of every flavor are arriving at the same realization at the same time: the work is now bottlenecked on *structured reasoning at scale*, and the tooling does not exist. The market for an open substrate is being formed in front of us.

The window to set the canonical substrate is narrow. Closed alternatives are racing to lock in. The chance to set the open standard exists today and probably for two more years; after that, ecosystems calcify.

---

## 11. The Endgame

In three years, the Jarvis Engine is the substrate for autonomous research the way Linux is the substrate for servers and Postgres is the substrate for serious data. Every serious research organization runs a fork — public, private, sovereign — against their own connectors and their own models, with their own people as architects of their own graphs.

In five years, every nontrivial question in the institutions that pay for being right — funds, agencies, ministries, labs, hospitals — has a graph behind it that has been running for months. The graphs are the institutional memory. The memos write themselves. The humans are the architects, the skeptics, and the editors.

The thesis is not that we replace the analyst, the strategist, the doctor, the case officer, the engineer. The thesis is that we **collapse the distance between a question and a defensible, current, sourced view** — from weeks, to hours, to minutes — and we keep that view alive forever after, contributed to by a society of subagents that never sleeps.

Whoever owns the substrate that does this owns the next decade of how serious institutions know what is true. We intend for the substrate to be open, and we intend for it to be ours to build.

---

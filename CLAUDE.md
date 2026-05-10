# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repo state

This repository is **pre-code**. There is no Rust crate yet — only `VISION.md` (the product/architecture vision), `DEVELOPMENT.md` (binding rules for any agent working here), and `scratch/` (ideation surface). Treat new work as bootstrapping; do not assume a `Cargo.toml`, build commands, or directory layout exists until you confirm by reading the tree.

## Non-negotiable workflow rules (from DEVELOPMENT.md)

These override any default behavior. Read `DEVELOPMENT.md` in full before non-trivial work; key points:

- **Language is Rust.** Stable toolchain. Code must pass `cargo build`, `cargo clippy -- -D warnings`, and `cargo fmt --check` before any change is "done." No other languages without explicit maintainer approval.
- **Smallest correct diff.** No drive-by refactors, new abstractions, feature flags, or renames outside the task. Note out-of-scope issues in the summary instead of silently fixing them. If scope must expand, **stop and ask**.
- **Tests ship with the change.** Written before/alongside, runnable via a single `cargo test`. Cover happy path, edge cases, and at least one failure mode per public-facing behavior. Unit tests next to code (`#[cfg(test)] mod tests`); integration tests in `tests/`.
- **End every task with a structured summary**: what changed, why, tests added, what was deliberately not done, follow-ups noticed, and the exact verification commands run with results.
- **Ideation goes in `scratch/<topic>.md`** — never in top-level docs, never lost to chat. When a scratch plan produces Linear tickets, paste the relevant section into each ticket so reviewers see the reasoning.

## Feature workflow: Linear-driven, planning before code

For anything larger than a trivial one-shot edit, the **first job is planning, not coding**. All work lives under the Linear team **"Jarvis Engine"**.

1. **Decompose into Linear tickets** sized to the request, **writing tickets in parallel** (or reusing existing ones if they already cover the work — check before filing):
   - Large (~10+ sub-tickets) → Linear **Project** holding the spec, with child issues.
   - Medium (~3–10 sub-tickets, one session) → **parent issue with sub-issues**.
   - Single task → **one issue**, no project, no parent.
   Sub-tickets that are themselves multi-step become parent issues with their own sub-issues.
2. Each ticket needs: clear title; description with goal, acceptance criteria, in-scope, explicitly-out-of-scope, dependencies; effort estimate. Order by dependency. Post the breakdown back to the maintainer (project if any, then ticket IDs/titles/one-line scopes).
3. **Stop and wait for review of the ticket set.** Do not implement until the maintainer has reviewed the breakdown and says go.
4. Once approved, **execute the tickets in parallel via subagents** — one subagent per ticket (or per independent batch), each applying the rules above against its own ticket. Sequence only across true dependency edges; siblings run concurrently. The default loop is *plan all tickets → approve → parallel execution → review*.

The Linear MCP server is the mechanism for filing tickets. If it is not configured, ask the maintainer to configure it or post the breakdown as text — do not skip the planning step.

## Architectural orientation (from VISION.md)

When designing or reviewing code, keep the engine's intended shape in mind — it constrains what abstractions belong in the kernel vs. in extensions:

- The product is an **OS for autonomous research**: a runtime for graphs of long-lived agents that wake on signal, do narrow work, and feed outputs to their parents. The graph — not the request — is the unit of computation.
- The kernel is **intentionally small**. It knows about graphs, agents, mandates, ticks, messages, files, and parent–child relationships. It does **not** know about any vertical (finance, OSINT, medicine, etc.) — those live in applications above the Application API.
- Eight loose layers, kernel inward to application outward: **Kernel** (process model, scheduling, durable state, message bus, lifecycle) · **Graph layer** (versioned, time-scrubbable nodes/edges/mandates/outputs/source trails; human override is a first-class mutation) · **Agent runtime** (mandate execution, model routing, tool dispatch, retries, cost accounting) · **Per-agent filesystem** (durable, versioned, inspectable working memory across wakeups — state is files, not hidden context) · **Data layer** (MCP-native — every external data fetcher is an MCP server; engine adds rate-limit/dedup/cache/auth) · **Execution & tool layer** (sandboxed code/REPLs/browsers, separate from data fetching) · **Observability & audit** (per-claim provenance, per-node calibration, conflict-log replay) · **Application API** (stable, versioned, language-agnostic contract).
- Load-bearing principles for design choices: continuous (not episodic) processes; atomic monitorability (narrow mandates, decompose when uncertain); **provenance by construction** (no claim without a trail to evidence); conflicts resolved by parents and human-overridable; MCP is the connector ecosystem (don't build a parallel one); open kernel, sovereign default (must be deployable on user compute with their models).
- The performance target is **millions of subagents continuously**, not tens. Scheduling, inference economics (caching, sibling batching, model routing), and MCP traffic multiplexing are first-class kernel concerns rather than afterthoughts.

When in doubt about whether something belongs in the kernel: if a finance app and a clinical-trial app would both need it in the same shape, it's kernel; if either could swap it out, it's extension.

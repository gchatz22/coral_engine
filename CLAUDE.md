# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repo state

Cargo workspace at the root. Members live under `crates/` (currently `coral_node`, `coral_temporal`, `coral_graph`); confirm the current set by reading `Cargo.toml` and `crates/` before assuming a layout. `VISION.md` holds the product/architecture vision, `DEVELOPMENT.md` the binding workflow rules, `scratch/` the ideation surface.

## Non-negotiable workflow rules (from DEVELOPMENT.md)

These override any default behavior. Read `DEVELOPMENT.md` in full before non-trivial work; key points:

- **Language is Rust.** Stable toolchain. Code must pass `cargo build`, `cargo clippy -- -D warnings`, and `cargo fmt --check` before any change is "done." No other languages without explicit maintainer approval.
- **Smallest correct diff.** No drive-by refactors, new abstractions, feature flags, or renames outside the task. Note out-of-scope issues in the summary instead of silently fixing them. If scope must expand, **stop and ask**.
- **Tests ship with the change.** Written before/alongside, runnable via a single `cargo test`. Cover happy path, edge cases, and at least one failure mode per public-facing behavior. Unit tests next to code (`#[cfg(test)] mod tests`); integration tests in `tests/`.
- **End every task with a structured summary**: what changed, why, tests added, what was deliberately not done, follow-ups noticed, and the exact verification commands run with results.
- **Ideation goes in `scratch/<topic>.md`** — never in top-level docs, never lost to chat. When a scratch plan produces GitHub issues, paste the relevant section into each issue so reviewers see the reasoning.

## Comment policy

The codebase has accumulated ~700 references to issue-tracker IDs in comments and several files where comment lines outnumber code. This is treated as a defect, not a style preference. New code must follow these rules; when editing existing code, strip violations you encounter in the lines you touch (this is in-scope for any change, not a drive-by refactor).

- **Default to no comments.** A comment is justified only when the *why* is non-obvious — a hidden constraint, a subtle invariant, a workaround for a specific bug, behavior that would surprise a reader. If removing the comment wouldn't confuse a future reader, don't write it. Never explain *what* well-named code already says.
- **When a comment is genuinely warranted, write it.** "Default to no comments" is about cutting noise, not about leaving readers stranded in front of subtle code. If you judge that a future reader (including you) will hit a real "wait, why?" moment without an explanation, add the comment — keep it short, focused on the *why*, and put it next to the surprising line. The bar is "this saves a future reader a non-trivial investigation," not "this might possibly be useful." Err on the side of omitting; but when in doubt and the reasoning is load-bearing, write it.
- **No issue-tracker IDs in source code, ever.** No GitHub issue or PR numbers (`#123`, `GH-123`), no legacy Linear IDs (`JAR2-NN`), no "Stage X.Y", no "added for issue …", no "see #… for context" in `//`, `///`, `//!`, doc comments, module headers, identifiers, log strings, or test names. Issues belong in the commit message and PR description, which git already binds to the diff. Code outlives issues; the reference rots and the comment becomes noise. The same rule applies to phrasings like "previously …", "before #XX …", "now that …" — write the code as it is, not as a story of how it got here.
- **No narration of history or process** in comments: not "removed X", not "renamed from Y", not "this used to …", not "TODO from the previous PR". Use `git log`/`git blame` for history.
- **Doc comments (`///`, `//!`)** are for the public contract of an item — what it does, inputs/outputs, invariants, panics, examples. They are not a place to explain implementation history, related tickets, or which stage of a roadmap introduced them. Keep them short; one paragraph is usually enough, multi-section module headers almost never are.
- **Inline `//` comments** should be rare and one line. If you find yourself writing a paragraph, the code probably needs to be clearer or the explanation belongs in a doc comment on the surrounding item.

If a comment seems necessary but you can rewrite the code (rename, extract, restructure) so the comment becomes redundant, do that instead.

## Feature workflow: GitHub-Issues-driven, planning before code

For anything larger than a trivial one-shot edit, the **first job is planning, not coding**. All work is tracked as **GitHub issues** in the `gchatz22/coral_engine` repo.

1. **Decompose into GitHub issues** sized to the request, **writing issues in parallel** (or reusing existing ones if they already cover the work — check before filing):
   - Large (~10+ sub-issues) → a **GitHub Project (v2) board** holding the spec, with child issues tracked on it.
   - Medium (~3–10 sub-issues, one session) → a **parent issue with native sub-issues** (the parent shows the sub-issue progress bar).
   - Single task → **one issue**, no project, no parent.
   Sub-issues that are themselves multi-step become parent issues with their own sub-issues nested underneath.
2. Each issue needs: clear title; body with goal, acceptance criteria, in-scope, explicitly-out-of-scope, dependencies; effort estimate (as a label or body line). Order by dependency. Post the breakdown back to the maintainer (project if any, then issue numbers/titles/one-line scopes).
3. **Stop and wait for review of the issue set.** Do not implement until the maintainer has reviewed the breakdown and says go.
4. Once approved, **execute the issues in parallel via subagents** — one subagent per issue (or per independent batch), each applying the rules above against its own issue. Sequence only across true dependency edges; siblings run concurrently. The default loop is *plan all issues → approve → parallel execution → review*.

Issues are filed with the **`gh` CLI** (`gh issue create`, `gh issue edit`, `gh issue develop` to start a linked branch). Sub-issue nesting and Project-board placement aren't yet first-class `gh` subcommands — link a sub-issue to its parent and add cards to a Project board via the GitHub UI or `gh api` GraphQL. `gh` is already authenticated in this repo; if it is not, ask the maintainer to run `gh auth login` rather than skip the planning step. See `DEVELOPMENT.md` for the full workflow, status board, and stacked-PR rules.

## Architectural orientation (from VISION.md)

When designing or reviewing code, keep the engine's intended shape in mind — it constrains what abstractions belong in the kernel vs. in extensions:

- The product is an **OS for autonomous research**: a runtime for graphs of long-lived agents that wake on signal, do narrow work, and feed outputs to their parents. The graph — not the request — is the unit of computation.
- The kernel is **intentionally small**. It knows about graphs, agents, mandates, ticks, messages, files, and parent–child relationships. It does **not** know about any vertical (finance, OSINT, medicine, etc.) — those live in applications above the Application API.
- Eight loose layers, kernel inward to application outward: **Kernel** (process model, scheduling, durable state, message bus, lifecycle) · **Graph layer** (versioned, time-scrubbable nodes/edges/mandates/outputs/source trails; human override is a first-class mutation) · **Agent runtime** (mandate execution, model routing, tool dispatch, retries, cost accounting) · **Per-agent filesystem** (durable, versioned, inspectable working memory across wakeups — state is files, not hidden context) · **Data layer** (MCP-native — every external data fetcher is an MCP server; engine adds rate-limit/dedup/cache/auth) · **Execution & tool layer** (sandboxed code/REPLs/browsers, separate from data fetching) · **Observability & audit** (per-claim provenance, per-node calibration, conflict-log replay) · **Application API** (stable, versioned, language-agnostic contract).
- Load-bearing principles for design choices: continuous (not episodic) processes; atomic monitorability (narrow mandates, decompose when uncertain); **provenance by construction** (no claim without a trail to evidence); conflicts resolved by parents and human-overridable; MCP is the connector ecosystem (don't build a parallel one); open kernel, sovereign default (must be deployable on user compute with their models).
- The performance target is **millions of subagents continuously**, not tens. Scheduling, inference economics (caching, sibling batching, model routing), and MCP traffic multiplexing are first-class kernel concerns rather than afterthoughts.

When in doubt about whether something belongs in the kernel: if a finance app and a clinical-trial app would both need it in the same shape, it's kernel; if either could swap it out, it's extension.

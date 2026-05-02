# Development Rules

These rules apply to **every agent** working on the Jarvis Engine. Read them before touching the repo. Re-read them before declaring work done.

---

## 1. Language: Rust

The Jarvis Engine is written in **Rust**. No exceptions without explicit approval from the maintainer.

- Use stable Rust unless a feature has a documented, justified reason to require nightly.
- Prefer the standard library and well-established crates over hand-rolled abstractions.
- Code must compile clean (`cargo build`) and pass `cargo clippy -- -D warnings` and `cargo fmt --check` before any change is declared complete.

## 2. Minimal, Scoped Diffs

When asked to implement a feature, deliver **the smallest correct diff** that satisfies the request. Nothing more.

- Do not refactor adjacent code that is not part of the task.
- Do not introduce new abstractions, traits, or modules unless the task literally cannot be completed without them.
- Do not add configuration, feature flags, or extension points for hypothetical future needs.
- Do not rename, reformat, or "clean up" unrelated code in the same change.
- If you discover something that needs fixing outside the scope of the task, **note it in the summary** — do not silently expand the diff.

If you genuinely believe scope must expand to do the task correctly, **stop and ask** before writing the larger change.

## 3. Test-First Mindset

Every change ships with tests. No test-free PRs.

- Write tests **before or alongside** the implementation, not as an afterthought.
- Tests must be **easily runnable**: a single `cargo test` (or a clearly documented command) must run them with no extra setup.
- Cover the happy path, the obvious edge cases, and at least one failure mode per public-facing behavior.
- Prefer fast, deterministic unit tests. Use integration tests where the unit boundary is artificial.
- Tests live next to the code they test (Rust convention: `#[cfg(test)] mod tests` or `tests/` directory for integration).
- A change is **not done** until its tests pass locally and the existing test suite is still green.

## 4. Comprehensive Summary After Changes

Every completed task ends with a summary the maintainer can read in under a minute and trust without re-reading the diff. The summary must include:

1. **What changed** — files touched, the shape of the change, key functions or types added.
2. **Why** — the rationale tying the change back to the requested feature.
3. **Tests added or updated** — what they cover and how to run them.
4. **What was deliberately not done** — anything in the neighborhood that could have been changed but was left alone, and why.
5. **Follow-ups noticed** — out-of-scope issues observed during the work, listed for the maintainer to triage (not silently fixed).
6. **Verification** — exact commands run and their results (`cargo build`, `cargo test`, `cargo clippy`).

## 5. Ideation and Planning: Scratch Markdown

When the maintainer is **ideating, planning, or designing** (anything before tickets are filed and code is written), capture the thinking in a markdown file under `scratch/`.

- One file per topic: `scratch/<topic>.md` (e.g. `scratch/backend-architecture.md`, `scratch/scheduler-design.md`).
- Use it as the working surface for proposals, tradeoffs, open questions, and decisions.
- Update the file as the conversation evolves. Do not lose context to chat scrollback.
- `scratch/` is for thinking, not deliverables. Once a plan crystallizes into Linear tickets, the tickets become the source of truth and the scratch file can be archived or deleted.
- Do not put scratch files anywhere else in the repo. Do not commit ideation noise into top-level docs.
- **Attach the relevant plan to the Linear tickets it produced.** When tickets are filed, every ticket (project, parent, or sub-issue) must carry the slice of the scratch plan that motivates it — paste the relevant section into the ticket description, or attach/link the `scratch/<topic>.md` file. A reviewer reading only the ticket should see the reasoning, not just the acceptance criteria. For Projects, attach the full plan to the project page; for sub-issues, include the section specific to that unit of work.

## 6. Feature Workflow: Linear-Driven, Staged Execution

When the maintainer asks for a **feature** (anything larger than a trivial one-shot edit), the agent's first job is **planning, not coding**.

### Step 1 — Decompose into Linear tickets

All work lives under team **"Jarvis Engine"**. Pick the right grouping primitive based on the size of the request:

- **Large feature (~10+ sub-tickets, big effort)** → create a **Linear Project**. The project page holds the spec, target date, and the progress bar across all child issues. Every sub-ticket is created as an issue inside that project.
- **Medium feature (~3–10 sub-tickets, lighter effort, a session)** → create a **parent issue with sub-issues**. The parent issue holds the spec; sub-issues show as a checklist with live progress on the parent. No project needed.
- **Single task (one-shot, fits in one reviewable diff)** → create a **single issue**. No project, no parent. Just the ticket.

If a sub-ticket inside a Project is itself multi-step, make it a parent issue with its own sub-issues underneath it — Projects and parent/sub-issues compose.

Break work into the smallest meaningful, **independently reviewable** units. Each ticket (project, parent, sub-issue, or standalone) must include:

- A clear, specific title.
- A description with: goal, acceptance criteria, in-scope items, explicitly out-of-scope items, and dependencies on other tickets.
- A reasonable effort estimate.

Order tickets so that dependencies come first. Note ticket-to-ticket dependencies in the descriptions. After creating the tickets, post a summary back to the maintainer: the project (if any), then each ticket with its ID, title, and one-line scope.

### Step 2 — Stop and wait

Do **not** start implementing any ticket until the maintainer explicitly says so. The maintainer reviews the ticket breakdown, may revise it, and then prompts the agent to attack a specific ticket.

### Step 3 — Execute one ticket at a time

When prompted to work a ticket:

- Re-read the ticket. Confirm the scope.
- Apply rules 1–4 above (Rust, minimal diff, tests first, comprehensive summary).
- Update the ticket status as work progresses (in progress → in review → done).
- Deliver the change and the summary, then **stop**. Do not roll on to the next ticket. Wait for the maintainer's approval and the next prompt.

This staged loop — *plan → approve → one ticket → review → next ticket* — is the default. Skip it only when the maintainer explicitly says "just do it."

### Linear setup note

The Linear MCP server must be configured for an agent to create tickets. If it is not, the agent should ask the maintainer to configure it (or post the proposed ticket breakdown as text for the maintainer to file) rather than skip the planning step.

---

## TL;DR

1. Rust.
2. Smallest correct diff.
3. Tests with the change, runnable in one command.
4. Summary at the end the maintainer can trust.
5. Ideation/planning → `scratch/<topic>.md`.
6. Features → Linear tickets under "Jarvis Engine" → wait → one ticket at a time.

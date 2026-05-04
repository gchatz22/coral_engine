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
- Work on a dedicated branch named after the ticket (e.g. `jar-123-short-slug`). Never commit ticket work directly to `main`.
- Update the ticket status as work progresses (in progress → in review → done).
- **Every ticket ends as a Pull Request.** When the change is ready, push the branch and open a PR whose description links the Linear ticket, restates the acceptance criteria, and includes the structured summary from rule 4. One ticket = one branch = one PR. No ticket is "done" until its PR is open and linked back to the ticket; merge happens only after maintainer review.
- **Dependent tickets ship as stacked PRs, managed with Graphite.** See *Stacked PRs — use Graphite* below. When a ticket depends on another ticket still in review, the new branch goes on top of the dependency via `gt create`, not branched manually off `main`. Each PR in the stack stays small and reviewable on its own; the description must call out the stack order and the parent PR. Do not bundle dependent tickets into one mega-PR to avoid stacking.
- Deliver the change, open the PR (or `gt submit` for a stack), and then **stop**. Do not roll on to the next ticket. Wait for the maintainer's approval and the next prompt.

This staged loop — *plan → approve → one ticket → branch → PR → review → next ticket* — is the default. Skip it only when the maintainer explicitly says "just do it."

### Stacked PRs — use Graphite

When a ticket depends on another ticket that is still in review (not yet merged to `main`), the work goes on top of an open PR — a **stacked PR**. Manage all stacks with the **Graphite CLI (`gt`)**. Do not stack manually with `git checkout -b` and PR retargeting; do not click "Update branch" in the GitHub UI. Both create cascade-rebase work that Graphite avoids.

**Why:** every merge to `main` rewrites commit SHAs (rebase-merge rewrites the committer; squash-merge collapses commits). Children's view of "what was the parent commit" diverges from `main` on every merge, producing spurious file-by-file conflicts. Graphite tracks the stack as metadata, runs the cascade-rebase automatically when a parent merges, and force-pushes children with `--force-with-lease`. We learned this the hard way; do not relearn it.

**Setup (once):**
- `brew install withgraphite/tap/graphite`
- `gt auth --token <token>` (token from app.graphite.dev → Settings → CLI)
- `gt init` inside the repo to register `main` as the trunk

**Daily usage:**
- Start a stacked branch on top of the current one: `gt create <branch-name> -m "<commit message>"`. Edits + commits proceed normally.
- Push the whole stack and open one PR per branch: `gt submit --stack`. PR bases are wired to the right parent automatically.
- After a parent PR merges to `main`: follow the cascade recipe below.
- Inspect the stack: `gt log` (current stack) or `gt log long` (full forest).

**Recipe: a parent PR just merged — cascade the children.**

In a clean single-worktree setup `gt sync` does this end-to-end. When children live in separate worktrees (our normal mode), gt cannot touch a checked-out branch, so the recipe is four steps:

1. **Remove the merged worktree first**, before any sync. If gt sees the merged branch still checked out, it refuses to clean it up and the cascade stalls. Use `--force` if `target/` build artifacts block removal:
   ```
   git worktree remove --force ../jarvis-<merged-slug>
   git branch -D <merged-slug>
   ```
2. **`gt sync`** from any worktree (the main repo is fine). This pulls trunk, untracks the merged branch, and re-parents children onto trunk in metadata. It will *attempt* to restack each child but will skip ones that are checked out elsewhere — that's expected.
3. **For each affected child worktree**, `cd` in and run `gt restack`. If a real merge conflict appears (typically `Cargo.toml` dep additions or `src/lib.rs` `pub mod` lines colliding), fix the file, `git add <file>`, `gt continue`.
4. **Push:** `git push --force-with-lease origin <branch>` (or `gt submit --force` once the local state is clean).

**What not to do:**
- Do not `git rebase` or `git push --force` by hand for routine cascades — `gt` owns those operations. The exception is the push step in the recipe above, where `git push --force-with-lease` is the cleanest tool.
- Do not click "Update branch" on a stacked PR in the GitHub UI — it creates a merge commit that Graphite then has to clean up.
- Do not change a stacked PR's base via the GitHub UI — `gt sync` and `gt submit` keep bases correct.

**Two gotchas to know:**
- **`gt submit` blocks with "fetched then tracked" warning.** If the local branch was rewritten (rebased, conflict-resolved) and the remote still has the old version, gt blocks the submit out of caution. `git push --force-with-lease` is fine here because you know exactly what changed; long-term `gt submit --force` is the gt-native equivalent.
- **`gt` saying "no restack needed" can be misleading.** If the local branch is already on top of the right trunk tip but the remote PR still shows `[BEHIND]` on GitHub, gt is right that local is current — but you still need to push. The GitHub PR state is the signal, not gt's restack check.

Graphite composes with worktrees: when running parallel agents, each worktree runs its own `gt` commands on the branch it owns.

### Parallel agents — use worktrees freely

When multiple agents are working tickets concurrently, use **git worktrees** without asking. One worktree per agent per ticket keeps branches, build artifacts, and uncommitted state fully isolated, and avoids agents stomping each other's working tree.

**Spawn a worktree per ticket:**
- For a ticket branched off `main`: `git worktree add ../jarvis-<ticket-slug> -b <ticket-slug> main`.
- For a stacked ticket on top of an open PR's branch: `git worktree add ../jarvis-<ticket-slug> -b <ticket-slug> <parent-branch>`.
- Inside the new worktree, immediately register with Graphite: `gt track --parent <parent-branch>` (use `main` for unstacked).
- Do all the ticket's work — edits, `cargo build`, `cargo test`, commits, `gt submit`/`gt submit --stack` — inside that worktree.
- Worktrees are cheap. Prefer creating one over coordinating shared checkout state.

**Clean up after a PR merges:**
1. From the worktree: `gt sync` — pulls trunk, cascades the rest of the stack, and (if Graphite recognises the merge) drops the merged branch from the stack.
2. Remove the worktree: `git worktree remove ../jarvis-<ticket-slug>`. Use `--force` only if there's intentional uncommitted state worth nuking.
3. Delete the local branch if it lingers: `git branch -D <ticket-slug>`. ("Branch already merged" is fine — `-D` skips the safety check, which is correct here because the on-main commit has a different SHA than your local branch tip.)
4. Delete the origin branch if it lingers: `git push origin --delete <ticket-slug>`. **Better:** enable repo Settings → General → Pull Requests → "Automatically delete head branches" so GitHub does this for every merged PR. Then this step is unneeded.

**Audit at any time:** `git worktree list`, `git branch -vv | grep <prefix>`, `gt log long`. If you see a branch whose PR is closed/merged but whose worktree is still around, clean it.

### Linear setup note

The Linear MCP server must be configured for an agent to create tickets. If it is not, the agent should ask the maintainer to configure it (or post the proposed ticket breakdown as text for the maintainer to file) rather than skip the planning step.

---

## TL;DR

1. Rust.
2. Smallest correct diff.
3. Tests with the change, runnable in one command.
4. Summary at the end the maintainer can trust.
5. Ideation/planning → `scratch/<topic>.md`.
6. Features → Linear tickets under "Jarvis Engine" → wait → one ticket at a time → one PR per ticket → Graphite (`gt`) for stacks.

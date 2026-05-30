# Development Rules

These rules apply to **every agent** working on the Coral Engine. Read them before touching the repo. Re-read them before declaring work done.

---

## 0. Collaboration: Speak Up, Push Back, Share Opinions

This is **collaborative coding** — the maintainer and the agent building this together, not a request/response transaction. The agent is expected to have opinions and voice them.

- If you disagree with an approach, **say so before you implement it.** A respectful "I'd do this differently because X" is always wanted, never noise. Silent compliance with an approach you think is wrong is a failure mode, not politeness.
- If you see a better design — a cleaner abstraction, a simpler path, a missed edge case, a smell in the request itself — **vocalize it.** Surface the alternative explicitly: what you'd do, why, and what the tradeoff is against the asked-for approach.
- If a request feels off, ambiguous, or contradicts something already in the repo (`VISION.md`, prior decisions, an existing pattern), **flag the tension** before papering over it. "This conflicts with X; here's how I'd reconcile it" beats quietly picking one side.
- **Opinions are welcome even when not asked.** "Here's what you asked for; here's also what I noticed and what I'd consider differently" is the default mode, not a special case.
- Pushback ends when the maintainer makes the call. Once a direction is chosen, execute it cleanly and without re-litigating — but the conversation that *gets* to that direction is genuinely two-sided. Both sides are allowed to change the other's mind.
- This applies to non-code decisions too: ticket decomposition, scope boundaries, architectural framing, even these rules. If something in `DEVELOPMENT.md` itself feels wrong in context, say so.

The goal is alignment, not deference. If you find yourself thinking *"I'd do this differently but I'll just do what was asked,"* stop and write the disagreement first. The worst outcome is the maintainer discovering on review that you had a better idea and swallowed it.

---

## 1. Language: Rust

The Coral Engine is written in **Rust**. No exceptions without explicit approval from the maintainer.

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
- `scratch/` is for thinking, not deliverables. Once a plan crystallizes into GitHub issues, the issues become the source of truth and the scratch file can be archived or deleted.
- Do not put scratch files anywhere else in the repo. Do not commit ideation noise into top-level docs.
- **Attach the relevant plan to the GitHub issues it produced.** When issues are filed, every issue (project-tracked, parent, or sub-issue) must carry the slice of the scratch plan that motivates it — paste the relevant section into the issue body, or link the `scratch/<topic>.md` file. A reviewer reading only the issue should see the reasoning, not just the acceptance criteria. For a Project, attach the full plan to a tracking issue or the project description; for sub-issues, include the section specific to that unit of work.

## 6. Feature Workflow: GitHub-Issues-Driven, Staged Execution

When the maintainer asks for a **feature** (anything larger than a trivial one-shot edit), the agent's first job is **planning, not coding**.

All tracking lives in **GitHub issues** in the `gchatz22/coral_engine` repo. Issues are filed and edited with the **`gh` CLI** — `gh issue create`, `gh issue edit`, `gh issue list`. There is no separate tracker to keep in sync; the issue, its linked branch, and its PR all live in the same repo.

### Step 1 — Decompose into GitHub issues

Pick the right grouping primitive based on the size of the request:

- **Large feature (~10+ sub-issues, big effort)** → create a **GitHub Project (v2) board**. The board holds the spec (in a tracking issue) and a Status column for every child issue. Every sub-issue is created as a normal issue and added to the board.
- **Medium feature (~3–10 sub-issues, lighter effort, a session)** → create a **parent issue with native sub-issues**. The parent holds the spec; GitHub renders the sub-issues with a live progress bar on the parent. No board needed.
- **Single task (one-shot, fits in one reviewable diff)** → create a **single issue**. No project, no parent. Just the issue.

If a sub-issue inside a Project is itself multi-step, make it a parent issue with its own sub-issues nested underneath — Projects and parent/sub-issues compose.

`gh` covers issue creation, editing, labelling, and linked branches (`gh issue develop`). It does **not** yet expose sub-issue nesting or Project-board membership as first-class subcommands — wire a sub-issue to its parent and add a card to a board through the GitHub web UI, or via `gh api graphql` (`addSubIssue` / `addProjectV2ItemById` mutations). Note in the breakdown which step you used so the maintainer can audit it.

Break work into the smallest meaningful, **independently reviewable** units. Each issue (project-tracked, parent, sub-issue, or standalone) must include:

- A clear, specific title.
- A body with: goal, acceptance criteria, in-scope items, explicitly out-of-scope items, and dependencies on other issues (reference them by `#number`).
- A reasonable effort estimate (as a label or a body line).

Order issues so that dependencies come first. Note issue-to-issue dependencies in the bodies. After creating the issues, post a summary back to the maintainer: the project (if any), then each issue with its number, title, and one-line scope.

### Step 2 — Stop and wait

Do **not** start implementing any issue until the maintainer explicitly says so. The maintainer reviews the issue breakdown, may revise it, and then prompts the agent to attack a specific issue.

### Step 3 — Execute one issue at a time

When prompted to work an issue:

- Re-read the issue. Confirm the scope.
- Apply rules 1–4 above (Rust, minimal diff, tests first, comprehensive summary).
- Work on a dedicated branch linked to the issue. Use `gh issue develop <number> --checkout` to create and check out a branch GitHub links back to the issue (named `<number>-short-slug`). Never commit issue work directly to `main`.
- Move the issue across the status board as work progresses (Todo → In Progress → In Review → Done — see *Status board* below).
- **Every issue ends as a Pull Request.** When the change is ready, push the branch and open a PR whose description closes the issue (`Closes #<number>`), restates the acceptance criteria, and includes the structured summary from rule 4. One issue = one branch = one PR. No issue is "done" until its PR is open and linked back to the issue; merge happens only after maintainer review.
- **Dependent issues ship as stacked PRs, managed with Graphite.** See *Stacked PRs — use Graphite* below. When an issue depends on another issue still in review, the new branch goes on top of the dependency via `gt create`, not branched manually off `main`. Each PR in the stack stays small and reviewable on its own; the description must call out the stack order and the parent PR. Do not bundle dependent issues into one mega-PR to avoid stacking.
- Deliver the change, open the PR (or `gt submit` for a stack), and then **stop**. Do not roll on to the next issue. Wait for the maintainer's approval and the next prompt.

This staged loop — *plan → approve → one issue → branch → PR → review → next issue* — is the default. Skip it only when the maintainer explicitly says "just do it."

### Status board

GitHub issues are only open/closed, so work status lives on a **GitHub Project (v2) board** with a single `Status` field. The columns are:

`Backlog` → `Todo` → `In Progress` → `In Review` → `Done`

- Newly filed issues land in `Backlog` (or `Todo` once the maintainer approves the breakdown).
- Moving a branch from work to review moves the card `In Progress` → `In Review`; opening the PR is the trigger.
- Merging the PR (which `Closes #<number>`) moves the card to `Done` and closes the issue.
- A linked **draft** PR signals `In Progress`; marking it **ready for review** signals `In Review`. Keep the board column and the PR draft state consistent.
- Move cards via the board UI, or `gh api graphql` (`updateProjectV2ItemFieldValue`) when scripting. `gh project item-edit` also works once you have the project and item IDs.

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
   git worktree remove --force ../coral-<merged-slug>
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

### Merging a stack — rebase-merge, pre-retarget children, avoid admin bypass

The cascade-rebase pain `gt` is designed to absorb assumes the merge method preserves commit equivalence. **Squash-merge collapses N commits into 1**, so every child branch's history references orphan commits — GitHub flags the child as DIRTY/CONFLICTING and `gt restack` has to rewrite + force-push the child to recover. **Rebase-merge preserves commits 1:1**, so `git rebase origin/main` on each child cleanly detects "already applied" patches and skips them. Both methods are allowed by the repo's ruleset; pick per situation:

- **Single non-stacked PR**: squash-merge. Matches the existing commit history on `main` and keeps the log compact.
- **Stacked PR**: **rebase-merge**. `gh pr merge $PR --rebase --delete-branch`, or "Rebase and merge" in the GitHub UI. The downstream cascade after each merge is then almost trivial: each child just needs a `gt sync` or `git rebase origin/main` to recognize the parent's commits already on trunk.

**Pre-emptively retarget every child PR's base to `main` BEFORE merging the parent.** When `--delete-branch` removes the parent's head branch, GitHub *sometimes* auto-retargets the child PR's base to the repo default — but *sometimes* it auto-closes the child instead, especially if the child's CI was in flight or admin-bypass was in use. Recovery from an auto-closed PR is painful: GitHub refuses to reopen the PR once the base branch is gone, and the only path is to open a fresh PR from the same head branch to `main` (loses the review thread). Pre-retargeting immunizes every child before the parent merge can trigger the race:
```
gh api -X PATCH repos/<owner>/<repo>/pulls/<N> -f base=main
```
(`gh pr edit --base main` works in theory but has hit transient `Something went wrong` GraphQL errors in our runs; the REST `PATCH` is more reliable. **This conflicts with the "do not change a stacked PR's base via the GitHub UI" rule above** — that rule is about mid-development hygiene, where `gt sync` keeps bases correct. The pre-merge retarget is a one-shot pivot right before merging, not a mid-development edit.)

**Avoid `--admin` for stack cascades.** Admin merge bypasses review and the required-status check, but does NOT bypass the GitHub auto-retarget bookkeeping race that auto-closes children. If you genuinely need admin merge (solo dev, no reviewer), pre-retarget every child first so the race can't bite.

**The no-infrastructure stack-merge recipe (no merge queue, no Graphite Cloud):**

1. **Pre-retarget every child PR** to `main` (REST PATCH one-liner above), one shot per child PR in the stack.
2. **From the bottom of the stack**: `gh pr merge $PR --rebase --delete-branch`.
3. **After each merge**: in the stack worktree, `git fetch origin --prune`. Then for each remaining child, in dependency order: `git checkout <child> && git rebase origin/main`. Rebase-merge means git skips the already-applied parent commits cleanly; no manual conflict resolution typically needed.
4. **Force-push each rebased child**: `git push --force-with-lease origin <branch>`.
5. **Move up one rung**: repeat from step 2 with the next PR in the stack.

### Parallel agents — use worktrees freely

When multiple agents are working tickets concurrently, use **git worktrees** without asking. One worktree per agent per ticket keeps branches, build artifacts, and uncommitted state fully isolated, and avoids agents stomping each other's working tree.

**Spawn a worktree per ticket:**
- For a ticket branched off `main`: `git worktree add ../coral-<ticket-slug> -b <ticket-slug> main`.
- For a stacked ticket on top of an open PR's branch: `git worktree add ../coral-<ticket-slug> -b <ticket-slug> <parent-branch>`.
- Inside the new worktree, immediately register with Graphite: `gt track --parent <parent-branch>` (use `main` for unstacked).
- Do all the ticket's work — edits, `cargo build`, `cargo test`, commits, `gt submit`/`gt submit --stack` — inside that worktree.
- Worktrees are cheap. Prefer creating one over coordinating shared checkout state.

**Clean up after a PR merges:**
1. From the worktree: `gt sync` — pulls trunk, cascades the rest of the stack, and (if Graphite recognises the merge) drops the merged branch from the stack.
2. Remove the worktree: `git worktree remove ../coral-<ticket-slug>`. Use `--force` only if there's intentional uncommitted state worth nuking.
3. Delete the local branch if it lingers: `git branch -D <ticket-slug>`. ("Branch already merged" is fine — `-D` skips the safety check, which is correct here because the on-main commit has a different SHA than your local branch tip.)
4. Delete the origin branch if it lingers: `git push origin --delete <ticket-slug>`. **Better:** enable repo Settings → General → Pull Requests → "Automatically delete head branches" so GitHub does this for every merged PR. Then this step is unneeded.

**Audit at any time:** `git worktree list`, `git branch -vv | grep <prefix>`, `gt log long`. If you see a branch whose PR is closed/merged but whose worktree is still around, clean it.

### GitHub CLI setup note

The `gh` CLI must be authenticated for an agent to create issues (`gh auth status` to check; `gh auth login` to fix). If it is not, the agent should ask the maintainer to authenticate it (or post the proposed issue breakdown as text for the maintainer to file) rather than skip the planning step. No external tracker or MCP server is involved — issues, branches, and PRs all live in the GitHub repo.

---

## TL;DR

0. **Speak up.** Disagree, propose alternatives, share opinions — this is collaborative coding, not order-taking.
1. Rust.
2. Smallest correct diff.
3. Tests with the change, runnable in one command.
4. Summary at the end the maintainer can trust.
5. Ideation/planning → `scratch/<topic>.md`.
6. Features → GitHub issues (`gh`) → wait → one issue at a time → one PR per issue (`Closes #N`) → Graphite (`gt`) for stacks.

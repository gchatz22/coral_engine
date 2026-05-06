# Claim seed persistence — convention for stable claim ids across ticks

**Status:** convention design + prompt snippet for JAR2-28. Once the
prompt-template module lands (JAR2-16), the snippet section migrates into
that module verbatim.

## Problem

`Decision::CallTool` carries a `ClaimSeed` — an opaque string the agent
picks. The kernel uses it (today: identity; tomorrow: hash) to derive a
stable claim id so that multiple pieces of evidence supporting the same
conceptual claim collapse into one row in the conflict log and the
output's evidence bundle.

LLMs are non-deterministic. The same agent, woken on tick 7 to keep
investigating "did this drug clear phase 2?", may pick a different seed
string than it did on tick 3 — `phase-2-clearance`, `clears-phase-2`,
`drug-x-passed-p2`, ... — and produce three claim ids for what is
conceptually one claim. Provenance fragments. The conflict log doesn't
know these belong together.

The fix is not kernel-side determinism. We don't want the kernel
canonicalizing seed strings on the agent's behalf — that bakes a
particular notion of "same claim" into the runtime, hides intent, and
can't represent claims whose phrasing genuinely matters (legal,
clinical). Instead, we give the agent a durable place in its FS to
write seeds it has already minted, and a documented convention for
reading them back before issuing a new `CallTool`. State as files, per
VISION § 4. The agent doesn't have to *remember* a seed across
ticks; it gets to *look it up*.

## Convention

### Layout

```text
<root>/claims/<slug>.json
```

One file per claim. The directory is created at `AgentFs::open`.

### Slug

A claim's filename slug is derived deterministically from the seed
string by `crate::fs::claim_slug`:

1. Lowercase ASCII; non-`[a-z0-9]` runs collapse to a single `-`.
2. Trim leading and trailing `-`.
3. Truncate the resulting body to 80 bytes.
4. Always append `-<8 hex chars>` where the hex is the leading 8
   characters of `sha256(seed)`. Always — not "only on collision."
   Conditional suffixing makes the slug a function of *prior writes*
   instead of of the seed alone, which breaks idempotent lookups.

If the body is empty after trimming (e.g. seed = `"!!!"`) the slug
falls back to just the hash suffix.

The hash suffix means two semantically different seeds that happen to
slugify the same way still get different filenames; the kebab body
keeps the directory humanly browsable.

### File contents

```json
{
  "seed": "phase-2-clearance",
  "description": "Did drug X pass phase 2?",
  "status": "open",
  "created_at": "2026-05-06T12:00:00Z"
}
```

`status` is one of `open`, `resolved`, `abandoned`. The agent owns
status transitions; the kernel does not interpret them. `description`
is free-form prose the agent writes for its own future self —
sufficient context to recognize "yes, this is the same claim I'm
working on" on a later tick.

### Helpers

`AgentFs` exposes:

- `write_claim(&Claim) -> anyhow::Result<()>` — writes
  `claims/<slug>.json`. Slug derived from `claim.seed`. Overwrites
  any existing file at that slug (status updates use this path).
- `read_claim(seed: &str) -> anyhow::Result<Option<Claim>>` — `None`
  if the file is absent.
- `list_claims() -> anyhow::Result<Vec<Claim>>` — every claim file
  on disk, in ascending filename order. Used by the agent at the
  top of a tick to decide whether a new claim is needed.

Out of scope: cross-agent sharing, claim deletion, status indexing,
surfacing `claims/` into `ContextBundle` (JAR2-10).

## Mandate / prompt snippet

This is the addendum the prompt-template module will add to system
prompts once JAR2-16 lands. It is written in the voice the agent
receives.

> **Stable claim ids.** Before you emit a `call_tool` decision, look at
> `claims/` in your filesystem. If the evidence you are about to gather
> would support a claim you have already opened — even if you would
> phrase it differently today — reuse that claim's existing
> `claim_seed` verbatim. Use `list_claims` (or `read_claim` if you
> remember the seed) to check.
>
> If no existing claim matches, mint a new seed: a short kebab-case
> string that captures the *thing being claimed*, not the tool call
> itself. Then, in the same tick that you first emit a `call_tool`
> with that seed, also emit a `rewrite_fs` op (or otherwise record
> the claim through your normal write path) so future ticks find it.
> A claim with no record cannot be reused.
>
> When a claim is settled — answered, contradicted, or abandoned —
> update its `status` field to `resolved` or `abandoned` so future
> you knows not to keep gathering evidence under it.
>
> The runtime will use each `claim_seed` to derive a deterministic
> claim id. Same seed string, same claim id. Different seed strings
> for the same conceptual question, different ids — and your
> evidence will fragment. Reuse is the whole point of this directory.

## Out of scope (deliberate)

- **Surfacing `claims/` in `ContextBundle`.** JAR2-10's territory.
  Today the agent reads `claims/` via tool calls (`list_claims`,
  `read_claim`); the v2 context-assembly redesign will decide whether
  the claim list belongs in the warm cache or stays self-directed.
- **Conflict-log surfacing of mismatched-seed cases.** Later
  observability work; once the conflict log lands, an agent that
  emits a new seed every tick for the same evidence will be visible
  there.
- **Kernel-side seed canonicalization.** The agent owns its own
  notion of "same claim." Imposing canonicalization at the kernel
  would conflate verticals that have legitimately different needs
  (legal exact-phrase claims vs. clinical fuzzy-match claims).
- **Cross-agent claim sharing.** Each agent's `claims/` is private,
  same as `notes/`.

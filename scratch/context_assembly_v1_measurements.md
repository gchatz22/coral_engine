# Context assembly v1 — phase 1 measurement spike

*Status: spike note attached to JAR2-36 (context-assembly v2 phase 1).
Pins the `ContextPolicy` defaults the warm cache ships with. Sibling
to `scratch/context_assembly_v2.md`, which holds the unchanging design;
this file holds the empirical-but-shaky basis for the numbers. Re-run
the spike honestly once retrieval tools (phase 2) and live-LLM A/B
become possible.*

---

## 1. What this spike was asked for

The JAR2-36 ticket carves out a time-boxed measurement step before
freezing the `ContextPolicy` defaults. The design doc (§ 12) lists three
moves:

1. Run the recorded-fixture integration tests with bundle field counts
   logged, so we can read the *actual* shapes the existing tests exercise.
2. Compare model behavior on a representative mandate with (a) warm
   cache only (the current shape) vs. (b) warm cache + a synthetic
   `read_file`-style retrieval bolted on.
3. Use the findings to set the final default `open_claims_max` and
   confirm / adjust the `recent_outputs` / `recent_evidence` defaults.

## 2. What is honestly possible inside phase 1

**Constraint 1 — no live A/B without keys.** The vendor fixture tests
(`tests/llm_fixture_anthropic.rs`, `tests/llm_fixture_cohere.rs`) run
hermetic against pre-recorded JSON responses in
`tests/fixtures/llm/{anthropic,cohere}/`. They serve a fixed reply
regardless of what the bundle looks like; flipping a field on the
bundle does not change what the model "decides" because the model never
sees the request — the mock server just returns the next recorded
response. A real A/B requires a live vendor key, which this spike does
not assume.

**Constraint 2 — retrieval tools don't exist yet.** Move (2) wants to
contrast warm-cache-only vs. warm-cache + synthetic `read_file`-style
retrieval. The retrieval tools are explicitly out-of-scope for phase 1
(they're phase 2 of the design). Faking the comparison by pre-loading
extra bundle content would just measure "does a bigger bundle change
the recorded JSON the mock returns?" — answer: no, see constraint 1.

**What we *can* do.** Read the bundle field counts the recorded
fixtures and the run-loop smoke tests actually exercise, and use those
+ the rule-of-thumb argument from `context_assembly_v2.md` § 2 to
ground the defaults. Anything stronger has to wait for phase 2 +
live-vendor budget.

## 3. Observations from the existing test corpus

Read with `tracing::debug!` added to `assemble_context` (kept in code —
it's cheap observability that benefits production too, not noise to
remove) and a manual sweep of the test setups:

- **Recorded-fixture tests** (`tests/llm_fixture_*.rs`,
  `tests/smoke_llm_mcp_*.rs`). They build `ContextBundle` by hand via
  `empty_bundle()` rather than going through `assemble_context`. Every
  exercised bundle is all-zeros for `recent_outputs`, `recent_evidence`,
  and `open_claims`. Triggers are also empty except for the live-API
  smokes, which carry one `ScheduledWake`. Nothing in this corpus
  stresses the warm cache.

- **Hermetic run-loop tests** (`tests/loop_smoke.rs`). These go through
  the real `assemble_context`. Across 17 pre-JAR2-36 tests, the largest
  recent_outputs count observed is 1 (the single-`emit_output` happy
  path), recent_evidence tops out at 1 (single `record_evidence` setups
  used by the correction-loop tests), and `open_claims` is zero
  everywhere — no smoke writes a claim. The new JAR2-36 smoke
  (`per_mandate_recent_outputs_cap_reaches_the_run_loop`) deliberately
  seeds 5 outputs and asserts the cap shrinks them to 2.

- **Unit tests in `src/decision.rs`** that explicitly exercise the
  windowing path (`assemble_context_reads_outputs_and_evidence_deterministically`)
  seed `default_window + 2 = 10` of each and assert the cap returns
  the configured `default_window = 8`. This was the JAR2-6 sanity test
  for the old `RECENT_WINDOW`; it now reads the default off the new
  `ContextPolicy`.

- **`AgentFs::list_claims` order.** The phase-1 implementation
  inherits filename ordering per `context_assembly_v2.md` § 8.
  Filename order is slug-based, not chronological — that's noted in
  `fs.rs` already and re-noted in the `ContextBundle.open_claims`
  docstring. The integration test asserts the bundle order matches
  `list_claims().filter(Open).take(open_claims_max)` literally.

**Headline:** the existing test corpus does not exercise any bundle
shape that would distinguish `recent_outputs = 4` from `= 8` from `= 16`.
The defaults can't be tuned against this evidence alone.

## 4. Defaults chosen (and why)

| Knob | Default | Basis |
|---|---|---|
| `recent_outputs` | **8** | Matches the pre-JAR2-36 hardcoded `RECENT_WINDOW = 8`. The point of phase 1 is to make this *tunable*, not to retune the default with no data. Existing graphs round-trip unchanged. |
| `recent_evidence` | **8** | Same reasoning. Stays paired with `recent_outputs` until we have a real reason to decouple them. |
| `open_claims_max` | **32** | The design-doc strawman. With `claim_slug` capping the kebab body at 80 bytes, 32 open claims plus 8-char hash suffix fits comfortably under the prompt budget headroom we've seen across the fixtures. The rule-of-thumb argument from `context_assembly_v2.md` § 2 — *"surface enough to make the seed-reuse decision without a tool roundtrip every tick"* — points at "tens, not hundreds." 32 is the round number under "tens." |

Each default carries a one-line comment in `src/mandate.rs` pointing at
this file, per JAR2-36's comment guidance.

## 5. Defaults *not* chosen (and follow-up to file)

- **No retuning of `recent_outputs` / `recent_evidence`.** A real
  retune needs (a) production telemetry from a long-lived agent, or
  (b) live-vendor A/B. Neither is available inside phase 1. The right
  scope for this work is a follow-up once phase 2 (retrieval tools) is
  in tree, because the question is no longer "how much warm cache?" in
  isolation — it's "how much warm cache *given the tool fallback path
  exists*?" The split is the whole point of the design.

- **No claim-status-aware ordering.** Phase 1 inherits
  `list_claims`'s filename order. The design doc § 8 already records
  the deferral: true-recency order needs the B1 index, which is its
  own follow-up.

- **No conflict-log tail.** The conflict log itself doesn't exist yet
  (lands with C2 parent–child). The design reserves the bundle slot
  in prose; phase 1 deliberately does not reserve it in code.

## 6. Phase-2 spike to re-run

Once `read_file` / `list_dir` retrieval tools exist and at least one
live-vendor key is wired into CI, repeat the comparison the design doc
asks for:

1. Pick a representative mandate from the recorded fixtures (the
   FDA-holds renderer in `decide_llm/prompt.rs::tests::mandate()` is
   a fine stand-in).
2. Snapshot agent behavior over N ticks with `recent_outputs = 8` and
   no retrieval tools.
3. Snapshot the same N ticks with `recent_outputs = 4` *plus*
   `read_file` available.
4. Diff: same decisions emitted? More cost? Fewer redundant tool
   calls? File the conclusions as a phase-2 follow-up note here.

If the live A/B shows the agent burns tool roundtrips re-fetching
older outputs whenever `recent_outputs` drops below some threshold,
that's the empirical signal for retuning. We can't get that signal
from the current test corpus, so phase 1 stops here.

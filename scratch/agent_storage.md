# Agent storage — pluggable backend for the per-agent FS

*Status: ideation, ready for ticket-filing review once `scratch/temporal_staged_plan.md` stage 2.5 is approved. Captures the design for making the per-agent filesystem a pluggable module so the engine can deploy against local disk (today) or remote object storage (production cloud shape), with the trait shape, backend candidates, deployment models, performance/cost wrinkles, and phasing.*

*Read order: `VISION.md` § 4 ("every agent has a filesystem" — state-as-files), `scratch/agent_runtime.md` § 5 (state placement), `scratch/durability_substrate.md` (Temporal locked, FS owns working memory), `scratch/temporal_staged_plan.md` (stages 0–8; stage 2.5 lands the trait), then this. Also relevant: `src/fs.rs` (the existing `AgentFs`), `src/agent.rs` (the consumer).*

---

## 1. Goal of this layer

Today's per-agent FS (`src/fs.rs::AgentFs`) is a directory on the local filesystem. This is the right shape for the bootstrap and for single-host development. It is the **wrong shape for the production deployment we're aiming at** — a Kubernetes-style cluster of stateless workers where any worker can pick up any agent's workflow, and no node has persistent local state.

The goal of this doc is to define the abstraction that lets the engine deploy across both shapes — and to think honestly about how agents should treat "their filesystem" when it lives somewhere else.

**This is preemptive design.** S3 (or compat) integration is months out per the staged plan; we land the trait now so the integration is *implementing*, not *refactoring*. The cost of designing now: a few days of careful thinking and a clean refactor of `AgentFs`. The cost of designing later: a major refactor mid-flight through multi-agent topology or snapshots.

---

## 2. Status quo — what `AgentFs` does today

Every method on `AgentFs` is already **object-shaped** in disguise: write a named blob, read a named blob, list a prefix. Walking the existing surface:

| `AgentFs` method | Operation in object terms |
|---|---|
| `persist_mandate` | put `mandate.json` |
| `read_mandate` | get `mandate.json` |
| `persist_output(ulid, content, evidence)` | put `outputs/<ulid>.json` (with provenance check) |
| `read_recent_json` outputs | list `outputs/` + parallel gets of last N |
| `record_evidence(record)` | put-if-absent `evidence/<sha256>.json` |
| `read_recent_json` evidence | list `evidence/` + parallel gets of last N |
| `evidence_exists(id)` | get-or-head `evidence/<sha256>.json` |
| `apply_ops` (notes writes) | put under `notes/<path>` after path validation |
| `read_note(path)` | get `notes/<path>` |
| `write_claim(claim)` | put `claims/<slug>.json` |
| `read_claim(seed)` | get `claims/<slug>.json` |
| `list_claims()` | list `claims/` + parallel gets |
| `write_health(state)` | put `health.json` |
| `archive_health` | put `health/<ts>.json` (copy `health.json` first) |
| `persist_retirement(reason)` | put `retirement.json` |

There is no method that requires POSIX semantics we can't model on S3: no advisory locking, no atomic rename across keys, no `fsync` directly exposed to callers, no symlinks, no extended attributes. Atomic writes today use the "write-to-tmp + rename" pattern *internally* — that pattern is an implementation detail of the local backend, not a contract the callers depend on. They depend on "the named object either exists with the complete content or doesn't exist at all," which is exactly what a single-shot PUT gives us.

This is the key insight: **`AgentFs` is already an object-store client in disguise.** Extracting the trait is mostly a rename and a small async-trait-ification, not a rethink.

---

## 3. The deployment shapes we're designing for

Three concrete shapes, each putting different pressure on the abstraction:

### 3.1. Single host, local disk (today)

One process. Per-agent FS at `<root>/<graph_id>/<agent_id>/`. POSIX directory. Microsecond reads. Atomic rename internally. Operator can `cat`, `ls`, `grep` directly.

This is what the bootstrap built. It survives in dev forever — there is no reason to make the dev experience worse to support production. The trait must keep this experience intact.

### 3.2. Multi host, shared network FS (NFS / EFS / GCS Filestore / CephFS)

The middle ground. Multiple workers, but they all mount the same FS. Looks like local FS to the code; latency is higher (millisecond reads); locking semantics get squishy. Operator inspection works via the mount.

This shape is what users with existing on-prem POSIX-FS infrastructure will reach for first. It's also a useful "transitional" deployment — strong durability with minimal code changes from single-host.

**Design implication:** the trait must work with a backend that has POSIX semantics but higher latency. The `LocalStorage` impl works against any POSIX directory, so it transparently supports this case. No second backend needed.

### 3.3. Stateless workers, remote object storage (the production cloud shape)

What the maintainer's question is really about. Concrete architecture:

```
Kubernetes (or equivalent):
  ┌───────────────────────────────────┐
  │ Worker pod 1 (stateless)          │  ┐
  │   - Rust binary                   │  │
  │   - In-memory cache only          │  ├─→ N fungible workers,
  │   - No persistent volume          │  │   any can pick up any agent
  │ Worker pod 2 (stateless)          │  │
  │   - same                          │  ┘
  └───────────────────────────────────┘
              │                  │
              ↓                  ↓
  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐
  │ Temporal cluster   │  │ Postgres           │  │ S3 / MinIO         │
  │  (durable exec)    │  │  (structural state)│  │  (per-agent FS)    │
  └────────────────────┘  └────────────────────┘  └────────────────────┘
              │
  External:
  - LLM APIs (Anthropic, Cohere, ...)
  - MCP servers (per-graph configurable)
```

Properties:
- **Workers are fungible.** Any worker can host any agent's workflow. Temporal hands the workflow context to whichever worker is available. Workers can scale up, scale down, crash, get rescheduled — agent state must not depend on them.
- **No node-local state for agents.** All durable agent state lives in S3 (FS), Postgres (topology), or Temporal (execution).
- **Cold-start latency is real.** A worker that hasn't seen agent X recently has nothing in cache; tick 1 takes a network round-trip per state object.
- **Cost is per-operation.** S3 charges per PUT/GET/LIST. At scale this becomes a budget line.
- **Inspection is via API, not POSIX.** Operators look at agent state through the TUI, a `coral fs cat` debug command, or the S3 console — never `cat` directly.

This is the shape we have to make work. Once it does, the simpler shapes (3.1, 3.2) come for free.

---

## 4. How agents should treat the FS — the mental model

This is the maintainer's deeper question, answered directly.

**Headline: agents treat the FS as their durable working memory, with no awareness of where it lives.** The remoteness is a kernel concern, not an agent concern. An agent writes a note; later, it reads it back; the engine guarantees it's there. Latency characteristics change between deployments, but the *semantics* never do.

But there's a second-order story underneath, because pretending remote and local are identical leaks performance off a cliff in deployment shape 3.3. Three principles the kernel obeys to make the abstraction hold up:

### 4.1. Reads are batched and prefetched at tick boundaries

`assemble_context` already knows what it needs at the start of a tick — the mandate, recent outputs, recent evidence, open claims, current health. Today it issues these reads sequentially as it walks the FS. In the remote shape this would mean 10+ serial round-trips. The kernel fixes this without changing agent code: `assemble_context` prefetches everything in **one parallel batch** via `AgentStorage::get_many`. ~10 round-trips collapse to 1 round-trip's worth of wall-clock.

This pattern is universal: any code path that knows multiple keys upfront uses `get_many`. The agent never writes `get_many` itself; the kernel composes it.

### 4.2. Writes within a tick are durable-on-completion, not durable-on-call

The provenance contract requires: evidence is durable before the output that cites it. We can't lazily batch writes across these boundaries. **But within a single CallTools dispatch**, the N evidence records of N parallel tool calls write in parallel — N PUTs concurrent, not serial.

`persist_output` (the activity, per stage 3 staging) flushes synchronously before completing — Temporal then has a durable record of the output. If the workflow crashes between evidence-PUT and output-PUT, the cited evidence is already on S3; the output replay re-PUTs the same content under the same key (idempotent).

### 4.3. Each worker process maintains a per-agent read-through cache

The same worker pod often handles consecutive ticks of the same agent (Temporal's stickiness, when it applies). For those ticks, the cache is warm: tick 2's `assemble_context` doesn't re-fetch tick 1's writes — they're already in memory. Cache invalidation is trivial because **the agent is the only writer to its own FS** — the worker writing a key knows it just wrote it; nothing else can change it.

Cold start (new worker picks up an agent it's never seen): cache empty, all reads go to S3. This is the unavoidable cost of stateless workers and we accept it.

### 4.4. The escape hatch — when agents *do* need to know

99% of agent code is unaware of remote-ness. The 1% case: an agent designed to do extensive FS exploration (e.g. a research agent walking its own prior outputs). For those, the retrieval tools designed in `scratch/context_assembly_v2.md` (`list_dir`, `read_file`, `semantic_search`) are the API. The kernel implements them efficiently against the backend; the agent issues them as ordinary tool calls and pays the same provenance cost as any other tool.

This split — kernel prefetches the predictable, agents tool-call the exploratory — is the right shape for both performance and audit.

---

## 5. The `AgentStorage` trait

The minimum surface that supports every pattern in § 2 and maps cleanly to both backends. Trait-object-safe (no generic methods, all `Send + Sync + 'static`):

```rust
#[async_trait]
pub trait AgentStorage: Send + Sync + 'static {
    /// Atomic single-shot write. Overwrites if key exists.
    /// Used for mandate, output, note, claim, health writes.
    async fn put(&self, key: &str, value: Bytes) -> Result<()>;

    /// Atomic conditional write. Returns Existed if the key already exists,
    /// Created if the write happened. Used for content-addressed evidence
    /// (PUT once, dedup naturally on retry) and for immutable outputs.
    async fn put_if_absent(&self, key: &str, value: Bytes) -> Result<PutOutcome>;

    /// Fetch a single object. None if not present.
    async fn get(&self, key: &str) -> Result<Option<Bytes>>;

    /// Fetch multiple objects in parallel. Order of results matches input order.
    /// Implementations optimize: local does parallel file reads, S3 issues
    /// parallel GETs (with a concurrency cap).
    async fn get_many(&self, keys: &[&str]) -> Result<Vec<Option<Bytes>>>;

    /// Delete a single object. No-op if absent.
    /// Used sparingly — most agent state is append/immutable.
    async fn delete(&self, key: &str) -> Result<()>;

    /// List object keys under a prefix, paginated.
    /// `after` is exclusive — start listing strictly after this key.
    /// `limit` is the maximum count for this page; impls may return fewer.
    async fn list(&self, prefix: &str, after: Option<&str>, limit: usize)
        -> Result<ListPage>;
}

#[derive(Debug)]
pub enum PutOutcome { Created, Existed }

#[derive(Debug)]
pub struct ListPage {
    pub keys: Vec<String>,
    /// Set if more keys exist after this page. Pass as `after` on the next call.
    pub next_cursor: Option<String>,
}
```

`AgentFs` becomes a thin facade over `Arc<dyn AgentStorage>` + a key prefix (`graphs/<graph_id>/agents/<agent_id>/`). Every method on `AgentFs` today translates to one or a few trait calls — see § 2 table.

### 5.1. Deliberately excluded from the trait

Each absence is a choice, not an oversight:

- **`head` (metadata-only fetch).** Could add later if we discover hot paths that need only size/etag. Today we don't.
- **Streaming put/get.** All objects are < ~1MB (outputs, evidence, claims). If MCP starts returning multi-MB results, add `put_stream`/`get_stream` then.
- **Object versioning APIs.** S3 has native object versioning that could power snapshots (stage 8) very cheaply. Exposing it would require version-id-typed handles. Defer to stage 8 when we know the snapshot shape.
- **Copy / rename.** S3 has `CopyObject`; local has `rename`. We don't use either in current patterns. Content-addressed writes mean we just PUT the new key.
- **Locking / CAS beyond put-if-absent.** Single-writer-per-agent obviates locking. The only CAS we need is "first writer wins on a content-addressed key," which `put_if_absent` covers.
- **Batched put.** Agents write a handful of objects per tick; complexity-to-benefit ratio doesn't justify it. The N concurrent PUTs in a parallel-tool batch are good enough.
- **Watch / subscribe.** The TUI's live-tailing today uses `notify` against local FS. On S3 we'd poll or use SNS/EventBridge — out of scope here; lands when the TUI's `KernelGraphSource` (stage 7 phase 3) takes over.

These can be added later without breaking existing impls. Keeping the surface small now means fewer impl bugs, fewer edge cases, and a clearer mental model.

---

## 6. Backend implementations

### 6.1. `LocalStorage` (today's `AgentFs` refactored)

The existing implementation, repackaged behind the trait. Properties:

- **`put`**: write to `<root>/<key>.tmp`, then `rename` to `<root>/<key>`. Atomic on POSIX.
- **`put_if_absent`**: O_CREAT | O_EXCL open. Atomic on POSIX.
- **`get`**: read file. Return None on `ENOENT`.
- **`get_many`**: `futures::join_all` of N `get` calls (each is a local read, no benefit from concurrency cap).
- **`delete`**: unlink. Idempotent on `ENOENT`.
- **`list`**: `read_dir`, filter by prefix, sort lexicographically, paginate by `after` + `limit`. (Stage 2's B1 index optimizes the common case; see § 7.)

Hermetic-test impl (`MemoryStorage`) implements the same trait against a `HashMap<String, Bytes>` for unit tests. Trivial.

### 6.2. `S3Storage` (future — design now, build when needed)

Implemented against `aws-sdk-s3`. Configured with bucket + (optional) prefix + credentials chain. Compatible with MinIO out of the box (S3-compatible API).

Method mapping:
- **`put`**: `PutObject`. Atomic single-shot.
- **`put_if_absent`**: `PutObject` with `If-None-Match: *` (supported by S3 since 2024, MinIO supports it). Returns `Existed` on `412 PreconditionFailed`. Fall back to `HeadObject` + `PutObject` if the backend doesn't support it (rare).
- **`get`**: `GetObject`. Returns None on `404 NoSuchKey`.
- **`get_many`**: `tokio::spawn` N `GetObject`s with a configurable concurrency cap (default 32). The cap matters at scale — unbounded parallel GETs can saturate the connection pool or trip rate limits.
- **`delete`**: `DeleteObject`. Idempotent.
- **`list`**: `ListObjectsV2` with `prefix` + `start-after` + `max-keys`. S3 returns lex-sorted keys natively. Cursor = last key in page if `is_truncated`.

Consistency: S3 has been strongly consistent for read-after-write since December 2020. No eventual-consistency workarounds needed.

Authentication: standard AWS credential chain (env vars, IAM role, profile, instance metadata). MinIO uses access-key + secret-key, configured the same way.

### 6.3. Backends we considered and rejected

- **NFS / EFS / CephFS** — works transparently behind `LocalStorage` against any mounted POSIX directory. Not a separate impl; users who want this just point `--storage local --root /mnt/nfs/coral`. We test against this configuration but don't ship a special backend.
- **GCS, Azure Blob** — same API shape as S3 with different SDKs. If demand emerges, implement against the relevant SDK; the trait already accommodates them. MinIO + S3 SDK covers the "S3-compatible" mass.
- **JuiceFS / S3-FUSE** — S3 mounted as a POSIX FS. Reintroduces semantics (rename, append) that we'd just abstracted away; behavior is hard to predict under load. Not worth the indirection.
- **Postgres bytea** — wrong shape; Postgres isn't a blob store; outputs grow without bound.
- **Database-only state (no blob store at all)** — would require putting outputs/evidence/notes in Postgres. Possible but mixes concerns: structural state and blob state have different access patterns and lifecycle. Keep them separate.
- **Cloudflare R2 / Backblaze B2** — S3-compatible; covered by `S3Storage` with a different endpoint URL.

---

## 7. The B1 index on object storage

`scratch/post_bootstrap_followups_later.md` B1 specs an `outputs/index.jsonl` and `evidence/index.jsonl` — append-only files that make "list recent N" O(1) instead of "walk every entry." Append-only works on local FS. **It does not work on S3 — there is no append primitive.** Designing for both:

### 7.1. The tail-index pattern

Keep a single bounded object per index:

```
<agent_root>/outputs/_tail.json    # { entries: [{filename, created_at}; ≤ TAIL_K] }
<agent_root>/evidence/_tail.json   # same shape
```

On each write to `outputs/` or `evidence/`:
1. PUT the new object first (the actual output/evidence file).
2. Read `_tail.json`, prepend the new entry, truncate to `TAIL_K` (e.g. 100), PUT it back.

Properties:
- Single-writer-per-agent means we don't need CAS on the tail — last write wins, and the only writer is this worker.
- "List recent N" for N ≤ TAIL_K is one GET. Fast on local, fast on S3.
- "List recent N" for N > TAIL_K falls back to the full LIST path (rare).
- Crash recovery: if the agent crashes after PUTting the file but before PUTting the tail, the tail lags. The file is still present; the next list_recent will pick it up via the fallback LIST and reconcile.

### 7.2. The full-history index (deferred)

For workflows that need to list "everything since 2024-01," we'd want a sharded index — `outputs/_index/<YYYY-MM-DD>/<ulid>.json` or similar. Defer until we have a workload that needs it. The tail index plus an as-needed full LIST covers v1.

### 7.3. Trait neutrality

The B1 design is **kept inside `AgentFs`, not pushed into `AgentStorage`.** The trait stays storage-shaped (put/get/list); the index pattern is a `AgentFs`-layer optimization built on top. This keeps backends thin and makes the index switchable per-backend if local-FS impl wants to keep the JSONL append form (faster) while S3 uses the tail-index form.

---

## 8. Atomicity, consistency, idempotency

### 8.1. Per-key atomicity

S3 PUT is atomic per object — readers see either the previous version or the new one, never a partial. Local backend gets the same property via write-to-tmp-then-rename. The trait contract is "successful `put` makes the new content visible atomically; failed `put` leaves either the prior value or no value at all."

### 8.2. Cross-key consistency

There is no cross-key transaction in either backend. The provenance contract relies on **ordering**: evidence is PUT before the output that cites it. If the output PUT succeeds but the evidence PUT had silently failed, we'd have a dangling reference. Two layers of defense:

1. **In-tick ordering**: `persist_output` checks each cited evidence ID via `get` (or cached presence from this tick's writes) before PUTting the output. If any cited evidence is missing, the output is rejected with a provenance error. This is what `AgentFs::persist_output` already does today.
2. **Idempotent replay**: under Temporal's activity retry, a replayed `record_evidence` is a `put_if_absent` against a content-addressed key — same content → same key → no-op. Replayed `persist_output` is the same — same ULID + same content + same evidence list → re-PUT with identical bytes. Both safe.

### 8.3. Idempotency on retry

Every write activity is designed to be safe to re-execute:

- **Evidence** is content-addressed (`evidence/<sha256>.json`); `put_if_absent` makes replay a no-op.
- **Outputs** are ULID-keyed (`outputs/<ulid>.json`); ULIDs are generated *inside* `AgentCore::dispatch` and carried in `DispatchOutcome`, so a replayed activity gets the same ULID and PUTs identical bytes to the same key.
- **Mandate / health / retirement** are last-write-wins keys; replay just re-asserts the same state.
- **Notes** are arbitrary writes by the agent; the agent owns idempotency semantics through `RewriteFs` ops, which are pure writes.

### 8.4. What we explicitly don't support

- **Atomic multi-key writes.** No backend gives us this. Provenance ordering covers the one case we need it.
- **Optimistic locking.** Single-writer-per-agent means we don't need it.
- **Cross-agent atomicity.** Agents are isolated; cross-agent coordination happens via Temporal signals + reconciliation, not via FS state.

---

## 9. Performance and cost

### 9.1. Latency

| Op | Local | S3 |
|---|---|---|
| `put` | ~100 µs | ~20–50 ms |
| `get` | ~100 µs | ~10–30 ms |
| `get_many(N)` | ~N × 100 µs (or less with parallel) | ~20–50 ms (parallel, capped) |
| `list(prefix, ≤1000)` | ~ms (read_dir + sort) | ~20–100 ms |

Implication: a tick that does 10 sequential reads is ~1 ms locally, ~200–500 ms on S3. With batching, S3 collapses to ~30–50 ms — slower but workable for tick cadences > 1/sec, which is essentially all our use cases.

### 9.2. Cost (S3, illustrative)

Standard S3 pricing (subject to change):
- PUT: ~$0.005 per 1000
- GET: ~$0.0004 per 1000
- LIST: ~$0.005 per 1000

Per-agent tick at remote shape — rough budget:
- 3 puts (output, evidence, tail) → $0.000015
- 5 gets (assemble context) → $0.000002
- 1 list (tail miss) → $0.000005

≈ $0.00002 per tick. At 1000 agents × 1 tick/min × 60 min × 24 hr × 30 days = 43.2M ticks/month → ~$860/month for storage ops alone. Tractable at this scale.

At "millions of agents continuously" (VISION § 7), this becomes a real budget line and motivates: lower tick cadences for idle agents, sibling deduplication of MCP traffic, cache aggressively in-worker. Not a v1 concern — flagging for the scheduler design.

### 9.3. Storage capacity

Per-agent footprint: maybe 100 KB to a few MB depending on output volume. 1M agents × 1 MB = 1 TB at the upper end. S3 Standard is ~$23/TB/month. Cold tier (Glacier) is ~$1/TB/month for archived agents. Lifecycle policies handle the tiering automatically. Capacity is essentially free.

### 9.4. Cold start

When a worker pod that has never seen agent X picks up X's workflow: assemble_context does 1 parallel batch fetch of ~10 keys = ~50 ms wallclock on S3. Acceptable. Mitigation if it ever bites: Redis cache shared across workers (defer).

---

## 10. Inspection

Without `cat`/`ls`/`grep`, operators need another path to read agent state. Three:

1. **The TUI** (`scratch/graph_tui.md`). The `GraphSource` trait already plans for `FsGraphSource` (today) → `KernelGraphSource` (later). Both read through `AgentStorage` once the trait lands. Operator opens the TUI, walks agents, drills into outputs/evidence/notes/claims — works the same way regardless of backend.
2. **A `coral fs` debug command.** `coral fs cat <key>`, `coral fs ls <prefix>`, `coral fs get <key> --raw > file.json`. Small CLI wrapper around `AgentStorage`. Useful for forensics.
3. **The S3 console / `aws s3 ls`.** Backend-native inspection. Operators familiar with S3 don't need anything from us; the bucket is laid out predictably (`graphs/<graph_id>/agents/<agent_id>/outputs/<ulid>.json` etc.).

The TUI is the primary surface; the CLI and console are escape hatches.

---

## 11. Phasing

Mapped to `scratch/temporal_staged_plan.md`:

| Step | Where | Scope |
|---|---|---|
| 1 — `AgentStorage` trait + `LocalStorage` impl | **Stage 2.5** (new, between stages 2 and 3) | Define trait, refactor `AgentFs` into facade over `Arc<dyn AgentStorage>`, ship `LocalStorage` matching today's behavior exactly, add `MemoryStorage` for tests. All existing tests stay green — pure refactor. |
| 2 — B1 tail-index | **Stage 2** (existing) | The tail-index pattern (§ 7.1) is what B1 ships. Append-only JSONL stays a local-FS optimization; the cross-backend story uses the tail pattern. (B1 design doc gets updated to reflect this.) |
| 3 — `S3Storage` impl | **Follow-up stage post-stage-5** (call it stage 9) | When the maintainer wants to deploy against remote storage, implement `S3Storage` against `aws-sdk-s3`, add config plumbing, smoke against MinIO in CI + against real S3 in a live-test job, document deployment. |
| 4 — Snapshot integration | **Stage 8** | The snapshot/fork primitives use `AgentStorage` (and may extend the trait for object versioning support if it's a clean fit). |

Stage 2.5 is the upfront-cost step the maintainer signed off on. It costs ~3–4 days, lands a clean abstraction, and means stages 3 onward can take `&dyn AgentStorage` without further restructuring.

### 11.1. Stage 2.5 sub-tickets (proposed)

- **2.5.1 — Trait definition + `MemoryStorage`.** Define `AgentStorage` trait in `coral_node` (or a new submodule). Implement `MemoryStorage` (HashMap-backed) for unit tests. Trait tests against `MemoryStorage`.
- **2.5.2 — `LocalStorage` impl.** Port today's `AgentFs` behavior (atomic write-then-rename, `O_EXCL` for `put_if_absent`, `read_dir` for `list`). Reuses existing path-resolution code where applicable.
- **2.5.3 — `AgentFs` facade refactor.** Refactor `AgentFs` to hold `Arc<dyn AgentStorage>` + a key prefix; every existing method translates to one or a few trait calls. All callers (`Agent`, tests, binaries) take `AgentFs` exactly as before — no upstream changes. The 71+ existing tests stay green.
- **2.5.4 — Tail-index integration.** Replace the current `read_recent_json` directory-scan with the tail-index pattern in `AgentFs`. Local-FS impl can keep using `read_dir` as a fallback; the tail index is the primary path. Aligns with B1.

### 11.2. Stage 9 sub-tickets (proposed for the follow-up S3 stage)

Sketched here so future-us knows what's coming. File when actually needed.

- 9.1 `aws-sdk-s3` dep + `S3Storage` skeleton (auth, bucket config, retry policy).
- 9.2 Method impls per § 6.2.
- 9.3 MinIO-in-docker-compose for hermetic-ish tests.
- 9.4 Live-S3 smoke (env-gated).
- 9.5 Deployment doc: bucket layout, IAM/credentials, lifecycle policy templates.
- 9.6 `coral fs` debug CLI.
- 9.7 Per-worker in-memory cache (read-through, invalidate-on-own-write).

---

## 12. What this doc deliberately does not address

- **Authentication, IAM, credentials.** Standard AWS SDK credential chain handles it. Per-tenant credentials are a stage-9.x concern.
- **Multi-tenancy / bucket isolation.** Lean: one bucket per deployment, key prefix per `graphs/<graph_id>/agents/<agent_id>/`. Tenant isolation is a future infra concern, not a trait concern.
- **Cross-region replication.** S3 native feature; configure at the bucket level. We don't model it in the trait.
- **Encryption at rest.** S3 native (SSE-S3 / SSE-KMS); configure at the bucket level. Local backend leaves it to disk-level encryption.
- **Bandwidth budgets and egress.** Cloud cost concern, not a trait concern.
- **Pluggable structural-DB.** Out of scope here; the structural DB (Postgres) has its own swap story (sqlite → Postgres → managed) that's not coupled to FS storage.
- **Temporal workflow state.** Temporal owns its own persistence. Not part of `AgentStorage`.

---

## 13. Decisions (resolved at plan review)

The seven questions raised during plan review have been resolved. Each decision and its rationale, recorded for the implementation tickets to reference.

1. **Trait location.** Module inside `coral_node` (path: `coral_node::storage`) at stage 2.5. Promote to a standalone `coral_storage` sub-crate at stage 9 when the AWS SDK dep needs to be gated behind a feature flag and the local-vs-s3 surface area grows. *Why:* avoid premature crate-splitting; the trait stays small at stage 2.5 and doesn't justify its own crate. Once S3 lands with its dep footprint, the crate boundary earns its keep.

2. **Bytes representation.** `bytes::Bytes` everywhere — both arguments and returns. *Why:* de facto standard across `aws-sdk-s3`, `reqwest`, `tokio::io`. Cheap clone (Arc-backed) avoids the copy that `Vec<u8>` would force on every layer transition.

3. **`TAIL_K` default.** 64 entries. *Why:* covers `recent_outputs` / `recent_evidence` defaults (currently 8 each per `context_assembly_v2.md`) with 8× headroom for per-mandate `ContextPolicy` overrides. Tail object stays under ~8 KB even with verbose entries. Configurable per-deployment if a workload needs more.

4. **Error type.** Trait-level `Result<T, StorageError>` where `StorageError` is a `thiserror`-derived enum with variants `NotFound`, `Conflict` (failed put-if-absent), `Transient` (retryable network/IO), `Permanent` (auth, bad key, exceeded quota), and `Other(anyhow::Error)` as the escape hatch. *Why:* `AgentFs` and the Temporal activity layer need to distinguish "not found" (often expected — `get` returns `None` semantically) from "transient" (worth retrying) from "permanent" (fail the activity hard). Anyhow alone loses that distinction; typed errors recover it.

5. **Crate name when promoted.** `coral_storage`. *Why:* matches the workspace naming convention (`coral_node`, `coral_temporal`, `coral_graph`, `coral_tui`).

6. **`MemoryStorage` placement.** Shipped in the storage module, gated behind `#[cfg(any(test, feature = "memory-storage"))]`. *Why:* tests in any workspace crate get it for free via `#[cfg(test)]`; production callers who want ephemeral / spike-testing behavior opt in via the feature. Avoids accidentally shipping in-memory storage into a production binary.

7. **B1 design supersession.** The B1 doc gets updated to reflect tail-index as the primary path; the original `outputs/index.jsonl` append form is retained as a local-only optimization the `LocalStorage` impl may use under the hood, with the tail-object remaining the canonical cross-backend surface. *Why:* fewer docs disagreeing with each other; the tail-index pattern is what stage 2.5.4 ships, and the B1 doc should match what landed.

---

## 14. References

- `VISION.md` § 4 ("every agent has a filesystem"; state-as-files).
- `scratch/agent_runtime.md` § 5 (state placement — FS owns working memory).
- `scratch/durability_substrate.md` (Temporal locked).
- `scratch/temporal_staged_plan.md` — this doc is referenced from stage 2.5.
- `scratch/post_bootstrap_followups_later.md` B1 (index) — superseded by § 7 of this doc for the cross-backend story.
- `scratch/graph_tui.md` (`GraphSource` reads through `AgentStorage`).
- `scratch/context_assembly_v2.md` (retrieval tools are the agent-facing escape hatch when exploration is needed).
- `src/fs.rs` (today's `AgentFs` — the refactor target).
- AWS Rust SDK: <https://docs.rs/aws-sdk-s3/latest/aws_sdk_s3/>.
- MinIO (S3-compatible self-hosted): <https://min.io/>.

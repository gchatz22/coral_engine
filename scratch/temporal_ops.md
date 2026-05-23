# Temporal-backed engine — operational deployment scoping

*Status: scoping artifact for stage 3+ implementation tickets. Consolidates the deployment-shape decisions already locked in `scratch/temporal_staged_plan.md` § 8 into one readable page so reviewers of later Project tickets land against a shared understanding of where the services live, how they're addressed, and how state is partitioned. Not implementation. Not a runbook.*

*Read order: `scratch/durability_substrate.md` (Temporal won — the substrate fork), `scratch/temporal_staged_plan.md` § 8 (locked decisions), `scratch/agent_storage.md` (the FS abstraction this doc names concrete backends for), then this.*

---

## 1. Purpose

The engine deploys, in production, as: **a Temporal cluster orchestrating stateless Rust workers, backed by managed Postgres for structural state and a single S3 bucket for per-agent FS state.** This doc names the concrete topology and the addressing scheme each layer uses. Everything operational — TLS, auth, observability, multi-tenancy — is post-stage-5 and deferred (§ 7).

---

## 2. Temporal deployment

**Server topology.** Self-hosted Temporal Server, four-service split (frontend, history, matching, internal-worker), deployed as containers behind a load balancer for the frontend service. Single-region in v1. Temporal UI deployed alongside for operator inspection.

**Persistence backend.** Postgres (same engine as our structural DB, separate logical database). Sized for our workload: history shards drive the bulk of Temporal's write load, so v1 provisions a Postgres instance scaled for `~10k workflow events/sec` peak (i.e. moderate — not "millions of agents" yet; that is a scheduling problem § 7 punts on). Visibility uses Postgres advanced visibility (supported by recent Temporal Server versions) rather than Elasticsearch — one fewer service to operate in v1; Elasticsearch becomes a stage-9+ concern if visibility queries get heavy.

**Namespace.** **One Temporal namespace per deployment** (`scratch/temporal_staged_plan.md` § 8 decision 3). Namespace-per-tenant and namespace-per-graph were considered and rejected as v1 over-engineering. The workflow ID scheme below is namespace-independent, so a future migration to namespace-per-tenant is non-breaking.

**Dev vs. prod shape.** Dev runs the whole Temporal cluster + Postgres in `docker-compose` (per stage 0.3). Prod uses the same images on Kubernetes (manifests out of scope here — see § 7). Temporal Cloud as a hosted alternative is noted in § 8 as a future decision; the workflow ID scheme and namespace strategy don't change between self-host and Cloud.

---

## 3. Workflow ID scheme

**Scheme.** `graphs/<graph_id>/agents/<agent_id>` (`scratch/temporal_staged_plan.md` § 8 decision 2). URL-shaped, REST-resource-pluralized, no leading slash. Flat within a graph — parent–child topology lives in the structural DB, not in the ID.

**Examples.**

```
graphs/macro-watch/agents/cpi-monitor
graphs/macro-watch/agents/cpi-summary
graphs/clinical-trials-2026Q1/agents/eligibility-screener-005
```

**Supersession.** This replaces the `{graph_id}/{node_id}` sketch in `scratch/agent_runtime.md` § 3. The motivation for the simpler form (operator-readable, deterministic, no separate registry) survives unchanged in the URL form; the URL form additionally mirrors the eventual HTTP API (`GET /api/v1/graphs/<id>/agents/<id>/...`) and stays flat under reparenting (which doesn't happen — see edge cases).

**Allowed characters.** `<graph_id>` and `<agent_id>` are constrained to `[a-zA-Z0-9_-]` (alphanumerics, underscore, hyphen). No slashes, no whitespace, no Unicode. This keeps IDs URL-safe, shell-safe, and Temporal-safe without escaping. Validation lives in `jarvis_graph` at `jarvis apply` time; the structural DB rejects writes that violate the constraint.

**Length budget.** Temporal limits Workflow IDs to **255 bytes** total ([docs](https://docs.temporal.io/workflows#workflow-id)). Our prefix `graphs//agents/` consumes 15 bytes; the remaining 240 bytes split across `<graph_id>` + `<agent_id>`. v1 caps each component at **64 bytes**, leaving ample headroom (15 + 64 + 64 = 143 bytes used, 112 free). The cap is enforced at `jarvis apply` validation; raising it later is non-breaking.

**Edge cases.**

- **Reparenting an agent (changing its parent).** Does not happen. Workflow IDs are flat within a graph by design; parent–child is a logical relation in the `edges` table of the structural DB, mutated by ordinary DB writes. Reparenting an agent rewrites edges, not its workflow ID. This means a long-running agent keeps its workflow ID across topology changes — important for log continuity and human bookmarks.
- **`<graph_id>` collision on `jarvis apply`.** Return a hard error and refuse the apply. Detection and validation live in `jarvis_graph` (stage 1.4 CRUD layer enforces the `graphs.id` uniqueness constraint; the `apply` binary in stage 4.3 surfaces the error to the operator). Operators rename or delete the existing graph explicitly — no silent overwrite.
- **`<agent_id>` collision within a graph.** Same — `jarvis_graph` enforces a uniqueness constraint on `(graph_id, agent_id)` and `jarvis apply` rejects the YAML with a source-located error.
- **Special characters in user-provided IDs.** Rejected at YAML parse / `jarvis apply` validation (stage 4.2). The error names the offending character and column.

---

## 4. Postgres deployment

**Two logical databases on one instance** in v1: one for Temporal persistence, one for the structural DB (`jarvis_graph` per stage 1). They share a Postgres instance because their workloads are unrelated and they're cheap to colocate; if Temporal's write load eventually warrants isolation, the structural DB moves to its own instance — single-line DSN swap.

**Dev → prod migration path.**

- **Dev:** single Postgres container in `docker-compose`. Volume-mounted data dir. No HA.
- **Prod:** managed Postgres (Cloud SQL / RDS / Neon / equivalent). Choice of provider is per-deployment and not a code concern — `DATABASE_URL` configures it. Backups, point-in-time recovery, and HA are the managed service's responsibility.

**Connection pooling.** Stateless workers hold short-lived connections per activity invocation. With N workers × M activities-in-flight, connection counts climb. v1 mitigation: per-worker connection pool capped at 8 (via `sqlx::PgPool`). If we cross ~200 concurrent workers, introduce `PgBouncer` in transaction-pooling mode as a sidecar. Not v1 work — flagged for stage 5 sizing.

**Schema migrations.** `sqlx::migrate!` from the `jarvis_graph` crate. Migration files live at `crates/jarvis_graph/migrations/`. CI runs migrations against an ephemeral test DB on every PR (per `scratch/temporal_staged_plan.md` § 8 decision 5). Production migrations run from CD pipeline at deploy time against the managed DB — same `sqlx migrate run`, different DSN. Temporal's own schema is managed by the Temporal admin tools, not our migration system.

---

## 5. Per-agent FS volume topology

**Production shape.** Single S3 bucket per deployment. Bucket name: deployment-specific (`jarvis-prod`, `jarvis-staging`). Keys laid out to match the workflow ID structure:

```
s3://jarvis-prod/graphs/<graph_id>/agents/<agent_id>/<file>
                 graphs/macro-watch/agents/cpi-monitor/mandate.json
                 graphs/macro-watch/agents/cpi-monitor/outputs/<ulid>.json
                 graphs/macro-watch/agents/cpi-monitor/evidence/<sha256>.json
                 ...
```

The key prefix is literally the workflow ID. This is a load-bearing alignment: given a workflow, the worker knows its FS prefix without a lookup; given a key, the operator knows the workflow.

**Single-host / dev shape.** Local mount at `<JARVIS_ROOT>/<graph_id>/<agent_id>/`. Same key layout, just on a POSIX filesystem. The `AgentStorage` trait (`scratch/agent_storage.md` § 5) abstracts the difference; `LocalStorage` impl lands in stage 2.5, `S3Storage` impl lands in stage 9 (unchanged from the staged plan).

**Bucket configuration.** Versioning **off** in v1 — content-addressed evidence and ULID-keyed outputs make in-place mutation rare, and snapshot semantics (stage 8) are not yet specified. Lifecycle policies are a stage-9 concern. Encryption at rest uses SSE-S3 (bucket-level default). Cross-region replication is out of scope (§ 7).

**Cost / capacity.** See `scratch/agent_storage.md` § 9 — not re-derived here.

---

## 6. Worker pool

**Stateless Rust workers,** each registering against the single Temporal namespace, scaled horizontally on demand. A worker process holds:

- a Temporal SDK connection,
- a Postgres connection pool to the structural DB,
- an S3 client,
- an in-memory per-agent read-through cache (`scratch/agent_storage.md` § 4.3).

No node-local durable state. Workers are interchangeable; any worker can host any agent's workflow tick.

**Task affinity.** Temporal's sticky-task-queue routes successive ticks of the same workflow to the same worker when possible. This is a **hint, not a guarantee** — the in-memory cache in § 4.3 of `agent_storage.md` warms when affinity holds and is cold-start cost when it doesn't. Workers must function correctly without affinity; cache is an optimization.

**Scaling policy.** v1: manual or HPA-driven, on CPU. Autoscaling on Temporal's task-queue depth (the more correct signal) is a stage-6+ refinement once we have real load to tune against.

---

## 7. Explicit non-goals

The following are deliberately out of scope for this doc and for the stage 0–5 implementation it scopes. They are listed so future-us doesn't think they were forgotten:

- **TLS** between workers and Temporal, between workers and Postgres, between workers and S3.
- **Authentication / authorization** at any boundary (Temporal mTLS, Postgres roles beyond a single application role, S3 IAM beyond a single service principal).
- **Multi-tenancy.** One deployment serves one organization in v1.
- **RBAC.** No per-user permission model.
- **Observability and metrics export.** Temporal exposes Prometheus metrics; our workers should too; nothing is wired to a metrics sink in v1.
- **Log aggregation.** Stdout in dev; whatever the orchestrator captures in prod. No structured-log pipeline.
- **Production-grade IaC.** No Kubernetes manifests, Helm charts, or Terraform shipped from this repo in stage 0–5. Deployment is `docker-compose` for dev and "operators bring their own orchestration" for prod.
- **Cost modeling at scale.** `scratch/agent_storage.md` § 9 sketches the FS-side cost; Temporal-side and Postgres-side cost modeling waits for real load data.
- **Multi-region / geographic redundancy.** Single region in v1.

Each of these gets its own design round when it becomes the next bottleneck.

---

## 8. Open questions

These are not decided. They are not blockers for the stage 0–5 implementation work; they will need answers before a hardening pass.

1. **Temporal Cloud vs. self-host.** Operationally simpler (no cluster to run); adds a hosted dependency that some deployment targets (regulated industries — VISION § 4's "sovereign deploy") will reject outright. v1 self-hosts; Cloud is an option to revisit when we have a deployment that wants it.
2. **Worker autoscaling thresholds.** CPU is the v1 trigger; the correct signal is Temporal task-queue depth. Defer until we have real load.
3. **Postgres instance sizing class.** The "moderate" sizing in § 2 is a placeholder; real numbers come from stage-3 smoke runs.
4. **Visibility backend at scale.** Postgres advanced visibility works for v1; Elasticsearch becomes worth the operational cost only if visibility queries dominate.

---

## 9. References

- `scratch/temporal_staged_plan.md` § 8 (decisions 2, 3 — workflow ID, namespace).
- `scratch/temporal_staged_plan.md` § 5 stage 0.4 (the ticket this doc satisfies).
- `scratch/durability_substrate.md` (substrate decision — Temporal won).
- `scratch/agent_storage.md` (FS abstraction; § 4.3 worker cache, § 9 cost analysis).
- `scratch/agent_runtime.md` § 3 (workflow ID sketch — superseded by § 3 of this doc).
- Temporal Workflow ID docs: <https://docs.temporal.io/workflows#workflow-id>.

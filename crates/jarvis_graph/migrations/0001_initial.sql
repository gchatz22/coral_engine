-- Initial structural-DB schema for the Jarvis Engine.
--
-- Tables here own the structural state layer: what graphs exist, what
-- agents are in each, parent->child edges, and registered tools.
-- Authored mandates live outside this DB (git-versioned `graph.yaml` +
-- per-node versioning); `agents.mandate_ref` is an opaque text handle
-- to one. Working memory (outputs, evidence, notes, claims, health)
-- stays on disk under the per-agent FS; execution state (trigger
-- queue, scheduler cursor, in-flight ticks) lives in Temporal.
--
-- Per-table design notes:
--
-- - UUID primary keys generated client-side (Rust `uuid::Uuid::new_v4`).
--   We do not depend on a Postgres UUID-generation extension so a stock
--   `postgres:16-alpine` (per `docker-compose.yml`) is sufficient.
-- - `TIMESTAMPTZ DEFAULT now()` everywhere a created_at exists, matching
--   the dev compose stack's UTC default.
-- - Foreign keys use `ON DELETE CASCADE` so dropping a graph deletes its
--   agents/edges/etc. cleanly without orphan rows.
-- - `_sqlx_migrations` (added implicitly by `sqlx::migrate!`) tracks
--   which migration files have been applied; re-runs are idempotent.
--   These files use plain `CREATE TABLE` rather than `IF NOT EXISTS`
--   so a corrupted migration tracker surfaces loudly.
-- - No extra indexes beyond PK / FK / explicit UNIQUE constraints.
--
-- Decisions worth flagging for review (see PR body):
--
-- 1. `agents.mandate_ref` is `TEXT NULL` — an opaque text handle to an
--    authored mandate (e.g. a YAML key in the git-versioned
--    `graph.yaml`). Not a FK because authored mandates are not stored
--    in this DB. The cost is no referential integrity for the handle;
--    this matches how other ref-by-name shapes in the engine work.
-- 2. `tools.kind` is `TEXT NOT NULL`, not a Postgres enum, because
--    enums are sticky (every new variant is a migration). New kinds
--    stay one-line.
-- 3. `edges` has no `graph_id` column. A cross-graph edge would be
--    nonsense, but the minimal-diff move is to skip the constraint and
--    flag as a follow-up — adding a column + CHECK constraint later is
--    a single migration. (Same-graph invariant can be enforced in
--    application code if a query path requires it.)

CREATE TABLE graphs (
    id          UUID         PRIMARY KEY,
    name        TEXT         NOT NULL,
    metadata    JSONB        NOT NULL DEFAULT '{}'::JSONB,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE agents (
    id            UUID         PRIMARY KEY,
    graph_id      UUID         NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    name          TEXT         NOT NULL,
    -- Opaque text handle to the authored mandate (e.g. a YAML key in
    -- the git-versioned `graph.yaml`). Authored mandates are not
    -- stored in this DB, so there's no FK target. See decision (1)
    -- above.
    mandate_ref   TEXT         NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE edges (
    id               UUID         PRIMARY KEY,
    parent_agent_id  UUID         NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    child_agent_id   UUID         NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- One edge per parent->child pair. The decision schema (`Decision`
    -- in `jarvis_node`) doesn't model multi-edges between the same two
    -- agents, so the structural DB shouldn't allow them either.
    UNIQUE (parent_agent_id, child_agent_id)
);

CREATE TABLE tools (
    id         UUID         PRIMARY KEY,
    -- `TEXT NOT NULL` per decision (2) above. Convention for known
    -- values: `echo`, `mcp`, etc. — defined in application code, not
    -- the DB.
    kind       TEXT         NOT NULL,
    -- Process command for the tool (e.g. the executable to spawn for
    -- an MCP server). NULL is allowed for tool kinds that don't shell
    -- out (e.g. an in-process `echo`).
    command    TEXT         NULL,
    args       JSONB        NOT NULL DEFAULT '[]'::JSONB,
    env_refs   JSONB        NOT NULL DEFAULT '[]'::JSONB,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE agent_tools (
    agent_id    UUID  NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    tool_id     UUID  NOT NULL REFERENCES tools(id)  ON DELETE CASCADE,
    PRIMARY KEY (agent_id, tool_id)
);

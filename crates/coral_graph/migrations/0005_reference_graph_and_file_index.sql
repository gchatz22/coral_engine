-- Provenance/version graph: the DB half of the content/metadata split.
-- Per-agent files (pure content) live in the git-versioned FS; the DB owns
-- the reference graph (citations) and a filepath<->blob-sha index over them.

-- filepath <-> blob-sha index. Current-state: one row per (agent, path),
-- upserted in place. Per-file version history lives in git (`git log -- path`),
-- so this table is the integrity lookup (path -> current sha) and the dedup
-- lookup (sha -> path(s), one-to-many for identical content). It is
-- FS-derivable, so the consistency sweep can rebuild it by re-hashing files.
CREATE TABLE file_index (
    agent_id    UUID         NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    filepath    TEXT         NOT NULL,
    blob_sha    TEXT         NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- One current binding per path => "exactly-once allocation" of a path.
    -- This is version-update, not slug-collision detection: a re-write of
    -- the same path updates the pointer. Disambiguating two different files
    -- that want the same slug is the writer's job (existence-check first).
    PRIMARY KEY (agent_id, filepath)
);

-- Dedup / reverse lookup: which path(s) hold this content. Non-unique:
-- identical bytes across files share a blob sha.
CREATE INDEX file_index_blob_sha ON file_index (blob_sha);

-- Reference graph: each row is one citation edge, version-pinned on BOTH
-- ends. Pinning the citing version too keeps provenance time-scrubbable —
-- an old output stays bound to the exact evidence versions it cited, while
-- the current output cites current versions. Named `citations` because
-- `references` is a SQL reserved word.
--
-- Append-only: superseded citations are retained, never updated/deleted.
-- FK targets are `agents` ONLY: `cited_blob_sha` is frequently a historical
-- version with no current `file_index` row, so an FK to `file_index` would
-- wrongly reject legitimate pins to superseded content.
CREATE TABLE citations (
    id               UUID         PRIMARY KEY,
    citing_agent_id  UUID         NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    citing_filepath  TEXT         NOT NULL,
    citing_blob_sha  TEXT         NOT NULL,
    cited_agent_id   UUID         NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    cited_filepath   TEXT         NOT NULL,
    cited_blob_sha   TEXT         NOT NULL,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    -- A citation edge is fully identified by its two version-pinned ends.
    -- Uniqueness lets the write path INSERT ... ON CONFLICT DO NOTHING, so a
    -- retried write activity converges to one row instead of duplicating the
    -- edge (which would double-count dependents and over-fire wakes).
    UNIQUE (citing_agent_id, citing_filepath, citing_blob_sha,
            cited_agent_id, cited_filepath, cited_blob_sha)
);

-- Propagation read: who cites this file (any pinned version)? The staleness
-- reactor compares each pin's `cited_blob_sha` against the file's current sha.
CREATE INDEX citations_cited ON citations (cited_agent_id, cited_filepath);

-- Resolve read: what does this output version cite?
CREATE INDEX citations_citing ON citations (citing_agent_id, citing_filepath, citing_blob_sha);

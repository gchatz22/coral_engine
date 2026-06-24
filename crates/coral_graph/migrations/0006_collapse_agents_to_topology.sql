-- Collapse the `agents` row to topology.
--
-- `mandate_ref`, `persistent`, and `model` were never read back by the
-- runtime: authored config reaches an agent through its workflow input
-- (the durable `Mandate`), not these columns, so the columns were a
-- vestigial dual-write. Dropping them leaves the row as identity +
-- topology only (`id`, `graph_id`, `name`, `created_at`); edges carry
-- the parent->child relation.
--
-- No `IF EXISTS`: a corrupted migration tracker should surface loudly
-- rather than be masked by idempotent DDL.

ALTER TABLE agents
    DROP COLUMN mandate_ref,
    DROP COLUMN persistent,
    DROP COLUMN model;

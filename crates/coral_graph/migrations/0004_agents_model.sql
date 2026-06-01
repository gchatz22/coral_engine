-- Per-agent model override on the `agents` table.
--
-- An optional model id (e.g. `claude-opus-4-8`) letting one agent run on a
-- stronger model than its siblings — typically a reconciling parent over
-- narrow children. `NULL` (the default and the shape for any agent applied
-- without the field) means "use the worker's configured default model", so
-- every existing agent row is unchanged. The id is interpreted within the
-- worker's configured vendor; cross-vendor ids are an operator misconfig
-- that surfaces at decide time, not a schema concern.
--
-- No `IF NOT EXISTS`: a corrupted migration tracker should surface loudly
-- rather than be masked by idempotent DDL.

ALTER TABLE agents ADD COLUMN model TEXT;

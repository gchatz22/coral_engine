-- Per-agent `persistent` flag on the `agents` table.
--
-- One bit of intent: may this agent terminate itself (the default), or
-- must it persist and refresh? `NOT NULL DEFAULT false` keeps every
-- existing agent row — and any agent applied without the field — at
-- today's one-shot behavior. The flag carries no behavior on its own;
-- the runtime consumes it in later work (stop contract + wake/refresh).
--
-- No `IF NOT EXISTS`: a corrupted migration tracker should surface
-- loudly rather than be masked by idempotent DDL.

ALTER TABLE agents ADD COLUMN persistent BOOLEAN NOT NULL DEFAULT false;

-- Tool definitions become directly graph-scoped; per-agent assignment
-- leaves the DB.
--
-- Before: a tool def was tied to a graph only transitively, through the
-- `agent_tools` junction (`list_tools_for_graph` joined tools -> agent_tools
-- -> agents). Assignment (which agent may call which tool) was authored and
-- persisted here but never read at dispatch. Assignment is per-agent config,
-- which now rides the agent's durable workflow input (`Mandate.tools`), so
-- the junction retires; the def gets a direct `graph_id` instead.
--
-- `tools` rows are re-materialized by `coral apply` from `graph.yaml`, so no
-- data needs preserving — TRUNCATE lets the new NOT NULL column land cleanly
-- on a populated dev DB as well as a fresh one.

DROP TABLE agent_tools;

TRUNCATE tools;

ALTER TABLE tools
    ADD COLUMN graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE;

CREATE INDEX tools_graph_id_idx ON tools (graph_id);

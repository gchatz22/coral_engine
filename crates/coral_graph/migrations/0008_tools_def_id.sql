-- Persist the operator-authored tool id (`graph.yaml` `tools[].id`, e.g.
-- `web-search`) on each tool def. It is the id agents reference in their
-- assignment (`Mandate.tools`), so dispatch enforcement needs it to map a
-- model's advertised tool-call name (e.g. `web_search_exa`) back to the
-- owning def and check membership in the caller's assigned set.
--
-- Until now the table held only the surrogate UUID `id`; the human def id
-- was discarded after apply-time validation, leaving no way to correlate a
-- live MCP server's advertised names with the def ids an agent was granted.
--
-- `tools` rows are re-materialized by `coral apply` from `graph.yaml`, so no
-- data needs preserving — TRUNCATE lets the new NOT NULL column land cleanly
-- on a populated dev DB as well as a fresh one. `graph.yaml` already rejects
-- duplicate `tools[].id`, so the per-graph uniqueness constraint is a schema
-- guard, not a new authoring rule.

TRUNCATE tools;

ALTER TABLE tools
    ADD COLUMN def_id TEXT NOT NULL;

CREATE UNIQUE INDEX tools_graph_def_id_idx ON tools (graph_id, def_id);

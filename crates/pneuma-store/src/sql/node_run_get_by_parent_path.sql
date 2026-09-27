-- A parent's children, ordered by fan-out index.
--
-- The two id filters are optional in the original and both are real: every
-- caller of the aggregation path passes `node_ids=parent_step.target_children`.
-- Dropping them -- as an earlier version
-- of this file did -- makes an aggregator collect children outside its target
-- set, so the list handed to the parent has the wrong length and the wrong
-- elements, silently, because the positional ORDER BY still succeeds.
--
-- Expressed as nullable parameters so one statement serves all three call
-- shapes: pass NULL to skip a filter.
--
-- The casts are `text`, not `varchar`, because that is what the driver sends
-- for a `String`/`Vec<String>` bind. Declaring `varchar[]` made Postgres expect
-- element type 1043 and the driver supply 25, which fails at bind time with
-- "binary data has array element type 25 (text) instead of expected 1043". The
-- columns are `VARCHAR` and compare with `text` without trouble.
SELECT id, path, node_id, name, kind, pipeline_id, run_id, parent_id, parent_path, parent_kind, child_index, sibling_index, status, step_input, step_output, error_code, error_message, extra, created_at, updated_at, started_at, finished_at, is_deleted
FROM node_run
WHERE parent_path = $1
  AND ($2::text IS NULL OR node_id = $2)
  AND ($3::text[] IS NULL OR node_id = ANY($3))
ORDER BY child_index

-- Copy every node_run of the given runs into node_run_history.
--
-- Set-based rather than a round trip. The original reads rows with
-- `get_by_run_id`, ships them to the application, and inserts them back;
-- doing it in one statement is the same effect
-- without moving the rows twice, and it cannot copy a partially-read set.
--
-- ON CONFLICT (id) DO NOTHING is the defect notes Rows carry their
-- source ids, and `id` is this table's primary key, so a repeated backup is a
-- key violation. In the original that violation aborts the janitor's cleanup on
-- every subsequent pass -- permanently, because the step that would stop the
-- runs being re-selected never runs. The Mongo half of the same function was
-- already written idempotent (`UpdateOne(..., upsert=True)`); this is that,
-- for Postgres.
INSERT INTO node_run_history (id, path, node_id, name, kind, pipeline_id, run_id, parent_id, parent_path, parent_kind, child_index, sibling_index, status, step_input, step_output, error_code, error_message, extra, created_at, updated_at, started_at, finished_at, is_deleted)
SELECT id, path, node_id, name, kind, pipeline_id, run_id, parent_id, parent_path, parent_kind, child_index, sibling_index, status, step_input, step_output, error_code, error_message, extra, created_at, updated_at, started_at, finished_at, is_deleted
FROM node_run
WHERE run_id = ANY($1)
ON CONFLICT (id) DO NOTHING

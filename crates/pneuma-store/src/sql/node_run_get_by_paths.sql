-- Many rows by path. Uses = ANY rather than IN so the
-- whole list binds as one parameter.
SELECT id, path, node_id, name, kind, pipeline_id, run_id, parent_id, parent_path, parent_kind, child_index, sibling_index, status, step_input, step_output, error_code, error_message, extra, created_at, updated_at, started_at, finished_at, is_deleted
FROM node_run
WHERE path = ANY($1)

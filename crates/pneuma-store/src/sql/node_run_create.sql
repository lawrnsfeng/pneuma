-- Insert one node run. Mirrors NodeRunRepository.create.
--
-- `parent_path` is a foreign key onto this table's own `path`, so a child
-- cannot be inserted before its parent.
INSERT INTO node_run (
    id, path, node_id, name, kind, pipeline_id, run_id,
    parent_id, parent_path, parent_kind, child_index, sibling_index,
    status, step_input, step_output, created_at, updated_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, $10, $11, $12,
    $13, $14, $15, NOW(), NOW()
)
ON CONFLICT (path) DO NOTHING
RETURNING id, path, node_id, name, kind, pipeline_id, run_id, parent_id, parent_path, parent_kind, child_index, sibling_index, status, step_input, step_output, error_code, error_message, extra, created_at, updated_at, started_at, finished_at, is_deleted

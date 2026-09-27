-- Move a node to a new status.
--
-- The original reads the row `WITH FOR UPDATE`, evaluates
-- `should_update_status_from` in original, then issues a separate UPDATE. Here the
-- guard is the WHERE clause, so the decision and the write are one statement and
-- there is no window between them. Zero rows returned means the guard refused;
-- the caller distinguishes that from "no such path" by looking the row up.
--
-- The guard, the original:
--   * a finalised row is never moved, and
--   * a row may only become FORKED from CREATED.
--
-- CANCELLED is in the finalised list here, which the original's
-- NODERUN_FINISHED_STATUSES is not. That is the design notes 1 -- a signed-off
-- deviation, and the exact defect the port exists to close: without it a late
-- result resurrects a cancelled node. pneuma-core's NodeStatus::is_terminal
-- already includes Cancelled and admit() rejects every transition out of it, so
-- omitting it here would have left the domain type and the statement that
-- actually writes the database disagreeing -- with the database winning. An
-- earlier version of this file did omit it, and pinned the omission with a
-- test.
-- `started_at` is stamped when entering a started status
-- (NODERUN_STARTED_STATUSES, the original) and otherwise left alone.
-- `error_message` follows `error_code`: the original writes it only when a code
-- is supplied.
--
-- `$2` carries an explicit `::node_status` at every use. Without it Postgres
-- infers the parameter's type from its first occurrence -- inside the CASE,
-- where it is compared to string literals -- fixes it as `text`, and then
-- refuses the assignment with "column status is of type node_status but
-- expression is of type text". The PREPARE test caught that.
UPDATE node_run
SET status        = $2::node_status,
    updated_at    = NOW(),
    started_at    = CASE WHEN $2::node_status IN ('PROCESSING', 'FORKED') THEN NOW() ELSE started_at END,
    error_code    = CASE WHEN $3::varchar IS NOT NULL THEN $3 ELSE error_code END,
    error_message = CASE WHEN $3::varchar IS NOT NULL THEN $4 ELSE error_message END
WHERE path = $1
  AND status NOT IN (
        'FINISHED', 'ERROR', 'TIMED_OUT',
        'AGGREGATED', 'HAS_CHILD_ERROR', 'HAS_CHILD_TIMED_OUT',
        'CANCELLED'
      )
  AND NOT (status <> 'CREATED' AND $2::node_status = 'FORKED')
RETURNING id, path, node_id, name, kind, pipeline_id, run_id, parent_id, parent_path, parent_kind, child_index, sibling_index, status, step_input, step_output, error_code, error_message, extra, created_at, updated_at, started_at, finished_at, is_deleted

-- Record a node's result: its output and the status that goes with it
-- (the original and :797-804).
--
-- Those call sites go through the inherited generic `EntityRepository.update`
-- rather than `update_status`, which is why this needs its own statement:
-- UPDATE_STATUS writes status, error and started_at, and never step_output. An
-- earlier version of this query set had no way to persist a node's output at
-- all.
--
-- `finished_at` is deliberately not written. Nothing in the original writes it
-- on `noderun` -- the only `finished_at=` assignment is on the Mongo run
-- document -- so writing it here would invent behaviour.
--
-- Carries the same guard as UPDATE_STATUS, but note this is WIDER than the
-- original, which is not what an earlier version of this comment claimed. Both
-- call sites go through the unguarded generic `EntityRepository.update`; the
-- only check anywhere is `if noderun.status == NodeStatus.FINISHED` in
-- the original, and the aggregation path has none. Refusing ERROR, TIMED_OUT, AGGREGATED, HAS_CHILD_ERROR and
-- HAS_CHILD_TIMED_OUT as well is a deliberate deviation -- the design notes --
-- and it drops a late result for a timed-out node rather than recording it.
UPDATE node_run
SET status      = $2::node_status,
    step_output = $3,
    updated_at  = NOW()
WHERE path = $1
  AND status NOT IN (
        'FINISHED', 'ERROR', 'TIMED_OUT',
        'AGGREGATED', 'HAS_CHILD_ERROR', 'HAS_CHILD_TIMED_OUT',
        'CANCELLED'
      )
RETURNING id, path, node_id, name, kind, pipeline_id, run_id, parent_id, parent_path, parent_kind, child_index, sibling_index, status, step_input, step_output, error_code, error_message, extra, created_at, updated_at, started_at, finished_at, is_deleted

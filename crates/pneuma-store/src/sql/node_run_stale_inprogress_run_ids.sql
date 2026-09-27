-- Runs with a non-finalised node_run older than a threshold.
-- This is what the janitor uses to
-- find work that has stopped moving.
--
-- The condition is per ROW, not per run: any single node_run that is both
-- unfinished and stale makes its run eligible. An earlier draft of this file
-- guessed at `GROUP BY run_id HAVING MAX(updated_at) < $1`, which is a
-- different query -- a run with one stale unfinished node and one recently
-- touched node is returned by this and was not by that.
--
-- The status list is NODERUN_FINISHED_STATUSES
-- plus CANCELLED. Adding CANCELLED is deliberate and is the same deviation
-- recorded in the design notes: the original does not treat it as terminal, so a
-- cancelled run keeps being reported as stale and re-terminated. Treating it as
-- terminal here is what makes the port's domain rule and this query agree.
-- Ordered, and the LIMIT is why. Without an ORDER BY, `LIMIT` takes an
-- arbitrary subset -- so two executions of this query against unchanged data
-- may return different runs. Two things depended on that not happening: the
-- janitor's `preview_pass` and `pass` each run it once and are supposed to
-- report the same list, and a run outside the limit could be passed over for
-- ever while other stale runs kept being chosen ahead of it.
--
-- Oldest first, which is also the order worth working in: the run that has been
-- stuck longest is the one to terminate first. `GROUP BY` rather than
-- `DISTINCT` because ordering by `min(updated_at)` needs the aggregate --
-- grouping on the same single column dedupes identically, and the WHERE above
-- is untouched, so this is the same *set* of runs as before, only ordered.
-- (That is a different thing from the `GROUP BY run_id HAVING MAX(...)` an
-- early draft guessed at, which changed the predicate and so the set.)
-- `run_id` breaks ties, so the order is total.
SELECT run_id
FROM node_run
WHERE status NOT IN (
        'FINISHED', 'ERROR', 'TIMED_OUT',
        'AGGREGATED', 'HAS_CHILD_ERROR', 'HAS_CHILD_TIMED_OUT',
        'CANCELLED'
      )
  AND updated_at < $1
GROUP BY run_id
ORDER BY min(updated_at), run_id
LIMIT $2

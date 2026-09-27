-- Take exactly the submissions the fair selection chose.
--
-- Deliberately **not** `SELECT ... FOR UPDATE SKIP LOCKED`, which is the usual
-- shape for a work queue. `select_batch` has to see a flow's whole bounded
-- backlog to compute its quota, so the fair choice cannot be made inside a
-- row-locking query that hands out whatever it happens to reach first. The
-- choice is made outside, and this is the atomic arbiter for it: two
-- dispatchers that both selected the same run_id race here, and `state =
-- 'queued'` in the WHERE means exactly one of them gets the row back.
--
-- So a short returned list is not an error. It is this working.
-- Rows are locked in `run_id` order, by the `ORDER BY` inside the `FOR UPDATE`
-- subquery. A bare `WHERE run_id = ANY($1)` locks them in whatever order the
-- chosen plan visits, and two dispatchers whose selections overlap but whose
-- arrays differ in size can get different plans -- an index scan against a
-- sequential one -- take the shared rows in opposite orders and deadlock.
-- Postgres resolves that by aborting one with 40P01, which arrives here as an
-- error rather than as the empty list a loser is supposed to get.
UPDATE submission
SET state = 'claimed', claimed_at = now()
WHERE run_id IN (
    SELECT run_id FROM submission
    WHERE run_id = ANY($1) AND state = 'queued'
    ORDER BY run_id
    FOR UPDATE
)
RETURNING run_id

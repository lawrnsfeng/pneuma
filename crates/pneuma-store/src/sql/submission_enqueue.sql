-- Accept a submission, and say what was already here if anything was.
--
-- `DO UPDATE ... SET run_id = submission.run_id` rather than `DO NOTHING`, and
-- the difference is what makes the answer honest. `DO NOTHING` returns no row
-- on conflict, so the only thing the caller can be told is "something was
-- there" -- and `done` rows are kept for ever, so "something was there" covered
-- a run that finished last month as well as one waiting to be dispatched. A
-- caller retrying a failed submission was told `AlreadyQueued` and dropped it.
--
-- The no-op update returns the existing row, so its state comes back. `xmax = 0`
-- is what separates the two cases: Postgres leaves `xmax` zero on a fresh
-- insert and non-zero on a row this statement updated.
INSERT INTO submission (run_id, tenant_id, payload)
VALUES ($1, $2, $3)
ON CONFLICT (run_id) DO UPDATE SET run_id = submission.run_id
RETURNING (xmax = 0) AS inserted, state::text AS state

-- Record how a claimed submission ended.
--
-- Only from `claimed`: settling a `queued` row would mark work done that no
-- dispatcher ever took, and settling an already-settled one would overwrite the
-- first outcome with a later duplicate's. Both return no row, which is what
-- lets a caller tell "settled" from "there was nothing of mine to settle".
UPDATE submission
-- `$2::submission_state`, because the value is bound as text and Postgres
-- will not coerce text into an enum column on its own: without the cast this
-- fails with "column \"state\" is of type submission_state but expression is of
-- type text", every time, at run time.
SET state = $2::submission_state, settled_at = now(), detail = $3
WHERE run_id = $1 AND state = 'claimed'
RETURNING run_id

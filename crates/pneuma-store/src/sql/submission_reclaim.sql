-- Return claims nobody settled to the queue.
--
-- Without this a dispatcher that dies between `claim` and `settle` strands its
-- rows for ever: `claim` only moves `queued -> claimed`, `settle` only moves
-- `claimed -> done|failed`, and nothing else looks at `claimed` at all. The
-- work is then invisible -- no query finds it, and re-submitting the same
-- `run_id` collides with the primary key and reports that it is already here.
-- Silently losing work is the one thing a durable queue must not do, and this
-- module exists because a submission has to survive a restart.
--
-- `claimed_at < $1` rather than a fixed interval so the caller owns the
-- definition of "too long", which depends on how long a dispatch legitimately
-- takes. Uses `ix_submission_claimed`.
--
-- `claimed_at` is cleared, so a reclaimed row is indistinguishable from one
-- that was never claimed -- which is what it is.
UPDATE submission
SET state = 'queued', claimed_at = NULL
WHERE state = 'claimed' AND claimed_at < $1
RETURNING run_id

-- How many rows `history_delete_outdated.sql` would remove.
--
-- The `WHERE` clause must match that file's exactly. It cannot be shared as a
-- value the way the Mongo filter is -- SQL has no cheap way to lift a predicate
-- out of two statements without a view -- so the guard is a test:
-- `counting_outdated_history_agrees_with_deleting_it` archives one row, then
-- counts and deletes at three cutoffs -- the row's own timestamp, one
-- microsecond after it, and a day after -- requiring the two numbers to agree at
-- each. The first is the one that matters: cutoffs far from the data agree
-- under either `<` or `<=`, so only the exact boundary can see that spelling
-- drift. Measured, after a version using now±1 day passed with the predicates
-- deliberately mismatched.
--
-- Why bother: the original plan runs the janitor with `--dry-run` against
-- production for a week and diffs it against the original one. A preview built
-- from a predicate that has drifted from the deleting one reports confidently
-- about a decision the real code does not make, which is worse than having no
-- preview at all.
SELECT count(*)
FROM node_run_history
WHERE created_at < $1::timestamptz

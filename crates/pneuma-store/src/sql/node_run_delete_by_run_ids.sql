-- Hard delete of every row for the given runs.
--
-- A hard DELETE despite the `is_deleted` column, which comes from the original's shared
-- base and which neither delete path in the original ever sets.
--
-- Parent and child rows go in one statement, which is what keeps the
-- self-referential FK satisfied: Postgres fires the constraint triggers at the
-- end of the statement, by when both are gone.
DELETE FROM node_run
WHERE run_id = ANY($1)

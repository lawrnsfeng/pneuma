-- The newest updated_at per run, used to decide
-- whether a run has gone quiet.
SELECT run_id, MAX(updated_at) AS max_updated_at
FROM node_run
WHERE run_id = ANY($1)
GROUP BY run_id

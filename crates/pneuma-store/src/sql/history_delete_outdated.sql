-- Delete history older than a cutoff.
--
-- The cutoff is a bound parameter, taken as an aware instant.
--
-- An earlier version of this comment claimed that binding it naively would make
-- the comparison depend on the session's TimeZone, citing the defect notes
-- §16. That entry is retracted: it generalised from a `TIMESTAMP` literal, and
-- a bound parameter against a TIMESTAMPTZ column is typed from the column and
-- encoded absolutely. There is no session dependence to guard against here.
--
-- `DateTime<Utc>` remains the right parameter type -- it says what it means and
-- cannot be misread -- but it is not load-bearing.
--
-- The original's `retention_days <= 0` early return is a caller-side decision,
-- not part of this statement: there is no cutoff to compute, so there is no
-- query to run.
DELETE FROM node_run_history
WHERE created_at < $1::timestamptz

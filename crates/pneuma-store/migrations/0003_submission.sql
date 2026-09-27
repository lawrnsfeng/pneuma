-- The durable submission queue the dispatcher selects from.
--
-- Not a port. Nothing in the original has this table: the original accepts a run
-- and dispatches it in the same breath, so a submission that arrives while the
-- controller is down is a submission that never happened. This is the durable
-- half of what `pneuma-fairness` needs to be usable at all -- a flow's backlog
-- has to survive a restart, or "fair over time" means "fair until the next
-- deploy".
--
-- `run_id` is the primary key rather than a surrogate. That is what makes
-- `enqueue` idempotent under `ON CONFLICT DO NOTHING`, and a redelivered
-- submission -- the ordinary case for any at-least-once transport -- is then a
-- no-op rather than a second run of work already queued.
CREATE TYPE submission_state AS ENUM ('queued', 'claimed', 'done', 'failed');

CREATE TABLE submission (
    run_id       VARCHAR          NOT NULL,
    tenant_id    VARCHAR          NOT NULL,
    payload      JSONB            NOT NULL,
    state        submission_state NOT NULL DEFAULT 'queued',
    enqueued_at  TIMESTAMPTZ      NOT NULL DEFAULT now(),
    claimed_at   TIMESTAMPTZ,
    settled_at   TIMESTAMPTZ,
    -- Why a claim ended, when it ended badly. Free text because it comes from
    -- whatever refused -- an ingress status, a transport error -- and inventing
    -- an enum for it would mean a migration every time something new can fail.
    detail       TEXT,
    CONSTRAINT submission_pkey PRIMARY KEY (run_id)
);

-- The dispatcher's only read: every queued submission, oldest first within a
-- tenant. Partial, because `queued` is the minority state in a healthy system
-- and the index has no reason to carry the rows that have already run -- which
-- are all of them, for ever, since `done` rows are kept.
--
-- `(tenant_id, enqueued_at)` in that order because the read groups by tenant
-- and then wants the oldest first inside each group, which is exactly what
-- `select_batch` consumes: a flow's backlog in arrival order.
CREATE INDEX ix_submission_queued
    ON submission (tenant_id, enqueued_at)
    WHERE state = 'queued';

-- Claimed submissions, for the watchdog that finds the ones nobody settled.
-- Also partial: a claim is meant to be brief, so this index stays small even
-- when the table does not.
CREATE INDEX ix_submission_claimed
    ON submission (claimed_at)
    WHERE state = 'claimed';

-- The dispatch round counter.
--
-- A sequence, not a column and not a per-process variable, and that is
-- load-bearing. `pneuma_fairness::select_batch` rotates its tie-break by the
-- round, and a batch that does not divide evenly must hand the remainder to
-- somebody: with a fixed round that is the same flow every time -- measured at
-- 800 items against 600 over 200 rounds, while every individual batch was
-- provably fair. A per-process counter resets on restart *and is per replica*,
-- so two dispatchers would both sit near zero and favour the same tenant. A
-- sequence is the one counter that is shared and monotonic across both.
--
-- `CYCLE` because the rotation only uses this modulo the number of flows, so
-- wrapping at the end of `bigint` is meaningless rather than fatal -- and
-- `NO CYCLE` would make it an error at a point no one will be alive for.
CREATE SEQUENCE dispatch_round AS BIGINT START 1 CYCLE;

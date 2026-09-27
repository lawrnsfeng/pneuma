-- The `noderun_history` table as the original migration tool created it.
--
-- Transcribed the original
-- plus the original's ix_noderun_history_created_at.
--
-- Same columns as `noderun`, and deliberately fewer constraints: the primary key
-- on `id` is the only one. There is no unique `slug` and no self-referential
-- foreign key on `parent_slug`, so history can hold many rows for one slug and
-- insert order does not matter.
--
-- The `id` primary key is load-bearing in a way worth stating: rows are copied
-- from `noderun` with their ids intact, so backing the same node run up twice is
-- a key violation. That is what wedges the janitor today
-- (the defect notes), and why HISTORY_BACKUP here is idempotent.

CREATE TABLE noderun_history (
    id            UUID        NOT NULL,
    slug          VARCHAR     NOT NULL,
    node_id       VARCHAR     NOT NULL,
    name          VARCHAR     NOT NULL,
    type          nodetype    NOT NULL,
    pipeline_id   VARCHAR     NOT NULL,
    run_id        VARCHAR     NOT NULL,
    parent_id     VARCHAR,
    parent_slug   VARCHAR,
    parent_type   VARCHAR,
    child_idx     INTEGER,
    nth           INTEGER,
    status        nodestatus  NOT NULL,
    step_input    JSON,
    step_output   JSON,
    error_code    VARCHAR,
    error_message VARCHAR,
    extra         JSONB,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    is_deleted    BOOLEAN,
    CONSTRAINT noderun_history_pkey PRIMARY KEY (id)
);

CREATE INDEX ix_noderun_history_id ON noderun_history (id);
CREATE INDEX ix_noderun_history_run_id ON noderun_history (run_id);
CREATE INDEX ix_noderun_history_slug ON noderun_history (slug);
CREATE INDEX ix_noderun_history_created_at ON noderun_history (created_at);

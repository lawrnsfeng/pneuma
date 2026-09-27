-- The `noderun` table as the original migration tool actually created it.
--
-- Transcribed from three the original migration tool revisions, in order:
--   the original   the table and its two enums
--   the original    ix_noderun_run_id
--   the original the two parent_slug indexes
-- An earlier version cited the first and third while actually containing the
-- second's index and neither of the third's -- which would have made
-- `pneuma-migrate baseline` report drift against a database that is correct. This is not a new schema: it is
-- the existing one written in SQL so that `pneuma-migrate baseline` can
-- fingerprint a live database against it, and so tests can stand up a real
-- Postgres that matches production rather than one that matches our hopes.
--
-- Two details that are easy to get wrong and are load-bearing:
--   * `nodetype` labels are PascalCase; `nodestatus` labels are
--     SCREAMING_SNAKE_CASE. Same table, different conventions.
--   * step_input/step_output are JSON; extra is JSONB. Not interchangeable.

CREATE TYPE nodetype AS ENUM ('Model', 'ListAggregator', 'DictAggregator', 'Condition');

CREATE TYPE nodestatus AS ENUM (
    'CREATED', 'PROCESSING', 'FINISHED', 'ERROR', 'TIMED_OUT', 'CANCELLED',
    'FORKED', 'AGGREGATED', 'HAS_CHILD_ERROR', 'HAS_CHILD_TIMED_OUT'
);

CREATE TABLE noderun (
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
    CONSTRAINT noderun_pkey PRIMARY KEY (id),
    CONSTRAINT uq_noderun_slug UNIQUE (slug),
    -- Self-referential: a child row cannot be inserted before its parent.
    CONSTRAINT noderun_parent_slug_fkey FOREIGN KEY (parent_slug) REFERENCES noderun (slug)
);

CREATE INDEX ix_noderun_id ON noderun (id);
CREATE UNIQUE INDEX ix_noderun_slug ON noderun (slug);
CREATE INDEX ix_noderun_run_id ON noderun (run_id);
-- From a7f3c9e2b1d4. The composite is what GET_BY_PARENT_SLUG's
-- `WHERE parent_slug = $1 ORDER BY child_idx` was indexed for.
CREATE INDEX ix_noderun_parent_slug ON noderun (parent_slug);
CREATE INDEX ix_noderun_parent_slug_child_idx ON noderun (parent_slug, child_idx);

-- Every name the original migration tool chose, replaced by one that says what the thing is.
--
-- The wire keys moved first (`slug` -> `path`, `nth` -> `sibling_index`,
-- `child_idx` -> `child_index`, `type` -> `kind`), and a column is the same
-- field one layer down. Leaving the columns behind would mean every query in
-- `src/sql/` translating between two vocabularies for ever, which is the state
-- this port was written to get out of.
--
-- **Renames, not drop-and-create.** `ALTER ... RENAME` is a catalogue update:
-- it takes an ACCESS EXCLUSIVE lock for the instant it runs, rewrites no rows,
-- and keeps every index, constraint and grant attached to the object. A
-- drop-and-create would lose the data, and a create-copy-drop would take as
-- long as the table is large. On a `noderun` of any production size the
-- difference is the whole maintenance window.
--
-- Postgres does **not** rename indexes and constraints when the table they are
-- on is renamed -- `noderun_pkey` survives a `noderun` -> `node_run` rename
-- under its old name. That is not cosmetic here: `pneuma-migrate` fingerprints
-- indexes and foreign keys *by name* (`pneuma-migrate/src/schema.rs`), so an
-- index left with its old name is a permanent `baseline` mismatch against a
-- database that is in fact correct. Each one is renamed explicitly below.
--
-- `0001` and `0002` are deliberately left describing the old names. They are
-- transcriptions of the original migration tool revisions and `ORIGINAL_THROUGH` points at them --
-- `baseline` adopts a live the original migration tool-era database by comparing against exactly
-- those two, so editing them would make every existing database mismatch at the
-- one moment it must not.

-- The enums first: a column's type name is resolved at rename time, so doing
-- these before the tables keeps the two steps independent.
ALTER TYPE nodetype   RENAME TO node_kind;
ALTER TYPE nodestatus RENAME TO node_status;

ALTER TABLE noderun         RENAME TO node_run;
ALTER TABLE noderun_history RENAME TO node_run_history;

ALTER TABLE node_run RENAME COLUMN slug        TO path;
ALTER TABLE node_run RENAME COLUMN type        TO kind;
ALTER TABLE node_run RENAME COLUMN parent_slug TO parent_path;
ALTER TABLE node_run RENAME COLUMN parent_type TO parent_kind;
ALTER TABLE node_run RENAME COLUMN child_idx   TO child_index;
ALTER TABLE node_run RENAME COLUMN nth         TO sibling_index;

ALTER TABLE node_run_history RENAME COLUMN slug        TO path;
ALTER TABLE node_run_history RENAME COLUMN type        TO kind;
ALTER TABLE node_run_history RENAME COLUMN parent_slug TO parent_path;
ALTER TABLE node_run_history RENAME COLUMN parent_type TO parent_kind;
ALTER TABLE node_run_history RENAME COLUMN child_idx   TO child_index;
ALTER TABLE node_run_history RENAME COLUMN nth         TO sibling_index;

ALTER TABLE node_run RENAME CONSTRAINT noderun_pkey             TO node_run_pkey;
ALTER TABLE node_run RENAME CONSTRAINT uq_noderun_slug          TO uq_node_run_path;
ALTER TABLE node_run RENAME CONSTRAINT noderun_parent_slug_fkey TO node_run_parent_path_fkey;
ALTER TABLE node_run_history RENAME CONSTRAINT noderun_history_pkey TO node_run_history_pkey;

-- `uq_noderun_slug` and `ix_noderun_slug` are two objects over one column --
-- the original migration tool created both, and the unique constraint carries an index of its own
-- name. Renaming the constraint above renamed that index with it; this renames
-- the separate one the original migration tool also made.
ALTER INDEX ix_noderun_id                   RENAME TO ix_node_run_id;
ALTER INDEX ix_noderun_slug                 RENAME TO ix_node_run_path;
ALTER INDEX ix_noderun_run_id               RENAME TO ix_node_run_run_id;
ALTER INDEX ix_noderun_parent_slug          RENAME TO ix_node_run_parent_path;
ALTER INDEX ix_noderun_parent_slug_child_idx RENAME TO ix_node_run_parent_path_child_index;

ALTER INDEX ix_noderun_history_id         RENAME TO ix_node_run_history_id;
ALTER INDEX ix_noderun_history_run_id     RENAME TO ix_node_run_history_run_id;
ALTER INDEX ix_noderun_history_slug       RENAME TO ix_node_run_history_path;
ALTER INDEX ix_noderun_history_created_at RENAME TO ix_node_run_history_created_at;

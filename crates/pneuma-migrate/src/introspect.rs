//! Reading a live schema out of Postgres into a [`Schema`].
//!
//! The I/O half of the fingerprint. Everything it produces is compared by
//! [`Schema::differences`], which is pure, so the rule that decides "these two
//! databases are the same" stays testable without a server and this module
//! stays a transcription of four queries.
//!
//! # Why four
//!
//! `information_schema.columns` gives columns; `pg_indexes` gives indexes;
//! `pg_constraint` gives the foreign keys and checks that produce no index and
//! would otherwise be invisible; `pg_enum` gives enum labels in declaration
//! order. Nothing here summarises or interprets — a fingerprint that decided
//! what mattered while reading would be deciding it twice.
//!
//! Every pass is restricted to real tables, and all three must be or they
//! disagree about what a table is — but each is guarding against a *different*
//! relation, which is why the three filters are not spelled the same way.
//! Measured on PG 16:
//!
//! - `information_schema.columns` includes plain views, so the column pass
//!   would give a view columns and make it a table.
//! - A *materialized* view is the sharper case and the opposite shape: it
//!   appears in `pg_indexes` and **not in `information_schema.tables` at all**.
//!   Filtering only the column pass therefore invents a table with indexes and
//!   no columns, and `baseline` reports `UnexpectedTable` against a database
//!   whose real tables match perfectly.
//! - The constraint pass is guarding against neither of those. Postgres refuses
//!   `ADD CONSTRAINT` on a view and on a materialized view, so no view can ever
//!   reach it. What it must exclude is a **foreign table**, which can carry a
//!   `CHECK` and which `information_schema` classifies as `FOREIGN`.
//!
//! All three therefore filter through the *same* `information_schema.tables`
//! subquery rather than each expressing "a real table" in its own dialect. The
//! constraint pass could have said `relkind IN ('r', 'p')` -- measured, that
//! selects exactly the same relations, since a partitioned parent and its
//! partitions are all `BASE TABLE`. It does not, for the reason in the next
//! paragraph.
//!
//! # Privileges, and why that decides the shape above
//!
//! `information_schema` is filtered by the current role's privileges;
//! `pg_indexes` and `pg_constraint` are not. Measured on PG 16 with a role
//! holding only `USAGE` on the schema: `pg_indexes` returns every index and
//! `pg_constraint` every constraint, while `information_schema.tables` and
//! `.columns` both return nothing.
//!
//! So the filter is not only about what a table *is*, it is what makes the
//! three table passes agree about what is *visible*. Routing all three through
//! `information_schema.tables` means an under-privileged role reads **no
//! tables** -- which `baseline` reports as every table missing, and refuses to
//! write a baseline row for. Had the constraint pass kept `relkind`, that same
//! role would instead produce tables carrying a constraint and no columns and
//! no indexes: a phantom, reported as drift, against a database that matches.
//!
//! The enum pass is the exception, and it is stated rather than papered over.
//! `pg_type`, `pg_enum` and `pg_namespace` are not privilege-filtered, and
//! `information_schema` exposes no view of enum labels to route it through, so
//! there is nothing to filter it by. Measured: the same `USAGE`-only role that
//! reads zero tables reads every enum label in full. A schema read by such a
//! role therefore comes back with no tables and all its enums -- not empty.
//!
//! That is a wrong answer either way, and `baseline` should be run as a role
//! that owns the schema. It is the wrong answer that fails loudly: the missing
//! tables dominate the report, and no baseline row is written.
//!
//! # Definitions are normalised on the way in
//!
//! `pg_get_indexdef` and `pg_get_constraintdef` both qualify a table with its
//! schema when that schema is not on `search_path`. The live database and a
//! scratch schema therefore disagree on every index and every foreign key by
//! construction, so [`strip_schema_qualifier`] runs
//! here rather than at comparison time: the stored [`Schema`] is then already
//! in the form two databases can be compared in, and no caller can forget.

use std::collections::BTreeMap;

use sqlx::{PgPool, Row};

use crate::schema::{strip_schema_qualifier, Column, Schema, Table};

/// Why a schema could not be read.
#[derive(Debug, thiserror::Error)]
pub enum IntrospectError {
    /// The database refused, or was unreachable.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Reads `schema` out of the database it is in.
///
/// An empty [`Schema`] for a schema that does not exist, which is the same
/// answer as one that exists and is empty. That is deliberate: `baseline`
/// compares against what the migrations produce, and both cases differ from it
/// in exactly the way the report should say — every table missing.
pub async fn introspect(pool: &PgPool, schema: &str) -> Result<Schema, IntrospectError> {
    let mut tables: BTreeMap<String, Table> = BTreeMap::new();

    let columns = sqlx::query(
        "SELECT table_name, column_name, data_type, udt_name, is_nullable, \
                character_maximum_length, numeric_precision, numeric_scale, \
                datetime_precision, column_default, identity_generation, \
                generation_expression \
         FROM information_schema.columns \
         WHERE table_schema = $1 \
           AND table_name IN ( \
             SELECT table_name FROM information_schema.tables \
             WHERE table_schema = $1 AND table_type = 'BASE TABLE' \
           )",
    )
    .bind(schema)
    .fetch_all(pool)
    .await?;
    for row in columns {
        let table: String = row.try_get("table_name")?;
        let name: String = row.try_get("column_name")?;
        let nullable: String = row.try_get("is_nullable")?;
        tables.entry(table).or_default().columns.insert(
            name,
            Column {
                data_type: row.try_get("data_type")?,
                udt_name: row.try_get("udt_name")?,
                // `information_schema` spells this as the strings `YES` and
                // `NO` rather than a boolean.
                nullable: nullable == "YES",
                max_length: row.try_get("character_maximum_length")?,
                numeric_precision: row.try_get("numeric_precision")?,
                numeric_scale: row.try_get("numeric_scale")?,
                datetime_precision: row.try_get("datetime_precision")?,
                // Not parsed. Postgres renders a default in its own normalised
                // form (`now()`, `'x'::text`), and both sides of the comparison
                // come from the same renderer, so they are comparable as text
                // -- parsing them here would be deciding what a default means,
                // which this module does not do. The one thing removed is the
                // schema's own name: an enum default renders as
                // `'queued'::<schema>.submission_state`, which would otherwise
                // differ between two schemas built from identical SQL.
                default: row
                    .try_get::<Option<String>, _>("column_default")?
                    .map(|default| strip_schema_qualifier(schema, &default)),
                identity: row.try_get("identity_generation")?,
                generated: row.try_get("generation_expression")?,
            },
        );
    }

    let indexes = sqlx::query(
        "SELECT tablename, indexname, indexdef FROM pg_indexes \
         WHERE schemaname = $1 \
           AND tablename IN ( \
             SELECT table_name FROM information_schema.tables \
             WHERE table_schema = $1 AND table_type = 'BASE TABLE' \
           )",
    )
    .bind(schema)
    .fetch_all(pool)
    .await?;
    for row in indexes {
        let table: String = row.try_get("tablename")?;
        let name: String = row.try_get("indexname")?;
        let definition: String = row.try_get("indexdef")?;
        tables
            .entry(table)
            .or_default()
            .indexes
            .insert(name, strip_schema_qualifier(schema, &definition));
    }

    let constraints = sqlx::query(
        "SELECT rel.relname AS table_name, con.conname, \
                pg_get_constraintdef(con.oid) AS definition \
         FROM pg_constraint con \
         JOIN pg_class rel ON rel.oid = con.conrelid \
         JOIN pg_namespace nsp ON nsp.oid = con.connamespace \
         WHERE nsp.nspname = $1 AND con.contype IN ('f', 'c') \
           AND rel.relname IN ( \
             SELECT table_name FROM information_schema.tables \
             WHERE table_schema = $1 AND table_type = 'BASE TABLE' \
           )",
    )
    .bind(schema)
    .fetch_all(pool)
    .await?;
    for row in constraints {
        let table: String = row.try_get("table_name")?;
        let name: String = row.try_get("conname")?;
        let definition: String = row.try_get("definition")?;
        tables
            .entry(table)
            .or_default()
            .constraints
            .insert(name, strip_schema_qualifier(schema, &definition));
    }

    let mut enums: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let labels = sqlx::query(
        "SELECT t.typname, e.enumlabel \
         FROM pg_type t \
         JOIN pg_enum e ON e.enumtypid = t.oid \
         JOIN pg_namespace n ON n.oid = t.typnamespace \
         WHERE n.nspname = $1 \
         ORDER BY t.typname, e.enumsortorder",
    )
    .bind(schema)
    .fetch_all(pool)
    .await?;
    for row in labels {
        let name: String = row.try_get("typname")?;
        let label: String = row.try_get("enumlabel")?;
        // `ORDER BY enumsortorder` is load-bearing, not tidiness: Postgres
        // compares and sorts enum values by declaration order, so two schemas
        // whose labels differ only in order are different schemas.
        enums.entry(name).or_default().push(label);
    }

    Ok(Schema { tables, enums })
}

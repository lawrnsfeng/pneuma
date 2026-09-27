//! What a Postgres schema contains, and how two of them differ.
//!
//! Pure: nothing here connects to anything. Introspection fills these types in
//! and the comparison is decided on the values, so the rule that says "these
//! two databases are the same" can be tested exhaustively without a server.
//!
//! # Every difference, not the first
//!
//! [`Schema::differences`] returns all of them. An operator running `baseline`
//! against a database that turns out not to match wants the list, not the
//! alphabetically-first item — otherwise reconciling a schema is one round trip
//! per discrepancy. The order is deterministic (everything is a `BTreeMap`) so
//! two runs against the same pair produce the same report.
//!
//! # Extra is a difference too
//!
//! A live database with a column the migrations do not create is *not* a match,
//! and saying so matters more than it looks: the usual cause is a migration
//! that ran and was then edited, so the schema is ahead of the file that claims
//! to describe it. Baselining that database records a lie about which
//! migrations produced it.
//!
//! # What this fingerprint does not see
//!
//! Stated because "the live schema matches the migrations" is what `baseline`
//! reports on the strength of it, and a reader has to know how much that
//! sentence covers. Two schemas can compare equal here and still differ in:
//!
//! - **Row-level security** — whether RLS is enabled on a table, and every
//!   policy on it.
//! - **Index validity** — an index left `indisvalid = false` by a failed
//!   `CREATE INDEX CONCURRENTLY` is present, named, and reported as matching.
//! - **Triggers**, and therefore anything implemented with them.
//! - **Grants and ownership** — who may read the table, and who owns it.
//! - **Column order** — the columns are a `BTreeMap`, so a table whose columns
//!   were added in a different order compares equal. That is deliberate for a
//!   fingerprint and wrong for anything relying on `SELECT *` or `INSERT`
//!   without a column list.
//! - **Everything on a column that is not one of [`Column`]'s fields**:
//!   collation, storage, statistics targets, comments.
//! - **Anything that is not a table, an enum, an index or a foreign key** —
//!   sequences that are not owned by a serial column, views, functions,
//!   extensions, domains, other constraint kinds.
//!
//! None of these is an oversight to be fixed by adding them: each would make
//! the fingerprint fail for a difference nobody can act on in the middle of a
//! baseline. The point is that this answers "do the migrations describe these
//! tables", not "are these two databases the same".

use std::collections::BTreeMap;

/// One column, as far as matching is concerned.
///
/// Deliberately not every attribute Postgres records. Collation, storage and
/// statistics targets do not change whether a query written against this table
/// works, and including them would make the fingerprint fail for reasons nobody
/// can act on.
///
/// What is here is enough to catch the drift a baseline must not affirm — a
/// column of the wrong type, nullability or default. It is **not** everything a
/// mismatch could break, which is what this used to claim: see the module
/// header for what the fingerprint does not see at all.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Column {
    /// The type as `information_schema` reports it, e.g. `uuid`,
    /// `timestamp with time zone`, or `USER-DEFINED` for an enum.
    pub data_type: String,
    /// The underlying type name: `int4`, `varchar`, or the enum's own name.
    ///
    /// Separate from `data_type` because `information_schema.columns` reports
    /// every enum as `USER-DEFINED`; without this, a `node_status` column and a
    /// `node_kind` column compare equal.
    ///
    /// Not an `Option`. It was one, on the assumption that it is set only for
    /// user-defined types; the database says otherwise — `integer` reports
    /// `int4` and `character varying` reports `varchar` — so an `Option` here
    /// modelled a `None` Postgres never produces and invited handling for it.
    pub udt_name: String,
    /// Whether the column accepts nulls.
    pub nullable: bool,
    /// `character_maximum_length`, for the types that have one.
    ///
    /// `VARCHAR(255)` and unbounded `VARCHAR` both report `data_type =
    /// 'character varying'` and `udt_name = 'varchar'`, so without this they
    /// compare equal. The migrations declare unbounded throughout; a live
    /// column created bounded by an older revision would fingerprint as
    /// matching, be baselined, and then fail at runtime on the first
    /// over-length `error_message`.
    pub max_length: Option<i32>,
    /// `numeric_precision`, for the same reason one step further out.
    ///
    /// No column in the current schema is `NUMERIC`, so this is always `None`
    /// today. It is carried anyway because the cost is a field and the cost of
    /// omitting it is discovering the omission from a rounding difference in
    /// production.
    pub numeric_precision: Option<i32>,
    /// `numeric_scale`, which is the half that actually decides rounding.
    ///
    /// Precision alone does not: `NUMERIC(10,2)` and `NUMERIC(10,4)` report the
    /// same `data_type`, `udt_name` and `numeric_precision`, so without this
    /// they are byte-identical and fingerprint as a match. Carrying precision
    /// and claiming it guards against "a rounding difference in production"
    /// while omitting scale was a guarantee this type did not provide.
    pub numeric_scale: Option<i32>,
    /// `datetime_precision`, which separates `TIMESTAMPTZ(0)` from
    /// `TIMESTAMPTZ`.
    ///
    /// Unlike the numeric pair this is not hypothetical: every timestamp column
    /// in `0001_noderun.sql` is `TIMESTAMPTZ`, so a live column created with an
    /// explicit precision would otherwise compare equal to one without.
    pub datetime_precision: Option<i32>,
    /// `column_default`, verbatim as Postgres renders it.
    ///
    /// The gap that made this type's "exhaustively" claim untrue. A live
    /// `created_at TIMESTAMPTZ NOT NULL DEFAULT now()` and a migration's
    /// `created_at TIMESTAMPTZ NOT NULL` agree on every other field here --
    /// measured, byte for byte -- so `baseline` would affirm that the
    /// migrations produced a schema they did not. A default is not cosmetic:
    /// it decides what an `INSERT` that omits the column writes.
    pub default: Option<String>,
    /// `identity_generation`: `ALWAYS`, `BY DEFAULT`, or `None`.
    ///
    /// Carried for the same reason as the default and with the same failure:
    /// an identity column and a plain one differ in nothing else this type
    /// records, and inserting into the wrong one fails at runtime rather than
    /// at baseline time.
    pub identity: Option<String>,
    /// `generation_expression`, for a `GENERATED ALWAYS AS` column.
    ///
    /// The expression rather than a flag, because two generated columns
    /// computing different things are not the same column.
    pub generated: Option<String>,
}

/// One table: its columns and its indexes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Table {
    /// By column name.
    pub columns: BTreeMap<String, Column>,
    /// Index name to its definition, as `pg_indexes.indexdef` gives it,
    /// normalised by [`strip_schema_qualifier`].
    ///
    /// The definition rather than just the name, because two indexes can share
    /// a name across schemas while covering different columns, and an index on
    /// the wrong column is exactly the kind of drift a fingerprint exists to
    /// catch. It also carries `UNIQUE`, which a name cannot.
    pub indexes: BTreeMap<String, String>,
    /// Foreign-key and check constraint names to their definitions.
    ///
    /// Only those two kinds. Primary keys and unique constraints surface in
    /// `pg_indexes` already, so including them here would report a missing
    /// primary key twice; a foreign key produces no index and is invisible
    /// without this. `migrations/0001_noderun.sql` has a self-referential
    /// `parent_path` foreign key, and a live database that lost it would
    /// otherwise fingerprint as identical and be baselined as current, with
    /// the ordering it enforces silently gone.
    pub constraints: BTreeMap<String, String>,
}

/// A whole schema: its tables and its enum types.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Schema {
    /// By table name.
    pub tables: BTreeMap<String, Table>,
    /// Enum type name to its labels, **in declaration order**.
    ///
    /// Order is part of the type: Postgres compares and sorts enum values by
    /// it, so two schemas whose labels differ only in order are not the same
    /// schema. A `Vec`, not a set, for that reason.
    pub enums: BTreeMap<String, Vec<String>>,
}

/// Removes a schema's own name where Postgres rendered it as a qualifier.
///
/// Postgres qualifies an object when its schema is not on `search_path`, so the
/// live database renders `ON public.node_run` and the scratch schema the
/// expectation is built in renders `ON pneuma_scratch.node_run`. Compared
/// verbatim, *every* index mismatches on a database that matches perfectly --
/// measured: thirteen spurious differences across two tables built from
/// identical SQL. The tempting fix, comparing names only, is precisely the
/// check that cannot see an index on the wrong column.
///
/// # Why the schema name, and not the position
///
/// This replaced two functions that stripped the token after ` ON ` and after
/// `REFERENCES `, chosen because those are the positions where a qualifier is
/// structural. That worked for `0001` and `0002` and stopped working the moment
/// `0003_submission` landed a partial index and an enum default: a qualifier
/// also appears in `WHERE (state = 'queued'::pneuma_scratch.submission_state)`
/// and in `'queued'::pneuma_scratch.submission_state` as a column default,
/// neither of which is after either keyword. The old version recorded that as a
/// known limit; this is the fix.
///
/// Matching on the name is *exact* rather than positional: introspection is
/// told which schema it is reading, so `schema.` is a qualifier the renderer
/// added and nothing else. That also makes it safe where the positional rule
/// was not -- a `CHECK` could not be normalised at all before, because
/// stripping "the token after a keyword" out of arbitrary SQL deletes content.
///
/// # What is still not handled
///
/// A reference to a *different* schema. `REFERENCES other.thing` is left as it
/// is, and rightly: that is not a rendering artefact, it is a real difference
/// between two databases.
///
/// **A qualifier Postgres renders *inside* a string literal.** Text in a
/// literal is content, so a default of `'pneuma_scratch.x'` survives intact --
/// which is right for a literal somebody wrote and wrong for the one place
/// Postgres writes one itself: a sequence-backed default renders as
/// `nextval('<schema>.foo_seq'::regclass)`, and the qualifier is inside the
/// quotes. Two schemas built from identical SQL would differ on that default,
/// and `baseline` would report drift on a database that matches -- exactly the
/// failure `0003_submission` caused in the structural position.
///
/// No migration has such a default: `submission.run_id` is a `VARCHAR` primary
/// key and `dispatch_round` is a standalone `SEQUENCE` owned by no column, so
/// nothing renders a `nextval`. The first `SERIAL` or identity column added
/// here brings it back, and the fix then is to recognise the `nextval('…')`
/// form specifically rather than to stop skipping literals -- which would break
/// every default a person wrote. Recorded so the limit is known rather than
/// discovered.
pub fn strip_schema_qualifier(schema: &str, rendered: &str) -> String {
    if schema.is_empty() {
        return rendered.to_owned();
    }
    // Both spellings Postgres can produce for one schema: bare when the name is
    // a plain identifier, quoted when it is not.
    let qualifiers = [format!("{schema}."), format!("\"{schema}\".")];

    let mut out = String::with_capacity(rendered.len());
    let mut rest = rendered;
    let mut in_literal = false;
    while let Some(next) = rest.chars().next() {
        if next == '\'' {
            // `''` inside a literal is an escaped quote, and flipping twice
            // leaves the state right without special-casing it.
            in_literal = !in_literal;
            out.push(next);
            rest = &rest[next.len_utf8()..];
            continue;
        }
        if !in_literal {
            if let Some(stripped) = qualifiers
                .iter()
                .find_map(|qualifier| rest.strip_prefix(qualifier.as_str()))
            {
                // Only where the name starts, so `x_pneuma_scratch.t` -- a
                // different schema whose name ends with this one -- is left
                // alone. `is_some_and`, so "nothing written yet" is the same
                // answer as "the last character cannot end an identifier"
                // rather than a second arm nothing reaches.
                if !out.chars().next_back().is_some_and(ends_identifier) {
                    rest = stripped;
                    continue;
                }
            }
        }
        out.push(next);
        rest = &rest[next.len_utf8()..];
    }
    out
}

/// Whether a character could be the last one of an identifier already written.
fn ends_identifier(previous: char) -> bool {
    previous.is_alphanumeric() || previous == '_' || previous == '$' || previous == '"'
}

/// One way two schemas fail to match.
///
/// Every variant names the thing and both sides where there are two, so a
/// report can be acted on without going back to the database.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Difference {
    /// A table the migrations create and the live database does not have.
    #[error("table {table} is missing")]
    MissingTable {
        /// Which table.
        table: String,
    },
    /// A table the live database has and the migrations do not create.
    #[error("table {table} is present but unexpected")]
    UnexpectedTable {
        /// Which table.
        table: String,
    },
    /// A column the migrations create and the live database does not have.
    #[error("{table}.{column} is missing")]
    MissingColumn {
        /// Which table.
        table: String,
        /// Which column.
        column: String,
    },
    /// A column the live database has and the migrations do not create.
    #[error("{table}.{column} is present but unexpected")]
    UnexpectedColumn {
        /// Which table.
        table: String,
        /// Which column.
        column: String,
    },
    /// A column that exists on both but does not match.
    #[error("{table}.{column} is {found:?}, expected {expected:?}")]
    ColumnMismatch {
        /// Which table.
        table: String,
        /// Which column.
        column: String,
        /// What the live database has.
        found: Box<Column>,
        /// What the migrations produce.
        expected: Box<Column>,
    },
    /// An index the migrations create and the live database does not have.
    #[error("index {index} on {table} is missing")]
    MissingIndex {
        /// Which table.
        table: String,
        /// Which index.
        index: String,
    },
    /// An index the live database has and the migrations do not create.
    #[error("index {index} on {table} is present but unexpected")]
    UnexpectedIndex {
        /// Which table.
        table: String,
        /// Which index.
        index: String,
    },
    /// An index that exists on both but covers something else.
    #[error("index {index} on {table} is {found:?}, expected {expected:?}")]
    IndexMismatch {
        /// Which table.
        table: String,
        /// Which index.
        index: String,
        /// The live definition.
        found: String,
        /// The definition the migrations produce.
        expected: String,
    },
    /// A constraint the migrations create and the live database does not have.
    #[error("constraint {constraint} on {table} is missing")]
    MissingConstraint {
        /// Which table.
        table: String,
        /// Which constraint.
        constraint: String,
    },
    /// A constraint the live database has and the migrations do not create.
    #[error("constraint {constraint} on {table} is present but unexpected")]
    UnexpectedConstraint {
        /// Which table.
        table: String,
        /// Which constraint.
        constraint: String,
    },
    /// A constraint that exists on both but says something else.
    #[error("constraint {constraint} on {table} is {found:?}, expected {expected:?}")]
    ConstraintMismatch {
        /// Which table.
        table: String,
        /// Which constraint.
        constraint: String,
        /// The live definition.
        found: String,
        /// The definition the migrations produce.
        expected: String,
    },
    /// An enum type the migrations create and the live database does not have.
    #[error("type {name} is missing")]
    MissingEnum {
        /// Which type.
        name: String,
    },
    /// An enum type the live database has and the migrations do not create.
    #[error("type {name} is present but unexpected")]
    UnexpectedEnum {
        /// Which type.
        name: String,
    },
    /// An enum that exists on both with different labels, or a different order.
    #[error("type {name} has labels {found:?}, expected {expected:?}")]
    EnumMismatch {
        /// Which type.
        name: String,
        /// The live labels, in order.
        found: Vec<String>,
        /// The labels the migrations produce, in order.
        expected: Vec<String>,
    },
}

impl Schema {
    /// How this schema differs from `expected`, exhaustively.
    ///
    /// `self` is the live database and `expected` is what the migrations
    /// produce; the variant names read from that direction. Empty means the two
    /// match and a baseline row would be truthful.
    pub fn differences(&self, expected: &Schema) -> Vec<Difference> {
        let mut found = Vec::new();
        self.compare_enums(expected, &mut found);
        self.compare_tables(expected, &mut found);
        found
    }

    fn compare_enums(&self, expected: &Schema, out: &mut Vec<Difference>) {
        for (name, want) in &expected.enums {
            match self.enums.get(name) {
                None => out.push(Difference::MissingEnum { name: name.clone() }),
                Some(have) if have != want => out.push(Difference::EnumMismatch {
                    name: name.clone(),
                    found: have.clone(),
                    expected: want.clone(),
                }),
                Some(_) => {}
            }
        }
        for name in self.enums.keys() {
            if !expected.enums.contains_key(name) {
                out.push(Difference::UnexpectedEnum { name: name.clone() });
            }
        }
    }

    fn compare_tables(&self, expected: &Schema, out: &mut Vec<Difference>) {
        for (name, want) in &expected.tables {
            match self.tables.get(name) {
                None => out.push(Difference::MissingTable {
                    table: name.clone(),
                }),
                Some(have) => have.compare(name, want, out),
            }
        }
        for name in self.tables.keys() {
            if !expected.tables.contains_key(name) {
                out.push(Difference::UnexpectedTable {
                    table: name.clone(),
                });
            }
        }
    }
}

impl Table {
    /// Appends how this table differs from `expected`.
    fn compare(&self, table: &str, expected: &Table, out: &mut Vec<Difference>) {
        for (column, want) in &expected.columns {
            match self.columns.get(column) {
                None => out.push(Difference::MissingColumn {
                    table: table.to_owned(),
                    column: column.clone(),
                }),
                Some(have) if have != want => out.push(Difference::ColumnMismatch {
                    table: table.to_owned(),
                    column: column.clone(),
                    found: Box::new(have.clone()),
                    expected: Box::new(want.clone()),
                }),
                Some(_) => {}
            }
        }
        for column in self.columns.keys() {
            if !expected.columns.contains_key(column) {
                out.push(Difference::UnexpectedColumn {
                    table: table.to_owned(),
                    column: column.clone(),
                });
            }
        }

        for (index, want) in &expected.indexes {
            match self.indexes.get(index) {
                None => out.push(Difference::MissingIndex {
                    table: table.to_owned(),
                    index: index.clone(),
                }),
                Some(have) if have != want => out.push(Difference::IndexMismatch {
                    table: table.to_owned(),
                    index: index.clone(),
                    found: have.clone(),
                    expected: want.clone(),
                }),
                Some(_) => {}
            }
        }
        for index in self.indexes.keys() {
            if !expected.indexes.contains_key(index) {
                out.push(Difference::UnexpectedIndex {
                    table: table.to_owned(),
                    index: index.clone(),
                });
            }
        }

        for (constraint, want) in &expected.constraints {
            match self.constraints.get(constraint) {
                None => out.push(Difference::MissingConstraint {
                    table: table.to_owned(),
                    constraint: constraint.clone(),
                }),
                Some(have) if have != want => out.push(Difference::ConstraintMismatch {
                    table: table.to_owned(),
                    constraint: constraint.clone(),
                    found: have.clone(),
                    expected: want.clone(),
                }),
                Some(_) => {}
            }
        }
        for constraint in self.constraints.keys() {
            if !expected.constraints.contains_key(constraint) {
                out.push(Difference::UnexpectedConstraint {
                    table: table.to_owned(),
                    constraint: constraint.clone(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(data_type: &str, nullable: bool) -> Column {
        Column {
            data_type: data_type.to_owned(),
            udt_name: "int4".to_owned(),
            nullable,
            max_length: None,
            numeric_precision: None,
            numeric_scale: None,
            datetime_precision: None,
            default: None,
            identity: None,
            generated: None,
        }
    }

    /// A small schema standing in for the real one: an enum, a table with two
    /// columns and an index.
    fn expected() -> Schema {
        let mut columns = BTreeMap::new();
        columns.insert("id".to_owned(), column("uuid", false));
        columns.insert(
            "status".to_owned(),
            Column {
                data_type: "USER-DEFINED".to_owned(),
                udt_name: "node_status".to_owned(),
                nullable: false,
                max_length: None,
                numeric_precision: None,
                numeric_scale: None,
                datetime_precision: None,
                default: None,
                identity: None,
                generated: None,
            },
        );
        let mut indexes = BTreeMap::new();
        indexes.insert(
            "ix_noderun_id".to_owned(),
            "CREATE INDEX ix_noderun_id ON public.node_run USING btree (id)".to_owned(),
        );
        let mut constraints = BTreeMap::new();
        constraints.insert(
            "noderun_parent_slug_fkey".to_owned(),
            "FOREIGN KEY (parent_path) REFERENCES node_run(path)".to_owned(),
        );
        let mut tables = BTreeMap::new();
        tables.insert(
            "node_run".to_owned(),
            Table {
                columns,
                indexes,
                constraints,
            },
        );
        let mut enums = BTreeMap::new();
        enums.insert(
            "node_status".to_owned(),
            vec!["CREATED".to_owned(), "FINISHED".to_owned()],
        );
        Schema { tables, enums }
    }

    #[test]
    fn a_schema_matches_itself() {
        assert!(expected().differences(&expected()).is_empty());
    }

    #[test]
    fn a_missing_table_column_index_or_enum_is_reported() {
        let live = Schema::default();
        let found = live.differences(&expected());
        assert!(found.contains(&Difference::MissingTable {
            table: "node_run".to_owned()
        }));
        assert!(found.contains(&Difference::MissingEnum {
            name: "node_status".to_owned()
        }));
        // A missing *table* does not also report each of its columns: the
        // report should say the one actionable thing, not fifteen consequences
        // of it.
        assert_eq!(found.len(), 2, "{found:?}");
    }

    #[test]
    fn a_table_present_but_incomplete_reports_what_it_lacks() {
        // The case a *missing table* does not reach, and the likelier one in
        // practice: the original migration tool ran an early revision and not a later one, so the
        // table is there and an index or column added afterwards is not.
        // `pneuma-store`'s own migrations are three the original migration tool revisions squashed,
        // and a database stopped between them looks exactly like this.
        let mut live = expected();
        match live.tables.get_mut("node_run") {
            Some(table) => {
                table.columns.remove("status");
                table.indexes.remove("ix_noderun_id");
            }
            None => panic!("the fixture has that table"),
        }

        let found = live.differences(&expected());
        assert!(found.contains(&Difference::MissingColumn {
            table: "node_run".to_owned(),
            column: "status".to_owned()
        }));
        assert!(found.contains(&Difference::MissingIndex {
            table: "node_run".to_owned(),
            index: "ix_noderun_id".to_owned()
        }));
        assert_eq!(found.len(), 2, "{found:?}");
    }

    #[test]
    fn a_column_that_differs_is_reported_with_both_sides() {
        let mut live = expected();
        match live.tables.get_mut("node_run") {
            Some(table) => {
                table.columns.insert("id".to_owned(), column("text", true));
            }
            None => panic!("the fixture has that table"),
        }
        let found = live.differences(&expected());
        let Some(Difference::ColumnMismatch {
            table,
            column: name,
            found: have,
            expected: want,
        }) = found.first()
        else {
            panic!("expected a column mismatch, got {found:?}");
        };
        assert_eq!(table, "node_run");
        assert_eq!(name, "id");
        assert_eq!(have.data_type, "text", "the live side");
        assert_eq!(want.data_type, "uuid", "and what the migrations produce");
        assert_eq!(found.len(), 1, "one column, one difference");
    }

    #[test]
    fn two_enums_reported_as_user_defined_are_still_told_apart() {
        // `information_schema.columns` reports every enum as `USER-DEFINED`, so
        // without `udt_name` a `node_status` column and a `node_kind` column
        // compare equal -- a schema with the two swapped would fingerprint as
        // matching, and every read of it would fail at runtime.
        let mut live = expected();
        match live.tables.get_mut("node_run") {
            Some(table) => {
                table.columns.insert(
                    "status".to_owned(),
                    Column {
                        data_type: "USER-DEFINED".to_owned(),
                        udt_name: "node_kind".to_owned(),
                        nullable: false,
                        max_length: None,
                        numeric_precision: None,
                        numeric_scale: None,
                        datetime_precision: None,
                        default: None,
                        identity: None,
                        generated: None,
                    },
                );
            }
            None => panic!("the fixture has that table"),
        }
        assert_eq!(
            live.differences(&expected()).len(),
            1,
            "the swap is caught by the type name, not by `data_type`"
        );
    }

    #[test]
    fn enum_label_order_is_part_of_the_type() {
        // Postgres compares and sorts enum values by declaration order, so two
        // schemas whose labels differ only in order are not the same schema --
        // an `ORDER BY status` would return rows in a different sequence.
        let mut live = expected();
        live.enums.insert(
            "node_status".to_owned(),
            vec!["FINISHED".to_owned(), "CREATED".to_owned()],
        );
        let found = live.differences(&expected());
        assert!(
            matches!(found.first(), Some(Difference::EnumMismatch { .. })),
            "{found:?}"
        );
    }

    #[test]
    fn an_index_on_the_wrong_column_is_reported() {
        let mut live = expected();
        match live.tables.get_mut("node_run") {
            Some(table) => {
                table.indexes.insert(
                    "ix_noderun_id".to_owned(),
                    "CREATE INDEX ix_noderun_id ON public.node_run USING btree (path)".to_owned(),
                );
            }
            None => panic!("the fixture has that table"),
        }
        let found = live.differences(&expected());
        let Some(Difference::IndexMismatch { found: have, .. }) = found.first() else {
            panic!("expected an index mismatch, got {found:?}");
        };
        assert!(
            have.contains("path"),
            "the report carries the live definition"
        );
    }

    #[test]
    fn what_the_live_database_has_extra_is_a_difference_too() {
        // A schema ahead of the migrations that claim to describe it is not a
        // match. The usual cause is a migration that ran and was then edited,
        // and baselining that records a lie about which migrations produced it.
        let mut live = expected();
        live.tables.insert("leftover".to_owned(), Table::default());
        live.enums
            .insert("oldtype".to_owned(), vec!["X".to_owned()]);
        match live.tables.get_mut("node_run") {
            Some(table) => {
                table
                    .columns
                    .insert("extra".to_owned(), column("text", true));
                table
                    .indexes
                    .insert("ix_extra".to_owned(), "CREATE INDEX ...".to_owned());
            }
            None => panic!("the fixture has that table"),
        }

        let found = live.differences(&expected());
        assert!(found.contains(&Difference::UnexpectedTable {
            table: "leftover".to_owned()
        }));
        assert!(found.contains(&Difference::UnexpectedEnum {
            name: "oldtype".to_owned()
        }));
        assert!(found.contains(&Difference::UnexpectedColumn {
            table: "node_run".to_owned(),
            column: "extra".to_owned()
        }));
        assert!(found.contains(&Difference::UnexpectedIndex {
            table: "node_run".to_owned(),
            index: "ix_extra".to_owned()
        }));
        assert_eq!(found.len(), 4, "{found:?}");
    }

    #[test]
    fn every_difference_is_reported_not_just_the_first() {
        // An operator reconciling a schema wants the list. One round trip per
        // discrepancy is how a fingerprint becomes useless.
        let live = Schema::default();
        let mut want = expected();
        want.enums
            .insert("node_kind".to_owned(), vec!["Model".to_owned()]);
        want.tables.insert("other".to_owned(), Table::default());
        assert_eq!(live.differences(&want).len(), 4, "two tables and two enums");
    }
    #[test]
    fn a_definition_compares_without_the_schema_it_was_read_from() {
        // `pg_get_indexdef` qualifies the table when its schema is not on
        // `search_path`, so the live side and a scratch schema differ on every
        // index by construction. Verified against the running Postgres: the
        // live definition really does read `ON pneuma_test_store.node_run`.
        let live = "CREATE INDEX ix_node_run_id ON pneuma_test_store.node_run USING btree (id)";
        let scratch = "CREATE INDEX ix_node_run_id ON pneuma_scratch.node_run USING btree (id)";
        assert_eq!(
            strip_schema_qualifier("pneuma_test_store", live),
            strip_schema_qualifier("pneuma_scratch", scratch)
        );
        assert_eq!(
            strip_schema_qualifier("pneuma_test_store", live),
            "CREATE INDEX ix_node_run_id ON node_run USING btree (id)"
        );

        // And nothing else is removed: the column list, the method and
        // `UNIQUE` all still decide the comparison, which comparing names
        // alone would have thrown away.
        let unique = "CREATE UNIQUE INDEX ix_path ON s.node_run USING btree (path)";
        let plain = "CREATE INDEX ix_path ON s.node_run USING btree (path)";
        assert_ne!(
            strip_schema_qualifier("s", unique),
            strip_schema_qualifier("s", plain)
        );
        let other_column = "CREATE INDEX ix_path ON s.node_run USING btree (run_id)";
        assert_ne!(
            strip_schema_qualifier("s", plain),
            strip_schema_qualifier("s", other_column)
        );
    }

    #[test]
    fn a_qualifier_outside_the_structural_position_is_removed_too() {
        // The case that retired the positional version. `0003_submission` has
        // a partial index and an enum default, and Postgres qualifies the type
        // in both -- neither of them after ` ON ` or `REFERENCES `.
        let live = "CREATE INDEX ix_submission_queued ON live.submission \
                    USING btree (tenant_id) WHERE (state = 'queued'::live.submission_state)";
        let scratch = "CREATE INDEX ix_submission_queued ON scratch.submission \
                    USING btree (tenant_id) WHERE (state = 'queued'::scratch.submission_state)";
        assert_eq!(
            strip_schema_qualifier("live", live),
            strip_schema_qualifier("scratch", scratch)
        );

        // A column default, which is the other half of the same failure.
        assert_eq!(
            strip_schema_qualifier("live", "'queued'::live.submission_state"),
            strip_schema_qualifier("scratch", "'queued'::scratch.submission_state")
        );

        // And a `CHECK`, which the positional rule could not touch at all
        // because stripping the token after a keyword out of arbitrary SQL
        // deletes content. Matching the schema's own name has no such problem.
        assert_eq!(
            strip_schema_qualifier("live", "CHECK ((state <> 'ON HOLD'::live.workstate))"),
            "CHECK ((state <> 'ON HOLD'::workstate))"
        );
    }

    #[test]
    fn only_this_schema_is_removed_and_never_from_a_literal() {
        // A reference to a *different* schema stays: that is not a rendering
        // artefact, it is a real difference between two databases.
        let elsewhere = "FOREIGN KEY (parent_path) REFERENCES other.node_run(path)";
        assert_eq!(strip_schema_qualifier("live", elsewhere), elsewhere);

        // A schema whose name merely ends with this one is not a match either.
        let longer = "CREATE INDEX i ON x_live.t USING btree (c)";
        assert_eq!(strip_schema_qualifier("live", longer), longer);

        // A definition that *is* a qualified name, so the qualifier is at the
        // very start and there is no preceding character to judge.
        assert_eq!(strip_schema_qualifier("live", "live.t"), "t");

        // Text inside a string literal is content, not a qualifier.
        let literal = "DEFAULT 'live.thing'::text";
        assert_eq!(
            strip_schema_qualifier("live", literal),
            "DEFAULT 'live.thing'::text"
        );

        // Two literals, so the scanner's quote state has to come back down --
        // a single flip would leave everything after the first quote immune.
        let two = "CHECK ((a <> 'x') AND (b = 'y'::live.t))";
        assert_eq!(
            strip_schema_qualifier("live", two),
            "CHECK ((a <> 'x') AND (b = 'y'::t))"
        );
    }

    #[test]
    fn a_definition_that_carries_no_qualifier_is_left_alone() {
        // Which is what Postgres emits when the schema *is* on `search_path`.
        // Stripping is idempotent, so a definition that arrives without a
        // prefix compares equal to one that arrived with it and had it
        // removed -- the whole point, since the two sides of the comparison may
        // differ in exactly that.
        let bare = "CREATE INDEX ix_node_run_id ON node_run USING btree (id)";
        assert_eq!(strip_schema_qualifier("s", bare), bare);
        assert_eq!(
            strip_schema_qualifier("s", bare),
            strip_schema_qualifier(
                "s",
                "CREATE INDEX ix_node_run_id ON s.node_run USING btree (id)"
            )
        );
        assert_eq!(
            strip_schema_qualifier("s", &strip_schema_qualifier("s", bare)),
            strip_schema_qualifier("s", bare),
            "idempotent"
        );
        assert_eq!(strip_schema_qualifier("s", "nonsense"), "nonsense");

        // An empty schema name would otherwise match at every character
        // boundary, so it is refused up front rather than producing nonsense.
        assert_eq!(strip_schema_qualifier("", "a.b"), "a.b");
    }

    #[test]
    fn a_schema_needing_quotes_is_recognised_in_the_form_postgres_writes() {
        // Postgres quotes an identifier that is not a plain one, so the same
        // schema can render either way depending on its name.
        assert_eq!(
            strip_schema_qualifier("a.b", "CREATE INDEX i ON \"a.b\".t USING btree (c)"),
            "CREATE INDEX i ON t USING btree (c)"
        );
        // And a quoted *table* is not mistaken for the schema: the qualifier
        // has to come first.
        let quoted_table = "CREATE INDEX i ON s.\"dot.table\" USING btree (c)";
        assert_eq!(
            strip_schema_qualifier("s", quoted_table),
            "CREATE INDEX i ON \"dot.table\" USING btree (c)"
        );
    }

    #[test]
    fn a_lost_foreign_key_is_a_difference() {
        // It produces no index, so columns-and-indexes alone cannot see it. A
        // database that lost `noderun_parent_slug_fkey` would fingerprint as
        // identical, be baselined as current, and the ordering it enforces
        // would be silently gone.
        let mut live = expected();
        match live.tables.get_mut("node_run") {
            Some(table) => table.constraints.clear(),
            None => panic!("the fixture has that table"),
        }
        let found = live.differences(&expected());
        assert_eq!(
            found,
            vec![Difference::MissingConstraint {
                table: "node_run".to_owned(),
                constraint: "noderun_parent_slug_fkey".to_owned(),
            }]
        );

        // And one pointing somewhere else, or one nobody asked for.
        let mut altered = expected();
        match altered.tables.get_mut("node_run") {
            Some(table) => {
                table.constraints.insert(
                    "noderun_parent_slug_fkey".to_owned(),
                    "FOREIGN KEY (parent_path) REFERENCES other(path)".to_owned(),
                );
                table.constraints.insert(
                    "extra_check".to_owned(),
                    "CHECK (sibling_index > 0)".to_owned(),
                );
            }
            None => panic!("the fixture has that table"),
        }
        let found = altered.differences(&expected());
        assert!(found
            .iter()
            .any(|d| matches!(d, Difference::ConstraintMismatch { .. })));
        assert!(found
            .iter()
            .any(|d| matches!(d, Difference::UnexpectedConstraint { .. })));
    }

    #[test]
    fn a_bounded_varchar_is_not_an_unbounded_one() {
        // Both report `character varying` / `varchar`, so without the length
        // they compare equal -- and a column created bounded by an older
        // revision would be baselined as matching, then fail at runtime on the
        // first over-length `error_message`.
        let mut live = expected();
        match live.tables.get_mut("node_run") {
            Some(table) => {
                table.columns.insert(
                    "id".to_owned(),
                    Column {
                        data_type: "character varying".to_owned(),
                        udt_name: "int4".to_owned(),
                        nullable: false,
                        max_length: Some(255),
                        numeric_precision: None,
                        numeric_scale: None,
                        datetime_precision: None,
                        default: None,
                        identity: None,
                        generated: None,
                    },
                );
            }
            None => panic!("the fixture has that table"),
        }
        let mut want = expected();
        match want.tables.get_mut("node_run") {
            Some(table) => {
                table.columns.insert(
                    "id".to_owned(),
                    Column {
                        data_type: "character varying".to_owned(),
                        udt_name: "int4".to_owned(),
                        nullable: false,
                        max_length: None,
                        numeric_precision: None,
                        numeric_scale: None,
                        datetime_precision: None,
                        default: None,
                        identity: None,
                        generated: None,
                    },
                );
            }
            None => panic!("the fixture has that table"),
        }
        assert_eq!(live.differences(&want).len(), 1, "the length decides it");
    }

    #[test]
    fn every_message_says_which_direction_it_means() {
        // The eleven `#[error]` strings are this crate's entire operator-facing
        // output, and tarpaulin does not count derive-generated lines -- so a
        // message reading "is missing" on an `Unexpected` variant, or with
        // `found` and `expected` swapped, would ship with the gate green. This
        // is the only thing that reads them.
        let missing = Difference::MissingTable {
            table: "node_run".to_owned(),
        };
        assert_eq!(missing.to_string(), "table node_run is missing");
        let unexpected = Difference::UnexpectedTable {
            table: "leftover".to_owned(),
        };
        assert_eq!(
            unexpected.to_string(),
            "table leftover is present but unexpected"
        );

        // The variants carrying both sides must not swap them: `found` is the
        // live database, `expected` is what the migrations produce.
        let mismatch = Difference::IndexMismatch {
            table: "node_run".to_owned(),
            index: "ix".to_owned(),
            found: "LIVE".to_owned(),
            expected: "WANTED".to_owned(),
        };
        let rendered = mismatch.to_string();
        let Some(found_at) = rendered.find("LIVE") else {
            panic!("the live side is not in the message: {rendered}");
        };
        let Some(expected_at) = rendered.find("WANTED") else {
            panic!("the expected side is not in the message: {rendered}");
        };
        assert!(
            found_at < expected_at,
            "the live side is reported first, as the wording promises: {rendered}"
        );

        for (difference, wants) in [
            (
                Difference::MissingColumn {
                    table: "t".to_owned(),
                    column: "c".to_owned(),
                },
                "t.c is missing",
            ),
            (
                Difference::MissingIndex {
                    table: "t".to_owned(),
                    index: "i".to_owned(),
                },
                "index i on t is missing",
            ),
            (
                Difference::MissingConstraint {
                    table: "t".to_owned(),
                    constraint: "k".to_owned(),
                },
                "constraint k on t is missing",
            ),
            (
                Difference::MissingEnum {
                    name: "e".to_owned(),
                },
                "type e is missing",
            ),
        ] {
            assert_eq!(difference.to_string(), wants);
        }
    }
}

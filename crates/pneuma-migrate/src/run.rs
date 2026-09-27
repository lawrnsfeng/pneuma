//! Executing what the command line asked for.
//!
//! The I/O half. Everything decidable without a database is decided in
//! [`crate::cli`]; this connects, calls the library, and reports.

use mongodb::bson::Document;
use mongodb::Collection;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::baseline::{baseline, preview, BaselineError, Plan};
use crate::cli::Invocation;
use crate::introspect::{introspect, IntrospectError};
use crate::mongo::{duplicate_run_ids, ensure_run_id_index, Duplicate, MongoError};
use crate::schema::Schema;

/// Where Postgres is.
pub const DATABASE_URL: &str = "DATABASE_URL";
/// Where Mongo is.
pub const MONGODB_URL: &str = "PNEUMA_MONGODB_URL";
/// Which Mongo database holds the runs.
pub const MONGODB_DATABASE: &str = "PNEUMA_MONGODB_DATABASE";
/// Which collection holds the runs.
pub const MONGODB_RUNS_COLLECTION: &str = "PNEUMA_MONGODB_RUNS_COLLECTION";

/// The default Mongo database, matching the janitor's.
pub const DEFAULT_MONGO_DATABASE: &str = "pneuma";
/// The default collection, matching what the original migration tool revision indexes.
pub const DEFAULT_RUNS_COLLECTION: &str = "runs";

/// What a finished command produced.
#[derive(Debug, Clone, PartialEq)]
pub enum Report {
    /// Rows were written.
    Baselined(Plan),
    /// `--dry-run`: what would have been written.
    WouldBaseline(Plan),
    /// A schema was read.
    Fingerprint {
        /// How many tables it holds.
        tables: usize,
        /// How many enum types.
        enums: usize,
    },
    /// The Mongo index is present.
    IndexEnsured,
    /// Every `run_id` held by more than one document.
    Duplicates(Vec<Duplicate>),
}

/// Why a command could not be carried out.
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    /// A required variable is not set, or is unusable.
    ///
    /// Distinct from every other variant because it earns exit code 2: the
    /// operator asked for something impossible, rather than the database
    /// having refused.
    #[error("{0}")]
    Usage(String),

    /// The database could not be reached.
    #[error("could not connect: {0}")]
    Connect(#[from] sqlx::Error),

    /// Baselining refused, or failed.
    #[error("{0}")]
    Baseline(#[from] BaselineError),

    /// The live schema could not be read.
    #[error("{0}")]
    Introspect(#[from] IntrospectError),

    /// Mongo refused.
    #[error("{0}")]
    Mongo(#[from] MongoError),
}

/// Reads a variable that must be present and non-blank.
fn require(read: &impl Fn(&str) -> Option<String>, key: &str) -> Result<String, CliError> {
    match read(key) {
        Some(value) if !value.trim().is_empty() => Ok(value),
        // Blank is treated as absent, the same rule `pneuma-janitor`'s
        // `connect` applies: an empty variable in a compose file is an
        // operator who meant to set it and did not.
        _ => Err(CliError::Usage(format!("{key} is required"))),
    }
}

/// Reads a variable that has a default.
fn or_default(read: &impl Fn(&str) -> Option<String>, key: &str, fallback: &str) -> String {
    match read(key) {
        Some(value) if !value.trim().is_empty() => value,
        _ => fallback.to_owned(),
    }
}

/// Opens a Postgres pool.
///
/// One connection, and the comment beside it used to say so while the code
/// said two. `baseline`'s sequence -- derive the expectation, introspect the
/// live schema, then open the transaction that records -- never holds more
/// than one at a time, deliberately: an earlier version opened the transaction
/// *before* deriving, which deadlocks on a single-connection pool. A spare
/// connection would let that ordering bug pass silently, and it is exactly the
/// bug this crate has already had once. It would also mask a connection leaked
/// while still holding sqlx's session-level advisory lock.
async fn pool(read: &impl Fn(&str) -> Option<String>) -> Result<PgPool, CliError> {
    let url = require(read, DATABASE_URL)?;
    Ok(PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await?)
}

/// Opens the Mongo runs collection.
async fn runs(read: &impl Fn(&str) -> Option<String>) -> Result<Collection<Document>, CliError> {
    let uri = require(read, MONGODB_URL)?;
    let database = or_default(read, MONGODB_DATABASE, DEFAULT_MONGO_DATABASE);
    let collection = or_default(read, MONGODB_RUNS_COLLECTION, DEFAULT_RUNS_COLLECTION);
    let client = mongodb::Client::with_uri_str(&uri)
        .await
        .map_err(MongoError::from)?;
    Ok(client.database(&database).collection(&collection))
}

/// Carries out one invocation.
pub async fn run(
    invocation: Invocation,
    read: impl Fn(&str) -> Option<String>,
) -> Result<Report, CliError> {
    match invocation {
        // One arm branching on the flag, rather than two arms matching on it:
        // two arms meant two patterns, and neither was attributed to the arm
        // that ran. `Baselining` keeps this pattern to a single line for the
        // same reason. It also puts the shared `pool` in one place.
        Invocation::Baseline(spec) => {
            let pool = pool(&read).await?;
            // `original_migrator`, not `migrator`: baselining adopts a
            // database the original migration tool already manages, and recording a migration
            // whose table that database has never had would have
            // `sqlx migrate run` skip creating it. See
            // `pneuma_store::ORIGINAL_THROUGH`.
            let migrator = &pneuma_store::original_migrator();
            let (schema, scratch) = (&spec.schema, &spec.scratch);
            if spec.dry_run {
                Ok(Report::WouldBaseline(
                    preview(&pool, schema, scratch, migrator).await?,
                ))
            } else {
                Ok(Report::Baselined(
                    baseline(&pool, schema, scratch, migrator).await?,
                ))
            }
        }
        Invocation::Fingerprint { schema } => {
            let pool = pool(&read).await?;
            Ok(summarise(&introspect(&pool, &schema).await?))
        }
        Invocation::MongoIndex => {
            ensure_run_id_index(&runs(&read).await?).await?;
            Ok(Report::IndexEnsured)
        }
        Invocation::MongoDuplicates => Ok(Report::Duplicates(
            duplicate_run_ids(&runs(&read).await?).await?,
        )),
    }
}

/// Counts what a schema holds.
fn summarise(schema: &Schema) -> Report {
    Report::Fingerprint {
        tables: schema.tables.len(),
        enums: schema.enums.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn from(pairs: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        move |key| {
            pairs
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| (*value).to_owned())
        }
    }

    #[test]
    fn a_required_variable_that_is_blank_is_treated_as_absent() {
        // An empty variable in a compose file is an operator who meant to set
        // it and did not; reporting "is required" is more use than connecting
        // to the empty string and failing later.
        for pairs in [
            &[("OTHER", "x")][..],
            &[(DATABASE_URL, "")][..],
            &[(DATABASE_URL, "   ")][..],
        ] {
            let source = move |key: &str| {
                pairs
                    .iter()
                    .find(|(name, _)| *name == key)
                    .map(|(_, value)| (*value).to_owned())
            };
            let Err(CliError::Usage(message)) = require(&source, DATABASE_URL) else {
                panic!("blank must be absent: {pairs:?}");
            };
            assert!(message.contains(DATABASE_URL), "{message}");
        }

        let Ok(value) = require(&from(&[(DATABASE_URL, "postgres://x")]), DATABASE_URL) else {
            panic!("a set variable is read");
        };
        assert_eq!(value, "postgres://x");
    }

    #[test]
    fn an_optional_variable_falls_back_when_absent_or_blank() {
        assert_eq!(
            or_default(&from(&[]), MONGODB_DATABASE, DEFAULT_MONGO_DATABASE),
            DEFAULT_MONGO_DATABASE
        );
        assert_eq!(
            or_default(
                &from(&[(MONGODB_DATABASE, "  ")]),
                MONGODB_DATABASE,
                DEFAULT_MONGO_DATABASE
            ),
            DEFAULT_MONGO_DATABASE
        );
        assert_eq!(
            or_default(
                &from(&[(MONGODB_DATABASE, "other")]),
                MONGODB_DATABASE,
                DEFAULT_MONGO_DATABASE
            ),
            "other"
        );
    }

    #[test]
    fn a_schema_is_summarised_by_what_it_holds() {
        let mut schema = Schema::default();
        schema
            .tables
            .insert("node_run".to_owned(), Default::default());
        schema
            .tables
            .insert("node_run_history".to_owned(), Default::default());
        schema
            .enums
            .insert("node_status".to_owned(), vec!["a".to_owned()]);
        assert_eq!(
            summarise(&schema),
            Report::Fingerprint {
                tables: 2,
                enums: 1
            }
        );
    }
}

//! Parsing the command line, and deciding what an outcome means to a shell.
//!
//! Pure: [`parse`] never reads the process, never prints and never exits, so
//! every subcommand and every rejection is a unit test. [`exit_code`] is the
//! other half of the contract — a deploy pipeline has to be able to tell
//! "cannot reach the database" from "this database is not what the migrations
//! say", and that distinction is worth a named code rather than a grep of
//! stderr.

use std::ffi::OsString;

use clap::{Arg, ArgAction, Command};

use crate::baseline::{BaselineError, Plan};

/// Where the schema lives when nothing says otherwise.
pub const DEFAULT_SCHEMA: &str = "public";
/// The scratch schema `baseline` derives the expectation in.
pub const DEFAULT_SCRATCH: &str = "pneuma_migrate_scratch";

/// What `baseline` was asked to do.
///
/// A named struct rather than three fields on the variant, so that matching on
/// the variant is one pattern rather than four lines of destructuring. Line
/// coverage does not attribute a multi-line pattern to the arm that ran, so the
/// four-line form reported an executing arm as dead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Baselining {
    /// The live schema to fingerprint and record into.
    pub schema: String,
    /// Dropped and recreated to derive the expectation.
    pub scratch: String,
    /// Report the plan without writing anything.
    pub dry_run: bool,
}

/// What the operator asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Invocation {
    /// Record the migrations as already applied, if the schema matches them.
    Baseline(Baselining),
    /// Read the live schema and print what it contains.
    Fingerprint {
        /// Which schema.
        schema: String,
    },
    /// Create the Mongo `run_id` index if it is not there.
    MongoIndex,
    /// Report every `run_id` held by more than one document.
    MongoDuplicates,
}

/// The command line, as a value.
///
/// Separate from [`parse`] so `--help` renders in a test without a process.
pub fn command() -> Command {
    let schema = || {
        Arg::new("schema")
            .long("schema")
            .value_name("NAME")
            .default_value(DEFAULT_SCHEMA)
            .help("the live schema to read")
    };
    Command::new("pneuma-migrate")
        .about("Schema fingerprinting and baselining for a database the original migration tool already migrated")
        // `main` tells the operator that clap renders `--version`; without
        // this it renders "unexpected argument '--version'" instead, which is
        // the comment being wrong rather than clap being unhelpful.
        .version(env!("CARGO_PKG_VERSION"))
        // Neither `subcommand_required` nor `arg_required_else_help`: both
        // would make clap reject a bare invocation itself, leaving the arm
        // below permanently unreachable and unmeasurable. Handling it here
        // instead means `pneuma-migrate` with no arguments takes the same path
        // as `pneuma-migrate nonesuch` -- a usage error naming what is missing,
        // exit 2 -- and that path is tested.
        .subcommand_required(false)
        .subcommand(
            Command::new("baseline")
                .about("record the migrations as applied, if the schema already matches")
                .arg(schema())
                .arg(
                    Arg::new("scratch")
                        .long("scratch")
                        .value_name("NAME")
                        .default_value(DEFAULT_SCRATCH)
                        .help("scratch schema, dropped and recreated to derive the expectation"),
                )
                .arg(
                    Arg::new("dry-run")
                        .long("dry-run")
                        .action(ArgAction::SetTrue)
                        .help(
                            "report what would be recorded, and write nothing to --schema \
                             (--scratch is still dropped and rebuilt)",
                        ),
                ),
        )
        .subcommand(
            Command::new("fingerprint")
                .about("read the live schema and print what it contains")
                .arg(schema()),
        )
        .subcommand(
            Command::new("mongo")
                .about("the Mongo half of the schema")
                .subcommand_required(false)
                .subcommand(Command::new("index").about("create the run_id index if absent"))
                .subcommand(
                    Command::new("duplicates")
                        .about("report every run_id held by more than one document"),
                ),
        )
}

/// The refusal for "I do not know what you asked for".
///
/// One line at each call site on purpose: spread across four, the arguments of
/// this call read as uncovered even when the arm runs.
fn missing(message: &'static str) -> clap::Error {
    command().error(clap::error::ErrorKind::MissingSubcommand, message)
}

/// Turns an argument vector into an [`Invocation`].
///
/// `try_get_matches_from`, not `get_matches`: the latter prints and calls
/// `exit` inside the library, which cannot be tested and cannot be recovered
/// from. Here `--help` and a bad flag are both ordinary `Err` values.
pub fn parse<I, T>(argv: I) -> Result<Invocation, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let matches = command().try_get_matches_from(argv)?;
    // Every `expect`/`unwrap` avoided: clap guarantees these are present
    // because each has a default or is a required subcommand, but "guarantees"
    // is exactly the kind of claim this repository keeps finding to be untrue,
    // and the crate denies both lints anyway.
    let text = |m: &clap::ArgMatches, key: &str, fallback: &str| {
        m.get_one::<String>(key)
            .map_or_else(|| fallback.to_owned(), String::clone)
    };
    match matches.subcommand() {
        Some(("baseline", m)) => Ok(Invocation::Baseline(Baselining {
            schema: text(m, "schema", DEFAULT_SCHEMA),
            scratch: text(m, "scratch", DEFAULT_SCRATCH),
            dry_run: m.get_flag("dry-run"),
        })),
        Some(("fingerprint", m)) => Ok(Invocation::Fingerprint {
            schema: text(m, "schema", DEFAULT_SCHEMA),
        }),
        Some(("mongo", m)) => match m.subcommand() {
            Some(("duplicates", _)) => Ok(Invocation::MongoDuplicates),
            Some(("index", _)) => Ok(Invocation::MongoIndex),
            // `mongo` with nothing after it, or a name that is not wired.
            _ => Err(missing("mongo needs a subcommand: index or duplicates")),
        },
        // No subcommand, or one that is not wired. A usage error is the honest
        // answer to "I do not know what you asked for", and it is reachable --
        // see the note on `subcommand_required` above.
        _ => Err(missing("a subcommand is required")),
    }
}

/// What a shell should see.
///
/// `3` for a schema mismatch is the point of this function. A deploy pipeline
/// must be able to distinguish "the database is not what the migrations say"
/// -- which needs a human -- from "could not connect", which needs a retry.
/// Collapsing both into `1` means the pipeline greps stderr, and stderr is
/// prose.
///
/// `4` for duplicate `run_id` values is the same argument applied to the other
/// half of the schema. `mongo duplicates` exists as the precondition check for
/// making the index unique -- a unique build over duplicates fails
/// `DuplicateKey` *after* dropping the non-unique index, so "are there any"
/// has to be answered before anything is touched. Answering it with exit `0`
/// whether or not it found some would leave a pipeline parsing [`describe`]'s
/// prose, which is what this function exists to avoid. Finding none is `0`;
/// finding some is not an error, so it is not `1`.
///
/// Takes the [`Report`] rather than being generic, because that distinction
/// cannot be drawn without looking at what was found.
///
/// [`Report`]: crate::run::Report
pub fn exit_code(result: &Result<crate::run::Report, crate::run::CliError>) -> i32 {
    use crate::run::{CliError, Report};
    match result {
        Ok(Report::Duplicates(found)) if !found.is_empty() => 4,
        Ok(_) => 0,
        Err(CliError::Baseline(BaselineError::SchemaMismatch(_))) => 3,
        Err(CliError::Usage(_)) => 2,
        Err(_) => 1,
    }
}

/// How a finished command reports itself.
pub fn describe(report: &crate::run::Report) -> String {
    use crate::run::Report;
    match report {
        Report::Baselined(Plan::Record(versions)) => {
            format!("recorded {} migration(s): {versions:?}", versions.len())
        }
        Report::Baselined(Plan::AlreadyRecorded) => "already recorded; nothing to do".to_owned(),
        Report::WouldBaseline(Plan::Record(versions)) => {
            format!("would record {} migration(s): {versions:?}", versions.len())
        }
        Report::WouldBaseline(Plan::AlreadyRecorded) => {
            "would do nothing; already recorded".to_owned()
        }
        Report::Fingerprint { tables, enums } => {
            format!("{tables} table(s), {enums} enum(s)")
        }
        Report::IndexEnsured => "the run_id index is present".to_owned(),
        Report::Duplicates(found) if found.is_empty() => "no duplicate run_id values".to_owned(),
        Report::Duplicates(found) => {
            let mut lines = vec![format!("{} duplicated run_id value(s):", found.len())];
            for duplicate in found {
                lines.push(format!("  {:?} x{}", duplicate.run_id, duplicate.count));
            }
            lines.join("\n")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mongo::RunKey;
    use crate::run::{CliError, Report};

    fn parsed(args: &[&str]) -> Invocation {
        let mut argv = vec!["pneuma-migrate"];
        argv.extend_from_slice(args);
        match parse(argv) {
            Ok(invocation) => invocation,
            Err(error) => panic!("{args:?} should parse: {error}"),
        }
    }

    #[test]
    fn baseline_defaults_its_schema_and_its_scratch() {
        assert_eq!(
            parsed(&["baseline"]),
            Invocation::Baseline(Baselining {
                schema: DEFAULT_SCHEMA.to_owned(),
                scratch: DEFAULT_SCRATCH.to_owned(),
                dry_run: false,
            })
        );
    }

    #[test]
    fn baseline_takes_a_schema_a_scratch_and_a_dry_run() {
        assert_eq!(
            parsed(&[
                "baseline",
                "--schema",
                "live",
                "--scratch",
                "tmp",
                "--dry-run"
            ]),
            Invocation::Baseline(Baselining {
                schema: "live".to_owned(),
                scratch: "tmp".to_owned(),
                dry_run: true,
            })
        );
    }

    #[test]
    fn the_other_subcommands_parse() {
        assert_eq!(
            parsed(&["fingerprint", "--schema", "s"]),
            Invocation::Fingerprint {
                schema: "s".to_owned()
            }
        );
        assert_eq!(parsed(&["mongo", "index"]), Invocation::MongoIndex);
        assert_eq!(
            parsed(&["mongo", "duplicates"]),
            Invocation::MongoDuplicates
        );
    }

    #[test]
    fn a_missing_or_unknown_subcommand_is_a_usage_error_not_a_panic() {
        // `try_get_matches_from`, not `get_matches`: the latter prints and
        // calls `exit` inside the library, which cannot be tested and cannot be
        // recovered from.
        for args in [vec!["pneuma-migrate"], vec!["pneuma-migrate", "nonesuch"]] {
            let Err(error) = parse(args.clone()) else {
                panic!("{args:?} is not a command");
            };
            assert!(
                !error.to_string().is_empty(),
                "the refusal says something: {args:?}"
            );
        }
        // A flag that does not exist, on a subcommand that does.
        assert!(parse(vec!["pneuma-migrate", "baseline", "--nope"]).is_err());
    }

    #[test]
    fn a_missing_subcommand_names_what_is_missing() {
        // Not just "is_err": clap would produce an error for a bare invocation
        // under `subcommand_required` too, so an `is_err` assertion here passes
        // whether or not the arm being tested runs. The message is what
        // distinguishes them, and the top-level and `mongo` messages differ.
        for (args, fragment) in [
            (vec!["pneuma-migrate"], "a subcommand is required"),
            (vec!["pneuma-migrate", "mongo"], "index or duplicates"),
        ] {
            let Err(error) = parse(args.clone()) else {
                panic!("{args:?} is not a command");
            };
            assert_eq!(error.kind(), clap::error::ErrorKind::MissingSubcommand);
            let rendered = error.to_string();
            assert!(rendered.contains(fragment), "{args:?} said: {rendered}");
        }
    }

    #[test]
    fn help_renders_without_touching_the_process() {
        // `command()` exists separately so this is possible at all: rendering
        // help through `parse` would be an `Err` carrying the text, and through
        // `get_matches` would exit the test runner.
        let rendered = command().render_help().to_string();
        for expected in ["baseline", "fingerprint", "mongo"] {
            assert!(rendered.contains(expected), "help lists {expected}");
        }
    }

    #[test]
    fn a_schema_mismatch_gets_its_own_exit_code() {
        // The reason `exit_code` exists. A deploy pipeline must tell "this
        // database is not what the migrations say" -- which needs a human --
        // from "could not connect", which needs a retry. Collapsing both into 1
        // means grepping stderr, and stderr is prose.
        let mismatch: Result<Report, CliError> =
            Err(CliError::Baseline(BaselineError::SchemaMismatch(vec![])));
        assert_eq!(exit_code(&mismatch), 3);

        let usage: Result<Report, CliError> = Err(CliError::Usage("x".to_owned()));
        assert_eq!(exit_code(&usage), 2);

        let other: Result<Report, CliError> = Err(CliError::Baseline(BaselineError::NoMigrations));
        assert_eq!(exit_code(&other), 1);

        assert_eq!(exit_code(&Ok(Report::IndexEnsured)), 0);

        // The same argument, applied to the Mongo half. `mongo duplicates` is
        // the precondition check for making the index unique, so a pipeline
        // has to be able to gate on it without reading prose.
        assert_eq!(exit_code(&Ok(Report::Duplicates(vec![]))), 0);
        assert_eq!(
            exit_code(&Ok(Report::Duplicates(vec![crate::mongo::Duplicate {
                run_id: RunKey::Null,
                count: 2,
            }]))),
            4
        );
    }

    #[test]
    fn every_report_describes_itself_without_panicking() {
        let cases = vec![
            Report::Baselined(Plan::Record(vec![1, 2])),
            Report::Baselined(Plan::AlreadyRecorded),
            Report::WouldBaseline(Plan::Record(vec![1])),
            Report::WouldBaseline(Plan::AlreadyRecorded),
            Report::Fingerprint {
                tables: 2,
                enums: 1,
            },
            Report::IndexEnsured,
            Report::Duplicates(vec![]),
            Report::Duplicates(vec![crate::mongo::Duplicate {
                run_id: RunKey::Null,
                count: 2,
            }]),
        ];
        for report in &cases {
            assert!(!describe(report).is_empty(), "{report:?} describes itself");
        }
        // The two that carry a number should say it, since that number is the
        // whole reason someone ran the command.
        assert!(describe(&cases[0]).contains('2'), "{}", describe(&cases[0]));
        assert!(
            describe(&cases[7]).contains("x2"),
            "{}",
            describe(&cases[7])
        );
        assert!(describe(&cases[6]).contains("no duplicate"));
    }
}

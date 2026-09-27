//! Where the two stores live, read from the environment.
//!
//! Separate from [`crate::settings`], which is about what a pass *does*. This
//! is about what it connects to, and it exists as a module rather than as
//! wiring in the binary for one reason: assembling a Postgres DSN out of parts
//! is not string concatenation, and getting it wrong fails only for some
//! passwords.
//!
//! # The four-part form is the deployed one
//!
//! the original takes `POSTGRES_HOST`,
//! `POSTGRES_USER`, `POSTGRES_PASSWORD` and `POSTGRES_DB` and builds a DSN with
//! its own DSN builder. A deployment already sets those four, so the
//! port reads them rather than requiring the environment be rewritten. A single
//! `DATABASE_URL` wins when set, because that is what every other crate here
//! documents and what a developer will reach for.
//!
//! # Percent-encoding is the whole point
//!
//! `PostgresDsn.build` encodes the userinfo. Interpolating it instead would
//! break on any password containing `@`, `/`, `:` or `?` — `p@ss` turns
//! `user:p@ss@host` into a URL whose host is `ss@host`, and the failure is a
//! connection error naming a host nobody configured. Passwords out of a secret
//! manager contain those characters routinely, so this is a question of which
//! deployments work rather than of strictness.

use pneuma_config::{ConfigError, Env};
use secrecy::{ExposeSecret, SecretString};

/// A whole Postgres DSN, which wins over the four parts below when set.
pub const DATABASE_URL: &str = "DATABASE_URL";
/// Where Postgres is, when the DSN is assembled from parts.
pub const POSTGRES_HOST: &str = "PNEUMA_POSTGRES_HOST";
/// Who to connect as.
pub const POSTGRES_USER: &str = "PNEUMA_POSTGRES_USER";
/// The password, percent-encoded into the DSN.
pub const POSTGRES_PASSWORD: &str = "PNEUMA_POSTGRES_PASSWORD";
/// Which Postgres database.
pub const POSTGRES_DB: &str = "PNEUMA_POSTGRES_DB";
/// Where MongoDB is.
pub const MONGODB_URL: &str = "PNEUMA_MONGODB_URL";
/// Which Mongo database holds the run documents.
pub const MONGODB_DATABASE: &str = "PNEUMA_MONGODB_DATABASE";
/// Which collection holds the live run documents.
pub const MONGODB_RUNS_COLLECTION: &str = "PNEUMA_MONGODB_RUNS_COLLECTION";
/// Which collection holds the archived ones.
pub const MONGODB_HISTORY_COLLECTION: &str = "PNEUMA_MONGODB_HISTORY_COLLECTION";

/// Where MongoDB is when [`MONGODB_URL`] is unset.
pub const DEFAULT_MONGODB_URL: &str = "mongodb://admin:password@metadb:27017";
/// The Mongo database when [`MONGODB_DATABASE`] is unset.
///
/// `pneuma`, not the original's name: the storage names were renamed with
/// everything else the port inherited -- the design notes
pub const DEFAULT_MONGO_DATABASE: &str = "pneuma";
/// The live-run collection when [`MONGODB_RUNS_COLLECTION`] is unset.
pub const DEFAULT_RUNS_COLLECTION: &str = "runs";
/// The archive collection when [`MONGODB_HISTORY_COLLECTION`] is unset.
///
/// `run_history`, where the original wrote `runhistory`. This is the one
/// storage rename no migration performs -- Mongo's `renameCollection` is a
/// deployment step, `docs/runbook.md` -- so a deployment mid-cutover sets the
/// variable to the old name and this default is what it moves to.
pub const DEFAULT_HISTORY_COLLECTION: &str = "run_history";

/// Everything needed to reach both stores.
#[derive(Debug, Clone)]
pub struct Endpoints {
    /// The Postgres DSN, assembled or taken whole.
    ///
    /// Secret because it carries the password, and a `Debug` of a config that
    /// prints a DSN is how credentials reach logs — the same reasoning as
    /// the design notes
    pub postgres: SecretString,
    /// The MongoDB connection string.
    pub mongo: SecretString,
    /// Which Mongo database holds the two collections below.
    pub mongo_database: String,
    /// The collection holding live run documents.
    pub runs_collection: String,
    /// The collection they are archived into.
    pub history_collection: String,
}

impl Endpoints {
    /// Reads the environment.
    ///
    /// `DATABASE_URL` if set, otherwise the four `PNEUMA_POSTGRES_*` parts the
    /// deployment already has. Mongo's default connection string is the
    /// original's; the database
    /// name is not, because it was renamed with the rest of the storage.
    /// The environment is a parameter -- see `pneuma_config::source` for why.
    pub fn from_env(env: &Env) -> Result<Self, ConfigError> {
        let postgres = match trimmed(env, DATABASE_URL)? {
            Some(url) => SecretString::from(url),
            None => {
                // Trimmed like every other identifier, and *then* required: a
                // host of `"db:5432\n"` would otherwise reach the DSN with the
                // newline in it, which is the parse failure this module claims
                // to have handled.
                let Some(host) = trimmed(env, POSTGRES_HOST)? else {
                    return Err(ConfigError::Missing {
                        key: POSTGRES_HOST.to_owned(),
                    });
                };
                SecretString::from(postgres_dsn(
                    &host,
                    credential(env, POSTGRES_USER)?.as_deref(),
                    credential(env, POSTGRES_PASSWORD)?.as_deref(),
                    trimmed(env, POSTGRES_DB)?.as_deref(),
                )?)
            }
        };
        Ok(Endpoints {
            postgres,
            mongo: SecretString::from(
                trimmed(env, MONGODB_URL)?.unwrap_or_else(|| DEFAULT_MONGODB_URL.to_owned()),
            ),
            mongo_database: trimmed(env, MONGODB_DATABASE)?
                .unwrap_or_else(|| DEFAULT_MONGO_DATABASE.to_owned()),
            runs_collection: collection(env, MONGODB_RUNS_COLLECTION, DEFAULT_RUNS_COLLECTION)?,
            history_collection: collection(
                env,
                MONGODB_HISTORY_COLLECTION,
                DEFAULT_HISTORY_COLLECTION,
            )?,
        })
    }

    /// The Postgres DSN, for handing to a pool.
    pub fn postgres_dsn(&self) -> &str {
        self.postgres.expose_secret()
    }

    /// The Mongo connection string, for handing to a client.
    pub fn mongo_uri(&self) -> &str {
        self.mongo.expose_secret()
    }
}

/// A collection name, defaulted and trimmed.
///
/// Blank is absent for the reason [`trimmed`] gives, and it matters twice as
/// much here: a Mongo collection *can* be named the empty string in the driver's
/// type, and the failure would be a namespace nobody can find.
fn collection(env: &Env, key: &str, default: &str) -> Result<String, ConfigError> {
    Ok(trimmed(env, key)?.unwrap_or_else(|| default.to_owned()))
}

/// A variable that is set to something, with surrounding whitespace removed.
///
/// For everything that is not a credential: URLs, host names, database names.
/// A value read out of a file or a rendered k8s secret routinely carries a
/// trailing newline, and none of these ever means to contain one — a database
/// called `appdb\n` is not a database anybody created, and Postgres reports it
/// as not existing, which reads as a wrong name rather than an edited one.
///
/// Blank is absent. `Env::lookup` returns `Some("")` for a variable
/// that is set and empty, which `POSTGRES_DB=${DB_NAME}` produces in a compose
/// file whenever `DB_NAME` is unset — so a blank is far more often an unset
/// variable that went through a template than a deliberate empty value.
fn trimmed(env: &Env, key: &str) -> Result<Option<String>, ConfigError> {
    Ok(env
        .lookup(key)?
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty()))
}

/// A credential, exactly as it was set.
///
/// Not trimmed, and not blank-checked on the trimmed value. Both would edit a
/// secret, and the two risks are not symmetric: a mangled URL fails loudly when
/// something parses it, while a mangled credential fails quietly at the server
/// and is indistinguishable from the operator having typed it wrong.
///
/// So whitespace survives — a password of `" "` is unusual but legal, and an
/// earlier version filtered it out as blank, which dropped the credential and
/// attempted an unauthenticated connection. That is the same outcome
/// [`postgres_dsn`] refuses for a missing user, reached by a different path:
/// succeeding against a server configured for trust auth is a security result
/// nobody chose.
///
/// Only a genuinely empty value is absent, which is still the templating case
/// `POSTGRES_PASSWORD=${DB_PASS}` produces when `DB_PASS` is unset.
fn credential(env: &Env, key: &str) -> Result<Option<String>, ConfigError> {
    Ok(env.lookup(key)?.filter(|value| !value.is_empty()))
}

/// Builds a `postgres://` DSN from the parts, encoding what must be encoded.
///
/// Userinfo and the database name are percent-encoded; the host is not, because
/// a host containing a reserved character is not a host. This mirrors what
/// `PostgresDsn.build` does for the original and is the difference between a
/// deployment working and failing on the contents of its password.
///
/// Fails on a password with no user at all — see the comment inside.
fn postgres_dsn(
    host: &str,
    user: Option<&str>,
    password: Option<&str>,
    db: Option<&str>,
) -> Result<String, ConfigError> {
    // A password with no user at all is refused rather than dropped. The
    // deployment plainly meant to authenticate, and discarding the credential
    // either fails later with an error naming the wrong thing or -- worse --
    // succeeds against a server configured for trust auth, which is a security
    // outcome nobody chose. Silently discarding configuration is the failure
    // class this port keeps finding; `settings.rs` refuses its two for the same
    // reason.
    //
    // Callers pass these through `credential`, which reads only a genuinely
    // empty value as absent -- so a blank password is not a password and does
    // not trip this, while a password of `" "` is one and survives.
    //
    // A deliberate empty username with a real password would have produced
    // `postgres://:secret@host`, which is what `PostgresDsn.build` gives and is
    // legal userinfo. It is not supported here, and it costs nothing: an empty
    // string is not a Postgres role, so that DSN could only ever fail at
    // authentication. Failing at startup, naming the variable to set, is the
    // better of the two.
    if user.is_none() && password.is_some() {
        return Err(ConfigError::Missing {
            key: "PNEUMA_POSTGRES_USER".to_owned(),
        });
    }
    let mut dsn = String::from("postgres://");
    if let Some(user) = user {
        dsn.push_str(&encode(user));
        if let Some(password) = password {
            dsn.push(':');
            dsn.push_str(&encode(password));
        }
        dsn.push('@');
    }
    dsn.push_str(host);
    if let Some(db) = db {
        dsn.push('/');
        dsn.push_str(&encode(db));
    }
    Ok(dsn)
}

/// Percent-encodes everything outside RFC 3986's unreserved set.
///
/// Deliberately conservative: encoding a character that did not need it is
/// harmless, while missing one that did produces a URL that parses as something
/// else. Hand-written rather than pulled in, because one call site does not
/// justify a dependency and the rule is four lines.
fn encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(char::from(byte));
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Endpoints read from exactly these values and nothing else.
    ///
    /// The previous helper had to name all seven keys and unset them first, so
    /// that a `DATABASE_URL` in the developer's own shell could not decide a
    /// result, and had to hold a crate-wide `Mutex` while it did -- `settings`
    /// reads the environment too, and a lock per module is no lock. An `Env`
    /// that only knows what it was handed makes both unnecessary.
    fn from(vars: &[(&str, &str)]) -> Result<Endpoints, ConfigError> {
        Endpoints::from_env(&Env::from_pairs(vars.iter().copied()))
    }

    #[test]
    fn a_password_with_reserved_characters_survives_assembly() {
        // The case this module exists for. `p@ss/word` interpolated raw makes
        // `user:p@ss/word@host` -- a URL whose host is `ss` and whose path is
        // `word@host`, failing with a connection error naming a host nobody
        // configured. Secret managers produce passwords like this routinely.
        let Ok(dsn) = postgres_dsn("db:5432", Some("user"), Some("p@ss/word"), Some("appdb"))
        else {
            panic!("should assemble");
        };
        assert_eq!(dsn, "postgres://user:p%40ss%2Fword@db:5432/appdb");
        assert!(
            !dsn.trim_start_matches("postgres://user:").contains('@')
                || dsn.matches('@').count() == 1,
            "exactly one unencoded @, separating userinfo from host: {dsn}"
        );
    }

    #[test]
    fn the_optional_parts_are_optional() {
        let Ok(bare) = postgres_dsn("db", None, None, None) else {
            panic!("a host alone is a DSN");
        };
        assert_eq!(bare, "postgres://db");
        let Ok(user_only) = postgres_dsn("db", Some("u"), None, None) else {
            panic!("a user without a password is ordinary");
        };
        assert_eq!(user_only, "postgres://u@db");
    }

    #[test]
    fn a_password_with_no_user_at_all_is_refused() {
        // An earlier version dropped it and a test here called that correct.
        // Discarding a configured credential either fails later with an error
        // naming the wrong thing, or succeeds against a server set to trust
        // auth -- a security outcome nobody chose.
        assert!(
            postgres_dsn("db", None, Some("secret"), Some("appdb")).is_err(),
            "an unusable credential is refused, not silently dropped"
        );

        // Callers reach this through `set`, which reads a blank as absent, so
        // the pairing that would produce `postgres://:secret@host` cannot arise
        // from the environment. It is unsupported deliberately: an empty string
        // is not a Postgres role, so that DSN could only fail at authentication
        // anyway, and failing at startup names the variable to set.
        let refused = from(&[
            ("PNEUMA_POSTGRES_HOST", "db"),
            ("PNEUMA_POSTGRES_USER", ""),
            ("PNEUMA_POSTGRES_PASSWORD", "secret"),
        ]);
        assert!(
            refused.is_err(),
            "a blank user is a missing user, so this is the same refusal"
        );
    }

    #[test]
    fn the_unreserved_set_is_left_alone() {
        let plain = "AZaz09-._~";
        assert_eq!(encode(plain), plain);
        // And everything else is not, including the space and non-ASCII, which
        // are encoded per byte rather than per character.
        assert_eq!(encode(" "), "%20");
        assert_eq!(encode("é"), "%C3%A9");
    }

    #[test]
    fn a_whole_database_url_wins() {
        let Ok(endpoints) = from(&[
            ("DATABASE_URL", "postgres://whole/url"),
            ("PNEUMA_POSTGRES_HOST", "ignored"),
        ]) else {
            panic!("should read");
        };
        assert_eq!(endpoints.postgres_dsn(), "postgres://whole/url");
    }

    #[test]
    fn a_blank_variable_is_read_as_unset_everywhere() {
        // One rule, and the review found three. `POSTGRES_PASSWORD=""` with no
        // user refused to start and reported `POSTGRES_USER` missing -- naming
        // the variable the operator had *not* set. `MONGODB_URI=""` was used
        // verbatim, giving a client that fails to parse "". `POSTGRES_DB=""`
        // produced `postgres://host/`, a nameless database rather than none.
        //
        // A blank is what `POSTGRES_PASSWORD=${DB_PASS}` renders to in a compose
        // file when `DB_PASS` is unset, so it is far more often an unset
        // variable that went through a template than a deliberate empty value.
        let Ok(endpoints) = from(&[
            ("PNEUMA_POSTGRES_HOST", "db"),
            ("PNEUMA_POSTGRES_USER", ""),
            ("PNEUMA_POSTGRES_PASSWORD", ""),
            ("PNEUMA_POSTGRES_DB", ""),
            ("PNEUMA_MONGODB_URL", ""),
            ("PNEUMA_MONGODB_DATABASE", ""),
        ]) else {
            panic!("blanks are absences, not a reason to refuse");
        };
        assert_eq!(
            endpoints.postgres_dsn(),
            "postgres://db",
            "no userinfo, no database"
        );
        assert_eq!(
            endpoints.mongo_uri(),
            "mongodb://admin:password@metadb:27017",
            "a blank falls back to the default rather than being handed to the client"
        );
        assert_eq!(endpoints.mongo_database, "pneuma");
    }

    #[test]
    fn a_url_is_trimmed_and_a_credential_is_not() {
        // A URL out of a file or a rendered k8s secret routinely carries a
        // trailing newline and will not parse with one.
        let Ok(endpoints) = from(&[("DATABASE_URL", "  postgres://whole/url\n")]) else {
            panic!("should read");
        };
        assert_eq!(endpoints.postgres_dsn(), "postgres://whole/url");

        // A password is left exactly as set. Whitespace in one is unusual but
        // legal, and trimming turns a working deployment into an
        // authentication failure -- which names nothing and reads as a wrong
        // password rather than an edited one. A mangled URL fails loudly at
        // parse time; a mangled credential fails quietly at the server, so the
        // two risks are not symmetric and do not get the same rule.
        let Ok(spaced) = from(&[
            ("PNEUMA_POSTGRES_HOST", "db"),
            ("PNEUMA_POSTGRES_USER", "u"),
            ("PNEUMA_POSTGRES_PASSWORD", " secret "),
        ]) else {
            panic!("should read");
        };
        assert_eq!(
            spaced.postgres_dsn(),
            "postgres://u:%20secret%20@db",
            "the spaces survive, encoded"
        );
    }

    #[test]
    fn an_identifier_is_trimmed_and_a_credential_is_not() {
        // Three kinds of value, not two. An earlier version split them into
        // URLs and credentials and left database and host names following the
        // credential rule -- so `POSTGRES_DB` out of a rendered secret became
        // `appdb%0A` in the DSN and Postgres reported a database that does not
        // exist, which reads as a wrong name rather than an edited one.
        let Ok(endpoints) = from(&[
            ("PNEUMA_POSTGRES_HOST", " db:5432\n"),
            ("PNEUMA_POSTGRES_USER", "u"),
            ("PNEUMA_POSTGRES_DB", "appdb\n"),
            ("PNEUMA_MONGODB_DATABASE", " other \n"),
        ]) else {
            panic!("should read");
        };
        assert_eq!(
            endpoints.postgres_dsn(),
            "postgres://u@db:5432/appdb",
            "host and database are trimmed, and no %0A reaches the DSN"
        );
        assert_eq!(
            endpoints.mongo_database, "other",
            "a Mongo database name is an identifier too, and there is no \
             encoding layer to make a newline visible there"
        );
    }

    #[test]
    fn a_whitespace_only_credential_is_a_credential() {
        // `" "` is unusual but legal. An earlier version blank-checked the
        // *trimmed* value, so it read as absent, the password was dropped, and
        // the connection was attempted unauthenticated -- the same outcome
        // `postgres_dsn` refuses for a missing user, reached another way.
        let Ok(endpoints) = from(&[
            ("PNEUMA_POSTGRES_HOST", "db"),
            ("PNEUMA_POSTGRES_USER", "u"),
            ("PNEUMA_POSTGRES_PASSWORD", " "),
        ]) else {
            panic!("should read");
        };
        assert_eq!(
            endpoints.postgres_dsn(),
            "postgres://u:%20@db",
            "the credential is used, not silently discarded"
        );

        // A genuinely empty one is still absent -- the templating case.
        let Ok(unset) = from(&[
            ("PNEUMA_POSTGRES_HOST", "db"),
            ("PNEUMA_POSTGRES_USER", "u"),
            ("PNEUMA_POSTGRES_PASSWORD", ""),
        ]) else {
            panic!("should read");
        };
        assert_eq!(unset.postgres_dsn(), "postgres://u@db");
    }

    #[test]
    fn an_empty_database_url_falls_through_to_the_parts() {
        // An empty value is a misconfiguration rather than an absence --
        // `pneuma-config` says so — and connecting to "" fails with a parse
        // error that names nothing useful.
        let Ok(endpoints) = from(&[
            ("DATABASE_URL", "  "),
            ("PNEUMA_POSTGRES_HOST", "db:5432"),
            ("PNEUMA_POSTGRES_DB", "appdb"),
        ]) else {
            panic!("should read");
        };
        assert_eq!(endpoints.postgres_dsn(), "postgres://db:5432/appdb");
    }

    #[test]
    fn without_either_form_it_says_which_key_is_missing() {
        let result = from(&[]);
        assert!(result.is_err(), "there is nothing to connect to");
    }

    #[test]
    fn mongo_takes_the_originals_address_and_this_ports_database_name() {
        let Ok(endpoints) = from(&[("PNEUMA_POSTGRES_HOST", "db")]) else {
            panic!("should read");
        };
        assert_eq!(
            endpoints.mongo_uri(),
            "mongodb://admin:password@metadb:27017"
        );
        assert_eq!(endpoints.mongo_database, "pneuma");

        let Ok(overridden) = from(&[
            ("PNEUMA_POSTGRES_HOST", "db"),
            ("PNEUMA_MONGODB_URL", "mongodb://elsewhere"),
            ("PNEUMA_MONGODB_DATABASE", "other"),
        ]) else {
            panic!("should read");
        };
        assert_eq!(overridden.mongo_uri(), "mongodb://elsewhere");
        assert_eq!(overridden.mongo_database, "other");
    }

    #[test]
    fn the_dsn_does_not_appear_in_a_debug() {
        // The design notes: a `Debug` that prints a DSN is how credentials
        // reach logs.
        let Ok(endpoints) = from(&[
            ("PNEUMA_POSTGRES_HOST", "db"),
            ("PNEUMA_POSTGRES_USER", "u"),
            ("PNEUMA_POSTGRES_PASSWORD", "hunter2"),
        ]) else {
            panic!("should read");
        };
        let printed = format!("{endpoints:?}");
        assert!(
            !printed.contains("hunter2"),
            "the password leaked: {printed}"
        );
    }
}

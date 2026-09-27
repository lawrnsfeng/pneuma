//! Where configuration values come from.
//!
//! # Why this exists rather than free functions
//!
//! Reading `std::env` directly makes every test that wants to exercise a
//! configuration rule mutate the process environment, and the process
//! environment is global. `pneuma-janitor` grew a crate-wide `Mutex` for
//! exactly this, and its own header records the reason: glibc's `setenv` may
//! reallocate `environ` under a concurrent reader, which is a real data race
//! and is why `std::env::set_var` is `unsafe` from edition 2024.
//!
//! A lock only works while every test cooperates. Six binaries and two config
//! modules make that a matter of time, and it is not the kind of failure that
//! shows up as a test failure -- it shows up as a flake, or as nothing.
//!
//! The lock also forces uncoverable-adjacent code into existence: recovering
//! from a poisoned mutex is an arm that cannot be reached while the suite
//! passes, and reaching it deliberately makes coverage depend on test order.
//!
//! [`Env`] takes the reader as a value instead. [`Env::from_process`] is what a
//! binary uses; [`Env::from_pairs`] is what a test uses, and two tests holding
//! different environments can run at the same time because neither is the
//! process's.
//!
//! ```
//! use pneuma_config::Env;
//!
//! let env = Env::from_pairs([("RETRIES", "3")]);
//! assert_eq!(env.parse_or("RETRIES", 1u32)?, 3);
//! assert_eq!(env.parse_or("TIMEOUT", 30u32)?, 30);
//! # Ok::<(), pneuma_config::ConfigError>(())
//! ```

use std::collections::BTreeMap;
use std::env::VarError;
use std::str::FromStr;

use secrecy::SecretString;

use crate::env::{expected_type, ConfigError};

/// How an [`Env`] answers a lookup.
///
/// Boxed, so `Env` is one concrete type rather than a type parameter. It was
/// generic, and the parameter leaked: every function that reads configuration
/// had to be generic over it and carry the `where` clause, which would have
/// spread through every service still to be written. Configuration is read a
/// few dozen times at startup, so the indirection costs nothing that can be
/// measured, and what it buys is that `&Env` is a type anyone can write down.
pub type Reader = Box<dyn Fn(&str) -> Result<Option<String>, ConfigError> + Send + Sync>;

/// Reads the process environment, distinguishing "not set" from "unreadable".
fn process(key: &str) -> Result<Option<String>, ConfigError> {
    classify(key, std::env::var(key))
}

/// What `std::env::var`'s three answers mean.
///
/// Separate from [`process`] so the `NotUnicode` arm can be reached at all.
/// Producing one through the process needs a non-UTF-8 value *set* on the
/// process, which is the mutation this module exists to remove -- and an arm
/// that cannot be reached is an arm nobody has checked. A `VarError` is
/// ordinary data, so as a function this is three cases and one test.
fn classify(key: &str, answer: Result<String, VarError>) -> Result<Option<String>, ConfigError> {
    match answer {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        // Not folded into "absent". An operator who set a variable meant
        // something by it, and silently treating a mis-encoded value as unset
        // hands them a default with no complaint.
        Err(VarError::NotUnicode(_)) => Err(ConfigError::NotUnicode {
            key: key.to_owned(),
        }),
    }
}

/// A source of configuration values, and the rules for reading them.
///
/// Every method distinguishes "not set" from "set and unusable", because those
/// need different things from an operator: the first is a variable to add, the
/// second is a value to correct.
pub struct Env {
    read: Reader,
}

impl std::fmt::Debug for Env {
    /// Names the type and stops. A reader is a closure, and the values behind
    /// one may be credentials -- rendering them here would undo
    /// [`Env::require_secret`], which exists so that a `{:?}` added later by
    /// someone debugging something else cannot print a DSN.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Env { .. }")
    }
}

impl Env {
    /// The process environment. What a binary uses.
    pub fn from_process() -> Self {
        Env::new(process)
    }

    /// A fixed set of values. What a test uses.
    ///
    /// Owns its pairs, so a test can build them in a loop -- an earlier helper
    /// of this shape borrowed `&'static [(&str, &str)]`, which forced every
    /// case to be a literal and every table to be a `const`.
    ///
    /// A later pair with the same key wins, which is what an operator setting a
    /// variable twice would expect.
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let values: BTreeMap<String, String> = pairs
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        Env::new(move |key| Ok(values.get(key).cloned()))
    }

    /// Any other reader: one that consults a file, or that fails on demand.
    pub fn new<F>(read: F) -> Self
    where
        F: Fn(&str) -> Result<Option<String>, ConfigError> + Send + Sync + 'static,
    {
        Env {
            read: Box::new(read),
        }
    }

    /// Reads a variable, distinguishing "not set" from "set but unreadable".
    ///
    /// The primitive the rest is built from: `None` means the operator did not
    /// set it, and an error means they did and it cannot be used.
    pub fn lookup(&self, key: &str) -> Result<Option<String>, ConfigError> {
        (self.read)(key)
    }

    /// Reads a required variable.
    ///
    /// An empty value is returned as-is — see [`Env::require_non_empty`].
    pub fn require(&self, key: &str) -> Result<String, ConfigError> {
        self.lookup(key)?.ok_or_else(|| ConfigError::Missing {
            key: key.to_owned(),
        })
    }

    /// Reads a required variable that must not be blank.
    pub fn require_non_empty(&self, key: &str) -> Result<String, ConfigError> {
        let value = self.require(key)?;
        if value.is_empty() {
            return Err(ConfigError::Empty {
                key: key.to_owned(),
            });
        }
        Ok(value)
    }

    /// Reads a required credential, redacted in `Debug` output.
    pub fn require_secret(&self, key: &str) -> Result<SecretString, ConfigError> {
        self.require_non_empty(key).map(SecretString::from)
    }

    /// Reads an optional variable, falling back to `default` when unset.
    ///
    /// A value that is set but unreadable is an error rather than a silent
    /// fallback: an operator who set a variable meant something by it.
    pub fn or_default(&self, key: &str, default: impl Into<String>) -> Result<String, ConfigError> {
        Ok(self.lookup(key)?.unwrap_or_else(|| default.into()))
    }

    /// Reads and parses a required variable.
    pub fn parse<T: FromStr>(&self, key: &str) -> Result<T, ConfigError> {
        let raw = self.require(key)?;
        raw.parse().map_err(|_| ConfigError::Invalid {
            key: key.to_owned(),
            expected: expected_type::<T>(),
        })
    }

    /// Reads and parses an optional variable, falling back to `default`.
    ///
    /// The fallback applies only when the variable is unset. A value that is
    /// set but unparseable is an error, so a typo in a tuning knob fails at
    /// startup rather than silently reverting to the default.
    pub fn parse_or<T: FromStr>(&self, key: &str, default: T) -> Result<T, ConfigError> {
        match self.lookup(key)? {
            None => Ok(default),
            Some(raw) => raw.parse().map_err(|_| ConfigError::Invalid {
                key: key.to_owned(),
                expected: expected_type::<T>(),
            }),
        }
    }

    /// Reads the first of several variables that is set.
    ///
    /// For migrating a key without breaking deployments that still set the old
    /// one: the gateway does exactly this with `NATS_URI` falling back to
    /// `BACKEND_URI`. Returns the name
    /// of the variable that supplied the value, so a caller can log which won.
    pub fn first_set<'a>(
        &self,
        keys: &[&'a str],
    ) -> Result<Option<(&'a str, String)>, ConfigError> {
        for key in keys {
            if let Some(value) = self.lookup(key)? {
                return Ok(Some((key, value)));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;

    /// A reader that fails, for the arms an unreadable variable reaches.
    ///
    /// The process environment can produce `NotUnicode`, and constructing that
    /// through `std::env` needs a non-UTF-8 value set on the process — the
    /// mutation this module exists to avoid. As a reader it is three lines.
    fn unreadable(key: &str) -> Result<Option<String>, ConfigError> {
        Err(ConfigError::NotUnicode {
            key: key.to_owned(),
        })
    }

    #[test]
    fn two_environments_are_read_at_once_without_a_lock() {
        // The point of the whole module. Both of these are live at the same
        // time, in one test, disagreeing about the same key — which is what a
        // process-wide environment plus a mutex exists to prevent and what an
        // injected reader makes ordinary.
        let one = Env::from_pairs([("MODE", "fast")]);
        let two = Env::from_pairs([("MODE", "slow")]);
        let (Ok(first), Ok(second)) = (one.require("MODE"), two.require("MODE")) else {
            panic!("both are set");
        };
        assert_eq!((first.as_str(), second.as_str()), ("fast", "slow"));
    }

    #[test]
    fn pairs_can_be_built_in_a_loop() {
        // The earlier helper borrowed `&'static [(&str, &str)]`, so a case
        // table had to be a `const` and could not be assembled.
        let built: Vec<(String, String)> = (1..=3)
            .map(|n| (format!("KEY{n}"), n.to_string()))
            .collect();
        let env = Env::from_pairs(built);
        assert_eq!(env.parse::<u8>("KEY2").unwrap_or(0), 2);
        // A later pair with the same key wins.
        let twice = Env::from_pairs([("K", "first"), ("K", "second")]);
        assert_eq!(twice.require("K").unwrap_or_default(), "second");
    }

    #[test]
    fn absent_and_present_are_told_apart() {
        let env = Env::from_pairs([("SET", "value"), ("BLANK", "")]);
        assert_eq!(env.lookup("SET").unwrap_or_default(), Some("value".into()));
        assert_eq!(env.lookup("UNSET").unwrap_or_default(), None);

        let Err(missing) = env.require("UNSET") else {
            panic!("an unset variable is missing");
        };
        assert!(matches!(missing, ConfigError::Missing { .. }), "{missing}");

        // Blank is a value to `require` and a refusal to `require_non_empty`:
        // an empty variable in a compose file is an operator who meant to set
        // it and did not.
        assert_eq!(env.require("BLANK").unwrap_or("x".into()), "");
        let Err(empty) = env.require_non_empty("BLANK") else {
            panic!("a blank variable is not a value");
        };
        assert!(matches!(empty, ConfigError::Empty { .. }), "{empty}");
    }

    #[test]
    fn an_env_does_not_print_what_it_holds() {
        // A reader answers with whatever it was built over, and for a binary
        // that is the process environment -- DSNs, passwords, tokens. A derived
        // `Debug` would have rendered a closure rather than its captures, but
        // `Env::from_pairs` captures the values themselves, so a `{:?}` added
        // later by someone debugging something else would print them and undo
        // what `require_secret` exists for.
        let env = Env::from_pairs([("DSN", "postgres://user:hunter2@db/app")]);
        let rendered = format!("{env:?}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(!rendered.contains("DSN"), "not even the key: {rendered}");
        assert!(
            rendered.contains("Env"),
            "it still says what it is: {rendered}"
        );
    }

    #[test]
    fn a_secret_does_not_print_itself() {
        let env = Env::from_pairs([("DSN", "postgres://user:hunter2@db/app")]);
        let Ok(secret) = env.require_secret("DSN") else {
            panic!("a set credential is read");
        };
        assert_eq!(secret.expose_secret(), "postgres://user:hunter2@db/app");
        let rendered = format!("{secret:?}");
        assert!(!rendered.contains("hunter2"), "redacted: {rendered}");

        let Err(error) = env.require_secret("ABSENT") else {
            panic!("an absent credential is an error");
        };
        assert!(matches!(error, ConfigError::Missing { .. }), "{error}");
    }

    #[test]
    fn a_default_applies_only_when_the_variable_is_unset() {
        let env = Env::from_pairs([("SIZE", "500"), ("BAD", "lots")]);
        assert_eq!(env.parse_or("SIZE", 100u32).unwrap_or(0), 500);
        assert_eq!(env.parse_or("ABSENT", 100u32).unwrap_or(0), 100);
        assert_eq!(
            env.or_default("ABSENT", "fallback").unwrap_or_default(),
            "fallback"
        );
        assert_eq!(
            env.or_default("SIZE", "fallback").unwrap_or_default(),
            "500"
        );

        // Set but unparseable is an error, not a silent fallback: a typo in a
        // tuning knob fails at startup rather than quietly reverting.
        let Err(error) = env.parse_or("BAD", 100u32) else {
            panic!("a typo is not a default");
        };
        let ConfigError::Invalid { key, expected } = &error else {
            panic!("wrong variant: {error:?}");
        };
        assert_eq!(key, "BAD");
        assert_eq!(*expected, "u32", "the bare type name, not its module path");

        let Err(required) = env.parse::<u32>("BAD") else {
            panic!("and the required form too");
        };
        assert!(matches!(required, ConfigError::Invalid { .. }));
        let Err(absent) = env.parse::<u32>("ABSENT") else {
            panic!("a required variable that is unset is missing");
        };
        assert!(matches!(absent, ConfigError::Missing { .. }));
    }

    #[test]
    fn the_first_variable_that_is_set_wins_and_says_which() {
        // Deliberately not real variable names. This test is about the ordering
        // rule, not about any particular pair -- and it was written with two
        // real ones, which a later rename collapsed into a single name. The
        // test then asserted that the first of two identical keys wins, which
        // is true of nothing and passed for the wrong reason until the values
        // disagreed.
        let env = Env::from_pairs([("SECOND_CHOICE", "old")]);
        assert_eq!(
            env.first_set(&["FIRST_CHOICE", "SECOND_CHOICE"])
                .unwrap_or_default(),
            Some(("SECOND_CHOICE", "old".to_owned())),
            "the name comes back so a caller can log which one won"
        );
        let both = Env::from_pairs([("FIRST_CHOICE", "new"), ("SECOND_CHOICE", "old")]);
        assert_eq!(
            both.first_set(&["FIRST_CHOICE", "SECOND_CHOICE"])
                .unwrap_or_default(),
            Some(("FIRST_CHOICE", "new".to_owned())),
            "order decides, not which is set"
        );
        assert_eq!(env.first_set(&["NEITHER"]).unwrap_or_default(), None);
        assert_eq!(env.first_set(&[]).unwrap_or_default(), None);
    }

    #[test]
    fn an_unreadable_variable_is_an_error_on_every_path() {
        // Not a silent fallback anywhere. A variable that is set and cannot be
        // read is an operator who meant something by it, and every helper that
        // has a default has to refuse rather than take it.
        let env = Env::new(unreadable);
        assert!(env.lookup("K").is_err());
        assert!(env.require("K").is_err());
        assert!(env.require_non_empty("K").is_err());
        assert!(env.require_secret("K").is_err());
        assert!(env.or_default("K", "fallback").is_err());
        assert!(env.parse::<u32>("K").is_err());
        assert!(env.parse_or("K", 1u32).is_err());
        assert!(env.first_set(&["K"]).is_err());
    }

    // Unix-only for `OsStringExt::from_vec`, which is how a non-UTF-8 value is
    // constructed at all. The test this replaces carried the same gate; without
    // it `cargo test -p pneuma-config` stops *compiling* on Windows rather than
    // skipping the case, which is a worse answer than not running it.
    #[cfg(unix)]
    #[test]
    fn a_mis_encoded_variable_is_an_error_rather_than_a_default() {
        // The arm the process cannot be made to produce without setting a
        // non-UTF-8 value on it. An operator who set a variable meant something
        // by it, so folding this into "absent" would hand them a default and no
        // complaint -- the gateway's `unwrap_or_else(|_| default)` does exactly
        // that, and it is one of this crate's two recorded departures.
        let mangled = std::os::unix::ffi::OsStringExt::from_vec(vec![0xff, 0xfe]);
        let Err(error) = classify("DSN", Err(VarError::NotUnicode(mangled))) else {
            panic!("a mis-encoded value is not a value");
        };
        let ConfigError::NotUnicode { key } = &error else {
            panic!("wrong variant: {error:?}");
        };
        assert_eq!(key, "DSN", "and it names the variable");
    }

    #[test]
    fn set_and_unset_are_told_apart_by_the_classifier() {
        // Ungated, unlike its `NotUnicode` sibling: these two arms are what
        // every platform takes, and gating them behind `cfg(unix)` would leave
        // the classifier unchecked everywhere else.
        assert_eq!(classify("K", Ok("v".to_owned())), Ok(Some("v".to_owned())));
        assert_eq!(classify("K", Err(VarError::NotPresent)), Ok(None));
    }

    #[test]
    fn a_type_name_is_reported_without_its_module_path() {
        // What an operator reads. `std::any::type_name` gives the internal
        // path, so this would otherwise say "must be a valid
        // core::net::socket_addr::SocketAddr".
        let env = Env::from_pairs([("BIND_ADDR", "not an address")]);
        let Err(ConfigError::Invalid { expected, .. }) =
            env.parse::<std::net::SocketAddr>("BIND_ADDR")
        else {
            panic!("that is not an address");
        };
        assert_eq!(expected, "SocketAddr");
    }

    #[test]
    fn the_process_reader_reads_the_process() {
        // Without mutating it, which is the whole point: `PATH` is set in every
        // environment this runs in, and a key of this shape is set in none.
        let env = Env::from_process();
        let Ok(Some(path)) = env.lookup("PATH") else {
            panic!("PATH is set in every environment this runs in");
        };
        assert!(!path.is_empty());
        assert_eq!(
            env.lookup("PNEUMA_CONFIG_DEFINITELY_NOT_SET")
                .unwrap_or_default(),
            None
        );
    }
}

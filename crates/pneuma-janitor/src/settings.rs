//! The knobs a pass reads, and the two that are dangerous when wrong.
//!
//! Transcribed the original, defaults
//! included, so a deployment can keep its existing environment. What is *not*
//! transcribed is the scheduling: `INTERVAL_MINUTES`, `ALERT_MISS_GRACED_SECONDS`
//! and `TIMEZONE` configure apscheduler, and when to run a pass is the
//! binary's business rather than the passes'.
//!
//! # Two settings can quietly disable or over-fire a pass
//!
//! Both are rejected here rather than left to be discovered in production.
//!
//! `LASTUPDATE_TIMEOUT` is how old a non-finalised node_run may be before its
//! run is called stale. The original constrains it `ge=1`.
//! At zero every in-progress
//! node_run is stale the
//! instant it is written, so the janitor submits *every active run* to the
//! gateway for termination. That is the difference between a janitor and an
//! outage.
//!
//! `RUN_BATCH_SIZE` at zero or below means cleanup selects nothing, for ever,
//! and says nothing about it — `RunStore::finalized_runs` returns empty for a
//! non-positive limit because MongoDB reads `limit: 0` as *no limit*, which
//! would otherwise stream every run document into memory. The store is right to
//! refuse it; a configuration that silently disables the pass is still worth
//! catching where it is written.
//!
//! `RETENTION_DAYS` at zero or below is *not* an error: the original logs and
//! skips, meaning "keep history for ever".
//! [`Settings::retention_cutoff`] returns `None`, and a caller that has nothing
//! to expire simply does not call the expiry pass.

use chrono::{DateTime, Duration, Utc};
use pneuma_config::{ConfigError, Env};

/// Why the environment could not be read as a janitor configuration.
#[derive(Debug, thiserror::Error)]
pub enum SettingsError {
    /// A variable was missing, unreadable, or not the right shape.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// A flag was set to something that is not a yes or a no.
    #[error("{key} is {value:?}, which is not a boolean")]
    NotABoolean {
        /// Which variable.
        key: String,
        /// What it was set to.
        value: String,
    },
    /// A value parsed but is outside the range that means anything.
    #[error("{key} is {value}, which {because}")]
    OutOfRange {
        /// Which variable.
        key: &'static str,
        /// What it was set to.
        value: i64,
        /// What that would do, in words.
        because: &'static str,
    },
}

/// What the janitor's passes need from the environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// How many finalised runs one cleanup pass takes.
    pub batch: i64,
    /// How long history is kept. Non-positive means for ever.
    pub retention_days: i64,
    /// How long a non-finalised node_run may sit before its run is stale.
    pub stale_after: Duration,
    /// Whether stale runs are submitted for termination at all.
    pub terminate_stale: bool,
}

/// A boolean the way the deployment already spells it.
///
/// `bool::from_str` accepts exactly `true` and `false`. The original's model
/// library represents `bool` more permissively, taking `1`, `0`, `True`,
/// `yes`, `on` and their opposites — and `True` is the original's own `repr`,
/// so it is what a hand-written env file is most likely to contain.
///
/// Rejecting those would mean a deployment that enables stale termination today
/// fails at startup after the port, which contradicts this module's whole claim
/// to read the environment a deployment already has. Anything else is still an
/// error: guessing at `maybe` would be worse than refusing it.
fn flag(env: &Env, key: &str, default: bool) -> Result<bool, SettingsError> {
    let Some(raw) = env.lookup(key)? else {
        return Ok(default);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "" => Ok(default),
        "1" | "true" | "yes" | "on" | "y" | "t" => Ok(true),
        "0" | "false" | "no" | "off" | "n" | "f" => Ok(false),
        _ => Err(SettingsError::NotABoolean {
            key: key.to_owned(),
            value: raw,
        }),
    }
}

/// How many runs one pass considers.
pub const RUN_BATCH_SIZE: &str = "PNEUMA_RUN_BATCH_SIZE";
/// How long a non-finalised node run may sit before its run is stale.
pub const STALE_AFTER_SECS: &str = "PNEUMA_STALE_AFTER_SECS";
/// How long history is kept.
pub const RETENTION_DAYS: &str = "PNEUMA_RETENTION_DAYS";
/// Whether a stale run is actually terminated, or only reported.
pub const STALE_TERMINATION_ENABLED: &str = "PNEUMA_STALE_TERMINATION_ENABLED";

impl Settings {
    /// Reads the environment, with the original's defaults.
    ///
    /// The environment is a parameter. It used to be the process's, which made
    /// every test here mutate a process global under a crate-wide `Mutex` --
    /// see `pneuma_config::source` for why that is the wrong shape.
    pub fn from_env(env: &Env) -> Result<Self, SettingsError> {
        let batch: i64 = env.parse_or(RUN_BATCH_SIZE, 100)?;
        if batch <= 0 {
            return Err(SettingsError::OutOfRange {
                key: RUN_BATCH_SIZE,
                value: batch,
                because: "would select no runs at all, silently disabling cleanup",
            });
        }

        let stale_seconds: i64 = env.parse_or(STALE_AFTER_SECS, 3600)?;
        if stale_seconds < 1 {
            return Err(SettingsError::OutOfRange {
                key: STALE_AFTER_SECS,
                value: stale_seconds,
                because:
                    "would call every in-progress node_run stale and terminate every active run",
            });
        }
        let Some(stale_after) = Duration::try_seconds(stale_seconds) else {
            return Err(SettingsError::OutOfRange {
                key: STALE_AFTER_SECS,
                value: stale_seconds,
                because: "is too large to be a duration",
            });
        };

        Ok(Settings {
            batch,
            retention_days: env.parse_or(RETENTION_DAYS, 14)?,
            stale_after,
            terminate_stale: flag(env, STALE_TERMINATION_ENABLED, false)?,
        })
    }

    /// The cutoff an expiry pass should use, or `None` to keep everything.
    ///
    /// Delegates rather than computing, so the two ends of a hostile
    /// `RETENTION_DAYS` are handled in one place — see
    /// `pneuma_store::retention_cutoff`, which also refuses a window too large
    /// to subtract from the current instant.
    pub fn retention_cutoff(&self) -> Option<DateTime<Utc>> {
        pneuma_store::retention_cutoff(self.retention_days)
    }

    /// The instant a non-finalised node_run must predate to be called stale.
    ///
    /// `None` only if the clock is near the end of representable time, which
    /// `from_env`'s range check makes unreachable through a configured value —
    /// it is an `Option` because the alternative is a panic in a janitor.
    pub fn stale_threshold(&self) -> Option<DateTime<Utc>> {
        Utc::now().checked_sub_signed(self.stale_after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Settings read from exactly these values and nothing else.
    ///
    /// No clearing, and no lock. The previous helper had to name every janitor
    /// key and unset it first, so that a value in the developer's own shell
    /// could not decide a result, and had to hold a crate-wide `Mutex` while it
    /// did -- because `connect` reads the environment too and two locks are no
    /// lock. An `Env` that only knows what it was handed makes both unnecessary
    /// and lets these tests run in parallel with each other and with
    /// `connect`'s.
    fn from(vars: &[(&str, &str)]) -> Result<Settings, SettingsError> {
        Settings::from_env(&Env::from_pairs(vars.iter().copied()))
    }

    #[test]
    fn an_empty_environment_gives_the_originals_defaults() {
        // the original. A deployment that sets
        // nothing must behave as it
        // did before the port.
        let settings = from(&[]);
        let Ok(settings) = settings else {
            panic!("defaults should be readable");
        };
        assert_eq!(settings.batch, 100);
        assert_eq!(settings.retention_days, 14);
        assert_eq!(settings.stale_after, Duration::seconds(3600));
        assert!(!settings.terminate_stale, "stale termination is opt-in");
    }

    #[test]
    fn a_zero_stale_timeout_is_refused() {
        // The original's `ge=1`. At zero every in-progress node_run is stale the
        // instant it is written, so the janitor submits every active run for
        // termination -- an outage, not a cleanup.
        for value in ["0", "-1"] {
            let result = from(&[("PNEUMA_STALE_AFTER_SECS", value)]);
            assert!(
                matches!(
                    result,
                    Err(SettingsError::OutOfRange {
                        key: STALE_AFTER_SECS,
                        ..
                    })
                ),
                "LASTUPDATE_TIMEOUT={value} should be refused"
            );
        }
        let ok = from(&[("PNEUMA_STALE_AFTER_SECS", "1")]);
        assert!(
            ok.is_ok(),
            "one second is the smallest meaning-bearing value"
        );
    }

    #[test]
    fn a_non_positive_batch_is_refused() {
        // It would select nothing for ever and say nothing about it, because
        // `finalized_runs` refuses a non-positive limit -- MongoDB reads
        // `limit: 0` as *no limit*. The store is right to refuse; a
        // configuration that silently disables the pass is worth catching
        // where it is written.
        for value in ["0", "-5"] {
            let result = from(&[("PNEUMA_RUN_BATCH_SIZE", value)]);
            assert!(
                matches!(
                    result,
                    Err(SettingsError::OutOfRange {
                        key: RUN_BATCH_SIZE,
                        ..
                    })
                ),
                "RUN_BATCH_SIZE={value} should be refused"
            );
        }
    }

    #[test]
    fn a_non_positive_retention_keeps_history_for_ever() {
        // Not an error: the original logs and skips, which means "keep it".
        for value in ["0", "-1"] {
            let Ok(settings) = from(&[("PNEUMA_RETENTION_DAYS", value)]) else {
                panic!("RETENTION_DAYS={value} is a valid setting, not a refusal");
            };
            assert!(
                settings.retention_cutoff().is_none(),
                "no cutoff means nothing is expired"
            );
        }

        let Ok(settings) = from(&[("PNEUMA_RETENTION_DAYS", "14")]) else {
            panic!("should read");
        };
        let Some(cutoff) = settings.retention_cutoff() else {
            panic!("a positive window has a cutoff");
        };
        let age = Utc::now() - cutoff;
        assert!(age >= Duration::days(14) && age < Duration::days(15));
    }

    #[test]
    fn a_value_that_is_not_a_number_is_a_config_error_not_a_default() {
        // Falling back to the default here would run the janitor on settings
        // nobody chose, which is how a typo becomes a silent policy change.
        let result = from(&[("PNEUMA_RUN_BATCH_SIZE", "lots")]);
        assert!(matches!(result, Err(SettingsError::Config(_))));
    }

    #[test]
    fn the_stale_threshold_is_that_far_back_from_now() {
        let Ok(settings) = from(&[("PNEUMA_STALE_AFTER_SECS", "60")]) else {
            panic!("should read");
        };
        let Some(threshold) = settings.stale_threshold() else {
            panic!("a minute back is representable");
        };
        let age = Utc::now() - threshold;
        assert!(
            age >= Duration::seconds(60) && age < Duration::seconds(120),
            "a minute back, not an hour or none: {age}"
        );
    }

    #[test]
    fn stale_termination_accepts_the_spellings_the_deployment_already_uses() {
        // `bool::from_str` takes only `true`/`false`. The original's model library `bool`
        // takes more, so `1`, `True`, `yes` and `on` are all live values today --
        // and `True` is the original's own repr, the likeliest thing in a hand-written
        // env file. Refusing them would fail at startup for a deployment that
        // works now.
        for (value, expected) in [
            ("true", true),
            ("True", true),
            ("TRUE", true),
            ("1", true),
            ("yes", true),
            ("on", true),
            ("false", false),
            ("False", false),
            ("0", false),
            ("no", false),
            ("off", false),
            ("  true  ", true),
        ] {
            let Ok(settings) = from(&[("PNEUMA_STALE_TERMINATION_ENABLED", value)]) else {
                panic!("{value} should parse");
            };
            assert_eq!(settings.terminate_stale, expected, "for {value:?}");
        }

        // Anything else is refused rather than guessed at.
        let result = from(&[("PNEUMA_STALE_TERMINATION_ENABLED", "maybe")]);
        assert!(matches!(result, Err(SettingsError::NotABoolean { .. })));

        // And an empty value means "unset", not "false by accident" -- the same
        // reading `pneuma-config` gives it elsewhere.
        let Ok(empty) = from(&[("PNEUMA_STALE_TERMINATION_ENABLED", "")]) else {
            panic!("an empty value falls back to the default");
        };
        assert!(!empty.terminate_stale);
    }

    #[test]
    fn a_stale_timeout_too_large_to_be_a_duration_is_refused() {
        // The far end, which `ge=1` says nothing about. `Duration::try_seconds`
        // is `None` past its range, and the alternative to catching it is a
        // panic inside a janitor.
        let result = from(&[("PNEUMA_STALE_AFTER_SECS", &i64::MAX.to_string())]);
        assert!(matches!(
            result,
            Err(SettingsError::OutOfRange {
                key: STALE_AFTER_SECS,
                ..
            })
        ));
    }
}

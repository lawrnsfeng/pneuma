//! What the service reads from its environment.
//!
//! Every value is decided here and nothing below re-reads the environment, so
//! the whole configuration of a deployment is one struct a test can build by
//! hand — and `Env` is injected, so those tests need no process globals.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use chrono::TimeDelta;
use pneuma_config::{ConfigError, Env};
use pneuma_fairness::{Weight, WeightError};
use secrecy::SecretString;

/// Where Postgres is.
pub const DATABASE_URL: &str = "DATABASE_URL";
/// Where to listen.
pub const LISTEN: &str = "PNEUMA_LISTEN";
/// Where Restate's ingress is.
pub const RESTATE_INGRESS: &str = "PNEUMA_RESTATE_INGRESS";
/// The durable handler a submission is sent to.
pub const RESTATE_HANDLER: &str = "PNEUMA_RESTATE_HANDLER";
/// How many submissions one round may dispatch.
pub const BATCH_SIZE: &str = "PNEUMA_BATCH_SIZE";
/// How many of each tenant's rows a round considers.
pub const PER_TENANT: &str = "PNEUMA_PER_TENANT";
/// How often a round runs, in seconds.
pub const DISPATCH_INTERVAL_SECS: &str = "PNEUMA_DISPATCH_INTERVAL_SECS";
/// How long a claim may go unsettled before it is swept back, in seconds.
pub const RECLAIM_AFTER_SECS: &str = "PNEUMA_RECLAIM_AFTER_SECS";
/// Per-tenant weights, as `tenant=weight` pairs separated by commas.
pub const TENANT_WEIGHTS: &str = "PNEUMA_TENANT_WEIGHTS";

/// The largest number of seconds any schedule may be set to.
///
/// A year. Not a stylistic limit: `Utc::now() + delta` **panics** once the
/// result leaves chrono's ±262143-year range, and `tokio::time::interval`
/// panics the same way on `Instant::now() + period`. Both of those are past
/// startup, so an unbounded number here is a process that binds, reports ready,
/// and dies on its first tick -- which is the failure the zero check on the
/// same variables exists to prevent, reached from the other end. A pasted
/// microsecond epoch is the way it happens.
pub const MAX_SECONDS: i64 = 365 * 24 * 60 * 60;

/// The largest a batch or a per-tenant limit may be.
///
/// A round holds every selected payload in memory at once, so this is a bound
/// on the process's resident size as much as on the query.
pub const MAX_COUNT: i64 = 1_000_000;

/// Where to listen when [`LISTEN`] is unset.
pub const DEFAULT_LISTEN: &str = "0.0.0.0:9081";
/// The handler when [`RESTATE_HANDLER`] is unset — what `pneuma-restate` serves.
pub const DEFAULT_HANDLER: &str = "PneumaRunner/run";

/// Everything the service needs to run.
#[derive(Debug, Clone)]
pub struct Config {
    /// The Postgres DSN.
    ///
    /// Secret because it carries the password, and a `Debug` of a config that
    /// prints a DSN is how credentials reach logs -- the design notes, and
    /// the same reasoning `pneuma_janitor::connect` applies to the same string.
    pub database_url: SecretString,
    /// Where the HTTP surface binds.
    pub listen: SocketAddr,
    /// Restate's ingress, e.g. `http://restate:8080`.
    pub ingress: String,
    /// The handler path, e.g. `PneumaRunner/run`.
    pub handler: String,
    /// How many submissions one round dispatches.
    pub batch_size: i64,
    /// How many of each tenant's rows a round considers.
    pub per_tenant: i64,
    /// How often a round runs.
    pub interval: Duration,
    /// How long a claim may go unsettled before the sweep takes it back.
    ///
    /// A `TimeDelta` rather than a [`Duration`] because that is what the sweep
    /// subtracts from `Utc::now()`, and the conversion between the two is
    /// fallible. Doing it here means a number too large to be a cutoff is a
    /// refusal at startup rather than a fallible conversion inside the loop,
    /// where the only honest thing left to do with it would be to guess.
    pub reclaim_after: TimeDelta,
    /// Each tenant's share. Absent means [`Weight::ONE`].
    pub weights: BTreeMap<String, Weight>,
}

/// Why the service will not start.
#[derive(Debug, thiserror::Error)]
pub enum ConfigureError {
    /// A variable is missing or unusable.
    #[error("{0}")]
    Variable(#[from] ConfigError),

    /// The listen address is not an address.
    #[error("{LISTEN} is not a socket address: {value:?}")]
    Listen {
        /// What was given.
        value: String,
    },

    /// A tuning knob is outside the range that means anything.
    ///
    /// Both ends are refused, and for the same reason: a value the process
    /// cannot act on has to be caught while somebody is watching the deploy.
    ///
    /// Zero is not "no limit". For three of the four it is a service that does
    /// nothing, and for the interval it is worse -- `tokio::time::interval`
    /// **panics** on a zero period, so the process would come up, report ready,
    /// and die at the first tick. The ceiling closes the same door from the
    /// other side: see [`MAX_SECONDS`].
    #[error("{name} must be between 1 and {max}, got {value}")]
    OutOfRange {
        /// Which variable.
        name: &'static str,
        /// What was given.
        value: i64,
        /// The largest value that means anything.
        max: i64,
    },

    /// A weight pair could not be read.
    ///
    /// Refused at startup rather than skipped. A tenant whose weight is
    /// mistyped would silently fall back to `Weight::ONE`, which is a *quieter*
    /// share than intended for anyone configured above one — so the deployment
    /// would run, look healthy, and quietly under-serve the customer somebody
    /// went to the trouble of prioritising.
    #[error("{TENANT_WEIGHTS} entry {entry:?} is not `tenant=weight`: {why}")]
    Weights {
        /// The entry that could not be read.
        entry: String,
        /// What was wrong with it.
        why: String,
    },
}

impl Config {
    /// Reads the whole configuration, or refuses to start.
    pub fn from_env(env: &Env) -> Result<Self, ConfigureError> {
        let listen_raw = defaulted(env, LISTEN, DEFAULT_LISTEN)?;
        let Ok(listen) = listen_raw.parse::<SocketAddr>() else {
            return Err(ConfigureError::Listen { value: listen_raw });
        };
        Ok(Config {
            database_url: SecretString::from(env.require_non_empty(DATABASE_URL)?),
            listen,
            ingress: env.require_non_empty(RESTATE_INGRESS)?,
            handler: defaulted(env, RESTATE_HANDLER, DEFAULT_HANDLER)?,
            batch_size: bounded(BATCH_SIZE, env.parse_or(BATCH_SIZE, 50_i64)?, MAX_COUNT)?,
            per_tenant: bounded(PER_TENANT, env.parse_or(PER_TENANT, 500_i64)?, MAX_COUNT)?,
            interval: seconds(
                DISPATCH_INTERVAL_SECS,
                env.parse_or(DISPATCH_INTERVAL_SECS, 2_i64)?,
            )?,
            reclaim_after: delta(
                RECLAIM_AFTER_SECS,
                env.parse_or(RECLAIM_AFTER_SECS, 300_i64)?,
            )?,
            weights: weights(&env.or_default(TENANT_WEIGHTS, "")?)?,
        })
    }
}

/// Reads `tenant=weight,tenant=weight` into a map.
///
/// Pure, so every way of getting it wrong is a test rather than a deployment.
/// Empty is an empty map, not an error: a deployment that has not prioritised
/// anyone is the ordinary case, and every tenant then gets [`Weight::ONE`].
pub fn weights(raw: &str) -> Result<BTreeMap<String, Weight>, ConfigureError> {
    let mut parsed = BTreeMap::new();
    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((tenant, value)) = entry.split_once('=') else {
            return Err(ConfigureError::Weights {
                entry: entry.to_owned(),
                why: "no `=`".to_owned(),
            });
        };
        let tenant = tenant.trim();
        if tenant.is_empty() {
            return Err(ConfigureError::Weights {
                entry: entry.to_owned(),
                why: "the tenant is blank".to_owned(),
            });
        }
        let number = match value.trim().parse::<u32>() {
            Ok(number) => number,
            Err(error) => {
                return Err(ConfigureError::Weights {
                    entry: entry.to_owned(),
                    why: error.to_string(),
                })
            }
        };
        // `Weight::new` refuses zero, and that refusal is worth keeping: a
        // weight of zero is a tenant that never gets a share, which is a way of
        // switching a customer off by editing a config line -- and it reads as
        // a typo for one.
        let weight = match Weight::new(number) {
            Ok(weight) => weight,
            Err(WeightError::Zero) => {
                return Err(ConfigureError::Weights {
                    entry: entry.to_owned(),
                    why: "a weight of zero would give that tenant no share at all".to_owned(),
                })
            }
        };
        parsed.insert(tenant.to_owned(), weight);
    }
    Ok(parsed)
}

/// A variable with a default, where blank means unset.
///
/// `Env::or_default` substitutes only when a variable is *absent*, and
/// `PNEUMA_RESTATE_HANDLER=` in a compose file is a variable that is present
/// and empty -- which is what `${HANDLER}` renders to when `HANDLER` is not
/// set. Taking that literally gave a handler of `""`, a submission URL of
/// `http://restate:8080//send`, a 404, and every submission settled `failed`
/// permanently. The value is trimmed for the same reason `pneuma_janitor`
/// trims its endpoints: a value read out of a rendered secret carries a
/// trailing newline routinely, and no path or address ever means to have one.
fn defaulted(env: &Env, key: &str, default: &str) -> Result<String, ConfigureError> {
    let set = env
        .lookup(key)?
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    Ok(set.unwrap_or_else(|| default.to_owned()))
}

/// Refuses a knob outside `1..=max`.
fn bounded(name: &'static str, value: i64, max: i64) -> Result<i64, ConfigureError> {
    if (1..=max).contains(&value) {
        return Ok(value);
    }
    Err(ConfigureError::OutOfRange { name, value, max })
}

/// A number of seconds in range, as a [`Duration`].
fn seconds(name: &'static str, value: i64) -> Result<Duration, ConfigureError> {
    let seconds = bounded(name, value, MAX_SECONDS)?;
    // The cast cannot lose anything: `bounded` has already refused everything
    // below 1, so the value is in `1..=MAX_SECONDS` and every one of those is a
    // `u64`.
    Ok(Duration::from_secs(seconds.unsigned_abs()))
}

/// A number of seconds in range, as the [`TimeDelta`] the sweep subtracts.
///
/// The range check and the conversion are one `match` rather than two steps,
/// because separating them leaves an arm nothing can reach: everything
/// `TimeDelta::try_seconds` rejects is already past [`MAX_SECONDS`], so a
/// second check for it would be a branch no input can take.
fn delta(name: &'static str, value: i64) -> Result<TimeDelta, ConfigureError> {
    match TimeDelta::try_seconds(value) {
        Some(delta) if (1..=MAX_SECONDS).contains(&value) => Ok(delta),
        _ => Err(ConfigureError::OutOfRange {
            name,
            value,
            max: MAX_SECONDS,
        }),
    }
}

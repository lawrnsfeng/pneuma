//! Bringing the service up.
//!
//! Everything `main` would otherwise do. A binary's `main` is never executed by
//! the test suite, so `scripts/coverage.sh` excludes it and refuses to let it
//! hold logic — which means the startup path lives here, where it is measured,
//! and `main` is one line.

use std::net::SocketAddr;

use secrecy::SecretString;

use restate_sdk::prelude::{Endpoint as RestateEndpoint, HttpServer};

use crate::component::HttpComponent;
use crate::endpoint::{Endpoint, EndpointError};
use crate::service::Runner;

/// Why the service could not start.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    /// A required variable is not set.
    ///
    /// Refused rather than defaulted. A default component endpoint would be
    /// wrong on every deployment and would fail at the first component call
    /// instead of at startup, which is the difference between a pod that never
    /// becomes ready and one that accepts work it cannot do.
    #[error("{} is not set", COMPONENT_ENDPOINT)]
    Missing,

    /// The listen address could not be parsed.
    #[error("{value:?} is not an address to listen on: {reason}")]
    BadAddress {
        /// What was given.
        value: String,
        /// Why it could not be used.
        reason: String,
    },

    /// The component endpoint template is unusable.
    #[error("{0}")]
    Endpoint(#[from] EndpointError),

    /// The component timeout is not a positive number of seconds.
    ///
    /// Zero is refused rather than meaning "no timeout": a client with no
    /// request timeout is the failure this whole knob exists to prevent, and
    /// spelling it `0` is the likeliest way someone would ask for it by
    /// accident.
    #[error(
        "{} must be a positive number of seconds, got {value:?}",
        COMPONENT_TIMEOUT
    )]
    BadTimeout {
        /// What was given.
        value: String,
    },

    /// The shared HTTP client could not be built.
    ///
    /// Only reachable if reqwest cannot initialise its TLS backend, which is a
    /// property of the machine rather than of the configuration -- but it is a
    /// startup failure either way, and reporting it as one beats discovering it
    /// at the first component call.
    #[error("the component HTTP client could not be built: {0}")]
    Client(#[from] reqwest::Error),

    /// The database the mirror writes to was not configured.
    #[error("{DATABASE_URL} is required")]
    NoDatabase,

    /// The database could not be reached.
    ///
    /// A `String` rather than the `sqlx::Error`, so a DSN cannot reach a log
    /// through a `Debug`
    #[error("could not reach the database: {0}")]
    Database(String),

    /// The database is there, but `node_run` is not.
    ///
    /// Separate from [`ServeError::Database`] because the operator action is
    /// different: one is a wrong address or a database that is down, the other
    /// is a migration that has not been run — or a `search_path` that does not
    /// reach the schema it ran in. Which of the two is decided by the
    /// SQLSTATE rather than by "the probe failed", so a role that may connect
    /// but not read `node_run` is not reported as a missing migration.
    #[error("the database has no node_run to mirror into: {0}")]
    Unmigrated(String),
}

/// The variable naming where components live.
pub const COMPONENT_ENDPOINT: &str = "PNEUMA_COMPONENT_ENDPOINT";
/// Where the `node_run` mirror is written.
///
/// Required, with no default, and that is the point. A service that silently
/// mirrors nothing looks exactly like one that is working, and the only visible
/// symptom is `pneuma-janitor` finding no stale runs — weeks later, and
/// attributed to the janitor. `DATABASE_URL` rather than a `PNEUMA_` name for
/// the reason every other crate here gives: it is a de-facto convention (sqlx,
/// Rails, Django) rather than an inheritance.
pub const DATABASE_URL: &str = "DATABASE_URL";
/// The variable naming what to listen on.
pub const LISTEN: &str = "PNEUMA_LISTEN";
/// Where to listen when [`LISTEN`] is unset.
pub const DEFAULT_LISTEN: &str = "0.0.0.0:9080";
/// How long one component call may take, in seconds.
pub const COMPONENT_TIMEOUT: &str = "PNEUMA_COMPONENT_TIMEOUT_SECS";

/// Reads the configuration a run needs from the environment.
///
/// Split from [`serve`] because everything decidable is decidable here: a bad
/// template and an unparseable address are both startup failures, and both are
/// testable without binding a port.
pub fn configuration(
    read: impl Fn(&str) -> Option<String>,
) -> Result<(Settings, SocketAddr), ServeError> {
    // A unit variant, not one carrying the variable's name. Only one variable
    // can be missing, so the field was always the same constant -- no
    // information, and its struct-literal line was a move tarpaulin could never
    // see run. The message still names it, from the constant.
    let Some(template) = read(COMPONENT_ENDPOINT) else {
        return Err(ServeError::Missing);
    };
    let endpoint = Endpoint::new(template)?;

    // The listen address has a default because a wrong one fails loudly at
    // bind; the component endpoint does not, because a wrong one fails at the
    // first component call, long after the pod reported ready.
    let listen = read(LISTEN).unwrap_or_else(|| DEFAULT_LISTEN.to_owned());
    let address = match listen.parse::<SocketAddr>() {
        Ok(address) => address,
        Err(error) => {
            return Err(ServeError::BadAddress {
                value: listen,
                reason: error.to_string(),
            })
        }
    };
    // A deployment property like the two above it, and defaulted for the same
    // reason the listen address is: a wrong value fails at startup here rather
    // than at the first component call.
    let timeout = match read(COMPONENT_TIMEOUT) {
        None => std::time::Duration::from_secs(HttpComponent::DEFAULT_TIMEOUT_SECS),
        Some(value) => match value.parse::<u64>() {
            Ok(0) | Err(_) => {
                return Err(ServeError::BadTimeout { value });
            }
            Ok(seconds) => std::time::Duration::from_secs(seconds),
        },
    };

    // Blank is unset, as everywhere else: `DATABASE_URL=` is what
    // `${PG_DSN}` renders to in a compose file when the outer variable is not
    // set, and taking it literally is a pod that starts and mirrors nothing.
    let database_url = read(DATABASE_URL)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let Some(database_url) = database_url else {
        return Err(ServeError::NoDatabase);
    };

    // Bound, then returned. Written as one `Ok((Settings { .. }, address))`
    // the closing lines of the literal are a move tarpaulin attributes to no
    // executed line -- `docs/verification.md` records the shape and the
    // measurement that established it.
    let settings = Settings {
        endpoint,
        timeout,
        database_url: SecretString::from(database_url),
    };
    Ok((settings, address))
}

/// Everything decided before anything connects.
///
/// Split from [`Runner`] because building the runner now needs a database pool,
/// and connecting is async while every decision here is not. Keeping the
/// decisions in a pure function is what lets a bad endpoint, a bad address and
/// a missing DSN all be tested without a server — which is the property
/// `configuration` was written for and would have lost.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Where components live.
    pub endpoint: Endpoint,
    /// How long one component call may take.
    pub timeout: std::time::Duration,
    /// Where the mirror is written.
    ///
    /// Secret because it carries the password, and a `Debug` of a config that
    /// prints a DSN is how credentials reach logs
    pub database_url: SecretString,
}

impl Settings {
    /// Builds the runner, given a pool.
    ///
    /// The retry bound is derived from the timeout rather than being a constant
    /// beside it: `PNEUMA_COMPONENT_TIMEOUT_SECS` would otherwise be able to
    /// reintroduce the exact defect a fixed 300 s was chosen to remove -- any
    /// timeout past half a hard-coded 600 s bound authorises a second attempt
    /// that runs beyond it, so a run takes longer than the policy documents and
    /// the component is paid for twice.
    pub fn runner(self, pool: sqlx::PgPool) -> Result<Runner, ServeError> {
        let mirror = pneuma_mirror::Mirror::new(pool, "pneuma-restate");
        let retry = HttpComponent::retry_for(self.timeout);
        Ok(Runner::with_retry(
            self.endpoint,
            retry,
            self.timeout,
            mirror,
        )?)
    }
}

/// Connects to the database and builds the runner.
///
/// The one async step between deciding and serving. Separate from
/// [`configuration`] so every refusal that can be decided without a server
/// still is, and separate from [`serve`] so a database that cannot be reached
/// is a pod that never becomes ready rather than one that reports healthy and
/// mirrors nothing.
pub async fn connect(prepared: (Settings, SocketAddr)) -> Result<(Runner, SocketAddr), ServeError> {
    let (settings, address) = prepared;
    let opening = sqlx::postgres::PgPoolOptions::new()
        .max_connections(MAX_CONNECTIONS)
        .connect(secrecy::ExposeSecret::expose_secret(&settings.database_url));
    // The bound is on *this* await and not on the pool. `acquire_timeout` would
    // have been the shorter spelling and the wrong one: sqlx applies it to
    // every acquire for the life of the pool, so a startup bound written there
    // becomes a runtime one, and a burst of concurrent runs contending for
    // MAX_CONNECTIONS would start dropping rows the moment a wait exceeded it.
    let pool = match tokio::time::timeout(CONNECT_TIMEOUT, opening).await {
        Ok(Ok(pool)) => pool,
        Ok(Err(error)) => return Err(ServeError::Database(error.to_string())),
        Err(_) => {
            return Err(ServeError::Database(format!(
                "no answer in {CONNECT_TIMEOUT:?}"
            )))
        }
    };
    let runner = settings.runner(pool)?;
    // Reachable is not the same as migrated. Without this, a replica pointed at
    // a database whose `node_run` is missing reports ready and then logs one
    // refusal per step for ever -- the silent mirror, one step removed.
    // Bounded like the open above it, and for the same reason: a database that
    // completes the handshake and then stalls -- a catalog lock, a hung standby
    // -- would otherwise block startup before anything binds, with no log line
    // and no port to ask.
    if let Err(why) = runner.mirror().ready_within(CONNECT_TIMEOUT).await {
        return Err(unready(why));
    }
    Ok((runner, address))
}

/// Which refusal an unready mirror is.
///
/// Two variants because the operator action is different, and only
/// `undefined_table` means "run the migrations" — a role that may connect but
/// not read `node_run` is a common least-privilege setup, and reporting it as a
/// missing migration sends somebody to re-run migrations that already applied.
fn unready(why: pneuma_mirror::NotReady) -> ServeError {
    match why {
        pneuma_mirror::NotReady::NoTable(reason) => ServeError::Unmigrated(reason),
        pneuma_mirror::NotReady::Unusable(reason) => ServeError::Database(reason),
    }
}

/// How long startup waits for the database before refusing.
///
/// `sqlx` defaults to thirty seconds, and retries a refused connection for the
/// whole of it. That is the wrong shape for a startup probe: a pod that takes
/// half a minute to say why it will not start looks like a pod that is hanging,
/// and the orchestrator's own restart backoff is the right place for the
/// patience. Five seconds is long enough to ride out a database still opening
/// its port and short enough that the refusal is legible.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// How many Postgres connections one replica may hold.
///
/// One replica drives many invocations at once — Restate decides how many —
/// and each records two or three short statements per step, so this is what
/// stands between a burst of concurrent runs and a queue at the pool. Sixteen
/// rather than the four this started as: the statements are short enough that
/// four is not a *throughput* problem, but a mirror that waits is a mirror that
/// eventually gives up, and a dropped row is invisible. It is still a bound on
/// what one replica takes from the database's connection limit, which is why it
/// is not larger.
pub const MAX_CONNECTIONS: u32 = 16;

/// Serves the runner until the process is stopped.
///
/// Never returns in normal operation, which is why it is thin: everything that
/// can be decided has been decided by [`configuration`].
pub async fn serve(runner: Runner, address: SocketAddr) {
    HttpServer::new(RestateEndpoint::builder().bind(runner).build())
        .listen_and_serve(address)
        .await;
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
    fn a_complete_environment_yields_an_endpoint_and_an_address() {
        let Ok((settings, address)) = configuration(from(&[
            (COMPONENT_ENDPOINT, "http://components.svc/{component}"),
            (DATABASE_URL, "postgres://user:pass@db/pneuma"),
            (LISTEN, "127.0.0.1:9999"),
        ])) else {
            panic!("a complete environment must start");
        };
        let Ok(url) = settings.endpoint.url_for("ocr") else {
            panic!("the template resolves");
        };
        assert_eq!(url, "http://components.svc/ocr");
        assert_eq!(address.port(), 9999);
    }

    #[test]
    fn the_listen_address_defaults_but_the_component_endpoint_does_not() {
        // The asymmetry is the point. A wrong listen address fails at bind,
        // immediately and visibly. A wrong component endpoint fails at the
        // first component call -- after the pod has reported ready and started
        // accepting work it cannot do.
        let Ok((_, address)) = configuration(from(&[
            (COMPONENT_ENDPOINT, "http://c/{component}"),
            (DATABASE_URL, "postgres://user:pass@db/pneuma"),
        ])) else {
            panic!("the listen address has a default");
        };
        assert_eq!(address.to_string(), DEFAULT_LISTEN);

        let Err(error) = configuration(from(&[(LISTEN, "0.0.0.0:1")])) else {
            panic!("there is no sensible default for where components live");
        };
        assert!(
            matches!(error, ServeError::Missing),
            "wrong variant: {error:?}"
        );
        assert!(error.to_string().contains(COMPONENT_ENDPOINT), "{error}");
    }

    #[test]
    fn the_component_timeout_defaults_and_can_be_set() {
        let Ok((settings, _)) = configuration(from(&[
            (COMPONENT_ENDPOINT, "http://c/{component}"),
            (DATABASE_URL, "postgres://user:pass@db/pneuma"),
        ])) else {
            panic!("the timeout has a default");
        };
        assert_eq!(
            settings.timeout,
            std::time::Duration::from_secs(HttpComponent::DEFAULT_TIMEOUT_SECS),
            "300s, the original's own model timeout"
        );

        let Ok((settings, _)) = configuration(from(&[
            (COMPONENT_ENDPOINT, "http://c/{component}"),
            (DATABASE_URL, "postgres://user:pass@db/pneuma"),
            (COMPONENT_TIMEOUT, "45"),
        ])) else {
            panic!("an explicit timeout is accepted");
        };
        assert_eq!(settings.timeout, std::time::Duration::from_secs(45));
    }

    #[test]
    fn the_retry_bound_follows_the_configured_timeout() {
        // The regression this guards: with the bound a constant, setting
        // `PNEUMA_COMPONENT_TIMEOUT_SECS` above half of it reinstates the
        // sixteen-minute, twice-charged run that `DEFAULT_TIMEOUT_SECS` was
        // corrected to remove -- the environment reintroducing a defect the
        // source no longer contains.
        let Ok((settings, _)) = configuration(from(&[
            (COMPONENT_ENDPOINT, "http://c/{component}"),
            (DATABASE_URL, "postgres://user:pass@db/pneuma"),
            (COMPONENT_TIMEOUT, "480"),
        ])) else {
            panic!("480 is a positive number of seconds");
        };
        assert_eq!(settings.timeout, std::time::Duration::from_secs(480));
        assert_eq!(
            format!("{:?}", HttpComponent::retry_for(settings.timeout)),
            format!(
                "{:?}",
                HttpComponent::retry_for(std::time::Duration::from_secs(480))
            ),
            "the policy tracks the timeout rather than a constant beside it"
        );
    }

    #[test]
    fn a_zero_or_unparseable_component_timeout_is_refused_at_startup() {
        // Zero is refused rather than meaning "no timeout". A client without a
        // request timeout is the exact failure this knob exists to prevent, and
        // `0` is the likeliest way to ask for it by accident.
        const CASES: &[(&str, &[(&str, &str)])] = &[
            (
                "0",
                &[
                    (COMPONENT_ENDPOINT, "http://c/{component}"),
                    (COMPONENT_TIMEOUT, "0"),
                ],
            ),
            (
                "-5",
                &[
                    (COMPONENT_ENDPOINT, "http://c/{component}"),
                    (COMPONENT_TIMEOUT, "-5"),
                ],
            ),
            (
                "forever",
                &[
                    (COMPONENT_ENDPOINT, "http://c/{component}"),
                    (COMPONENT_TIMEOUT, "forever"),
                ],
            ),
            (
                "",
                &[
                    (COMPONENT_ENDPOINT, "http://c/{component}"),
                    (COMPONENT_TIMEOUT, ""),
                ],
            ),
            (
                "300s",
                &[
                    (COMPONENT_ENDPOINT, "http://c/{component}"),
                    (COMPONENT_TIMEOUT, "300s"),
                ],
            ),
        ];
        for (value, pairs) in CASES {
            let Err(error) = configuration(from(pairs)) else {
                panic!("{value:?} is not a positive number of seconds");
            };
            let ServeError::BadTimeout { value: reported } = &error else {
                panic!("wrong variant for {value:?}: {error:?}");
            };
            assert_eq!(reported, value);
            // The message names the variable, so an operator knows which of the
            // three to look at.
            assert!(error.to_string().contains(COMPONENT_TIMEOUT), "{error}");
        }
    }

    #[test]
    fn a_missing_database_is_a_startup_refusal() {
        // Required, with no default. A service that mirrors nothing looks
        // exactly like one that works, and the only visible symptom is
        // `pneuma-janitor` finding no stale runs -- weeks later, and attributed
        // to the janitor.
        let Err(error) = configuration(from(&[(COMPONENT_ENDPOINT, "http://c/{component}")]))
        else {
            panic!("the mirror has nowhere to write");
        };
        assert!(matches!(error, ServeError::NoDatabase), "{error:?}");
        assert!(error.to_string().contains(DATABASE_URL), "{error}");
    }

    #[test]
    fn a_blank_database_url_is_an_unset_one() {
        // `DATABASE_URL=` is what `${PG_DSN}` renders to in a compose file when
        // the outer variable is not set. Taking it literally is a pod that
        // starts and mirrors nothing, which is the one outcome this refusal
        // exists to prevent.
        let Err(error) = configuration(from(&[
            (COMPONENT_ENDPOINT, "http://c/{component}"),
            (DATABASE_URL, "   "),
        ])) else {
            panic!("blank is not a DSN");
        };
        assert!(matches!(error, ServeError::NoDatabase), "{error:?}");
    }

    #[test]
    fn a_template_without_the_placeholder_fails_at_startup_not_at_the_first_call() {
        let Err(error) = configuration(from(&[(COMPONENT_ENDPOINT, "http://components.svc/")]))
        else {
            panic!("a fixed URL is not a template");
        };
        assert!(
            matches!(error, ServeError::Endpoint(_)),
            "wrong variant: {error:?}"
        );
    }

    #[test]
    fn an_unparseable_listen_address_names_what_was_given() {
        let Err(error) = configuration(from(&[
            (COMPONENT_ENDPOINT, "http://c/{component}"),
            (DATABASE_URL, "postgres://user:pass@db/pneuma"),
            (LISTEN, "not-an-address"),
        ])) else {
            panic!("it cannot be bound");
        };
        let ServeError::BadAddress { value, .. } = &error else {
            panic!("wrong variant: {error:?}");
        };
        assert_eq!(value, "not-an-address");
        // Named, because "invalid address" without saying which sends someone
        // to read a deployment manifest.
        assert!(error.to_string().contains("not-an-address"), "{error}");
    }
}

//! Bringing the service up, and deciding when a pass runs.
//!
//! `scripts/coverage.sh` excludes a binary's `main` and refuses to let it hold
//! definitions, so everything `main` would otherwise do lives here where it is
//! measured.
//!
//! # A daemon, where the original is a cron
//!
//! the original builds an apscheduler
//! `AsyncIOScheduler` and registers two or three `CronTrigger` jobs on
//! `INTERVAL_MINUTES`. Here it is one `pneuma_serve::every` on a plain
//! interval, which is a deliberate narrowing: a cron *minute field* can say
//! things an interval cannot (`0,17,42`), and nothing in the original's history
//! or its manifests uses that — `*/5` is the default and the only value anyone
//! sets. Reading the full grammar would mean carrying a cron parser to support
//! a schedule nobody writes.
//!
//! So `PNEUMA_INTERVAL_MINUTES` is a number of minutes. A deployment that has
//! `INTERVAL_MINUTES=*/5` today sets `PNEUMA_INTERVAL_MINUTES=5`, and one that
//! has something a plain interval cannot express is refused at startup rather
//! than silently rounded.
//!
//! # `TIMEZONE` is deliberately not read
//!
//! The original declares it, parses it into a `ZoneInfo`, and reads it in
//! exactly one place: `delete_outdated_runs` passes it to the retention query,
//! which computes
//! `datetime.now(tz) - timedelta(days=N)` and then
//! `.astimezone(UTC)`.
//!
//! Those two operations cancel. `now(tz)` and `now(UTC)` are the same instant,
//! subtracting a whole number of days from an instant is timezone-independent,
//! and converting back to UTC undoes the display conversion — so the cutoff is
//! identical for every value of `TIMEZONE`. The scheduler does not read it
//! either: the original pass `timezone=UTC`
//! explicitly.
//!
//! It is a knob that appears to do something and does nothing. Reading it here
//! would reproduce that appearance; the honest thing is to leave it unread and
//! say why. The design notes are the same finding reached from the query end.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mongodb::bson::Document;
use mongodb::{Client as MongoClient, Collection};
use pneuma_config::{ConfigError, Env};
use pneuma_store::{MongoHealth, NodeRunStore, PostgresHealth, RunHistoryStore, RunStore};
use tokio_util::sync::CancellationToken;

use crate::cleanup::{Janitor, Pass};
use crate::connect::{Endpoints, MONGODB_HISTORY_COLLECTION, MONGODB_RUNS_COLLECTION};
use crate::settings::{Settings, SettingsError};
use crate::terminate::{Disposition, Gateway, TerminateError, DEFAULT_GATEWAY_TIMEOUT_SECS};

/// The name every route is mounted under.
pub const SERVICE: &str = "pneuma-janitor";

/// How often a pass runs, in whole minutes.
pub const INTERVAL_MINUTES: &str = "PNEUMA_INTERVAL_MINUTES";
/// How late a pass may be before it counts as missed, in seconds.
pub const ALERT_MISS_GRACE_SECS: &str = "PNEUMA_ALERT_MISS_GRACE_SECS";
/// Where `pneuma-gateway` is.
pub const GATEWAY_URL: &str = "PNEUMA_GATEWAY_URL";
/// How long one call to the gateway may take, in seconds.
pub const GATEWAY_TIMEOUT_SECS: &str = "PNEUMA_GATEWAY_TIMEOUT_SECS";
/// Where to listen.
pub const LISTEN: &str = "PNEUMA_LISTEN";

/// The original's `*/5`, as a number of minutes.
pub const DEFAULT_INTERVAL_MINUTES: i64 = 5;
/// The original's `ALERT_MISS_GRACED_SECONDS`.
pub const DEFAULT_ALERT_MISS_GRACE_SECS: i64 = 30;
/// The original's `GATEWAY_BASE_URL`.
pub const DEFAULT_GATEWAY_URL: &str = "http://pneuma-gateway:8080";
/// Where to listen when [`LISTEN`] is unset.
pub const DEFAULT_LISTEN: &str = "0.0.0.0:9086";

/// The largest number of minutes an interval may be set to.
///
/// A year. Not a stylistic limit: `tokio::time::interval` panics on
/// `Instant::now() + period` once the result leaves the representable range,
/// and that panic is past startup — so an unbounded number here is a process
/// that binds, reports ready, and dies on its first tick. The same bound
/// `pneuma-admission` puts on its own schedule, and for the same reason.
pub const MAX_INTERVAL_MINUTES: i64 = 365 * 24 * 60;

/// The largest grace, in seconds. A year, for the same reason.
pub const MAX_GRACE_SECS: i64 = 365 * 24 * 60 * 60;

/// Whether a pass has happened recently enough.
///
/// The one check here that is not a dependency probe. `PNEUMA_INTERVAL_MINUTES`
/// says how often a pass should run and `PNEUMA_ALERT_MISS_GRACE_SECS` how late
/// one may be before that is worth saying — without this the two are knobs that
/// configure nothing an operator can observe, which is exactly the shape this
/// module's header refuses to build for `TIMEZONE`.
///
/// It degrades `liveness`, not `healthz`: the process is running and a restart
/// is not the remedy for a pass that is slow. What it does is put "the janitor
/// has stopped cleaning up" on the endpoint an operator already scrapes, rather
/// than leaving it to be noticed when the disk fills.
///
/// The clock starts at construction rather than at the epoch. A janitor that
/// has been up for ten seconds has not missed anything, and starting from
/// `None` would mean either a special case for "never ran" or a pod that
/// reports degraded for its whole first interval.
#[derive(Debug, Clone)]
pub struct PassFreshness {
    last: Arc<Mutex<DateTime<Utc>>>,
    period: Duration,
    grace: Duration,
}

impl PassFreshness {
    /// Starts the clock now.
    pub fn new(period: Duration, grace: Duration) -> Self {
        PassFreshness {
            last: Arc::new(Mutex::new(Utc::now())),
            period,
            grace,
        }
    }

    /// Records that a pass finished.
    ///
    /// When it *finished*, not when it started: measuring from the start would
    /// call a schedule healthy while every pass overran, because the starts
    /// would still be one period apart. [`pneuma_serve::missed`] documents the
    /// same choice from the other side.
    pub fn finished(&self) {
        *self.held() = Utc::now();
    }

    /// When the last pass finished.
    pub fn last(&self) -> DateTime<Utc> {
        *self.held()
    }

    /// The timestamp, poisoned or not.
    ///
    /// `unwrap_or_else(PoisonError::into_inner)` rather than a `match` with an
    /// `Err` arm, because that arm is one nothing can reach: the only thing
    /// that runs under this lock is an assignment, so there is no panic to
    /// poison it with. Recovering is still the right answer if one somehow
    /// did -- refusing to record a pass that happened would report the janitor
    /// as stalled for ever afterwards -- and this spelling says so without
    /// leaving a branch no test can take.
    fn held(&self) -> std::sync::MutexGuard<'_, DateTime<Utc>> {
        self.last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait]
impl pneuma_telemetry::HealthCheckable for PassFreshness {
    async fn ping(&self) -> Result<(), String> {
        let last = self.last();
        if !pneuma_serve::missed(last, Utc::now(), self.period, self.grace) {
            return Ok(());
        }
        Err(format!(
            "no pass has finished since {}, more than {}s + {}s ago",
            last.to_rfc3339(),
            self.period.as_secs(),
            self.grace.as_secs()
        ))
    }

    fn name(&self) -> &str {
        "passes"
    }
}

/// How many Postgres connections one janitor may hold.
///
/// Two. A pass issues its queries in sequence and the probe behind `liveness`
/// wants one of its own; nothing here fans out, and a background job holding a
/// share of the database's connection limit proportional to nothing is how a
/// cleanup job starves the services doing the work.
pub const MAX_CONNECTIONS: u32 = 2;

/// How long the pool waits for a connection before giving up.
///
/// sqlx's default is thirty seconds, which is a long time for a pod to look
/// stuck on a DSN that is simply wrong -- and unlike a request-serving service,
/// nothing here is waiting on the other end of that pause. Five seconds is long
/// enough for a database that is starting and short enough that a bad address
/// is a fast, obvious rollout failure.
pub const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

/// Everything the binary needs, decided once.
#[derive(Debug, Clone)]
pub struct Config {
    /// Where the two stores are.
    pub endpoints: Endpoints,
    /// What a pass does.
    pub settings: Settings,
    /// The gateway, already built — see [`Config::from_env`].
    pub gateway: Gateway,
    /// How often a pass runs.
    pub interval: Duration,
    /// How late a pass may be before [`pneuma_serve::missed`] calls it missed.
    pub alert_grace: Duration,
    /// Where the health surface binds.
    pub listen: SocketAddr,
}

/// Why the service will not start.
#[derive(Debug, thiserror::Error)]
pub enum ConfigureError {
    /// A variable is missing or unusable.
    #[error("{0}")]
    Variable(#[from] ConfigError),

    /// A pass setting is out of range.
    #[error("{0}")]
    Settings(#[from] SettingsError),

    /// The gateway address is unusable.
    #[error("{0}")]
    Gateway(#[from] TerminateError),

    /// The listen address is not an address.
    #[error("{LISTEN} is not a socket address: {value:?}")]
    Listen {
        /// What was given.
        value: String,
    },

    /// A schedule knob is outside the range that means anything.
    ///
    /// Zero is refused as loudly as too large. A zero interval is not "as often
    /// as possible": `tokio::time::interval` **panics** on a zero period, so
    /// the process would come up, report ready, and die at the first tick.
    #[error("{name} must be between 1 and {max}, got {value}")]
    OutOfRange {
        /// Which variable.
        name: &'static str,
        /// What was given.
        value: i64,
        /// The largest value that means anything.
        max: i64,
    },

    /// The two Mongo collections are the same one.
    ///
    /// Refused at startup, not left to the first pass. The archive is a
    /// `$merge` from the live collection into the history one, so with a single
    /// name it merges the collection into itself — every live run rewritten as
    /// its own archive — and the delete that follows then removes them. One
    /// typo in a manifest, and the pass that exists to preserve work destroys
    /// it. `RunHistoryStore::new` refuses it too; this is the same refusal
    /// moved to where it costs a rolled-back deploy instead of a pod that
    /// looked healthy for an interval.
    #[error("{MONGODB_HISTORY_COLLECTION} is {name:?}, the same collection as {MONGODB_RUNS_COLLECTION}: the archive would merge every live run over itself and the delete would then remove it")]
    SameCollection {
        /// The name both variables carry.
        name: String,
    },
}

impl Config {
    /// Reads the whole configuration, or refuses to start.
    pub fn from_env(env: &Env) -> Result<Self, ConfigureError> {
        let endpoints = Endpoints::from_env(env)?;
        if endpoints.runs_collection == endpoints.history_collection {
            return Err(ConfigureError::SameCollection {
                name: endpoints.history_collection.clone(),
            });
        }
        let listen_raw = defaulted(env, LISTEN, DEFAULT_LISTEN)?;
        let Ok(listen) = listen_raw.parse::<SocketAddr>() else {
            return Err(ConfigureError::Listen { value: listen_raw });
        };
        let gateway_timeout = seconds(
            GATEWAY_TIMEOUT_SECS,
            env.parse_or(GATEWAY_TIMEOUT_SECS, DEFAULT_GATEWAY_TIMEOUT_SECS)?,
            MAX_GRACE_SECS,
        )?;
        Ok(Config {
            endpoints,
            settings: Settings::from_env(env)?,
            // Built here rather than in `run`, so a mistyped address is a
            // configuration refusal like every other one -- exit 2, before a
            // connection pool exists -- rather than a startup failure two
            // database handshakes later.
            gateway: Gateway::new(
                &defaulted(env, GATEWAY_URL, DEFAULT_GATEWAY_URL)?,
                gateway_timeout,
            )?,
            interval: minutes(
                INTERVAL_MINUTES,
                env.parse_or(INTERVAL_MINUTES, DEFAULT_INTERVAL_MINUTES)?,
            )?,
            alert_grace: seconds(
                ALERT_MISS_GRACE_SECS,
                env.parse_or(ALERT_MISS_GRACE_SECS, DEFAULT_ALERT_MISS_GRACE_SECS)?,
                MAX_GRACE_SECS,
            )?,
            listen,
        })
    }
}

/// A variable with a default, where blank means unset.
///
/// `Env::or_default` substitutes only when a variable is *absent*, and
/// `PNEUMA_LISTEN=` in a compose file is a variable that is present and empty --
/// which is what `${JANITOR_LISTEN}` renders to when the outer one is not set.
/// Taking that literally gives a pod that never starts on a variable nobody
/// meant to set. Trimmed for the reason `connect::trimmed` gives: a value read
/// out of a rendered secret carries a trailing newline routinely.
///
/// The same helper `pneuma-driver` and `pneuma-executor` have, and the rule
/// `docs/runbook.md` states as the contract for every variable here.
fn defaulted(env: &Env, key: &str, default: &str) -> Result<String, ConfigureError> {
    let set = env
        .lookup(key)?
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    Ok(set.unwrap_or_else(|| default.to_owned()))
}

/// A number of minutes in range, as a [`Duration`].
fn minutes(name: &'static str, value: i64) -> Result<Duration, ConfigureError> {
    let value = bounded(name, value, MAX_INTERVAL_MINUTES)?;
    // The multiplication cannot overflow: `bounded` has already refused
    // everything above `MAX_INTERVAL_MINUTES`, and that times 60 is far inside
    // `u64`.
    Ok(Duration::from_secs(value.unsigned_abs() * 60))
}

/// A number of seconds in range, as a [`Duration`].
fn seconds(name: &'static str, value: i64, max: i64) -> Result<Duration, ConfigureError> {
    let value = bounded(name, value, max)?;
    // The cast cannot lose anything: `bounded` has refused everything below 1.
    Ok(Duration::from_secs(value.unsigned_abs()))
}

/// Refuses a knob outside `1..=max`.
fn bounded(name: &'static str, value: i64, max: i64) -> Result<i64, ConfigureError> {
    if (1..=max).contains(&value) {
        return Ok(value);
    }
    Err(ConfigureError::OutOfRange { name, value, max })
}

/// Why the service could not start, or could not keep running.
#[derive(Debug, thiserror::Error)]
pub enum BootError {
    /// Postgres could not be reached.
    #[error("could not reach postgres: {0}")]
    Postgres(#[from] sqlx::Error),

    /// Mongo could not be reached.
    #[error("could not reach mongo: {0}")]
    Mongo(#[from] mongodb::error::Error),

    /// The two Mongo collections could not be assembled into a janitor.
    ///
    /// Either they are the same collection, or the live one differs from the
    /// one the archive reads. Unreachable through [`run`] as it stands, because
    /// [`Config::from_env`] refuses the first and this function builds both
    /// from the same name — but `Config` is public and buildable by hand, and a
    /// `panic!` on a caller-supplied value is not a disposition this crate
    /// takes.
    #[error("{0}")]
    Collections(String),

    /// The health routes could not be built.
    #[error("{0}")]
    Router(#[from] pneuma_serve::RouterError),

    /// The listener could not be bound, or serving failed.
    #[error("could not serve on {address}: {reason}")]
    Serve {
        /// What was asked for.
        address: SocketAddr,
        /// What the operating system said.
        reason: String,
    },
}

/// Whether a pass does its work or only reports what it would do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Archive, delete, expire, and submit terminations.
    Live,
    /// Select and report, and change nothing.
    ///
    /// the original plan runs this against production for a week and diffs
    /// it against the original janitor. `Janitor::preview_pass` is what it calls,
    /// and without a flag reaching it that method had no caller anywhere in the
    /// workspace — the diff the phase gate asks for could not have been run.
    ///
    /// **Not** the same as `PNEUMA_STALE_TERMINATION_ENABLED=false`, which an
    /// earlier version of the runbook confused it with. That flag turns the
    /// stale *selection* off entirely, so a week of passes with it reports
    /// `0 stale` however much work is stuck. To preview terminations, turn the
    /// flag **on** and run with this.
    DryRun,
}

impl Mode {
    /// Reads the one flag this binary takes.
    ///
    /// Here rather than in `main`, and hand-matched rather than parsed.
    /// `pneuma-migrate` reaches for `clap` because it has subcommands and
    /// arguments; this has a single boolean, and a dependency to read it would
    /// be the larger thing. Here because `scripts/coverage.sh` excludes a
    /// binary's `main` — so a flag matched there would be the one piece of this
    /// service nothing measures, and the piece that decides whether a pass
    /// touches production.
    ///
    /// Anything else is refused rather than ignored: a typo'd `--dry_run` that
    /// quietly ran a live pass against production is the failure this flag
    /// exists to prevent.
    pub fn from_args<I: IntoIterator<Item = String>>(args: I) -> Result<Self, String> {
        let mut mode = Mode::Live;
        for argument in args {
            if argument != "--dry-run" {
                // Bound before the `return`, so the refusal is one line the
                // coverage tool attributes rather than a multi-line expression
                // it attributes to its first.
                let reason = format!("unknown argument {argument:?}; the only one is --dry-run");
                return Err(reason);
            }
            mode = Mode::DryRun;
        }
        Ok(mode)
    }
}

/// Runs the service until `token` is cancelled.
///
/// The health surface and the pass loop are peers: whichever stops first
/// cancels the other, so a bind that fails does not leave a janitor running
/// invisibly, and a cancelled token stops both.
pub async fn run(
    config: &Config,
    mode: Mode,
    token: CancellationToken,
    bound: impl FnOnce(SocketAddr),
) -> Result<(), BootError> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(MAX_CONNECTIONS)
        .acquire_timeout(ACQUIRE_TIMEOUT)
        .connect(config.endpoints.postgres_dsn())
        .await?;
    let client = MongoClient::with_uri_str(config.endpoints.mongo_uri()).await?;
    let database = client.database(&config.endpoints.mongo_database);
    let runs: Collection<Document> = database.collection(&config.endpoints.runs_collection);
    let history: Collection<Document> = database.collection(&config.endpoints.history_collection);

    let archive = RunHistoryStore::new(runs.clone(), history)
        .map_err(|error| BootError::Collections(error.to_string()))?;
    let janitor = Janitor::new(
        RunStore::new(runs),
        archive,
        NodeRunStore::new(pool.clone()),
    )
    .map_err(|error| BootError::Collections(error.to_string()))?;
    // Both stores are probed, unlike the brokers elsewhere in this workspace.
    // A janitor with no database is a janitor that does nothing at all, and
    // there is no reconnect loop underneath it to make that temporary -- so
    // "ready" has to mean "can reach both", or a pod that cannot clean up looks
    // exactly like one that has nothing to clean.
    // Bound one per line rather than built inside the call. A multi-line
    // argument list is one of the shapes `cargo-tarpaulin` attributes to its
    // first line only, so a probe added here would read as covered without ever
    // having been constructed.
    let postgres: Arc<dyn pneuma_telemetry::HealthCheckable> = Arc::new(PostgresHealth::new(pool));
    let mongo: Arc<dyn pneuma_telemetry::HealthCheckable> = Arc::new(MongoHealth::new(database));
    let freshness = PassFreshness::new(config.interval, config.alert_grace);
    let passes: Arc<dyn pneuma_telemetry::HealthCheckable> = Arc::new(freshness.clone());
    let health = pneuma_serve::router(SERVICE, vec![postgres, mongo, passes])?;
    let address = config.listen;
    let stopping = token.clone();
    let shutdown = token.clone();
    let serving = async move {
        let served = pneuma_serve::serve(health, address, bound, async move {
            shutdown.cancelled().await;
        })
        .await;
        stopping.cancel();
        served
    };

    // Half the interval, so archival and expiry -- which are bounded by the
    // batch -- always get the other half. Without a budget one pass is
    // `batch * gateway_timeout` against a gateway that black-holes
    // connections, which at the defaults is a hundred ten-second calls inside a
    // five-minute schedule: `every` cannot tick while the body is awaited, so
    // the cleanup simply stops happening. A run not attempted this pass is
    // still stale next pass, because the selection repeats.
    let budget = config.interval / 2;
    let passing = pneuma_serve::every(config.interval, token.clone(), || async {
        let report = cycle(&janitor, &config.gateway, &config.settings, mode, budget).await;
        eprintln!("{SERVICE}: {report}");
        freshness.finished();
    });

    let (served, ()) = tokio::join!(serving, passing);
    match served {
        Ok(()) => Ok(()),
        Err(error) => Err(BootError::Serve {
            address,
            reason: error.to_string(),
        }),
    }
}

/// One pass, and what to say about it.
///
/// Returns the line rather than logging it, so what a pass reports is a test
/// rather than something only a running process shows. A failure is a line and
/// not a stop: the next pass is minutes away and repeats from the top, which is
/// the whole reason every write in a pass is idempotent.
pub async fn cycle(
    janitor: &Janitor,
    gateway: &Gateway,
    settings: &Settings,
    mode: Mode,
    budget: Duration,
) -> String {
    let attempt = match mode {
        Mode::Live => janitor.pass(settings).await,
        Mode::DryRun => janitor.preview_pass(settings).await,
    };
    let pass = match attempt {
        Ok(pass) => pass,
        Err(error) => return format!("the pass failed: {error}"),
    };
    let terminated = match mode {
        Mode::Live => terminate_stale(gateway, &pass, budget).await,
        // Named rather than silent. A dry run that printed the same line as a
        // live one would be indistinguishable in the week of logs the phase-7
        // diff is built from.
        Mode::DryRun => format!("would terminate {}", pass.stale.len()),
    };
    format!("{}; {terminated}", describe(&pass))
}

/// What a pass moved, in one line.
///
/// Pure, so the numbers an operator diffs against the original janitor for a week
/// (the original plan) are decided without a database.
pub fn describe(pass: &Pass) -> String {
    let expired = match &pass.expired {
        Some(expired) => format!("{} runs and {} node runs", expired.runs, expired.node_runs),
        // Not `0`, which would say a retention pass ran and found nothing. A
        // non-positive `PNEUMA_RETENTION_DAYS` means keep for ever, and no
        // query is issued at all.
        None => "nothing (retention is off)".to_owned(),
    };
    format!(
        "archived {} runs ({} node runs), deleted {} node runs and {} run documents; \
         expired {expired}; {} stale",
        pass.cleaned.runs,
        pass.cleaned.noderuns_archived,
        pass.cleaned.noderuns_deleted,
        pass.cleaned.runs_deleted,
        pass.stale.len(),
    )
}

/// Submits every stale run for termination, and says how that went.
///
/// One line for the whole set rather than one per run: a pass that finds a
/// hundred stale runs because the gateway is down should be one alert, not a
/// hundred. Each disposition is counted; the run ids that were *refused* are
/// named, because that is the one outcome an operator has to act on.
pub async fn terminate_stale(gateway: &Gateway, pass: &Pass, budget: Duration) -> String {
    if pass.stale.is_empty() {
        return "nothing to terminate".to_owned();
    }
    let started = std::time::Instant::now();
    let (mut submitted, mut already, mut retryable) = (0_usize, 0_usize, 0_usize);
    let (mut refused, mut unreachable): (Vec<String>, Vec<String>) = (Vec::new(), Vec::new());
    let mut deferred = 0_usize;
    for run_id in &pass.stale {
        // Checked before the call, not after: the point is to stop *starting*
        // calls, and one already in flight is bounded by the client's own
        // timeout.
        if started.elapsed() >= budget {
            deferred += 1;
            continue;
        }
        match gateway.create_termination(run_id).await {
            Ok(Disposition::Submitted) => submitted += 1,
            // Counted as a success, not as a failure -- see `terminate`'s
            // module docs. The run this pass found stale is already being
            // cancelled, which is the outcome asked for.
            Ok(Disposition::AlreadyTerminating) => already += 1,
            Ok(Disposition::Retryable { .. }) => retryable += 1,
            Ok(Disposition::Refused { status }) => refused.push(format!("{run_id} ({status})")),
            Err(reason) => unreachable.push(format!("{run_id} ({reason})")),
        }
    }
    let mut line = format!(
        "terminated {submitted}, already terminating {already}, retryable {retryable}, \
         refused {}, unreachable {}, deferred {deferred}",
        refused.len(),
        unreachable.len()
    );
    if !refused.is_empty() {
        line.push_str(&format!("; refused: {}", refused.join(", ")));
    }
    if !unreachable.is_empty() {
        line.push_str(&format!("; unreachable: {}", unreachable.join(", ")));
    }
    line
}

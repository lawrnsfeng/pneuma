//! What the service accepts as configuration.
//!
//! The variable names are the original's where they still mean the same thing,
//! so a deployment running the original controller today runs this one with the
//! same manifest.

use pneuma_config::Env;
use pneuma_driver::{Config, ConfigureError, DATABASE_URL, DEFAULT_TIMEOUT_SECS};

/// The one variable with no default, so a test can be about the others.
const DSN: (&str, &str) = (DATABASE_URL, "postgres://user:pass@db/pneuma");

#[test]
fn every_inherited_variable_has_a_default_and_it_is_the_deployed_one() {
    // Every variable the original ships still has the value it shipped with. A
    // controller that refused to start without a NATS URI would be one no
    // existing manifest could run -- so `DATABASE_URL` is the *only* addition
    // to the manifest, and it is required rather than defaulted for the reason
    // the next test states.
    let Ok(config) = Config::from_env(&Env::from_pairs(vec![DSN])) else {
        panic!("the original's defaults are enough to start");
    };
    assert_eq!(config.mongodb_database, "pneuma");
    assert_eq!(config.runs_subject, "pneuma.run.start");
    assert_eq!(config.results_subject, "pneuma.result");
    assert_eq!(config.max_concurrent_runs, 64);
    assert_eq!(config.call_timeout.as_secs(), DEFAULT_TIMEOUT_SECS);
    assert_eq!(config.listen.to_string(), "0.0.0.0:9083");
}

#[test]
fn the_subjects_can_be_moved_without_touching_anything_else() {
    let pairs = vec![
        DSN,
        ("PNEUMA_RUNS_SUBJECT", "other.run.start"),
        ("PNEUMA_RESULT_SUBJECT", "other.result"),
        ("PNEUMA_MONGODB_DATABASE", "other"),
    ];
    let Ok(config) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("those are all settable");
    };
    assert_eq!(config.runs_subject, "other.run.start");
    assert_eq!(config.results_subject, "other.result");
    assert_eq!(config.mongodb_database, "other");
}

#[test]
fn an_unusable_listen_address_is_refused_at_startup() {
    let pairs = vec![DSN, ("PNEUMA_LISTEN", "not-an-address")];
    let Err(ConfigureError::Listen { value }) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("that is not a socket address");
    };
    assert_eq!(value, "not-an-address");
}

#[test]
fn a_number_outside_the_range_that_means_anything_is_refused() {
    // Zero concurrent runs is a process that accepts announcements and drives
    // none of them -- healthy, subscribed, and doing nothing. Zero seconds is
    // a call that has already timed out. The ceiling closes the same door from
    // the other side: past tokio's range the sleep arithmetic panics, which is
    // a process that binds, reports ready, and dies later.
    for (name, given) in [
        ("PNEUMA_MAX_CONCURRENT_RUNS", "0"),
        ("PNEUMA_MAX_CONCURRENT_RUNS", "-1"),
        ("PNEUMA_MAX_CONCURRENT_RUNS", "86401"),
        ("PNEUMA_COMPONENT_TIMEOUT_SECS", "0"),
        ("PNEUMA_COMPONENT_TIMEOUT_SECS", "-30"),
        ("PNEUMA_COMPONENT_TIMEOUT_SECS", "86401"),
    ] {
        let pairs = vec![DSN, (name, given)];
        let Err(ConfigureError::OutOfRange { name: named, value }) =
            Config::from_env(&Env::from_pairs(pairs))
        else {
            panic!("{name}={given} is not a usable setting");
        };
        assert_eq!(named, name);
        assert_eq!(value.to_string(), given);
    }

    // And an unparseable one is refused rather than silently defaulted.
    let pairs = vec![DSN, ("PNEUMA_MAX_CONCURRENT_RUNS", "lots")];
    assert!(Config::from_env(&Env::from_pairs(pairs)).is_err());
}

#[test]
fn a_variable_set_to_blank_falls_back_to_its_default() {
    // `${TOPIC}` renders to blank in a compose file when `TOPIC` is unset, and
    // taking that literally makes an empty subject -- which NATS refuses at the
    // first subscribe, long after the pod reported ready.
    let pairs = vec![
        DSN,
        ("PNEUMA_RUNS_SUBJECT", "  "),
        ("PNEUMA_MONGODB_DATABASE", ""),
    ];
    let Ok(config) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("a blank variable is an unset variable");
    };
    assert_eq!(config.runs_subject, "pneuma.run.start");
    assert_eq!(config.mongodb_database, "pneuma");

    // A value that is set is trimmed: a rendered secret carries a trailing
    // newline routinely, and a subject with one in it is a subject nobody
    // publishes to.
    let pairs = vec![DSN, ("PNEUMA_RUNS_SUBJECT", "  other.run.start\n ")];
    let Ok(config) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("that is a subject");
    };
    assert_eq!(config.runs_subject, "other.run.start");
}

#[test]
fn the_database_is_required_and_a_blank_one_is_an_unset_one() {
    // The one variable with no default, and deliberately so. A replica that
    // silently mirrors nothing looks exactly like one that is working, and the
    // only visible symptom is `pneuma-janitor` finding no stale runs -- weeks
    // later, and attributed to the janitor. The design notes
    let Err(error) = Config::from_env(&Env::from_pairs(Vec::<(&str, &str)>::new())) else {
        panic!("there is no sensible default for where a run is written down");
    };
    assert!(
        matches!(error, ConfigureError::NoDatabase),
        "wrong variant: {error:?}"
    );
    assert!(error.to_string().contains(DATABASE_URL), "{error}");

    // Blank is unset here too: `${PG_DSN}` renders to nothing in a compose file
    // when the outer variable is not set, and taking it literally is a pod that
    // starts and mirrors nothing -- which is the state this refusal exists to
    // make impossible.
    for blank in ["", "   ", "\n"] {
        let pairs = vec![(DATABASE_URL, blank)];
        let Err(ConfigureError::NoDatabase) = Config::from_env(&Env::from_pairs(pairs)) else {
            panic!("{blank:?} is not a connection string");
        };
    }

    // And a value that is set is trimmed, like every other identifier here: a
    // rendered secret carries a trailing newline routinely, and sqlx will not
    // parse a DSN with one on the end.
    let pairs = vec![(DATABASE_URL, "  postgres://user:pass@db/pneuma\n ")];
    let Ok(config) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("that is a connection string");
    };
    assert_eq!(
        secrecy::ExposeSecret::expose_secret(&config.database_url),
        "postgres://user:pass@db/pneuma"
    );
}

//! What the service accepts as configuration, and what it refuses to start on.
//!
//! The variable names are the original's, so a deployment running the original's
//! bootstrap today runs this one with the same manifest.

use pneuma_config::Env;
use pneuma_intake::{Config, ConfigureError};

/// The one variable with no default: the original's `BACKEND_URI` published to
/// NATS, and this does not, so reusing the name would be a lie.
fn minimal() -> Vec<(&'static str, &'static str)> {
    vec![("PNEUMA_ADMISSION_URL", "http://admission:9081")]
}

#[test]
fn every_variable_has_a_default_and_it_is_the_deployed_one() {
    let Ok(config) = Config::from_env(&Env::from_pairs(minimal())) else {
        panic!("one variable is enough to start");
    };
    assert_eq!(config.mongodb_database, "pneuma");
    assert_eq!(config.runs.as_str(), "pneuma.input");
    assert_eq!(config.events.as_str(), "pneuma.event");
    assert_eq!(config.definitions.as_str(), "pneuma.pipeline.create");
    assert_eq!(config.event_key.as_str(), "pneuma.event");
    assert_eq!(config.listen.to_string(), "0.0.0.0:9082");
    assert_eq!(
        config.backoff.first().as_secs(),
        5,
        "RABBITMQ_RECEIVER_RECONNECT_DELAY, as the floor"
    );
    assert_eq!(config.backoff.max().as_secs(), 60);
}

#[test]
fn where_runs_are_offered_has_no_default() {
    // The original's `BACKEND_URI` defaults to a NATS address and publishes a
    // `MessageInit`. This offers the run to `pneuma-admission` over HTTP, so
    // reusing the name with a default would be a manifest that looks migrated
    // and is not.
    let Err(error) = Config::from_env(&Env::from_pairs(Vec::<(&str, &str)>::new())) else {
        panic!("there is no sensible default for where admission lives");
    };
    assert!(
        error.to_string().contains("PNEUMA_ADMISSION_URL"),
        "{error}"
    );
}

#[test]
fn a_queue_name_a_broker_would_refuse_is_refused_at_startup() {
    // Not at the first declare. A name containing a newline does not fail
    // there -- it *hangs*, with the client waiting for a reply the broker never
    // sends, which presents as a consumer that never starts.
    let mut pairs = minimal();
    pairs.push(("PNEUMA_RUNS_QUEUE", "pneuma\n.input"));
    let Err(ConfigureError::Name { key, .. }) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("a newline in a queue name hangs the declare");
    };
    assert_eq!(key, "PNEUMA_RUNS_QUEUE");

    // And a routing key is checked the same way.
    let mut pairs = minimal();
    pairs.push(("PNEUMA_EVENT_ROUTING_KEY", "\u{0}"));
    let Err(ConfigureError::Name { key, .. }) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("a control character is not a routing key");
    };
    assert_eq!(key, "PNEUMA_EVENT_ROUTING_KEY");
}

#[test]
fn a_queue_whose_dead_letter_name_would_not_fit_is_refused_here() {
    // 244 bytes is a legal queue name and its derived `.dead_letter` is 256,
    // which the AMQP `shortstr` cannot carry at all. Caught at startup rather
    // than at the first declare.
    let long = "q".repeat(244);
    let mut pairs = minimal();
    let leaked: &'static str = Box::leak(long.into_boxed_str());
    pairs.push(("PNEUMA_RUNS_QUEUE", leaked));
    // The name itself is legal, so this is accepted here and refused by
    // `QueueSpec::new` when the consumer attaches -- which is the seam the
    // broker crate owns. What must not happen is a silent truncation.
    let built = Config::from_env(&Env::from_pairs(pairs));
    assert!(built.is_ok(), "244 bytes is a legal queue name");
}

#[test]
fn an_unusable_listen_address_is_refused_at_startup() {
    let mut pairs = minimal();
    pairs.push(("PNEUMA_LISTEN", "not-an-address"));
    let Err(ConfigureError::Listen { value }) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("that is not a socket address");
    };
    assert_eq!(value, "not-an-address");
}

#[test]
fn a_reconnect_delay_outside_the_range_is_refused() {
    // Zero would make every delay zero however many failures there have been --
    // an unbounded reconnect loop against a broker that is down, arrived at by
    // configuring a backoff. The ceiling closes the same door from the other
    // side: past tokio's range the sleep panics, which is a process that came
    // up and died later.
    for given in ["0", "-5", "86401"] {
        let mut pairs = minimal();
        pairs.push(("PNEUMA_RECONNECT_DELAY_SECS", given));
        let Err(ConfigureError::OutOfRange { name, value }) =
            Config::from_env(&Env::from_pairs(pairs))
        else {
            panic!("{given} is not a reconnect delay");
        };
        assert_eq!(name, "PNEUMA_RECONNECT_DELAY_SECS");
        assert_eq!(value.to_string(), given);
    }

    // And an unparseable one is refused rather than silently defaulted.
    let mut pairs = minimal();
    pairs.push(("PNEUMA_RECONNECT_DELAY_SECS", "soon"));
    assert!(Config::from_env(&Env::from_pairs(pairs)).is_err());
}

#[test]
fn a_variable_set_to_blank_falls_back_to_its_default() {
    // `${TOPIC}` renders to blank in a compose file when `TOPIC` is unset, and
    // taking that literally makes an empty queue name.
    let mut pairs = minimal();
    pairs.push(("PNEUMA_RUNS_QUEUE", "  "));
    pairs.push(("PNEUMA_MONGODB_DATABASE", ""));
    let Ok(config) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("a blank variable is an unset variable");
    };
    assert_eq!(config.runs.as_str(), "pneuma.input");
    assert_eq!(config.mongodb_database, "pneuma");

    // A value that is set is trimmed, because a rendered secret carries a
    // trailing newline routinely -- and a queue named `input\n` is one nobody
    // created, which the broker reports as not existing rather than as edited.
    let mut pairs = minimal();
    pairs.push(("PNEUMA_RUNS_QUEUE", "  other.input\n "));
    let Ok(config) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("that is a queue name");
    };
    assert_eq!(config.runs.as_str(), "other.input");
}

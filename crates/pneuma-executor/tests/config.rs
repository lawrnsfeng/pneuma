//! What the service accepts as configuration.
//!
//! The variable names are the original's, so a deployment running it today
//! runs this with the same manifest.

use pneuma_config::Env;
use pneuma_executor::{Config, ConfigureError};

/// The one variable with no default: where the component lives.
fn minimal() -> Vec<(&'static str, &'static str)> {
    vec![("PNEUMA_COMPONENT_ENDPOINT", "http://component:9000")]
}

#[test]
fn every_variable_but_one_has_a_default() {
    let Ok(config) = Config::from_env(&Env::from_pairs(minimal())) else {
        panic!("one variable is enough to start");
    };
    assert_eq!(config.work.as_str(), "pneuma.step");
    assert_eq!(config.results.as_str(), "pneuma.result");
    assert_eq!(config.events.as_str(), "pneuma.event");
    assert_eq!(config.request_timeout.as_secs(), 300);
    assert_eq!(config.attempts, 3);
    assert_eq!(config.senders, 8);
    assert_eq!(config.listen.to_string(), "0.0.0.0:9085");
}

#[test]
fn where_the_component_lives_has_no_default() {
    // Every other variable has a default. This one does not, and defaulting it
    // would be a manifest that looks migrated and calls nothing.
    let Err(error) = Config::from_env(&Env::from_pairs(Vec::<(&str, &str)>::new())) else {
        panic!("there is no sensible default for where a component lives");
    };
    assert!(
        error.to_string().contains("PNEUMA_COMPONENT_ENDPOINT"),
        "{error}"
    );
}

#[test]
fn the_queue_group_is_derived_from_the_work_subject() {
    // Derived rather than configured, and derived the way the original services
    // derive theirs. Two replicas reading one subject must be in the same group
    // or every message is handled twice -- making it a function of the subject
    // removes the way that goes wrong.
    let mut pairs = minimal();
    pairs.push(("PNEUMA_WORK_SUBJECT", "pneuma.input.tenant_acme"));
    let Ok(config) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("that is a subject");
    };
    assert_eq!(config.work.as_str(), "pneuma.input.tenant_acme");
    assert_eq!(config.queue.as_str(), "pneuma-input-tenant_acme");
}

#[test]
fn a_subject_nats_would_refuse_is_refused_at_startup() {
    for (name, bad) in [
        ("PNEUMA_WORK_SUBJECT", "pneuma..input"),
        ("PNEUMA_RESULT_SUBJECT", "legacy result"),
        ("PNEUMA_EVENT_SUBJECT", "pneuma.event."),
    ] {
        let mut pairs = minimal();
        pairs.push((name, bad));
        let Err(ConfigureError::Subject { key, value, .. }) =
            Config::from_env(&Env::from_pairs(pairs))
        else {
            panic!("{name}={bad} is not a subject");
        };
        assert_eq!(key, name);
        assert_eq!(value, bad);
    }
}

#[test]
fn a_number_outside_the_range_that_means_anything_is_refused() {
    // Zero senders is a process that subscribes and handles nothing. Zero
    // attempts is a call never made. Zero seconds is a call that has already
    // timed out. The ceiling closes the same door from the other side.
    for (name, given) in [
        ("PNEUMA_MAX_CONCURRENT_STEPS", "0"),
        ("PNEUMA_MAX_CONCURRENT_STEPS", "-4"),
        ("PNEUMA_COMPONENT_ATTEMPTS", "0"),
        ("PNEUMA_COMPONENT_TIMEOUT_SECS", "0"),
        ("PNEUMA_COMPONENT_TIMEOUT_SECS", "86401"),
    ] {
        let mut pairs = minimal();
        pairs.push((name, given));
        let Err(ConfigureError::OutOfRange { name: named, value }) =
            Config::from_env(&Env::from_pairs(pairs))
        else {
            panic!("{name}={given} is not a usable setting");
        };
        assert_eq!(named, name);
        assert_eq!(value.to_string(), given);
    }

    // And an unparseable one is refused rather than silently defaulted.
    let mut pairs = minimal();
    pairs.push(("PNEUMA_COMPONENT_ATTEMPTS", "thrice"));
    assert!(Config::from_env(&Env::from_pairs(pairs)).is_err());
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
fn a_variable_set_to_blank_falls_back_to_its_default() {
    let mut pairs = minimal();
    pairs.push(("PNEUMA_RESULT_SUBJECT", "  "));
    let Ok(config) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("a blank variable is an unset variable");
    };
    assert_eq!(config.results.as_str(), "pneuma.result");

    // And a value that is set is trimmed: a subject with a trailing newline is
    // one nobody publishes to.
    let mut pairs = minimal();
    pairs.push(("PNEUMA_RESULT_SUBJECT", "  other.result\n "));
    let Ok(config) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("that is a subject");
    };
    assert_eq!(config.results.as_str(), "other.result");
}

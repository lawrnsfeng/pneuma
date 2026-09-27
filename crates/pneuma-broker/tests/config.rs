//! What the service accepts as configuration.
//!
//! The variable names are the original's, so a deployment running it today
//! runs this with the same manifest — which is what makes this one of the two
//! services the plan calls independently deployable against the live system.

use pneuma_broker::{queue_group, subjects, Config, ConfigureError};
use pneuma_config::Env;

#[test]
fn an_empty_environment_is_enough_to_start() {
    let Ok(config) = Config::from_env(&Env::from_pairs(Vec::<(&str, &str)>::new())) else {
        panic!("the defaults are enough to start");
    };
    assert_eq!(config.inputs.len(), 1);
    assert_eq!(config.inputs[0].as_str(), "pneuma.input");
    assert_eq!(config.queue.as_str(), "pneuma-broker");
    assert_eq!(config.listen.to_string(), "0.0.0.0:9084");
}

#[test]
fn several_inputs_are_read_from_one_variable() {
    let Ok(parsed) = subjects("pneuma.input, pneuma.event ,other.thing") else {
        panic!("that is three subjects");
    };
    let names: Vec<&str> = parsed.iter().map(pneuma_nats::Subject::as_str).collect();
    assert_eq!(names, ["pneuma.input", "pneuma.event", "other.thing"]);

    // A trailing comma is a typo, not a subject.
    let Ok(parsed) = subjects("pneuma.input,") else {
        panic!("a trailing comma is skipped");
    };
    assert_eq!(parsed.len(), 1);
}

#[test]
fn a_broker_with_nothing_to_demultiplex_is_refused() {
    // It would subscribe to nothing and report healthy for ever, which is the
    // quietest way for a tenant's messages to stop arriving.
    for empty in ["", "   ", ",", " , "] {
        let Err(ConfigureError::NoInputs) = subjects(empty) else {
            panic!("{empty:?} is not a list of subjects");
        };
    }
}

#[test]
fn a_subject_nats_would_refuse_is_refused_at_startup() {
    // Not at the first subscribe, which happens after the process has already
    // reported ready -- so the consumer never attaches and the pod looks fine.
    for bad in ["pneuma..input", "pneuma input", "pneuma.input."] {
        let Err(ConfigureError::Subject { entry, .. }) = subjects(bad) else {
            panic!("{bad:?} is not a subject");
        };
        assert_eq!(entry, bad);
    }
}

#[test]
fn an_unusable_listen_address_is_refused_at_startup() {
    let pairs = vec![("PNEUMA_LISTEN", "not-an-address")];
    let Err(ConfigureError::Listen { value }) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("that is not a socket address");
    };
    assert_eq!(value, "not-an-address");
}

#[test]
fn a_variable_set_to_blank_falls_back_to_its_default() {
    let pairs = vec![("PNEUMA_INPUT_SUBJECTS", "  "), ("PNEUMA_QUEUE_GROUP", "")];
    let Ok(config) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("a blank variable is an unset variable");
    };
    assert_eq!(config.inputs[0].as_str(), "pneuma.input");
    assert_eq!(config.queue.as_str(), "pneuma-broker");

    // And a value that is set is trimmed.
    let pairs = vec![("PNEUMA_QUEUE_GROUP", "  other\n ")];
    let Ok(config) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("that is a queue name");
    };
    assert_eq!(config.queue.as_str(), "other");
}

#[test]
fn a_queue_group_nats_would_refuse_is_refused_at_startup() {
    // The same class as the tenant id, and the same reason: NATS applies the
    // subject-token grammar to a queue group, and the refusal arrives at the
    // *subscribe* -- after the process has reported ready, with a consumer that
    // never attaches and a pod that looks fine.
    for bad in ["bad name", "with.dot", "*", ">"] {
        let Err(ConfigureError::Queue { value, .. }) = queue_group(bad) else {
            panic!("{bad:?} is not a queue group");
        };
        assert_eq!(value, bad);
    }
    // The original's own default is one NATS accepts, hyphens and all.
    assert!(queue_group("pneuma-broker").is_ok());
}

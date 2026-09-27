//! What the service accepts as configuration, and what it refuses to start on.
//!
//! `Env` is injected, so none of this touches the process environment and two
//! of these can run at once holding different configurations.

use pneuma_admission::{
    weights, Config, ConfigureError, BATCH_SIZE, DEFAULT_HANDLER, DEFAULT_LISTEN,
    DISPATCH_INTERVAL_SECS, MAX_COUNT, MAX_SECONDS, PER_TENANT, RECLAIM_AFTER_SECS,
    RESTATE_HANDLER,
};
use pneuma_config::Env;
use pneuma_fairness::Weight;

/// The two variables with no default, so a config is buildable at all.
fn minimal() -> Vec<(&'static str, &'static str)> {
    vec![
        ("DATABASE_URL", "postgres://u:p@db/app"),
        ("PNEUMA_RESTATE_INGRESS", "http://restate:8080"),
    ]
}

#[test]
fn a_minimal_environment_gets_working_defaults() {
    let Ok(config) = Config::from_env(&Env::from_pairs(minimal())) else {
        panic!("two variables is enough to start");
    };
    assert_eq!(config.listen.to_string(), DEFAULT_LISTEN);
    assert_eq!(
        config.handler, DEFAULT_HANDLER,
        "what pneuma-restate serves"
    );
    assert_eq!(config.batch_size, 50);
    assert_eq!(config.per_tenant, 500);
    assert_eq!(config.interval.as_secs(), 2);
    assert_eq!(config.reclaim_after.num_seconds(), 300);
    assert!(
        config.weights.is_empty(),
        "nobody prioritised is the usual case"
    );
}

#[test]
fn the_two_variables_with_no_default_are_required() {
    for missing in ["DATABASE_URL", "PNEUMA_RESTATE_INGRESS"] {
        let pairs: Vec<(&str, &str)> = minimal()
            .into_iter()
            .filter(|(k, _)| *k != missing)
            .collect();
        let Err(error) = Config::from_env(&Env::from_pairs(pairs)) else {
            panic!("{missing} has no sensible default");
        };
        assert!(error.to_string().contains(missing), "{error}");
    }
    // And blank counts as missing: an empty variable in a compose file is an
    // operator who meant to set it and did not.
    let blank = vec![
        ("DATABASE_URL", ""),
        ("PNEUMA_RESTATE_INGRESS", "http://restate:8080"),
    ];
    assert!(Config::from_env(&Env::from_pairs(blank)).is_err());
}

#[test]
fn an_unusable_listen_address_is_refused_at_startup() {
    // Not at the first request. A service that binds nowhere should say so
    // while someone is still watching the deploy.
    for bad in ["not-an-address", "0.0.0.0", ":::", "9081"] {
        let mut pairs = minimal();
        pairs.push(("PNEUMA_LISTEN", bad));
        let Err(ConfigureError::Listen { value }) = Config::from_env(&Env::from_pairs(pairs))
        else {
            panic!("{bad:?} is not a socket address");
        };
        assert_eq!(value, bad);
    }
}

#[test]
fn weights_are_read_from_pairs() {
    let Ok(parsed) = weights("acme=5, globex = 2 ,quiet=1") else {
        panic!("that is three pairs");
    };
    assert_eq!(parsed.len(), 3);
    assert_eq!(parsed.get("acme").copied().map(Weight::get), Some(5));
    assert_eq!(
        parsed.get("globex").copied().map(Weight::get),
        Some(2),
        "spaces are trimmed"
    );
    assert_eq!(parsed.get("quiet").copied().map(Weight::get), Some(1));

    // Empty is an empty map, not an error -- and so is a trailing comma.
    for empty in ["", "   ", ",", "acme=1,"] {
        assert!(weights(empty).is_ok(), "{empty:?}");
    }
}

#[test]
fn a_mistyped_weight_stops_the_deploy_rather_than_falling_back() {
    // The failure this refusal exists for is quiet. A weight that silently fell
    // back to `Weight::ONE` would give a *smaller* share than intended to
    // anyone configured above one -- so the service would run, look healthy,
    // and under-serve the customer somebody went to the trouble of
    // prioritising.
    for (entry, fragment) in [
        ("acme", "no `=`"),
        ("=5", "blank"),
        ("acme=lots", "invalid digit"),
        ("acme=-1", "invalid digit"),
        ("acme=0", "no share at all"),
    ] {
        let Err(ConfigureError::Weights { entry: named, why }) = weights(entry) else {
            panic!("{entry:?} is not a weight");
        };
        assert_eq!(named, entry);
        assert!(why.contains(fragment), "{entry:?} said {why:?}");
    }
}

#[test]
fn a_bad_weight_refuses_the_whole_config_not_just_the_entry() {
    let mut pairs = minimal();
    pairs.push(("PNEUMA_TENANT_WEIGHTS", "acme=5,globex=0"));
    let Err(ConfigureError::Weights { .. }) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("one bad weight is a bad configuration");
    };
}

#[test]
fn a_knob_that_is_not_positive_is_refused_at_startup() {
    // Three of the four would be a service that does nothing. The fourth is
    // worse: `tokio::time::interval` panics on a zero period, so a mistyped
    // dispatch interval would take the process down at the first tick -- after
    // it had reported ready and started accepting submissions it will never
    // dispatch.
    for (name, given) in [
        (BATCH_SIZE, "0"),
        (PER_TENANT, "0"),
        (DISPATCH_INTERVAL_SECS, "0"),
        (RECLAIM_AFTER_SECS, "0"),
        (BATCH_SIZE, "-1"),
        (PER_TENANT, "-500"),
        (DISPATCH_INTERVAL_SECS, "-2"),
        // A negative sweep cutoff is the quietest of the lot: `now - (-300s)`
        // is a cutoff in the future, so every claim in the table is older than
        // it and the sweep returns work that was claimed a moment ago -- runs
        // submitted twice, on a timer.
        (RECLAIM_AFTER_SECS, "-300"),
    ] {
        let mut pairs = minimal();
        pairs.push((name, given));
        let Err(ConfigureError::OutOfRange {
            name: named, value, ..
        }) = Config::from_env(&Env::from_pairs(pairs))
        else {
            panic!("{name}={given} is not a usable setting");
        };
        assert_eq!(named, name);
        assert_eq!(value.to_string(), given);
    }
}

#[test]
fn a_knob_too_large_to_act_on_is_refused_at_startup_too() {
    // The ceiling closes the same door the zero check closes, from the other
    // side. `Utc::now() + delta` panics once the result leaves chrono's range,
    // and `tokio::time::interval` panics the same way -- both *past* startup,
    // so an unbounded value here is a pod that binds, reports ready, and dies
    // on its first tick. A pasted microsecond epoch is how it happens.
    for (name, given, max) in [
        (RECLAIM_AFTER_SECS, "1700000000000000", MAX_SECONDS),
        (RECLAIM_AFTER_SECS, "9223372036854775807", MAX_SECONDS),
        (DISPATCH_INTERVAL_SECS, "1700000000000000", MAX_SECONDS),
        (BATCH_SIZE, "1000001", MAX_COUNT),
        (PER_TENANT, "9223372036854775807", MAX_COUNT),
    ] {
        let mut pairs = minimal();
        pairs.push((name, given));
        let Err(ConfigureError::OutOfRange {
            name: named,
            value,
            max: ceiling,
        }) = Config::from_env(&Env::from_pairs(pairs))
        else {
            panic!("{name}={given} is past what the process can act on");
        };
        assert_eq!(named, name);
        assert_eq!(value.to_string(), given);
        assert_eq!(ceiling, max);
    }

    // And the ceiling itself is accepted, so the bound is a bound and not an
    // off-by-one that quietly refuses a legal setting.
    let mut pairs = minimal();
    pairs.push((RECLAIM_AFTER_SECS, "31536000"));
    let Ok(config) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("a year is a year");
    };
    assert_eq!(config.reclaim_after.num_seconds(), MAX_SECONDS);
}

#[test]
fn a_variable_set_to_blank_falls_back_to_its_default() {
    // `PNEUMA_RESTATE_HANDLER=` is what `${HANDLER}` renders to in a compose
    // file when `HANDLER` is not set, and taking it literally gave a handler of
    // `""`, a submission URL of `http://restate:8080//send`, a 404, and every
    // submission settled `failed` permanently -- the worst answer available,
    // because it is silent and irreversible.
    for (blank, expected_handler, expected_listen) in [
        (RESTATE_HANDLER, DEFAULT_HANDLER, DEFAULT_LISTEN),
        ("PNEUMA_LISTEN", DEFAULT_HANDLER, DEFAULT_LISTEN),
    ] {
        let mut pairs = minimal();
        pairs.push((blank, "   "));
        let Ok(config) = Config::from_env(&Env::from_pairs(pairs)) else {
            panic!("a blank {blank} is an unset {blank}");
        };
        assert_eq!(config.handler, expected_handler);
        assert_eq!(config.listen.to_string(), expected_listen);
    }

    // And a value that is set is trimmed, because a rendered secret carries a
    // trailing newline routinely and no handler path ever means to have one.
    let mut pairs = minimal();
    pairs.push((RESTATE_HANDLER, "  Other/run\n  "));
    let Ok(config) = Config::from_env(&Env::from_pairs(pairs)) else {
        panic!("that is a handler");
    };
    assert_eq!(config.handler, "Other/run");
}

#[test]
fn a_tuning_knob_that_is_set_but_unparseable_is_refused() {
    // Not silently defaulted: a typo in a batch size should fail at startup
    // rather than quietly running at 50 while someone believes it is 500.
    let mut pairs = minimal();
    pairs.push(("PNEUMA_BATCH_SIZE", "lots"));
    assert!(Config::from_env(&Env::from_pairs(pairs)).is_err());
}

//! The whole process, against a real NATS.
//!
//! What a fake cannot answer: whether the subject this service publishes to is
//! one a subscriber on the tenant's subject actually receives. That is the only
//! thing the service does, and it is a property of the broker's subject
//! grammar rather than of this crate.
//!
//! ```sh
//! PNEUMA_TEST_NATS_URL=nats://127.0.0.1:4223 \
//!     cargo test -p pneuma-broker --test service
//! ```

use std::net::SocketAddr;
use std::time::Duration;

use futures_lite::StreamExt;
use pneuma_broker::{boot, describe, forward, BootError, Config, RouteError};
use pneuma_nats::Subject;
use secrecy::SecretString;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

fn url() -> String {
    let Ok(url) = std::env::var("PNEUMA_TEST_NATS_URL") else {
        panic!("PNEUMA_TEST_NATS_URL is not set; see the header of this file");
    };
    url
}

fn subject(value: &str) -> Subject {
    match Subject::parse(value) {
        Ok(subject) => subject,
        Err(error) => panic!("{value} is a subject: {error}"),
    }
}

fn config(input: &str) -> Config {
    let Some(listen) = "127.0.0.1:0".parse::<SocketAddr>().ok() else {
        panic!("a real address");
    };
    Config {
        nats_uri: SecretString::from(url()),
        inputs: vec![subject(input)],
        queue: match pneuma_nats::SubjectToken::new("pneuma-test-broker") {
            Ok(queue) => queue,
            Err(error) => panic!("that is a queue group: {error}"),
        },
        listen,
    }
}

fn message(tenant: &str) -> Vec<u8> {
    let body = json!({
        "meta": {
            "job_id": "job-1", "tenant_id": tenant,
            "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
            "a_caller_extra": "kept",
        },
        "step_input": {"doc": "d"},
    });
    match serde_json::to_vec(&body) {
        Ok(bytes) => bytes,
        Err(error) => panic!("that serialises: {error}"),
    }
}

#[tokio::test]
async fn a_message_arrives_on_its_tenants_subject_unchanged() {
    let input = format!("pneuma.test.tb{}.input", std::process::id());
    let Ok(client) = async_nats::connect(&url()).await else {
        panic!("could not reach NATS");
    };
    let Ok(mut tenant_side) = client.subscribe(format!("{input}.tenant_acme")).await else {
        panic!("could not subscribe to the tenant's subject");
    };

    let token = CancellationToken::new();
    let (tx, rx) = oneshot::channel();
    let config = config(&input);
    let checks = tokio::spawn({
        let token = token.clone();
        let client = client.clone();
        let input = input.clone();
        async move {
            let Ok(address) = rx.await else {
                panic!("the service should bind and say where");
            };
            let http = reqwest::Client::new();
            for path in ["healthz", "liveness"] {
                let url = format!("http://{address}/pneuma-broker/{path}");
                let Ok(response) = http.get(&url).send().await else {
                    panic!("{url} should answer");
                };
                assert_eq!(response.status().as_u16(), 200, "{url}");
            }

            // Published in a loop until the broker has attached: a core-NATS
            // publish to a subject with no subscriber is discarded silently,
            // so sending once could race the subscription.
            let body = message("acme");
            for _ in 0..200_u32 {
                let sent = client.publish(input.clone(), body.clone().into()).await;
                if sent.is_ok() && client.flush().await.is_ok() {
                    if let Ok(Some(received)) =
                        tokio::time::timeout(Duration::from_millis(20), tenant_side.next()).await
                    {
                        // Republished byte for byte. A broker that re-encoded
                        // could silently drop a field it does not model, and
                        // everything downstream reads fields this service has
                        // no opinion about.
                        assert_eq!(received.payload, body, "unchanged");
                        assert_eq!(received.subject.as_str(), format!("{input}.tenant_acme"));
                        token.cancel();
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("the message never reached the tenant's subject");
        }
    });

    let served = boot::run(&config, token.clone(), |address| {
        let _ = tx.send(address);
    })
    .await;
    if let Err(error) = served {
        panic!("a cancelled service exits cleanly, not with {error}");
    }
    if let Err(error) = checks.await {
        panic!("the assertions panicked: {error}");
    }
}

#[tokio::test]
async fn a_message_that_cannot_be_routed_is_dropped_with_a_reason() {
    // Every routing failure is permanent, so the message is dropped rather than
    // retried -- and what a person gets is the reason, which is why `describe`
    // is separate from the loop that calls it.
    let Ok(client) = async_nats::connect(&url()).await else {
        panic!("could not reach NATS");
    };
    let input = subject("pneuma.test.tb.dropped.input");
    let said = forward(&client, &input, b"not a message").await;
    assert!(said.starts_with("dropped:"), "{said}");

    // And each reason reads differently, so a log tells one from another.
    let reasons = [
        describe(&RouteError::NoJob),
        describe(&RouteError::NoTenant),
        describe(&RouteError::NotAMessage("trailing comma".to_owned())),
    ];
    let mut unique = reasons.to_vec();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 3, "three distinguishable lines: {reasons:?}");
}

#[tokio::test]
async fn a_publish_the_broker_will_not_admit_is_reported() {
    // `publish` writes into a local buffer and returns `Ok` even when the
    // broker has gone -- measured on the controller's side, and true here for
    // the same client. Without the flush a message would be "routed" into
    // nothing and the log would say so.
    let Ok(client) = async_nats::ConnectOptions::new()
        .retry_on_initial_connect()
        .connect("nats://127.0.0.1:1")
        .await
    else {
        panic!("connecting with retry should not fail up front");
    };
    let input = subject("pneuma.test.tb.gone.input");
    let said = tokio::time::timeout(
        pneuma_broker::PUBLISH_TIMEOUT * 2,
        forward(&client, &input, &message("acme")),
    )
    .await;
    match said {
        Ok(line) => assert!(line.contains("did not admit"), "{line}"),
        Err(_) => panic!("a publish to a broker that is not there must not hang for ever"),
    }
}

#[tokio::test]
async fn an_address_already_in_use_stops_the_whole_process() {
    let mut config = config("pneuma.test.tb.bind.input");
    let Ok(occupied) = TcpListener::bind("127.0.0.1:0").await else {
        panic!("a loopback port");
    };
    let Ok(taken) = occupied.local_addr() else {
        panic!("a bound listener has an address");
    };
    config.listen = taken;

    let Err(error) = boot::run(&config, CancellationToken::new(), |_| {
        panic!("nothing should bind");
    })
    .await
    else {
        panic!("{taken} is already listening");
    };
    let BootError::Serve { address, .. } = &error else {
        panic!("wrong variant: {error:?}");
    };
    assert_eq!(*address, taken);
}

#[tokio::test]
async fn a_broker_that_is_not_there_is_a_startup_failure() {
    let mut config = config("pneuma.test.tb.nobroker.input");
    config.nats_uri = SecretString::from("nats://127.0.0.1:1".to_owned());
    let Err(error) = boot::run(&config, CancellationToken::new(), |_| {
        panic!("nothing should bind");
    })
    .await
    else {
        panic!("nothing is listening on port 1");
    };
    assert!(
        matches!(error, BootError::Nats(_)),
        "wrong variant: {error:?}"
    );
}

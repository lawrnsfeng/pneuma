//! The whole process, against a real NATS and a stub component — **and the
//! broker beside it**.
//!
//! The plan asks for these two to be tested as a pair, and the reason is
//! precise: each alone is only ever proved against a fake, and the thing worth
//! knowing is that the subject one *writes* is the subject the other *reads*.
//! A test that stubbed either end would agree with whatever this file asserted
//! about the subject grammar, which is the one thing neither service owns.
//!
//! ```sh
//! PNEUMA_TEST_NATS_URL=nats://127.0.0.1:4223 \
//!     cargo test -p pneuma-executor --test service
//! ```

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::routing::post;
use axum::Router;
use futures_lite::StreamExt;
use pneuma_executor::{boot, classify, handle, BootError, Component, Config, Transport};
use pneuma_nats::{Subject, SubjectToken};
use pneuma_proto::dispatch::MessageResult;
use pneuma_proto::event::MessageEvent;
use secrecy::SecretString;
use serde_json::{json, Value};
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

fn token(value: &str) -> SubjectToken {
    match SubjectToken::new(value) {
        Ok(token) => token,
        Err(error) => panic!("{value} is a token: {error}"),
    }
}

async fn connected() -> async_nats::Client {
    match async_nats::connect(&url()).await {
        Ok(client) => client,
        Err(error) => panic!("could not reach NATS at {}: {error}", url()),
    }
}

/// A component that answers every prediction with a fixed output.
async fn stub_component(seen: Arc<Mutex<Vec<Value>>>, status: u16) -> SocketAddr {
    let router = Router::new().route(
        "/predict",
        post(move |axum::Json(body): axum::Json<Value>| {
            let seen = Arc::clone(&seen);
            async move {
                match seen.lock() {
                    Ok(mut seen) => seen.push(body),
                    Err(poisoned) => poisoned.into_inner().push(body),
                }
                let code =
                    axum::http::StatusCode::from_u16(status).unwrap_or(axum::http::StatusCode::OK);
                (code, axum::Json(json!({"step_output": {"pages": 3}})))
            }
        }),
    );
    let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
        panic!("a loopback port");
    };
    let Ok(address) = listener.local_addr() else {
        panic!("a bound listener has an address");
    };
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    address
}

/// A component request, for the tests that call the client directly.
fn a_request() -> pneuma_proto::component::ComponentRequest {
    let Ok(run) = serde_json::from_slice::<pneuma_proto::dispatch::MessageRun>(&run_message("A"))
    else {
        panic!("the fixture is a run message");
    };
    pneuma_executor::boot::request_for(&run)
}

fn run_message(node: &str) -> Vec<u8> {
    let body = json!({
        "meta": {
            "job_id": "job-1", "tenant_id": "acme",
            "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
        },
        "node": {
            "path": "job-1.invoice.page.default.A",
            "node_id": node,
            "name": "invoice.page.default.A",
            "kind": "Model",
            "pipeline_id": "invoice.page.default",
            "run_id": "job-1",
        },
        "step_input": {"doc": "d"},
    });
    match serde_json::to_vec(&body) {
        Ok(bytes) => bytes,
        Err(error) => panic!("that serialises: {error}"),
    }
}

fn config(prefix: &str, component: SocketAddr) -> Config {
    let Some(listen) = "127.0.0.1:0".parse::<SocketAddr>().ok() else {
        panic!("a real address");
    };
    Config {
        nats_uri: SecretString::from(url()),
        work: subject(&format!("{prefix}.work")),
        queue: format!("{prefix}.work").replace('.', "-"),
        results: subject(&format!("{prefix}.result")),
        events: subject(&format!("{prefix}.event")),
        component: format!("http://{component}"),
        request_timeout: Duration::from_secs(5),
        attempts: 3,
        senders: 4,
        listen,
    }
}

/// Polls until `check` answers, or gives up.
async fn until<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..300_u32 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn a_step_reaches_the_component_and_its_output_comes_back() {
    let prefix = format!("pneuma.test.mp{}", std::process::id());
    let asked: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let component = stub_component(Arc::clone(&asked), 200).await;
    let config = config(&prefix, component);

    let client = connected().await;
    let Ok(mut results) = client.subscribe(format!("{prefix}.result")).await else {
        panic!("could not subscribe to the result subject");
    };
    let Ok(mut events) = client.subscribe(format!("{prefix}.event")).await else {
        panic!("could not subscribe to the event subject");
    };

    let token = CancellationToken::new();
    let (tx, rx) = oneshot::channel();
    let checks = tokio::spawn({
        let token = token.clone();
        let client = client.clone();
        let prefix = prefix.clone();
        async move {
            let Ok(address) = rx.await else {
                panic!("the service should bind and say where");
            };
            let http = reqwest::Client::new();
            for path in ["healthz", "liveness"] {
                let url = format!("http://{address}/pneuma-executor/{path}");
                let Ok(response) = http.get(&url).send().await else {
                    panic!("{url} should answer");
                };
                assert_eq!(response.status().as_u16(), 200, "{url}");
            }

            let work = run_message("A");
            until("the provider to be subscribed", || async {
                let sent = client
                    .publish(format!("{prefix}.work"), work.clone().into())
                    .await;
                sent.is_ok() && client.flush().await.is_ok()
            })
            .await;

            // Two events: the step starting, then the step finishing. The first
            // is what makes a step that never finishes visible at all.
            let mut statuses = Vec::new();
            for _ in 0..2 {
                let Ok(Some(message)) =
                    tokio::time::timeout(Duration::from_secs(10), events.next()).await
                else {
                    panic!("the provider should publish two events, got {statuses:?}");
                };
                let Ok(event) = serde_json::from_slice::<MessageEvent>(&message.payload) else {
                    panic!("and they should be events");
                };
                statuses.push(format!("{:?}", event.event));
            }
            assert_eq!(statuses, ["Processing", "Finished"]);

            let Ok(Some(message)) =
                tokio::time::timeout(Duration::from_secs(10), results.next()).await
            else {
                panic!("the provider should publish a result");
            };
            let Ok(result) = serde_json::from_slice::<MessageResult>(&message.payload) else {
                panic!("and it should be a result message");
            };
            assert_eq!(result.node.node_id.as_str(), "A");
            assert_eq!(
                result
                    .step_output
                    .as_object()
                    .and_then(|map| map.get("pages")),
                Some(&json!(3))
            );

            token.cancel();
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

    // The component was asked exactly once, and asked in the shape
    // `docs/as-built/component-api.md` publishes. This is the only place that
    // contract is checked against a real HTTP request, and it is checked
    // key-set-first: an earlier version asserted only that the step input was
    // in there somewhere, and passed while the whole body *was* the step input.
    let requests = match asked.lock() {
        Ok(asked) => asked.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert_eq!(requests.len(), 1, "one call, not a retry storm");
    let Some(request) = requests[0].as_object() else {
        panic!("a component is sent an object: {:?}", requests[0]);
    };
    let mut keys: Vec<&str> = request.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec!["meta", "step_input"],
        "the three optional keys are omitted when the run carries none: {request:?}"
    );
    assert_eq!(requests[0].pointer("/step_input/doc"), Some(&json!("d")));
    let Some(meta) = requests[0].pointer("/meta").and_then(Value::as_object) else {
        panic!("a component is sent a meta: {request:?}");
    };
    let mut meta_keys: Vec<&str> = meta.keys().map(String::as_str).collect();
    meta_keys.sort_unstable();
    assert_eq!(
        meta_keys,
        vec![
            "job_id",
            "pipeline_level",
            "pipeline_name",
            "pipeline_type",
            "tenant_id"
        ],
        "exactly the five keys, and no pipeline_id"
    );
}

#[tokio::test]
async fn the_subject_the_broker_writes_is_the_subject_this_service_reads() {
    // The paired test. Each service alone is only proved against a fake, and a
    // fake agrees with whatever this file believes about the subject grammar --
    // which is the one thing neither of them owns.
    //
    // So: publish to the broker's input, let *it* decide the tenant subject,
    // and have the provider consume that subject without being told what it is.
    let prefix = format!("pneuma.test.pair{}", std::process::id());
    let input = format!("{prefix}.input");
    let tenant_subject = format!("{input}.tenant_acme");

    let asked: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let component = stub_component(Arc::clone(&asked), 200).await;
    let mut provider = config(&prefix, component);
    // The provider reads exactly what the broker writes. Built from the same
    // rule rather than copied: if the two ever disagree, this is where it shows.
    provider.work = subject(&tenant_subject);
    provider.queue = tenant_subject.replace('.', "-");

    let broker = pneuma_broker::Config {
        nats_uri: SecretString::from(url()),
        inputs: vec![subject(&input)],
        queue: token("pneuma-test-pair-broker"),
        listen: match "127.0.0.1:0".parse::<SocketAddr>() {
            Ok(address) => address,
            Err(error) => panic!("a real address: {error}"),
        },
    };

    let client = connected().await;
    let Ok(mut results) = client.subscribe(format!("{prefix}.result")).await else {
        panic!("could not subscribe to the result subject");
    };

    let token_ = CancellationToken::new();
    let (broker_tx, broker_rx) = oneshot::channel();
    let broker_running = tokio::spawn({
        let token_ = token_.clone();
        async move {
            pneuma_broker::boot::run(&broker, token_, |address| {
                let _ = broker_tx.send(address);
            })
            .await
        }
    });
    let Ok(_broker_address) = broker_rx.await else {
        panic!("the broker should bind");
    };

    let (tx, rx) = oneshot::channel();
    let checks = tokio::spawn({
        let token_ = token_.clone();
        let client = client.clone();
        let input = input.clone();
        async move {
            let Ok(_address) = rx.await else {
                panic!("the provider should bind");
            };
            // Published in a loop rather than through `until`, because the
            // check has to hold the subscriber across the await and a `FnMut`
            // closure cannot lend it out. Both services have to have attached,
            // and a core-NATS publish to a subject with no subscriber is
            // discarded in silence -- so sending once would be a race.
            let work = run_message("A");
            let mut answered = false;
            for _ in 0..300_u32 {
                let sent = client.publish(input.clone(), work.clone().into()).await;
                if sent.is_ok() && client.flush().await.is_ok() {
                    let arrived =
                        tokio::time::timeout(Duration::from_millis(50), results.next()).await;
                    if matches!(arrived, Ok(Some(_))) {
                        answered = true;
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(answered, "the message never came out the other end");
            token_.cancel();
        }
    });

    let served = boot::run(&provider, token_.clone(), |address| {
        let _ = tx.send(address);
    })
    .await;
    if let Err(error) = served {
        panic!("a cancelled provider exits cleanly, not with {error}");
    }
    if let Err(error) = checks.await {
        panic!("the assertions panicked: {error}");
    }
    match broker_running.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("a cancelled broker exits cleanly, not with {error}"),
        Err(error) => panic!("the broker task panicked: {error}"),
    }

    let requests = match asked.lock() {
        Ok(asked) => asked.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    assert!(
        !requests.is_empty(),
        "the message went broker -> tenant subject -> provider -> component"
    );
}

#[tokio::test]
async fn a_transport_failure_is_classified_from_the_client_not_from_its_message() {
    // The defect this replaces: the original matches the original's error text for
    // `dial tcp`, `connection refused`, `EOF` and `connection reset by peer`,
    // with the strings copied into the comment above -- the honest admission
    // that nothing pins them. An original upgrade that rewords one turns a retryable
    // failure into a permanent one, silently.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(50))
        .build();
    let Ok(client) = client else {
        panic!("a client should build");
    };

    // Nothing listening: a refused connection.
    let Err(refused) = client.post("http://127.0.0.1:1/predict").send().await else {
        panic!("nothing is listening on port 1");
    };
    assert_eq!(classify(&refused), Transport::Unreachable);

    // A server that accepts and never answers: a timeout.
    let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
        panic!("a loopback port");
    };
    let Ok(address) = listener.local_addr() else {
        panic!("a bound listener has an address");
    };
    tokio::spawn(async move {
        let accepted = listener.accept().await;
        // Held open, answering nothing.
        tokio::time::sleep(Duration::from_secs(5)).await;
        drop(accepted);
    });
    let Err(slow) = client
        .post(format!("http://{address}/predict"))
        .send()
        .await
    else {
        panic!("nothing answered");
    };
    assert_eq!(classify(&slow), Transport::TimedOut);
}

#[tokio::test]
async fn an_endpoint_without_a_scheme_is_refused_at_startup() {
    // `Client::post` parses lazily, so an endpoint like `component:9000` would
    // start, report ready, and fail every call.
    for bad in ["component:9000", "", "mailto:ops@example.com"] {
        assert!(
            Component::new(bad, Duration::from_secs(1)).is_err(),
            "{bad:?} is not a component endpoint"
        );
    }
    let Ok(component) = Component::new("http://component:9000/", Duration::from_secs(1)) else {
        panic!("that is a component endpoint");
    };
    assert_eq!(component.url(), "http://component:9000/predict");
    assert_eq!(component.attempts(), 3);
    assert_eq!(
        component.with_attempts(0).attempts(),
        1,
        "at least one attempt"
    );
}

#[tokio::test]
async fn a_body_that_is_not_a_run_message_is_dropped_with_a_reason() {
    let client = connected().await;
    let Ok(component) = Component::new("http://127.0.0.1:1", Duration::from_millis(50)) else {
        panic!("that is a component endpoint");
    };
    let said = handle(
        &client,
        &component,
        &subject("pneuma.test.mp.result"),
        &subject("pneuma.test.mp.event"),
        b"not a message",
    )
    .await;
    assert!(said.starts_with("dropped:"), "{said}");
}

#[tokio::test]
async fn a_component_that_never_answers_ends_the_step_without_a_result() {
    // The run is told the step ended rather than left waiting for a result that
    // is never coming.
    let prefix = format!("pneuma.test.mpsilent{}", std::process::id());
    let client = connected().await;
    let Ok(mut events) = client.subscribe(format!("{prefix}.event")).await else {
        panic!("could not subscribe to the event subject");
    };
    let Ok(component) = Component::new("http://127.0.0.1:1", Duration::from_millis(50)) else {
        panic!("that is a component endpoint");
    };

    let said = handle(
        &client,
        &component,
        &subject(&format!("{prefix}.result")),
        &subject(&format!("{prefix}.event")),
        &run_message("A"),
    )
    .await;
    assert!(said.contains("without a result"), "{said}");

    let mut statuses = Vec::new();
    for _ in 0..2 {
        let Ok(Some(message)) = tokio::time::timeout(Duration::from_secs(5), events.next()).await
        else {
            panic!("two events should still be published, got {statuses:?}");
        };
        let Ok(event) = serde_json::from_slice::<MessageEvent>(&message.payload) else {
            panic!("and they should be events");
        };
        statuses.push(format!("{:?}", event.event));
    }
    assert_eq!(statuses, ["Processing", "Error"], "started, then failed");
}

#[tokio::test]
async fn an_address_already_in_use_stops_the_whole_process() {
    let asked: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let component = stub_component(asked, 200).await;
    let mut config = config("pneuma.test.mpbind", component);
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
    let asked: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let component = stub_component(asked, 200).await;
    let mut config = config("pneuma.test.mpnobroker", component);
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

#[tokio::test]
async fn a_broker_that_will_not_take_the_event_stops_the_message_there() {
    // The first publish is the step *starting*, and it is published before the
    // component is called -- so a broker that will not take it means nothing
    // downstream will ever hear about this step, and calling the component
    // anyway would spend a model call nobody can be told the result of.
    let Ok(client) = async_nats::ConnectOptions::new()
        .retry_on_initial_connect()
        .connect("nats://127.0.0.1:1")
        .await
    else {
        panic!("connecting with retry should not fail up front");
    };
    let Ok(component) = Component::new("http://127.0.0.1:1", Duration::from_millis(50)) else {
        panic!("that is a component endpoint");
    };
    let said = tokio::time::timeout(
        pneuma_executor::PUBLISH_TIMEOUT * 2,
        handle(
            &client,
            &component,
            &subject("pneuma.test.mp.dead.result"),
            &subject("pneuma.test.mp.dead.event"),
            &run_message("A"),
        ),
    )
    .await;
    match said {
        Ok(line) => assert!(line.contains("could not publish"), "{line}"),
        Err(_) => panic!("a publish to a broker that is not there must not hang for ever"),
    }
}

#[tokio::test]
async fn a_request_that_never_leaves_the_client_is_not_worth_retrying() {
    // The third transport outcome. Neither a refused connection nor a timeout:
    // the request could not be made at all, and making it again would fail the
    // same way.
    let Err(unsupported) = reqwest::Client::new()
        .post("ftp://example.invalid/predict")
        .send()
        .await
    else {
        panic!("ftp is not a scheme reqwest posts to");
    };
    assert_eq!(classify(&unsupported), Transport::Broken);
}

/// A server that promises more bytes than it sends, then hangs up.
///
/// The same event as the original's `EOF` and `connection reset by peer`: the component
/// took the request and died.
async fn truncating() -> SocketAddr {
    let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
        panic!("a loopback port");
    };
    let Ok(address) = listener.local_addr() else {
        panic!("a bound listener has an address");
    };
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut scratch = [0_u8; 2048];
            let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut scratch).await;
            let _ = tokio::io::AsyncWriteExt::write_all(
                &mut socket,
                b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nshort",
            )
            .await;
            let _ = tokio::io::AsyncWriteExt::shutdown(&mut socket).await;
        }
    });
    address
}

#[tokio::test]
async fn a_component_that_dies_mid_answer_is_retried() {
    // Through the client, not just through `classify`: the body-read failure
    // has its own call site inside an attempt, and a component that dies after
    // sending its headers is the case the original's `EOF` matching is for.
    let address = truncating().await;
    let Ok(component) = Component::new(&format!("http://{address}"), Duration::from_secs(2)) else {
        panic!("that is a component endpoint");
    };
    let verdict = component.with_attempts(2).call(&a_request()).await;
    // Retryable, and still `Retry` after the attempts run out -- the caller is
    // told what kept happening rather than a summary of how many times.
    assert!(
        matches!(verdict, pneuma_executor::Verdict::Retry(_)),
        "{verdict:?}"
    );
}

#[tokio::test]
async fn a_response_that_stops_mid_body_is_worth_retrying() {
    // The same event as the original's `EOF` and `connection reset by peer`, which the
    // original retries -- the component took the request and died, and another
    // worker may well answer. Classified from `is_body()`, not from the text
    // of the message.
    let address = truncating().await;
    let Err(truncated) = reqwest::Client::new()
        .post(format!("http://{address}/predict"))
        .send()
        .await
        .expect("headers arrive")
        .text()
        .await
    else {
        panic!("a truncated body is not a body");
    };
    assert_eq!(classify(&truncated), Transport::Unreachable);
}

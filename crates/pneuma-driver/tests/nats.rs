//! The transport, against a real NATS.
//!
//! What a fake could not answer: that a message published to a component's own
//! subject is one a subscriber on that subject actually receives, and that a
//! result published to the shared subject reaches the run waiting for it. Both
//! are properties of the broker and of the wire shapes, not of this crate's
//! bookkeeping.
//!
//! ```sh
//! docker run -d --name pn-nats -p 4223:4222 nats:2.10 -js
//! PNEUMA_TEST_NATS_URL=nats://127.0.0.1:4223 \
//!     cargo test -p pneuma-driver --test nats
//! ```

use std::sync::Arc;
use std::time::Duration;

use futures_lite::StreamExt;
use pneuma_core::ids::{NodeId, PipelineId, RunId};
use pneuma_core::node::Pipeline;
use pneuma_core::resolver::resolve;
use pneuma_core::step::StepRegistry;
use pneuma_driver::{
    receive, CallError, Delivered, NatsComponent, Pending, Received, RunContext,
    DEFAULT_TIMEOUT_SECS, PUBLISH_TIMEOUT_SECS,
};
use pneuma_proto::component::{ComponentMeta, ComponentRequest};
use pneuma_proto::dispatch::{MessageResult, MessageRun};
use pneuma_proto::meta::Meta;
use pneuma_runner::driver::{Component, Dispatch};
use serde_json::json;

const PIPELINE: &str = include_str!("fixtures/pipeline1.yaml");

fn url() -> String {
    let Ok(url) = std::env::var("PNEUMA_TEST_NATS_URL") else {
        panic!("PNEUMA_TEST_NATS_URL is not set; see the header of this file");
    };
    url
}

async fn connected() -> async_nats::Client {
    match async_nats::connect(&url()).await {
        Ok(client) => client,
        Err(error) => panic!("could not reach NATS at {}: {error}", url()),
    }
}

fn registry() -> Arc<StepRegistry> {
    let Ok(pipeline) = serde_yaml::from_str::<Pipeline>(PIPELINE) else {
        panic!("the corpus fixture parses");
    };
    match resolve(&pipeline) {
        Ok(registry) => Arc::new(registry),
        Err(error) => panic!("the corpus fixture resolves: {error}"),
    }
}

fn meta() -> Meta {
    let raw = json!({
        "job_id": "job-1", "tenant_id": "acme",
        "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
        "a_caller_extra": "kept",
    });
    match serde_json::from_value(raw) {
        Ok(meta) => meta,
        Err(error) => panic!("that is a meta: {error}"),
    }
}

fn context(run: &str) -> RunContext {
    RunContext {
        meta: meta(),
        run_id: RunId::new(run),
        pipeline_id: PipelineId::new("invoice.page.default"),
    }
}

/// A dispatch for node `A`, published to `subject`.
fn dispatch(subject: &str) -> Dispatch {
    Dispatch {
        node_id: NodeId::new("A"),
        component: subject.to_owned(),
        request: ComponentRequest {
            meta: ComponentMeta {
                job_id: pneuma_core::ids::JobId::new("job-1"),
                tenant_id: pneuma_core::ids::TenantId::new("acme"),
                pipeline_type: "invoice".into(),
                pipeline_level: "page".into(),
                pipeline_name: "default".into(),
            },
            step_input: json!({"doc": "d"}),
            custom_data: None,
            node_env_vars: None,
            headers: Default::default(),
        },
    }
}

#[tokio::test]
async fn a_call_reaches_the_components_subject_and_its_result_comes_back() {
    let client = connected().await;
    let subject = format!("pneuma.test.ctrl.{}", std::process::id());
    let Ok(mut component_side) = client.subscribe(subject.clone()).await else {
        panic!("could not subscribe to {subject}");
    };

    let pending = Arc::new(Pending::new());
    let transport = NatsComponent::new(
        client.clone(),
        Arc::clone(&pending),
        registry(),
        context("job-1"),
    );

    // The call is started, not awaited: nothing answers until the stand-in
    // component below does, which is the whole shape being checked.
    let calling = tokio::spawn(async move {
        let value = transport.call(dispatch(&subject)).await;
        value.map(|response| response.to_string())
    });

    let Some(message) = component_side.next().await else {
        panic!("the component's subject should receive the run message");
    };
    let Ok(run) = serde_json::from_slice::<MessageRun>(&message.payload) else {
        panic!("and it should be a run message");
    };
    assert_eq!(run.node.run_id.as_str(), "job-1");
    assert_eq!(run.node.node_id.as_str(), "A");
    assert!(
        run.node.path.starts_with("job-1."),
        "the path is the run and the step's path: {}",
        run.node.path
    );
    // The *full* controller envelope, caller extras and all -- not the narrowed
    // five-key meta a component sees.
    assert!(
        run.meta.extra.contains_key("a_caller_extra"),
        "the envelope travels whole: {:?}",
        run.meta.extra
    );

    // The stand-in original executor: echo the node back with an output.
    let result = json!({
        "meta": run.meta,
        "node": run.node,
        "step_output": {"pages": 3},
    });
    let Ok(body) = serde_json::to_vec(&result) else {
        panic!("that serialises");
    };
    assert_eq!(
        receive(&pending, &body).await,
        Received::Delivered(Delivered::Taken)
    );

    match calling.await {
        Ok(Ok(response)) => assert!(
            response.contains("step_output"),
            "re-wrapped for the driver: {response}"
        ),
        Ok(Err(error)) => panic!("the call should succeed: {error}"),
        Err(error) => panic!("the call task panicked: {error}"),
    }
}

#[tokio::test]
async fn a_component_that_never_answers_fails_the_run_rather_than_stopping_it() {
    // `drive` awaits each call before asking for the next task, so a call that
    // never resolves is not one lost step -- it is a run that stops for ever,
    // holding its execution and its place in the correlation table. The
    // original has the same exposure and no timeout at all.
    let client = connected().await;
    let subject = format!("pneuma.test.ctrl.silent.{}", std::process::id());
    let Ok(_listening) = client.subscribe(subject.clone()).await else {
        panic!("could not subscribe to {subject}");
    };

    let pending = Arc::new(Pending::new());
    let transport = NatsComponent::new(
        client,
        Arc::clone(&pending),
        registry(),
        context("job-silent"),
    )
    .with_timeout(Duration::from_millis(80));
    assert_eq!(transport.timeout(), Duration::from_millis(80));

    let Err(error) = transport.call(dispatch(&subject)).await else {
        panic!("nothing answered");
    };
    let CallError::Timeout { node_id, after } = &error else {
        panic!("wrong variant: {error:?}");
    };
    assert_eq!(node_id, "A");
    assert_eq!(*after, Duration::from_millis(80));
    assert_eq!(
        pending.outstanding().await,
        0,
        "and the abandoned call is not left in the table"
    );
}

#[tokio::test]
async fn a_second_call_for_the_same_node_is_refused() {
    // The redelivery case: the ingress is not acknowledged until a run
    // completes, so a second execution for the same run id is reachable.
    // Refusing is what stops it orphaning the first one's waiter.
    let client = connected().await;
    let subject = format!("pneuma.test.ctrl.twice.{}", std::process::id());
    let Ok(_listening) = client.subscribe(subject.clone()).await else {
        panic!("could not subscribe to {subject}");
    };

    let pending = Arc::new(Pending::new());
    let first = NatsComponent::new(
        client.clone(),
        Arc::clone(&pending),
        registry(),
        context("job-twice"),
    )
    .with_timeout(Duration::from_millis(400));
    let second = NatsComponent::new(
        client,
        Arc::clone(&pending),
        registry(),
        context("job-twice"),
    )
    .with_timeout(Duration::from_millis(400));

    let subject_for_first = subject.clone();
    let running =
        tokio::spawn(async move { first.call(dispatch(&subject_for_first)).await.is_err() });
    // Long enough for the first call to have registered.
    tokio::time::sleep(Duration::from_millis(80)).await;

    let Err(error) = second.call(dispatch(&subject)).await else {
        panic!("the first call is still outstanding");
    };
    assert!(
        matches!(error, CallError::Occupied(_)),
        "wrong variant: {error:?}"
    );
    let Ok(true) = running.await else {
        panic!("the first call times out rather than being disturbed");
    };
}

#[tokio::test]
async fn a_message_that_will_not_parse_does_not_end_the_subscription() {
    // The shared subject carries everything, and a message nobody here can read
    // is not this replica's problem to fix. A subscription that died on one bad
    // message would take every run on the replica with it.
    let pending = Pending::new();
    for bad in [&b"not json"[..], b"{}", b"{\"node\": 3}"] {
        let Received::Unreadable(why) = receive(&pending, bad).await else {
            panic!("{bad:?} is not a result message");
        };
        assert!(!why.is_empty(), "and says why");
    }
}

#[tokio::test]
async fn a_result_for_a_run_this_replica_is_not_driving_is_unclaimed() {
    let pending = Pending::new();
    let body = json!({
        "meta": {
            "job_id": "someone-else", "tenant_id": "acme",
            "pipeline_type": "invoice", "pipeline_level": "page", "pipeline_name": "default",
        },
        "node": {
            "path": "someone-else.A", "node_id": "A", "name": "A", "kind": "Model",
            "pipeline_id": "invoice.page.default", "run_id": "someone-else",
        },
        "step_output": {"pages": 1},
    });
    let Ok(bytes) = serde_json::to_vec(&body) else {
        panic!("that serialises");
    };
    assert_eq!(
        receive(&pending, &bytes).await,
        Received::Delivered(Delivered::Unclaimed)
    );
}

#[tokio::test]
async fn a_publish_that_could_not_happen_undoes_its_registration() {
    // A publish that never happened has no answer coming, so leaving the entry
    // behind would hold a place in the correlation table until the process
    // ended -- and would make the *next* attempt for that node look like a
    // duplicate.
    //
    // A client connected to nothing. Measured, because two more obvious
    // arrangements do not work: `publish` on a *drained* client still returns
    // `Ok`, and so does one to an empty subject -- async-nats writes into a
    // local buffer either way. Only waiting for the server to admit the write
    // distinguishes them, which is what `flush` is for and why it is bounded.
    let Ok(client) = async_nats::ConnectOptions::new()
        .retry_on_initial_connect()
        .connect("nats://127.0.0.1:1")
        .await
    else {
        panic!("connecting with retry should not fail up front");
    };
    let pending = Arc::new(Pending::new());
    let transport = NatsComponent::new(
        client,
        Arc::clone(&pending),
        registry(),
        context("job-gone"),
    )
    .with_timeout(Duration::from_millis(80));

    let Err(error) = transport.call(dispatch("pneuma.test.ctrl.drained")).await else {
        panic!("a drained client cannot publish");
    };
    assert!(
        matches!(error, CallError::Publish { .. }),
        "wrong variant: {error:?}"
    );
    assert_eq!(pending.outstanding().await, 0, "the entry was undone");
}

#[test]
fn the_deadline_matches_the_rest_of_the_port() {
    // The original's own model timeout, and what `pneuma-restate` defaults to.
    // Matching means a deployment that tunes one is not surprised by the other.
    assert_eq!(DEFAULT_TIMEOUT_SECS, 300);
    // And the publish deadline is much shorter, because the two fail
    // differently: a component thinking for five minutes is normal, a broker
    // taking ten seconds to admit a publish is a broker that is not there.
    assert_eq!(PUBLISH_TIMEOUT_SECS, 10);
}

#[tokio::test]
async fn a_result_whose_waiter_has_gone_is_reported_as_abandoned() {
    let pending = Pending::new();
    let Ok(waiting) = pending
        .register(pneuma_driver::CallKey::new("job-1", "A"))
        .await
    else {
        panic!("nothing is outstanding yet");
    };
    drop(waiting);
    let result = MessageResult {
        meta: meta(),
        node: match pneuma_driver::node_for(
            &registry(),
            &RunId::new("job-1"),
            &PipelineId::new("invoice.page.default"),
            &NodeId::new("A"),
        ) {
            Ok(info) => info,
            Err(error) => panic!("A is in the fixture: {error}"),
        },
        step_output: Default::default(),
        step_input: Default::default(),
        custom_data: Default::default(),
        reply_to_result: None,
        reply_to_error: None,
        reply_to_event: None,
        headers: Default::default(),
        extra: Default::default(),
    };
    let Ok(body) = serde_json::to_vec(&result) else {
        panic!("that serialises");
    };
    assert_eq!(
        receive(&pending, &body).await,
        Received::Delivered(Delivered::Abandoned)
    );
}

#[tokio::test]
async fn a_call_whose_waiter_is_forgotten_says_it_was_abandoned() {
    // Distinct from a timeout, because nothing was waited *for*: the
    // correlation table gave up on this call, which is what a cancelled run or
    // a shutdown does. A run told "timeout" for that would look like a slow
    // component.
    let client = connected().await;
    let subject = format!("pneuma.test.ctrl.dropped.{}", std::process::id());
    let Ok(_listening) = client.subscribe(subject.clone()).await else {
        panic!("could not subscribe to {subject}");
    };

    let pending = Arc::new(Pending::new());
    let transport = NatsComponent::new(
        client,
        Arc::clone(&pending),
        registry(),
        context("job-dropped"),
    )
    .with_timeout(Duration::from_secs(5));

    let watching = Arc::clone(&pending);
    let giving_up = tokio::spawn(async move {
        for _ in 0..200_u32 {
            if watching
                .forget(&pneuma_driver::CallKey::new("job-dropped", "A"))
                .await
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the call should have registered");
    });

    let Err(error) = transport.call(dispatch(&subject)).await else {
        panic!("its waiter was taken away");
    };
    assert!(
        matches!(error, CallError::Abandoned { .. }),
        "wrong variant: {error:?}"
    );
    if let Err(panicked) = giving_up.await {
        panic!("the watcher panicked: {panicked}");
    }
}

#[test]
fn every_way_a_result_can_land_reads_differently_in_the_log() {
    use pneuma_driver::describe_received;

    // Four outcomes, four lines, and the distinction that matters is between
    // the two that are not errors: "unclaimed" is the ordinary case on a shared
    // subject, and a log that warned about it would warn on almost every
    // message a replica sees.
    let lines = [
        describe_received(&Received::Unreadable("trailing comma".to_owned())),
        describe_received(&Received::Delivered(Delivered::Taken)),
        describe_received(&Received::Delivered(Delivered::Unclaimed)),
        describe_received(&Received::Delivered(Delivered::Abandoned)),
    ];
    for line in &lines {
        assert!(!line.is_empty());
    }
    let mut unique = lines.to_vec();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 4, "four distinguishable lines: {lines:?}");
    assert!(lines[0].contains("trailing comma"), "{}", lines[0]);
    assert!(lines[2].contains("another replica"), "{}", lines[2]);
}

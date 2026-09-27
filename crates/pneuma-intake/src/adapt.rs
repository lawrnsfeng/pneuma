//! The real implementations of [`crate::handle`]'s four traits.
//!
//! Each one is a translation and nothing else: a store call, an HTTP POST, an
//! AMQP publish. The rules they serve are in `handle.rs`, where they are tested
//! without any of this.
//!
//! # Why the traits take `String` errors and these throw the types away
//!
//! Neither `handle_run` nor its siblings do anything with an error's
//! structure — every failure to reach a store is the same `Retry`, and the
//! message is for the log. A typed error would be a type every fake has to
//! construct in order to say "the database was down", which is the one thing a
//! fake needs to say most often.

use async_trait::async_trait;
use mongodb::bson::Document;
use pneuma_amqp::RoutingKey;
use pneuma_store::{Created, PipelineStore, RunStore};
use pneuma_transport::Amqp;
use reqwest::Client;
use serde_json::Value;

use crate::handle::{Admits, Events, Pipelines, Runs};

#[async_trait]
impl Pipelines for PipelineStore {
    async fn by_pipeline_id(&self, pipeline_id: &str) -> Result<Option<Document>, String> {
        PipelineStore::by_pipeline_id(self, pipeline_id)
            .await
            .map_err(|error| error.to_string())
    }

    async fn create(&self, document: Document) -> Result<Created, String> {
        PipelineStore::create(self, document)
            .await
            .map_err(|error| error.to_string())
    }
}

#[async_trait]
impl Runs for RunStore {
    async fn create(&self, document: Document) -> Result<Created, String> {
        RunStore::create(self, document)
            .await
            .map_err(|error| error.to_string())
    }
}

/// `pneuma-admission`'s door.
#[derive(Debug, Clone)]
pub struct Admission {
    client: Client,
    url: String,
}

impl Admission {
    /// Points at the service's `runs` endpoint.
    pub fn new(client: Client, base: &str) -> Self {
        Admission {
            client: client.clone(),
            url: format!("{}/pneuma-admission/runs", base.trim_end_matches('/')),
        }
    }

    /// Where submissions are posted. Exposed so a test can see it.
    pub fn url(&self) -> &str {
        &self.url
    }
}

#[async_trait]
impl Admits for Admission {
    async fn submit(&self, run_id: &str, body: &Value) -> Result<(), String> {
        let response = self
            .client
            .post(&self.url)
            .json(body)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        let status = response.status();
        // Every 2xx is success, and that includes the 409 admission does *not*
        // send: its door answers 202 for a fresh submission and 202 again for
        // one already queued, because a redelivery is the ordinary case rather
        // than a conflict. Anything else is reported with its status, and the
        // caller retries -- a run whose document is already written is safe to
        // announce again.
        if status.is_success() {
            return Ok(());
        }
        Err(format!("admission answered {status} for {run_id}"))
    }
}

/// The queue validated events are forwarded to.
///
/// # The destination has to exist
///
/// `Amqp::publish` sets `mandatory`, so forwarding to a routing key with
/// nothing bound behind it fails rather than being discarded. That is louder
/// than the original, which publishes without `mandatory` and loses the event
/// in silence -- and loud is the right end of that trade: an event nobody is
/// listening for is a misconfiguration, and the alternative is discovering it
/// when somebody asks why a run never reported finishing. The failure is a
/// `Retry`, so the queue's redelivery limit is what bounds it.
#[derive(Debug, Clone)]
pub struct Forwarder {
    amqp: Amqp,
    key: RoutingKey,
}

impl Forwarder {
    /// Forwards to `key` over `amqp`.
    pub fn new(amqp: Amqp, key: RoutingKey) -> Self {
        Forwarder { amqp, key }
    }
}

#[async_trait]
impl Events for Forwarder {
    async fn forward(&self, event: &Value) -> Result<(), String> {
        let body = serde_json::to_vec(event).map_err(|error| error.to_string())?;
        self.amqp
            .publish(&self.key, &body)
            .await
            .map_err(|error| error.to_string())
    }
}

//! The real [`Invoker`]: Restate's ingress over HTTP.
//!
//! Everything about *what an answer means* lives in [`crate::restate`] and is
//! pure. What is left here is getting the bytes there and back, which is why
//! this file is short and why the only judgement in it is a URL and two
//! timeouts.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, Url};
use serde_json::Value;

use crate::restate::Invoker;

/// How long the *submission* may take.
///
/// Not how long the run may take. `/send` is fire-and-forget: Restate answers
/// as soon as it has journalled the invocation, so this bounds an ingress
/// write, not a pipeline. Ten seconds is therefore generous rather than tight,
/// and the failure it prevents is the one the dispatcher cannot survive -- a
/// round that hangs on one submission dispatches nothing else, for anybody, for
/// as long as the socket stays open.
pub const SUBMIT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long establishing the connection may take.
///
/// Separate from the request timeout because the two fail differently: a
/// refused or black-holed connect is Restate being absent, and there is no
/// reason to spend the whole request budget discovering it.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The header Restate deduplicates on.
///
/// The design notes: the key is the whole of the identity Restate compares,
/// and it never looks at the request body.
const IDEMPOTENCY_KEY: &str = "idempotency-key";

/// Why a client for Restate could not be built.
#[derive(Debug, thiserror::Error)]
pub enum IngressError {
    /// The ingress and the handler do not compose into a URL.
    #[error("{url:?} is not a URL to submit to: {reason}")]
    Url {
        /// What the two composed to.
        url: String,
        /// Why it is not a URL.
        reason: String,
    },

    /// The HTTP client itself could not be built.
    ///
    /// Reachable only if reqwest cannot initialise its TLS backend, which is a
    /// property of the machine rather than of the configuration.
    #[error("the submission HTTP client could not be built: {0}")]
    Client(#[from] reqwest::Error),
}

/// Restate's ingress, as this service uses it.
#[derive(Debug, Clone)]
pub struct Ingress {
    client: Client,
    url: Url,
}

impl Ingress {
    /// Builds a client for `handler` at `ingress`.
    ///
    /// The URL is **parsed** here, not merely formatted. `Client::post` takes
    /// anything string-shaped and defers parsing to the moment it sends, so an
    /// ingress written without a scheme -- `restate:8080`, an ordinary compose
    /// typo that `Config` cannot tell from a hostname -- would start, bind,
    /// report ready, and then fail *every* submission with a builder error.
    /// That error is a transport failure, so the dispatcher classifies it as
    /// `Retry`, leaves each row `claimed`, sweeps it back, and loops for ever
    /// with nothing ever reaching Restate. Parsing once at startup turns that
    /// into a pod that never becomes ready.
    ///
    /// Redirects are **not** followed. reqwest's default is to follow up to
    /// ten, and on a 301, 302 or 303 it rewrites the POST to a GET and drops
    /// the body -- so an `http` → `https` redirect on an ingress or load
    /// balancer would turn a submission into a bodiless GET, Restate would
    /// answer 404 or 405, `disposition` would read that as a permanent
    /// rejection, and the run would be settled `failed` without ever having
    /// been offered.
    pub fn new(ingress: &str, handler: &str) -> Result<Self, IngressError> {
        let client = Client::builder()
            .timeout(SUBMIT_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let raw = send_url(ingress, handler);
        let url = match Url::parse(&raw) {
            Ok(url) => url,
            Err(error) => {
                return Err(IngressError::Url {
                    url: raw,
                    reason: error.to_string(),
                })
            }
        };
        // Parsing alone is not enough, and the reason is the exact typo this
        // guard is for: `restate:8080` **parses**, as a URL whose scheme is
        // `restate` and whose path is `8080`. It is reqwest that refuses it,
        // and it refuses it per request rather than at startup. So the scheme
        // and the host are checked here, where a refusal is a pod that never
        // becomes ready rather than one that retries for ever.
        match (url.scheme(), url.host_str()) {
            ("http" | "https", Some(_)) => Ok(Ingress { client, url }),
            (scheme, _) => Err(IngressError::Url {
                url: raw,
                reason: format!("expected an http or https URL with a host, got scheme {scheme:?}"),
            }),
        }
    }

    /// Where submissions are posted. Exposed so a test can see it.
    pub fn url(&self) -> &str {
        self.url.as_str()
    }
}

/// Joins an ingress base and a handler into the `/send` URL.
///
/// Pure, and separate, because slashes are where this goes wrong: a base
/// configured as `http://restate:8080/` and a handler as `/PneumaRunner/run`
/// are both perfectly reasonable things for an operator to write, and
/// concatenating them gives `http://restate:8080//PneumaRunner/run`, which
/// Restate answers with a 404 that reads like an unregistered service.
pub fn send_url(ingress: &str, handler: &str) -> String {
    let base = ingress.trim_end_matches('/');
    let handler = handler.trim_matches('/');
    format!("{base}/{handler}/send")
}

#[async_trait]
impl Invoker for Ingress {
    async fn send(&self, idempotency_key: &str, payload: &Value) -> Result<(u16, Value), String> {
        let response = self
            .client
            .post(self.url.clone())
            .header(IDEMPOTENCY_KEY, idempotency_key)
            .json(payload)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        let status = response.status().as_u16();
        // The body is read as text and parsed leniently rather than through
        // `response.json()`, which would turn a proxy's HTML error page into a
        // transport failure -- and a transport failure is `Retry` forever,
        // while the status that page carried may well have been a permanent
        // rejection. `disposition` reads the status first and the body only to
        // describe it, so a body it cannot parse costs a log message, not a
        // misclassification.
        let body = response.text().await.map_err(|error| error.to_string())?;
        Ok((status, serde_json::from_str(&body).unwrap_or(Value::Null)))
    }
}

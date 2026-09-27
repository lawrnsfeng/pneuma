//! The component client, and the one decision it makes.
//!
//! # A typed predicate, not a substring of a printed message
//!
//! The original decides whether a
//! transport failure is worth retrying by matching its error *text* for
//! `dial tcp`, `connection refused`, `EOF` and `connection reset by peer` —
//! with the strings themselves copied into the comment above, which is the
//! honest admission that nothing pins them. Here the same decision comes off
//! `reqwest::Error`'s own predicates, which describe what happened rather than
//! how it prints.

use std::time::Duration;

use pneuma_proto::component::ComponentRequest;
use reqwest::Client;
use serde_json::Value;

use crate::verdict::{from_status, from_transport, is_retryable, Transport, Verdict};

/// How long one call to a component may take.
///
/// The original's `COMPONENT_TIMEOUT`, and the same figure the rest of the port
/// uses for a model call.
pub const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 300;

/// The same figure, as configuration reads it.
///
/// Two constants rather than a cast at the call site: the environment is
/// parsed as `i64` because a negative value has to be refusable, and casting a
/// `u64` default into that comparison is the one place a silent wrap could turn
/// "refuse this" into "accept it". The controller keeps the same pair for the
/// same reason.
pub const DEFAULT_REQUEST_TIMEOUT_SECS_I64: i64 = 300;

/// How many times a retryable failure is tried.
///
/// The original's `COMPONENT_ATTEMPTS`. Attempts, not retries: three means one call
/// and two more.
pub const DEFAULT_ATTEMPTS: u32 = 3;

/// The same figure, as configuration reads it.
pub const DEFAULT_ATTEMPTS_I64: i64 = 3;

/// The path a component answers on.
pub const PREDICT: &str = "predict";

/// Why a client could not be built.
#[derive(Debug, thiserror::Error)]
pub enum ComponentError {
    /// The endpoint is not a URL.
    #[error("{url:?} is not a component endpoint: {reason}")]
    Url {
        /// What was given.
        url: String,
        /// Why it is not a URL.
        reason: String,
    },

    /// The HTTP client itself could not be built.
    #[error("the component client could not be built: {0}")]
    Client(#[from] reqwest::Error),
}

/// What a transport failure was.
///
/// Pure over `reqwest::Error`, so the classification is a test rather than a
/// judgement made inside a retry loop.
///
/// A body that stops mid-read is [`Transport::Unreachable`], not `Broken`: it
/// is the same event as the original's `EOF` and `connection reset by peer`, which the
/// original retries — the component took the request and died, and another
/// worker may well answer.
pub fn classify(error: &reqwest::Error) -> Transport {
    if error.is_timeout() {
        return Transport::TimedOut;
    }
    if error.is_connect() || error.is_body() || error.is_decode() {
        return Transport::Unreachable;
    }
    Transport::Broken
}

/// A component, reached over HTTP.
#[derive(Debug, Clone)]
pub struct Component {
    client: Client,
    url: reqwest::Url,
    attempts: u32,
}

impl Component {
    /// A client for the component at `base`.
    ///
    /// The URL is parsed here rather than at the first call, for the reason
    /// `pneuma-admission`'s is: `Client::post` parses lazily, so an endpoint
    /// without a scheme would start, report ready, and fail every call.
    pub fn new(base: &str, timeout: Duration) -> Result<Self, ComponentError> {
        let client = Client::builder()
            .timeout(timeout)
            // Not followed, for the reason `pneuma-admission` does not follow
            // them: reqwest rewrites a redirected POST to a GET and drops the
            // body, so the component would be asked to predict nothing.
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let joined = format!("{}/{PREDICT}", base.trim_end_matches('/'));
        let url = reqwest::Url::parse(&joined).map_err(|error| ComponentError::Url {
            url: joined.clone(),
            reason: error.to_string(),
        })?;
        match url.scheme() {
            "http" | "https" => Ok(Component {
                client,
                url,
                attempts: DEFAULT_ATTEMPTS,
            }),
            scheme => Err(ComponentError::Url {
                url: joined,
                reason: format!("expected an http or https endpoint, got scheme {scheme:?}"),
            }),
        }
    }

    /// The same, trying a retryable failure a different number of times.
    pub fn with_attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts.max(1);
        self
    }

    /// Where calls go. Exposed so a test can see it.
    pub fn url(&self) -> &str {
        self.url.as_str()
    }

    /// How many attempts a retryable failure gets.
    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Calls the component, retrying what is worth retrying.
    ///
    /// The loop knows only which verdicts may be tried again — [`is_retryable`]
    /// — and nothing about what any of them mean. The last verdict is returned
    /// whether or not it was retryable, because a caller told "retry" after the
    /// attempts ran out would have nothing left to do with that.
    pub async fn call(&self, request: &ComponentRequest) -> Verdict {
        // No sleep between attempts. The original backs off,
        // and the reason not to here is that a component
        // call already has its own deadline: a backoff inside it makes the
        // *worst* case longer without making the common case better, and the
        // queue's own redelivery is what spaces out a component that is
        // genuinely down.
        let mut last = Verdict::Failed("no attempt was made".to_owned());
        for _ in 0..self.attempts {
            last = self.once(request).await;
            if !is_retryable(&last) {
                return last;
            }
        }
        last
    }

    /// One attempt.
    async fn once(&self, request: &ComponentRequest) -> Verdict {
        let sent = self
            .client
            .post(self.url.clone())
            .json(request)
            .send()
            .await;
        let response = match sent {
            Ok(response) => response,
            Err(error) => return from_transport(classify(&error)),
        };
        let status = response.status().as_u16();
        // The body is read as text and parsed leniently rather than through
        // `response.json()`, so a proxy's HTML error page is classified by its
        // status rather than becoming a transport failure -- the same choice
        // `pneuma-admission` makes against Restate.
        let body = match response.text().await {
            Ok(body) => body,
            Err(error) => return from_transport(classify(&error)),
        };
        let value = serde_json::from_str(&body).unwrap_or(Value::Null);
        from_status(status, &value)
    }
}

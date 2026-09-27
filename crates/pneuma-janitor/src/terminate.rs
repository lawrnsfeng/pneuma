//! Asking the gateway to cancel a run, and what its answer means.
//!
//! Selection is `pneuma-store`'s and sequencing is [`crate::cleanup`]'s; this
//! is the one thing the janitor does that leaves the process. The original
//! posts each stale run id to the gateway in a loop and swallows every
//! exception, so a gateway that is
//! down and a run that was already cancelled are the same log line.
//!
//! # 409 is success
//!
//! The gateway answers `409 Conflict` when a termination for that job already
//! exists. That is not a failure: it means the run this pass found stale is
//! *already* being cancelled, which is exactly the outcome asking for. Treating
//! it as an error would make a healthy steady state -- a long cancellation
//! spanning two janitor passes -- look like a broken one, and the operator
//! reading the count would learn nothing from it.
//!
//! The same reasoning as the defect notes and the duplicate insert,
//! from the other end of the system: an at-least-once actor that finds the work
//! already done has succeeded.
//!
//! # And a 404 is not
//!
//! There is no route to be missing on the create path -- `POST /terminations`
//! either exists or the gateway is not the gateway. A 404 here means the base
//! URL is wrong, which is a deployment error that must not be retried quietly
//! for ever, so it is refused rather than swallowed.

use std::time::Duration;

use pneuma_gateway_client::{CreateTerminationRequest, Endpoint};
use reqwest::Client;

/// The path a termination is created on, as `pneuma-gateway-client` spells it.
///
/// A `LazyLock` would be the way to hold the `Endpoint` itself; a `&str` is not
/// available from a `const fn`, so this is built per [`Gateway::new`] instead --
/// once per process, which is cheap enough not to be worth a lock.
fn terminations_path() -> String {
    Endpoint::create_termination().as_str().to_owned()
}

/// How long one call to the gateway may take, in seconds.
///
/// The original's `GATEWAY_HTTP_TIMEOUT`,
/// to the second.
///
/// `i64`, which is the type configuration reads, rather than the `u64`
/// [`Duration::from_secs`] wants. The environment is parsed as `i64` because a
/// negative value has to be refusable, and a `u64` default cast into that
/// comparison is the one place a silent wrap could turn "refuse this" into
/// "accept it". The conversion happens once, after the range check.
pub const DEFAULT_GATEWAY_TIMEOUT_SECS: i64 = 10;

/// What the gateway's answer to a termination request means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// The gateway accepted it and the run is being cancelled.
    Submitted,
    /// A termination for that run already exists — see the module docs.
    AlreadyTerminating,
    /// The gateway refused, and will refuse the same request again.
    ///
    /// A wrong base URL, a job id the gateway will not accept, a missing
    /// credential. Retrying is what turns a deployment mistake into a quiet
    /// loop, so this is reported and the pass moves on.
    Refused {
        /// What the gateway answered.
        status: u16,
    },
    /// The gateway could not answer, and might next time.
    Retryable {
        /// What the gateway answered.
        status: u16,
    },
}

/// What a status code means, decided without a network.
///
/// Pure, so every branch is a test rather than something only a running gateway
/// can show — and so the one judgement in this module is readable in one place.
pub fn disposition(status: u16) -> Disposition {
    /// The gateway's "there is already one of these".
    const CONFLICT: u16 = 409;

    if (200..300).contains(&status) {
        return Disposition::Submitted;
    }
    if status == CONFLICT {
        return Disposition::AlreadyTerminating;
    }
    // 5xx and anything above it. A gateway that is restarting answers 502 or
    // 503, and the next pass is minutes away, which is the right amount of
    // backoff for a cleanup job.
    if status >= 500 {
        return Disposition::Retryable { status };
    }
    Disposition::Refused { status }
}

/// Why a termination could not be submitted at all.
#[derive(Debug, thiserror::Error)]
pub enum TerminateError {
    /// The endpoint is not a URL.
    #[error("{url:?} is not a gateway endpoint: {reason}")]
    Url {
        /// What was given.
        url: String,
        /// Why it is not a URL.
        reason: String,
    },

    /// The HTTP client itself could not be built.
    #[error("the gateway client could not be built: {0}")]
    Client(#[from] reqwest::Error),
}

/// The gateway, as the janitor uses it.
///
/// One method. `pneuma-gateway-client` supplies the endpoint and the wire types
/// and deliberately links no HTTP client, so this is where the two meet.
#[derive(Debug, Clone)]
pub struct Gateway {
    client: Client,
    /// The full URL, built once.
    ///
    /// The endpoint is a constant, so there is exactly one URL to build and no
    /// reason to build it per call -- and building it at construction is what
    /// makes a bad base a startup failure rather than a per-pass one.
    url: reqwest::Url,
}

impl Gateway {
    /// Builds a client against `base`, e.g. `http://pneuma-gateway:8080`.
    pub fn new(base: &str, timeout: Duration) -> Result<Self, TerminateError> {
        // The base and the path are joined as text and parsed once, rather than
        // parsed and then `Url::join`ed. Two fallible steps would leave the
        // second one unreachable -- a base that parses cannot fail to join a
        // fixed relative path -- and an arm nothing can take is an arm nothing
        // checks. Trimmed so `http://gw:8080` and `http://gw:8080/` reach the
        // same path.
        let combined = format!(
            "{}{}",
            base.trim().trim_end_matches('/'),
            terminations_path()
        );
        let Ok(url) = reqwest::Url::parse(&combined) else {
            return Err(TerminateError::Url {
                url: base.to_owned(),
                reason: "not a URL".to_owned(),
            });
        };
        // Checked rather than left to the request. `reqwest` accepts a
        // `mailto:` or a `file:` URL and fails at send time with an error
        // naming the scheme, which is a long way from "the janitor's gateway
        // address is wrong" -- the same trap `pneuma-admission` documents for
        // its Restate ingress.
        if !matches!(url.scheme(), "http" | "https") {
            return Err(TerminateError::Url {
                url: base.to_owned(),
                reason: format!("scheme is {:?}, not http or https", url.scheme()),
            });
        }
        Ok(Gateway {
            client: Client::builder().timeout(timeout).build()?,
            url,
        })
    }

    /// The URL this posts to. For a test, and for an operator reading a log.
    pub fn url(&self) -> &str {
        self.url.as_str()
    }

    /// Asks the gateway to cancel one run.
    ///
    /// The transport failure and the gateway's own answer are different
    /// outcomes: an `Err` means nothing reached the gateway, and an `Ok`
    /// carries what it said. The original collapses both into a logged
    /// exception.
    pub async fn create_termination(&self, run_id: &str) -> Result<Disposition, String> {
        let body = CreateTerminationRequest {
            job_id: run_id.to_owned(),
        };
        match self.client.post(self.url.clone()).json(&body).send().await {
            Ok(response) => Ok(disposition(response.status().as_u16())),
            Err(error) => Err(error.to_string()),
        }
    }
}

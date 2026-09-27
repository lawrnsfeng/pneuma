//! Calling a component over HTTP, durably.
//!
//! The whole of what Restate contributes to a single step: the call happens
//! inside `ctx.run`, so its result is journalled. On a replay the HTTP request
//! is not reissued — the recorded answer is returned instead, which is what
//! makes a crashed run resume rather than restart.

use pneuma_runner::{Component, Dispatch};
use restate_sdk::prelude::*;
use serde_json::Value;

use crate::endpoint::{Endpoint, EndpointError};

/// Why a component call failed.
#[derive(Debug, thiserror::Error)]
pub enum CallError {
    /// The component's address could not be built.
    #[error("{0}")]
    Endpoint(#[from] EndpointError),

    /// The request could not be turned into bytes.
    ///
    /// This service's fault, and permanent: a payload that will not serialise
    /// will not serialise on the next attempt either. Raised before `ctx.run`,
    /// so it never enters the retry loop.
    #[error("the request for {url} could not be serialised: {source}")]
    Serialise {
        /// Where it was going, so the step is identifiable.
        url: String,
        /// What serde said.
        source: serde_json::Error,
    },

    /// The call failed, after Restate exhausted its retries.
    ///
    /// A `TerminalError` by the time it reaches here: `ctx.run` retries a
    /// failing closure under the invoker's policy, so this is the answer after
    /// retrying, not the first attempt.
    ///
    /// The code travels with the message because collapsing it loses two
    /// distinctions a caller needs. A **cancellation** is delivered as a
    /// `TerminalError` with code 409, and reported as a 500 it is
    /// indistinguishable from the run having genuinely broken -- so a
    /// dispatcher retries something a human deliberately stopped. And a
    /// component's own 4xx is the caller's fault, not the service's; answering
    /// 500 tells whoever submitted the run to retry a submission that will
    /// fail the same way for ever.
    #[error("the component call did not succeed: {message}")]
    Failed {
        /// The status the failure should be reported under.
        code: u16,
        /// What went wrong.
        message: String,
    },
}

impl CallError {
    /// A request that could not be serialised, as an error.
    ///
    /// A named constructor rather than an inline closure, because the arm is
    /// unreachable through `call`: `ComponentRequest` is `serde_json::Value`
    /// underneath and a `Value` always serialises. Unreachable is not the same
    /// as unexamined -- what such a failure *means* is a decision (this
    /// service's, and permanent), and as a function it has a test.
    pub fn unserialisable(url: &str, source: serde_json::Error) -> Self {
        CallError::Serialise {
            url: url.to_owned(),
            source,
        }
    }

    /// The status a run should fail with when this is what ended it.
    pub fn status(&self) -> u16 {
        match self {
            // 400, not 500. Both errors `url_for` can raise are properties of
            // the *pipeline in the request*: a step with no component name
            // (`EmptyComponent`) or one whose name cannot go in a URL
            // (`UnsafeComponent`). An earlier version called this "the
            // deployment's configuration" and answered 500, reasoning about
            // `NoPlaceholder` -- which belongs to the template, and which
            // `Endpoint::new` refuses at startup, so it cannot arrive here at
            // all. The justification was about the one variant that never
            // reaches this function, and the two that do got the wrong answer:
            // a submitted step named `""` told the submitter that this service
            // had broken, and to retry something that fails identically for
            // ever.
            CallError::Endpoint(_) => 400,
            // This service built a request it could not send. Not the
            // component's, and not the submitter's.
            CallError::Serialise { .. } => 500,
            CallError::Failed { code, .. } => *code,
        }
    }
}

/// Calls components over HTTP, journalling each call.
///
/// Concrete over [`Context`] rather than generic over `ContextSideEffects`.
/// That was the first shape and it does not work: the trait is implemented for
/// `Context<'ctx>` at one specific lifetime, while the `#[service]` macro
/// generates a handler that must hold for any — so a generic version compiles
/// alone and fails as "implementation of `Component` is not general enough"
/// the moment it is used from a handler. A second context kind, if one is ever
/// needed, is a second type rather than a type parameter.
pub struct HttpComponent<'a, 'ctx> {
    context: &'a Context<'ctx>,
    endpoint: &'a Endpoint,
    retry: RunRetryPolicy,
    client: reqwest::Client,
}

impl<'a, 'ctx> HttpComponent<'a, 'ctx> {
    /// Wraps a Restate context so the driver can call through it.
    ///
    /// One constructor, taking the policy. A convenience `new` that supplied
    /// [`HttpComponent::retry_for`] existed and had no caller — [`Runner`]
    /// passes its own — so it was API that could not be exercised, which is a
    /// different thing from API that is merely unused.
    ///
    /// [`Runner`]: crate::service::Runner
    pub fn new(
        context: &'a Context<'ctx>,
        endpoint: &'a Endpoint,
        retry: RunRetryPolicy,
        client: reqwest::Client,
    ) -> Self {
        HttpComponent {
            context,
            endpoint,
            retry,
            client,
        }
    }

    /// How long one component call may take before the client gives up.
    ///
    /// 300 s, which is the original's own model timeout
    /// (the survey notes) -- a component slower than its contract is
    /// not one this should wait for.
    ///
    /// The retry bound is derived from it by [`HttpComponent::retry_bound`],
    /// so the two cannot drift apart.
    pub const DEFAULT_TIMEOUT_SECS: u64 = 300;

    /// How long the retry loop may run, given how long one attempt may take.
    ///
    /// Twice the timeout, and this is arithmetic rather than taste. `ctx.run`
    /// checks `max_duration <= retry_loop_duration` *before* scheduling the
    /// next attempt,
    /// so at exactly 2x: attempt one ends at `timeout`, which is below the
    /// bound and authorises a second, and that one ends at `2 * timeout` where
    /// the bound is met. Two attempts, and the elapsed time is the bound.
    ///
    /// An earlier version had the two as independent constants -- a 480 s
    /// timeout under a hard-coded 600 s bound -- which authorises a second
    /// attempt at 480 s and runs to roughly **sixteen** minutes while the
    /// prose beside it said ten. Correcting the constant left the hazard
    /// intact, because `PNEUMA_COMPONENT_TIMEOUT_SECS` could reintroduce it
    /// from the environment and nothing enforced the relationship the comment
    /// asserted. Deriving one from the other is what makes the claim true;
    /// a comment saying "these move together" cannot make them.
    ///
    /// Saturating, because the timeout comes from the environment: a
    /// deliberately absurd value should clamp rather than panic at startup.
    pub fn retry_bound(timeout: std::time::Duration) -> std::time::Duration {
        timeout.saturating_mul(2)
    }

    /// The client every component call shares.
    ///
    /// Timeouts are the point. `reqwest::Client::new()` has **no** request or
    /// connect timeout, so a component that accepts the connection and then
    /// never answers hung the call for ever. `RunRetryPolicy::max_duration`
    /// does not save it: that bounds the interval across *attempts*, and an
    /// attempt that never returns never yields to one.
    ///
    /// Measured against `restatedev/restate:1.7.8`: the invoker's
    /// `inactivity-timeout` is 1 m and its `abort-timeout` 10 m, and component
    /// calls of 90 s and 310 s each completed normally and were called exactly
    /// once -- the inactivity timeout does not abort a long `ctx.run`. See
    /// the design notes
    ///
    /// The timeout is a parameter because it is a deployment property, like
    /// `PNEUMA_LISTEN` and `PNEUMA_COMPONENT_ENDPOINT` beside it, and because
    /// hard-coding it made the design notes's advice wrong: raising
    /// Restate's `abort-timeout` cannot help a call this client has already
    /// killed.
    pub fn client(timeout: std::time::Duration) -> Result<reqwest::Client, reqwest::Error> {
        reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(timeout)
            .build()
    }

    /// Bounded by duration, not by attempts, and with real backoff.
    ///
    /// The invoker's own default is to retry **indefinitely**, which for a
    /// transient outage is right and for a component that is simply broken is a
    /// run that never completes and never reports. Bounding by attempts instead
    /// would fail a run over a thirty-second restart, since a handful of
    /// attempts with backoff can elapse in seconds.
    ///
    /// Built from `RunRetryPolicy::default()` rather than `::new()`, and the
    /// difference is not cosmetic: `new()` is `factor: 1.0, max_delay: None`,
    /// so an earlier version of this — `new().initial_delay(100ms)
    /// .max_duration(600s)` — retried at a *constant* 100 ms, roughly six
    /// thousand requests to a failing component over ten minutes, per in-flight
    /// run. It hit hardest on the one path where it is least welcome: a 429 is
    /// classified transient, so a rate-limited component was answered at 10
    /// requests a second for ten minutes. `default()` carries factor 2.0 and a
    /// 2 s ceiling, which is the backoff the comment always claimed.
    ///
    /// The bound is [`HttpComponent::retry_bound`] of the request timeout, so
    /// at the default it is ten minutes -- long enough to ride out a deployment
    /// and short enough that someone hears about a component that is not coming
    /// back -- and at any other timeout it is still exactly two attempts.
    pub fn retry_for(timeout: std::time::Duration) -> RunRetryPolicy {
        RunRetryPolicy::default().max_duration(Self::retry_bound(timeout))
    }
}

impl Component for HttpComponent<'_, '_> {
    type Error = CallError;

    async fn call(&self, dispatch: Dispatch) -> Result<Value, CallError> {
        // Resolved *before* entering `ctx.run`, so a misconfigured template
        // fails immediately rather than being retried by the invoker. A bad
        // address is not a transient fault and retrying it indefinitely is how
        // a deployment mistake becomes a silent hang.
        let url = self.endpoint.url_for(&dispatch.component)?;
        // Serialised here for the same reason the URL is resolved here: the
        // bytes are the same on every attempt, so building them inside the
        // retried closure is work repeated for nothing -- and a request this
        // service cannot build is not a transient fault. `.json()` inside the
        // closure would have folded a serialisation failure into the same
        // `reqwest::Error` as a lost connection, making it retryable, so a
        // payload that will never serialise would be retried for the whole
        // bound and then reported as the component's fault.
        let body = serde_json::to_vec(&dispatch.request)
            .map_err(|error| CallError::unserialisable(&url, error))?;
        // Named for the journal, so an operator reading a run sees which step
        // each entry belongs to rather than a list of anonymous side effects.
        let name = format!("call:{}", dispatch.node_id);

        let client = self.client.clone();
        let response = self
            .context
            .run(move || call_once(client.clone(), url.clone(), body.clone()))
            .retry_policy(self.retry.clone())
            .name(&name)
            .await
            .map_err(|error| CallError::Failed {
                // 500 out of `ctx.run` means the retry loop was exhausted: the
                // SDK wraps a *retryable* failure as `CoreError::new(500, ..)`
                // (`restate-sdk-0.11.1/src/endpoint/context.rs`), and every
                // retryable failure `call_once` can raise is a component that
                // is down, slow, or answering 5xx. Reported as 500 it pages
                // whoever owns this service for something only the component's
                // owner can fix. So it is the component's: 502.
                //
                // That holds because the one failure in that closure which is
                // *this* service's -- a request that cannot be serialised -- is
                // raised terminally with its own 500 and so never reaches the
                // retry loop at all. If anything else retryable is ever added
                // to `call_once`, this arm has to be revisited with it.
                //
                // Every other code is deliberate and survives: 409 for a
                // cancellation, and whatever `classify` decided a permanent
                // answer should be reported as.
                code: match error.code() {
                    500 => 502,
                    code => code,
                },
                message: error.to_string(),
            })?;
        Ok(response.into_inner())
    }
}

/// One HTTP attempt.
///
/// Separate so the closure handed to `ctx.run` stays a call to a named
/// function: the closure is re-run on retry, and anything captured that is not
/// re-derived from its arguments is a way for two attempts to differ.
async fn call_once(
    client: reqwest::Client,
    url: String,
    body: Vec<u8>,
) -> HandlerResult<Json<Value>> {
    // The client is passed in and shared, not built here.
    //
    // It used to be `reqwest::Client::new()` per call, defended by "a pool held
    // across a replay would outlive the journal entry that justified it". That
    // reasoning is wrong: a connection pool is process-level resource state,
    // not journalled state, and it cannot affect replay determinism -- the
    // journal records the *result* of this closure, not how the socket was
    // obtained. The cost of being wrong was a fresh TCP connection and a fresh
    // rustls config for every step of every concurrent run.
    // The body arrives already serialised, so nothing this service could get
    // wrong happens inside the retry loop -- see `Component::call`.
    let response = client
        .post(&url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await?;
    match classify(&url, response.status()) {
        Answer::Body => {}
        Answer::Permanent { code, message } => {
            return Err(TerminalError::new_with_code(code, message).into())
        }
        Answer::Transient(message) => return Err(transient(message)),
    }
    // Read and parse are separated deliberately. `Response::json` is
    // `bytes().await?` followed by `serde_json::from_slice`, and reqwest maps
    // both failures to the same error kind -- so a connection reset partway
    // through a 200 is indistinguishable from malformed JSON. Collapsing them
    // made a component whose pod was killed mid-response fail *terminally*,
    // which is exactly the transient case the retry policy exists for.
    let body = response
        .bytes()
        .await
        .map_err(|error| transient(format!("reading the body from {url} failed: {error}")))?;
    // Only this half is the component breaking its contract, and it will break
    // it the same way next time.
    let parsed = serde_json::from_slice::<Value>(&body).map_err(|error| {
        // 502: the component answered, and answered something the contract
        // cannot use. Not 500, which would say this service broke.
        TerminalError::new_with_code(
            502,
            format!("component at {url} answered a body that is not JSON: {error}"),
        )
    })?;
    Ok(Json::from(parsed))
}

/// Wraps a message as a failure Restate should retry.
///
/// A function because the two callers -- a transient status and a body that
/// could not be read -- must produce the same kind of error, and because the
/// `anyhow!` invocation inside a closure is a line tarpaulin does not attribute
/// to the closure's caller. Written inline, the body-read arm was exercised by
/// a test that proved the retry happened and still reported as uncovered.
fn transient(message: String) -> HandlerError {
    HandlerError::from(anyhow::anyhow!(message))
}

/// What a component's status code means for trying again.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Answer {
    /// There is a body worth parsing.
    Body,
    /// The component will answer the same way next time.
    ///
    /// The code is what the run should fail with, which is not always the one
    /// the component sent: a component's 4xx becomes this run's 4xx, because
    /// the submission is what is wrong, while a shape the contract forbids is
    /// a 502 -- the component answered, and answered something invalid.
    Permanent {
        /// The status to report the run's failure under.
        code: u16,
        /// What went wrong.
        message: String,
    },
    /// Worth another attempt.
    Transient(String),
}

/// Classifies a component's status.
///
/// The distinction decides whether a run finishes. `ctx.run` retries a
/// non-terminal failure under the invoker's policy, which **by default retries
/// indefinitely** -- so calling a permanent fault "merely wasteful" and
/// returning it as retryable is not wasteful at all, it is a run that never
/// completes and never reports. That is what this used to do for every non-2xx.
///
/// A function rather than a branch inside the request, so every arm has a test.
/// Reaching them through a live server would mean a component that fails, and
/// an indefinitely retried failure is a hanging test rather than a failing one.
fn classify(url: &str, status: reqwest::StatusCode) -> Answer {
    // `204 No Content` is a success with nothing in it, and the contract is
    // that a component answers with a `step_output`. Accepting it and
    // then failing to parse an empty body reports the wrong cause.
    if status == reqwest::StatusCode::NO_CONTENT {
        return Answer::Permanent {
            // The component answered, and answered something the contract
            // forbids: a bad gateway, not a bad request.
            code: 502,
            message: format!(
                "component at {url} answered 204 with no body, but a step output is required"
            ),
        };
    }
    if status.is_success() {
        return Answer::Body;
    }
    // The two 4xx that genuinely mean "later": a timeout and a rate limit.
    if status == reqwest::StatusCode::REQUEST_TIMEOUT
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
    {
        return Answer::Transient(format!("component at {url} answered {status}"));
    }
    if status.is_client_error() {
        return Answer::Permanent {
            // The component's own refusal, carried through. A 400 from a model
            // means the submission is wrong, and reporting it as a 500 tells
            // the submitter to retry something that cannot succeed.
            code: match status {
                // Three the transport speaks for itself, so a component
                // answering them would be read as the transport answering.
                //
                // 409 is how the SDK reports a **cancelled** invocation
                // (`restate-sdk-0.11.1/src/endpoint/context.rs`), and by the
                // time either reaches the handler the two are the same shape --
                // so carrying a component's 409 through would make "a human
                // stopped this run" and "the component said Conflict"
                // indistinguishable, which is the ambiguity this exists to
                // remove.
                //
                // 401 and 403 are the ingress's own: a client, proxy or SDK
                // seeing either from `POST /PneumaRunner/run` concludes that
                // *its* credentials to Restate were refused and re-authenticates
                // -- when the truth is that a model refused the submission.
                //
                // All three are answered 502 instead, and the component's own
                // status stays in the message.
                reqwest::StatusCode::CONFLICT
                | reqwest::StatusCode::UNAUTHORIZED
                | reqwest::StatusCode::FORBIDDEN => 502,
                // 404 *is* carried through. It collides with the ingress's own
                // "invocation not found", but that answer carries
                // `"source":"ingress"` while every handler failure carries
                // `"source":"invocation"` (the design notes), so the two are
                // distinguishable without spending the status.
                other => other.as_u16(),
            },
            message: format!(
                "component at {url} answered {status}, which will not change on a retry"
            ),
        };
    }
    if status.is_server_error() {
        return Answer::Transient(format!("component at {url} answered {status}"));
    }
    // A 3xx, or anything else. The earlier reasoning here was backwards: it
    // said reqwest follows redirects so a 3xx means the limit was hit, and
    // classified it transient. reqwest's default policy is `limited(10)` and
    // *exceeding* it returns an `Err` from `send()` rather than a 3xx response
    // -- so a 3xx arriving here is one reqwest declined to follow at all: a
    // 304, a redirect with no `Location`, an https-to-http downgrade. Those are
    // configuration, and retrying them for the full ten minutes is the costly
    // way to find out.
    Answer::Permanent {
        code: 502,
        message: format!(
            "component at {url} answered {status}, which is not a response this \
             contract can use"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(code: u16) -> reqwest::StatusCode {
        match reqwest::StatusCode::from_u16(code) {
            Ok(status) => status,
            Err(error) => panic!("{code} is a status: {error}"),
        }
    }

    #[test]
    fn the_retry_bound_authorises_exactly_two_attempts() {
        use std::time::Duration;
        // The whole reason this is a function rather than a constant. `ctx.run`
        // compares `max_duration <= retry_loop_duration` *before* scheduling
        // the next attempt, so the bound must be reached exactly at the end of
        // the second attempt: one attempt shorter and a third is authorised,
        // one longer and the second never runs.
        for seconds in [1_u64, 45, HttpComponent::DEFAULT_TIMEOUT_SECS, 3_600] {
            let timeout = Duration::from_secs(seconds);
            let bound = HttpComponent::retry_bound(timeout);
            assert!(
                timeout < bound,
                "{seconds}s: a second attempt is authorised"
            );
            assert!(
                bound <= timeout + timeout,
                "{seconds}s: a third attempt is not"
            );
        }
        // At the default the bound is the ten minutes the policy documents.
        assert_eq!(
            HttpComponent::retry_bound(Duration::from_secs(HttpComponent::DEFAULT_TIMEOUT_SECS)),
            Duration::from_secs(600)
        );
        // Saturating rather than panicking: the timeout comes from the
        // environment, and an absurd value should clamp at startup, not abort.
        assert_eq!(
            HttpComponent::retry_bound(Duration::MAX),
            Duration::MAX,
            "an absurd timeout clamps"
        );
    }

    #[test]
    fn a_transient_failure_keeps_its_message() {
        // Both callers -- a 5xx status and a body that could not be read --
        // funnel through here, so the message an operator sees comes from one
        // place.
        let rendered = format!("{:?}", transient("component at c answered 503".to_owned()));
        assert!(rendered.contains("component at c"), "{rendered}");
        assert!(rendered.contains("503"), "{rendered}");
    }

    #[test]
    fn a_success_with_a_body_is_parsed() {
        for code in [200, 201, 202, 299] {
            assert_eq!(classify("http://c", status(code)), Answer::Body, "{code}");
        }
    }

    #[test]
    fn a_204_is_permanent_because_a_step_output_is_required() {
        // Accepted once, and then the empty body failed JSON parsing into a
        // retryable error -- so a component answering 204 hung the run and
        // reported the wrong cause when it did.
        let Answer::Permanent { code, message } = classify("http://c", status(204)) else {
            panic!("204 carries no step output");
        };
        assert!(message.contains("204"), "{message}");
        assert!(message.contains("step output"), "{message}");
        // 502, not 204 carried through and not 500: the component answered,
        // and answered something the contract cannot use.
        assert_eq!(code, 502);
    }

    #[test]
    fn an_ordinary_client_error_is_permanent_rather_than_retried_forever() {
        for code in [400, 404, 422, 410] {
            let Answer::Permanent {
                code: reported,
                message,
            } = classify("http://c", status(code))
            else {
                panic!("{code} will not change on a retry");
            };
            assert!(message.contains(&code.to_string()), "{message}");
            // Carried through rather than flattened. A 400 from a model means
            // the submission is wrong; reporting 500 tells whoever submitted
            // it to retry something that cannot ever succeed.
            assert_eq!(reported, code, "the component's own status survives");
        }
    }

    #[test]
    fn the_three_statuses_the_transport_speaks_for_itself_are_not_passed_on() {
        // 409 is how the SDK reports a cancelled invocation, so a component's
        // `409 Conflict` would be read as a run someone deliberately stopped.
        // 401 and 403 are the ingress's own, so a client seeing either from
        // `POST /PneumaRunner/run` re-authenticates against Restate when the
        // truth is that a model refused the submission. All three are the
        // component's fault, so 502 -- and the status it actually sent is
        // still in the message.
        for sent in [409, 401, 403] {
            let Answer::Permanent { code, message } = classify("http://c", status(sent)) else {
                panic!("{sent} will not change on a retry");
            };
            assert_eq!(code, 502, "{sent} is answered as the component's fault");
            assert!(
                message.contains(&sent.to_string()),
                "and {sent} is still in the message: {message}"
            );
        }
    }

    #[test]
    fn a_request_this_service_cannot_build_is_this_services_fault() {
        // Unreachable through `call` -- `ComponentRequest` is `serde_json::Value`
        // underneath and a `Value` always serialises -- which is why the
        // decision lives in a constructor rather than an inline closure.
        // Unreachable is not unexamined: what such a failure means is a choice,
        // and it is 500, because neither the component nor the submitter had
        // anything to do with it.
        let Err(source) = serde_json::from_str::<u8>("999") else {
            panic!("999 is not a u8");
        };
        let error = CallError::unserialisable("http://c/predict", source);
        assert_eq!(error.status(), 500);
        let rendered = error.to_string();
        assert!(rendered.contains("http://c/predict"), "{rendered}");
        assert!(rendered.contains("serialised"), "{rendered}");
    }

    #[test]
    fn an_unaddressable_component_is_the_submissions_fault_not_this_services() {
        // The two errors `url_for` can actually raise, which is the point:
        // both are properties of the submitted pipeline, so both are 400. An
        // earlier version answered 500 here, reasoning about `NoPlaceholder`
        // -- a variant `Endpoint::new` refuses at startup and that therefore
        // never reaches this code, so the justification was about the one case
        // that cannot happen.
        for error in [
            EndpointError::EmptyComponent,
            EndpointError::UnsafeComponent {
                component: "a&admin=1".to_owned(),
                character: '&',
            },
        ] {
            assert_eq!(
                CallError::Endpoint(error).status(),
                400,
                "a step that cannot be addressed is the request's fault"
            );
        }
        assert_eq!(
            CallError::Failed {
                code: 503,
                message: "x".to_owned()
            }
            .status(),
            503
        );
    }

    #[test]
    fn a_timeout_or_a_rate_limit_is_transient_even_though_it_is_a_4xx() {
        // The two 4xx that mean "later" rather than "no".
        for code in [408, 429] {
            assert!(
                matches!(classify("http://c", status(code)), Answer::Transient(_)),
                "{code} is worth another attempt"
            );
        }
    }

    #[test]
    fn a_redirect_is_permanent_because_reqwest_declined_to_follow_it() {
        // Not "the redirect limit was hit" -- exceeding the limit is an `Err`
        // from `send()`, not a status. A 3xx here is one reqwest would not
        // follow, which is configuration and will not change on a retry.
        for code in [300, 301, 304, 308] {
            assert!(
                matches!(classify("http://c", status(code)), Answer::Permanent { .. }),
                "{code} is configuration, not a transient fault"
            );
        }
    }

    #[test]
    fn a_server_error_is_transient() {
        for code in [500, 502, 503, 504] {
            let Answer::Transient(message) = classify("http://c", status(code)) else {
                panic!("{code} may heal");
            };
            assert!(message.contains("http://c"), "{message}");
        }
    }

    #[test]
    fn every_message_names_the_component_and_the_status() {
        // What an operator reads in a journal entry. "The call failed" without
        // saying which component or what it answered is the report that sends
        // someone reading logs.
        for code in [204, 400, 429, 503] {
            let message = match classify("http://comp.svc/predict", status(code)) {
                Answer::Body => panic!("{code} is not a body"),
                Answer::Permanent { message, .. } | Answer::Transient(message) => message,
            };
            assert!(message.contains("http://comp.svc/predict"), "{message}");
            assert!(message.contains(&code.to_string()), "{message}");
        }
    }
}

//! The durable handler: one invocation is one run.
//!
//! `spikes/restate/VERDICT.md` §2 is what makes this short. The whole run
//! executes inside a single replayed handler, so a fan-in is "have all
//! prerequisites arrived in this map yet" — a question the interpreter already
//! answers in memory. There is no barrier table, no `$addToSet`, no refcount
//! and no outbox, because there are no distributed workers to coordinate.

use pneuma_core::node::Pipeline;
use pneuma_proto::meta::Meta;
use pneuma_runner::{DriveError, RunInput, ScheduleError};
use restate_sdk::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::component::{CallError, HttpComponent};
use crate::endpoint::Endpoint;
#[cfg(test)]
use crate::endpoint::EndpointError;

/// What starts a run.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RunRequest {
    /// The pipeline definition, sent with the request rather than looked up.
    ///
    /// Deliberate: a handler that fetched a definition by id would read
    /// mutable state outside the journal, and a definition edited between the
    /// original execution and a replay would silently change what the replay
    /// replays. Sending it makes the definition part of the invocation.
    pub pipeline: Pipeline,
    /// The controller envelope.
    pub meta: Meta,
    /// What the first steps receive.
    pub input: Value,
    /// Caller passthrough, forwarded to every component.
    #[serde(default)]
    pub custom_data: Option<Value>,
}

/// What a finished run reports.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RunReport {
    /// Every top-level step nothing follows, with its output, in `NodeId`
    /// order.
    pub outputs: Vec<(String, Value)>,
    /// Every step that ran, in dispatch order.
    pub ran: Vec<String>,
}

/// The pipeline runner.
pub struct Runner {
    endpoint: Endpoint,
    retry: restate_sdk::context::RunRetryPolicy,
    timeout: std::time::Duration,
    /// Built once and cloned per call. `reqwest::Client` is an `Arc` inside, so
    /// cloning shares the connection pool rather than duplicating it.
    client: reqwest::Client,
    /// Where a run's steps are written down.
    ///
    /// Held here for the same reason `client` is: built once at startup, cloned
    /// per invocation, and a `PgPool` is an `Arc` inside so the clone shares
    /// the connections rather than opening more.
    mirror: pneuma_mirror::Mirror,
}

impl Runner {
    /// Builds a runner that calls components through `endpoint`, with
    /// [`HttpComponent::retry_for`] the default timeout.
    pub fn new(endpoint: Endpoint, mirror: pneuma_mirror::Mirror) -> Result<Self, reqwest::Error> {
        let timeout = std::time::Duration::from_secs(HttpComponent::DEFAULT_TIMEOUT_SECS);
        Self::with_retry(endpoint, HttpComponent::retry_for(timeout), timeout, mirror)
    }

    /// Where this runner resolves components.
    ///
    /// A read accessor so startup can be asserted end to end: that
    /// `PNEUMA_COMPONENT_ENDPOINT` is what the runner will actually dial,
    /// rather than merely that `configuration` returned `Ok`.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// How long one component call may take.
    ///
    /// Read back for the same reason as [`Runner::endpoint`]: so startup can be
    /// asserted as what the runner will actually do, rather than as
    /// `configuration` having returned `Ok`.
    pub fn timeout(&self) -> std::time::Duration {
        self.timeout
    }

    /// Where this runner writes a run down.
    ///
    /// Read back so startup can prove the mirror is usable before the service
    /// binds — see [`crate::serve::connect`], which asks it whether `node_run`
    /// is there at all.
    pub fn mirror(&self) -> &pneuma_mirror::Mirror {
        &self.mirror
    }

    /// The retry policy each component call runs under.
    ///
    /// Read back so a test can assert that the bound *follows the timeout*
    /// rather than sitting beside it as a constant. `RunRetryPolicy` is
    /// opaque, so the comparison is on its `Debug`, which is what the SDK
    /// exposes.
    pub fn retry(&self) -> &restate_sdk::context::RunRetryPolicy {
        &self.retry
    }

    /// The same, with a retry policy and a request timeout of the caller's
    /// choosing.
    ///
    /// Both bounds are deployment questions — how long a component may
    /// plausibly be down, and how long one call may take — so neither is this
    /// crate's to fix. They are also what let a test observe a component
    /// recovering, or a call being cut off, without waiting out the defaults.
    pub fn with_retry(
        endpoint: Endpoint,
        retry: restate_sdk::context::RunRetryPolicy,
        timeout: std::time::Duration,
        mirror: pneuma_mirror::Mirror,
    ) -> Result<Self, reqwest::Error> {
        Ok(Runner {
            endpoint,
            retry,
            timeout,
            client: HttpComponent::client(timeout)?,
            mirror,
        })
    }
}

/// The generated service surface.
///
/// A module of its own solely so `#![allow(missing_docs)]` can be scoped to
/// what the macro emits. `#[restate_sdk::service]` expands into a trait and a
/// client alongside the impl, documenting neither, and an `#[allow]` on the
/// impl does not reach siblings. Everything hand-written in this file stays
/// held to `missing_docs`.
mod generated {
    #![allow(missing_docs)]

    use super::{terminal, HttpComponent, RunInput, RunReport, RunRequest, Runner};
    use pneuma_core::resolver::resolve;
    use restate_sdk::prelude::*;

    #[restate_sdk::service(name = "PneumaRunner")]
    impl Runner {
        /// Runs one pipeline to completion.
        #[handler]
        async fn run(
            &self,
            ctx: Context<'_>,
            req: Json<RunRequest>,
        ) -> HandlerResult<Json<RunReport>> {
            let req = req.into_inner();

            // Resolution is pure and deterministic, so it is *not* wrapped in
            // `ctx.run`: journalling it would store a value the replay can derive,
            // and a failure here is the definition being wrong, which retrying
            // cannot fix. `TerminalError` says exactly that to the invoker.
            // 400: the definition arrives in the request body, so a pipeline
            // that does not resolve is the submission being wrong. Answering
            // 500 would tell the submitter that this service had broken and
            // invite a retry of something that fails identically for ever.
            let registry = resolve(&req.pipeline).map_err(|error| {
                TerminalError::new_with_code(400, format!("cannot resolve the pipeline: {error}"))
            })?;

            let component = HttpComponent::new(
                &ctx,
                &self.endpoint,
                self.retry.clone(),
                self.client.clone(),
            );
            let recorder = crate::journal::Journalled::new(&ctx, self.mirror.clone());
            let run = RunInput {
                meta: req.meta,
                input: req.input,
                custom_data: req.custom_data,
            };

            let completed = pneuma_runner::drive_recording(&registry, &run, &component, &recorder)
                .await
                .map_err(terminal)?;

            Ok(Json::from(RunReport {
                outputs: completed
                    .outputs
                    .into_iter()
                    .map(|(node, value)| (node.to_string(), value))
                    .collect(),
                ran: completed.ran.iter().map(ToString::to_string).collect(),
            }))
        }
    }
}

pub use generated::*;

/// Maps a drive failure onto what the invoker should do about it.
///
/// Always terminal, and that is a conclusion rather than a shortcut.
///
/// The first version split these into terminal and retryable, and classified
/// `DriveError::Call` as retryable. That was wrong on its own premise:
/// `ContextSideEffects::run` returns `Result<T, TerminalError>`, so the only
/// error a component call can *deliver* here is one Restate has already
/// retried under the invoker's policy and given up on — or a cancellation.
/// Handing it back as retryable asked the invoker to retry the whole
/// invocation over a failure it had just finished exhausting, and since the
/// default policy retries indefinitely, that is a run that never completes.
///
/// It was also inconsistent with itself: a step with no component name was
/// terminal, while a component name that could not be turned into a URL —
/// `CallError::Endpoint`, raised *before* `ctx.run` and equally deterministic —
/// went back as retryable. `component.rs` said in as many words that a bad
/// address must not be retried indefinitely, and this undid it.
///
/// Where retrying genuinely helps is one layer down, inside `ctx.run`, where
/// [`crate::component`] separates a component's transient answers from its
/// permanent ones. By the time a failure reaches here, that has already
/// happened.
///
/// Terminal, but not always 500. `TerminalError::new` is
/// `new_with_code(500, ...)`, and flattening everything to it loses two
/// distinctions the caller needs: a **cancellation**, which the SDK delivers as
/// a `TerminalError` with code 409, becomes indistinguishable from the run
/// having broken -- so a dispatcher retries what a human deliberately stopped
/// -- and a component's own 4xx, which says the *submission* is wrong, becomes
/// a 500 that says this service is. Only the component call carries a status
/// worth preserving; everything else here is the pipeline definition or this
/// service, and 500 is the honest answer for those.
///
/// Concrete over [`CallError`] rather than generic over the component error.
/// It was generic, and a generic version cannot ask what status a failure
/// should be reported under -- the type it would have to ask is the one that
/// knows. One production caller, one component implementation, and the tests
/// are better for using the real error type rather than a stand-in.
fn terminal(error: DriveError<CallError>) -> HandlerError {
    TerminalError::new_with_code(status_of(&error), error.to_string()).into()
}

/// Whose fault a failed run was, as a status.
///
/// Three answers, and one rule -- *who has to do something about it*:
///
/// - **400**, the submission. The definition names a step that is not there,
///   or a step names no component, or names one that cannot be addressed.
///   Resubmitting the same request fails identically, and nobody operating
///   this service can help.
/// - **502**, the component. It answered, and answered something the contract
///   cannot use -- no `step_output`, or a scalar where a join needs an object.
/// - **500**, this service or this deployment. The honest default, and what
///   the remaining cases are.
///
/// A component's own status is carried through ahead of all three, because a
/// component that says 400 has already answered the question.
///
/// The rule is applied here rather than at each site because it was applied at
/// some of them and not others, which is worse than not applying it at all: a
/// component returning `<html>` failed 502 while the same component returning
/// `{"foo":1}` failed 500, so two neighbouring contract violations sent an
/// operator to two different places.
fn status_of(error: &DriveError<CallError>) -> u16 {
    match error {
        DriveError::Call { source, .. } => source.status(),
        // The component answered valid JSON with no usable `step_output`.
        DriveError::Response { .. } => 502,
        // A prerequisite's *output* was not an object. A component returning a
        // scalar `step_output` is legal by the component contract and is
        // forwarded by the original executor (see the protocol notes), so
        // this fires at the join rather than where it entered -- but it is
        // still the component's answer that cannot be used.
        DriveError::Schedule(ScheduleError::UnmergeablePrerequisite { .. }) => 502,
        // A step in the submitted definition with nowhere to send it.
        DriveError::NoComponent { .. } => 400,
        // Everything else: the scheduler and the registry disagreeing, a run
        // that wedged, an internal inconsistency. This service's problem.
        _ => 500,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pneuma_core::ids::NodeId;

    #[tokio::test]
    async fn the_default_runner_derives_its_bound_from_its_timeout() {
        // `Runner::new` is the production constructor and the tests all use
        // `with_retry`, so without this the default path ships unexercised.
        let Ok(endpoint) = Endpoint::new("http://c/{component}") else {
            panic!("valid template");
        };
        // A lazy pool pointed nowhere: this asserts the retry bound, and the
        // mirror is a field the constructor carries rather than anything it
        // decides. `connect_lazy` still wants a runtime to hang its idle
        // reaper on, which is why this test is async.
        let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://nobody:nothing@127.0.0.1:1/nowhere")
        else {
            panic!("a lazy pool does not connect yet");
        };
        let mirror = pneuma_mirror::Mirror::new(pool, "test");
        let Ok(runner) = Runner::new(endpoint, mirror) else {
            panic!("the default client must build");
        };
        // Bounded by duration rather than attempts: a handful of attempts with
        // backoff can elapse in seconds, which would fail a run over a routine
        // restart.
        assert_eq!(
            format!("{:?}", runner.retry),
            format!(
                "{:?}",
                HttpComponent::retry_for(std::time::Duration::from_secs(
                    HttpComponent::DEFAULT_TIMEOUT_SECS
                ))
            ),
            "the default constructor uses the documented bound"
        );
    }

    use pneuma_proto::component::ComponentResponseError;

    #[test]
    fn every_failure_names_the_step_it_happened_at() {
        // `HandlerError`'s inner is private to the SDK, so what is assertable
        // is what an operator actually reads: the message. Each of these must
        // say which step, because finding that is the whole difficulty.
        let cases: Vec<(DriveError<CallError>, &str)> = vec![
            (
                DriveError::Schedule(ScheduleError::UnknownStep {
                    node_id: NodeId::new("sched"),
                }),
                "sched",
            ),
            (
                DriveError::UnknownStep {
                    node_id: NodeId::new("ghost"),
                },
                "ghost",
            ),
            (
                DriveError::NoComponent {
                    node_id: NodeId::new("nameless"),
                },
                "nameless",
            ),
            (
                DriveError::Response {
                    node_id: NodeId::new("badshape"),
                    source: ComponentResponseError::NoStepOutput,
                },
                "badshape",
            ),
            (
                DriveError::Call {
                    node_id: NodeId::new("unreachable"),
                    source: CallError::Failed {
                        code: 500,
                        message: "transport died".to_owned(),
                    },
                },
                "unreachable",
            ),
            (
                DriveError::Stalled {
                    blocked: vec![(pneuma_interpreter::FrameId::ROOT, NodeId::new("wedged"))],
                },
                "wedged",
            ),
        ];
        for (error, expected) in cases {
            let rendered = format!("{:?}", terminal(error));
            assert!(rendered.contains(expected), "the step is named: {rendered}");
        }
    }

    #[test]
    fn a_component_failure_is_reported_under_its_own_status() {
        // Not every terminal failure is a 500, and the two that are not matter
        // most. A cancellation arrives as code 409, and reported as 500 it is
        // indistinguishable from the run having broken -- so a dispatcher
        // retries what a human deliberately stopped. A component's own 4xx
        // says the submission is wrong, and 500 says this service is.
        //
        // `HandlerError`'s inner is private to the SDK, so what is assertable
        // is the `Debug`, which renders the code.
        for (code, expected) in [(409_u16, "409"), (400, "400"), (502, "502"), (500, "500")] {
            let error = DriveError::Call {
                node_id: NodeId::new("A"),
                source: CallError::Failed {
                    code,
                    message: "no".to_owned(),
                },
            };
            let rendered = format!("{:?}", terminal(error));
            assert!(
                rendered.contains(expected),
                "code {code} survives: {rendered}"
            );
        }

        // The scheduler and the registry disagreeing is this service's, and
        // 500 is the honest answer for it.
        let internal: DriveError<CallError> = DriveError::UnknownStep {
            node_id: NodeId::new("ghost"),
        };
        assert!(
            format!("{:?}", terminal(internal)).contains("500"),
            "an internal inconsistency is this service's 500"
        );
    }

    #[test]
    fn a_bad_submission_and_a_bad_answer_are_told_apart() {
        // The rule `status_of` exists to apply consistently: 400 for the
        // request, 502 for the component, 500 for this service. Applying it at
        // some sites and not others was worse than not applying it -- a
        // component returning `<html>` failed 502 while the same component
        // returning `{"foo":1}` failed 500, two neighbouring contract
        // violations pointing at two different people.
        let cases: Vec<(DriveError<CallError>, u16, &str)> = vec![
            (
                // A step naming a component that cannot be put in a URL. Both
                // errors `url_for` can raise are the *submitted pipeline's*.
                DriveError::Call {
                    node_id: NodeId::new("A"),
                    source: CallError::Endpoint(EndpointError::UnsafeComponent {
                        component: "a&admin=1".to_owned(),
                        character: '&',
                    }),
                },
                400,
                "unaddressable component",
            ),
            (
                DriveError::Call {
                    node_id: NodeId::new("A"),
                    source: CallError::Endpoint(EndpointError::EmptyComponent),
                },
                400,
                "no component name in the URL",
            ),
            (
                DriveError::NoComponent {
                    node_id: NodeId::new("A"),
                },
                400,
                "a step with nowhere to send it",
            ),
            (
                DriveError::Response {
                    node_id: NodeId::new("A"),
                    source: ComponentResponseError::NoStepOutput,
                },
                502,
                "valid JSON the contract cannot use",
            ),
            (
                DriveError::Schedule(ScheduleError::UnmergeablePrerequisite {
                    from: NodeId::new("A"),
                    into: NodeId::new("B"),
                    kind: "string",
                }),
                502,
                "a scalar step_output at a join",
            ),
            (
                DriveError::Stalled {
                    blocked: vec![(pneuma_interpreter::FrameId::ROOT, NodeId::new("wedged"))],
                },
                500,
                "a wedged run",
            ),
        ];
        for (error, expected, what) in cases {
            assert_eq!(status_of(&error), expected, "{what}");
        }
    }
}

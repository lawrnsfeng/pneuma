//! Deciding whether a submission may be queued at all.
//!
//! Pure: no database, no HTTP, no clock. Everything here is a property of the
//! request, so the whole rule is a unit test and the ingress that calls it has
//! nothing left to decide.

use pneuma_core::ids::{JobId, RunId, TenantId};
use pneuma_core::node::Pipeline;
use pneuma_core::resolver::{resolve, ResolveError};
use pneuma_fairness::{Dimension, FlowKey, FlowKeyError};
use pneuma_proto::meta::Meta;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The dimension name a tenant is classified by.
///
/// A constant rather than a caller's string because it is half of every flow
/// key this service writes: two spellings would partition the same tenants into
/// two flows, each getting its own quota, which is the unfairness the whole
/// mechanism exists to prevent.
pub const TENANT_DIMENSION: &str = "tenant";

/// What a caller submits.
///
/// The same shape `pneuma-restate`'s handler takes (`RunRequest`, in that
/// crate's `service.rs`), because this forwards it verbatim: a submission that
/// arrived here and a submission that reaches the runner must be the same
/// thing, or the resolution done at this door says nothing about the run that
/// eventually happens.
///
/// **Nothing enforces that today.** There is no shared type and no dependency
/// between the two crates, so a field added to one leaves this accepting bodies
/// the runner rejects. What pins it is
/// `admission_accepts_the_body_the_runner_takes` below, which drives one
/// literal document through this type -- and, when it exists, the end-to-end
/// test that submits through both. A shared crate would be the real fix and is
/// not worth one before there is a second caller.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Submission {
    /// The pipeline definition, sent with the request rather than looked up.
    pub pipeline: Pipeline,
    /// The controller envelope, carrying the tenant and the job.
    pub meta: Meta,
    /// What the first steps receive.
    pub input: Value,
    /// Caller passthrough, forwarded to every component.
    #[serde(default)]
    pub custom_data: Option<Value>,
}

/// A submission that may be queued, with what the queue needs to file it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admitted {
    /// The run this becomes. The queue's primary key, and what makes a
    /// redelivery a no-op rather than a second run.
    pub run_id: RunId,
    /// The tenant, as the queue stores it.
    pub tenant_id: TenantId,
    /// The flow this competes in for a share of each dispatch round.
    ///
    /// The submission queue stores and groups by `tenant_id`, not by this. The
    /// two are the same partition **only while `tenant` is the sole
    /// dimension**, because a one-dimension key is injective in its value. Add
    /// a second dimension and they diverge: the queue would keep grouping by
    /// tenant while this claims a finer classification, and `select_batch`
    /// would compute quotas for flows the backlog never separated. At that
    /// point the key has to be stored alongside the row.
    pub flow: FlowKey,
}

/// Why a submission was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Rejected {
    /// The pipeline definition does not describe a runnable pipeline.
    #[error("the pipeline cannot be resolved: {0}")]
    Pipeline(#[from] ResolveError),

    /// The tenant id is blank.
    ///
    /// Refused here because nothing below refuses it. `TenantId::new` takes any
    /// string, `Dimension::new` validates only the dimension *name* and escapes
    /// the value, and `FlowKey::new` fails only on an empty dimension list — so
    /// a blank id renders the perfectly valid key `tenant=` and every
    /// submission missing a tenant is silently merged into one flow, sharing
    /// one quota. That is the exact failure fair dispatch exists to prevent,
    /// arriving through the one field nobody validates.
    ///
    /// Whitespace is blank here. `FlowKey` escapes `\` and `|` and passes
    /// everything else through unchanged -- there is no percent-encoding -- so
    /// `"  "` renders `tenant=  `, which is neither the blank case nor any real
    /// tenant, and differs from every other run of spaces.
    #[error("the tenant id is blank, which would put every such run in one flow")]
    BlankTenant,

    /// The job id is blank.
    ///
    /// Worse than a blank tenant, and refused for the same reason: nothing
    /// below refuses it. `JobId::new` takes any string and `RunId::from_job`
    /// copies it, and the run id is the submission queue's **primary key**
    /// under `ON CONFLICT DO NOTHING`. So the first blank-job submission is
    /// queued and every later one -- from any tenant, for any pipeline -- comes
    /// back `AlreadyQueued` and is discarded as a redelivery of unrelated work.
    /// A merged quota is unfair; this drops runs.
    #[error("the job id is blank, and it is what the run is identified by")]
    BlankJob,

    /// The flow key could not be built.
    ///
    /// Not reachable through [`accept`], which always supplies exactly one
    /// dimension with a valid name — but `FlowKey`'s failures are its own to
    /// define, and swallowing one here would mean this function silently
    /// stopped classifying if that ever changed.
    #[error("the flow key could not be built: {0}")]
    Flow(#[from] FlowKeyError),
}

/// Decides whether a submission may be queued.
///
/// Two questions, both answerable from the request alone.
pub fn accept(submission: &Submission) -> Result<Admitted, Rejected> {
    // Resolved first, because a definition that cannot run is wrong regardless
    // of who sent it, and reporting the tenant problem for a pipeline that
    // would never have run sends the caller to fix the wrong thing.
    resolve(&submission.pipeline)?;

    // Trimmed *and then used*, not trimmed only to test. `FlowKey` escapes `\`
    // and `|` and passes everything else through verbatim, so `"acme "` renders
    // the key `tenant=acme ` -- a different flow from `tenant=acme`, and a
    // different `tenant_id` for the queue to group by. One tenant split in two
    // by a trailing newline out of a template is the same per-tenant split fair
    // dispatch exists to prevent, so the artefact is removed rather than
    // carried.
    let tenant = TenantId::new(submission.meta.tenant_id.as_str().trim());
    if tenant.as_str().is_empty() {
        return Err(Rejected::BlankTenant);
    }
    // The same treatment, for the same reason and with a sharper consequence:
    // this becomes the queue's primary key, so `"job-1\n"` and `"job-1"` would
    // be two runs of one job rather than one run submitted twice.
    let job = JobId::new(submission.meta.job_id.as_str().trim());
    if job.as_str().is_empty() {
        return Err(Rejected::BlankJob);
    }

    let flow = FlowKey::new(&[Dimension::new(TENANT_DIMENSION, tenant.as_str())?])?;
    Ok(Admitted {
        run_id: RunId::from_job(&job),
        tenant_id: tenant,
        flow,
    })
}

//! What may be queued, and what must not be.
//!
//! Pure, so these need no database and no server. The fixture is a real corpus
//! pipeline, because a resolution check that only ever sees hand-written
//! definitions is a check against this file's idea of a pipeline rather than
//! against the ones production sends.
//!
//! **Vendored** under `tests/fixtures/`, copied from
//! the original at commit
//! `0397327144dcd006cbd783de5f39b053a93f6341`, which is the convention
//! `pneuma-core/tests/fixtures` already established. It was an `include_str!`
//! reaching four directories up into the sibling checkout, and that compiles
//! only on a machine that happens to have one: `include_str!` resolves at build
//! time, so CI -- which checks out this repository and nothing else -- could not
//! build the test target at all, and the failure would have taken out `lint`
//! and every coverage job behind it. Green here and red everywhere else is the
//! failure this repository's gate exists to make impossible, arriving through a
//! path nothing checks.

use pneuma_admission::{accept, Rejected, Submission, TENANT_DIMENSION};
use serde_json::json;

/// A submission carrying a real corpus pipeline.
fn submission(tenant: &str) -> Submission {
    let yaml = include_str!("fixtures/pipeline1.yaml");
    let Ok(pipeline) = serde_yaml::from_str(yaml) else {
        panic!("the corpus fixture is a pipeline");
    };
    let meta = json!({
        "job_id": "job-1",
        "tenant_id": tenant,
        "pipeline_type": "invoice",
        "pipeline_level": "page",
        "pipeline_name": "default",
    });
    let Ok(meta) = serde_json::from_value(meta) else {
        panic!("that is a meta");
    };
    Submission {
        pipeline,
        meta,
        input: json!({"doc": "d"}),
        custom_data: None,
    }
}

#[test]
fn a_real_pipeline_from_a_real_tenant_is_admitted() {
    let Ok(admitted) = accept(&submission("acme")) else {
        panic!("a corpus pipeline resolves");
    };
    assert_eq!(admitted.tenant_id.as_str(), "acme");
    // The run id is the job id today, through the one place that assumption
    // lives (`RunId::from_job`).
    assert_eq!(admitted.run_id.as_str(), "job-1");
    assert_eq!(
        admitted.flow.as_str(),
        format!("{TENANT_DIMENSION}=acme"),
        "one dimension, so the key is exactly the tenant"
    );
}

#[test]
fn two_tenants_are_two_flows_and_one_tenant_is_one() {
    // The property the whole mechanism rests on: the key is what a quota is
    // computed per, so two tenants colliding on one key would share a quota
    // silently.
    let Ok(one) = accept(&submission("acme")) else {
        panic!("should admit");
    };
    let Ok(two) = accept(&submission("globex")) else {
        panic!("should admit");
    };
    let Ok(again) = accept(&submission("acme")) else {
        panic!("should admit");
    };
    assert_ne!(one.flow, two.flow);
    assert_eq!(one.flow, again.flow);
}

#[test]
fn a_blank_tenant_is_refused_because_nothing_below_refuses_it() {
    // `TenantId::new` takes any string, `Dimension::new` validates only the
    // dimension *name* and escapes the value, and `FlowKey::new` fails only on
    // an empty dimension list. So a blank id renders the perfectly valid key
    // `tenant=`, and every submission missing a tenant is merged into one flow
    // sharing one quota -- the exact failure fair dispatch exists to prevent,
    // arriving through the one field nobody validates.
    //
    // Whitespace counts as blank. `FlowKey` escapes `\` and `|` and passes
    // everything else through unchanged -- there is no percent-encoding -- so
    // `"  "` would render `tenant=  `: neither the blank case nor any real
    // tenant, and different from every other run of spaces.
    for tenant in ["", " ", "   ", "\t", "\n"] {
        let outcome = accept(&submission(tenant));
        assert_eq!(
            outcome.err(),
            Some(Rejected::BlankTenant),
            "{tenant:?} does not identify a tenant"
        );
    }
    // And one that merely *contains* whitespace is a tenant, because trimming
    // it would silently merge `"a b"` with `"ab"`.
    let Ok(admitted) = accept(&submission("a b")) else {
        panic!("an id with a space inside it is still an id");
    };
    assert_eq!(admitted.tenant_id.as_str(), "a b");
}

#[test]
fn surrounding_whitespace_is_removed_rather_than_carried() {
    // Trimmed *and then used*, which is the half that was missing: the id was
    // trimmed to test it and stored untrimmed, so `"acme "` was admitted as
    // flow `tenant=acme ` with tenant_id `"acme "` -- a different flow *and* a
    // different grouping key from `"acme"`. One tenant split in two by a
    // trailing newline out of a template is the same per-tenant split fair
    // dispatch exists to prevent.
    let Ok(padded) = accept(&submission("  acme\n")) else {
        panic!("an id with padding is still an id");
    };
    let Ok(clean) = accept(&submission("acme")) else {
        panic!("should admit");
    };
    assert_eq!(padded.tenant_id, clean.tenant_id);
    assert_eq!(padded.flow, clean.flow, "one tenant, one flow");
    assert_eq!(
        padded.flow.as_str(),
        "tenant=acme",
        "and no padding in the key"
    );
}

#[test]
fn a_blank_job_id_is_refused_because_it_becomes_the_queues_primary_key() {
    // Worse than a blank tenant. `JobId::new` takes any string and
    // `RunId::from_job` copies it, and the run id is the submission queue's
    // primary key under `ON CONFLICT DO NOTHING` -- so the first blank-job
    // submission is queued and every later one, from any tenant and for any
    // pipeline, comes back `AlreadyQueued` and is discarded as a redelivery of
    // unrelated work. A merged quota is unfair; this drops runs.
    for job in ["", " ", "\t\n"] {
        let mut blank = submission("acme");
        blank.meta.job_id = pneuma_core::ids::JobId::new(job);
        assert_eq!(
            accept(&blank).err(),
            Some(Rejected::BlankJob),
            "{job:?} does not identify a job"
        );
    }

    // And padding is removed here too, so `"job-1\n"` and `"job-1"` are one run
    // submitted twice rather than two runs of one job.
    let mut padded = submission("acme");
    padded.meta.job_id = pneuma_core::ids::JobId::new(" job-1\n");
    let Ok(admitted) = accept(&padded) else {
        panic!("a padded job id is still a job id");
    };
    assert_eq!(admitted.run_id.as_str(), "job-1");
}

#[test]
fn admission_accepts_the_body_the_runner_takes() {
    // `Submission` is a field-for-field duplicate of `pneuma-restate`'s
    // `RunRequest`, and nothing enforces that: no shared type, no dependency
    // between the crates. This pins the wire shape from one side by driving a
    // literal document -- the one the runner's own tests submit -- through this
    // type, so a field renamed here is caught even though a field added *there*
    // still is not.
    let body = serde_json::json!({
        "pipeline": {
            "pipeline_id": "p",
            "start": "A",
            "components": [{"node_id": "A", "name": "comp-a", "type": "Model", "children": ["end"]}],
        },
        "meta": {
            "job_id": "job-1",
            "tenant_id": "tenant-1",
            "pipeline_type": "invoice",
            "pipeline_level": "page",
            "pipeline_name": "default",
        },
        "input": {"doc": "d"},
        "custom_data": {"tenant_hint": "acme"},
    });
    let Ok(submission) = serde_json::from_value::<Submission>(body) else {
        panic!("the runner's request body is a submission");
    };
    let Ok(admitted) = accept(&submission) else {
        panic!("and it is admissible");
    };
    assert_eq!(admitted.run_id.as_str(), "job-1");
    assert_eq!(admitted.tenant_id.as_str(), "tenant-1");
}

#[test]
fn a_pipeline_that_cannot_run_is_refused_before_it_costs_a_quota_slot() {
    // The handler resolves this too and answers 400 -- but by then the
    // submission has been queued, selected against a tenant's quota, claimed
    // and sent, so a definition naming a step that does not exist consumes a
    // slot in somebody's fair share to produce an error that was knowable when
    // it arrived.
    let mut broken = submission("acme");
    let ghost = json!({
        "pipeline_id": "p",
        "start": "A",
        "components": [
            {"node_id": "A", "name": "comp-a", "type": "Model", "children": ["ghost"]}
        ],
    });
    let Ok(pipeline) = serde_json::from_value(ghost) else {
        panic!("that is a pipeline document");
    };
    broken.pipeline = pipeline;

    let Err(rejected) = accept(&broken) else {
        panic!("a step that does not exist is not runnable");
    };
    assert!(
        matches!(rejected, Rejected::Pipeline(_)),
        "wrong variant: {rejected:?}"
    );
    // And it names the step, because that is the whole difficulty.
    assert!(rejected.to_string().contains("ghost"), "{rejected}");
}

#[test]
fn the_pipeline_is_judged_before_the_tenant() {
    // A definition that cannot run is wrong whoever sent it, so reporting the
    // tenant problem first would send the caller to fix the wrong thing.
    let mut broken = submission("");
    let ghost = json!({
        "pipeline_id": "p",
        "start": "A",
        "components": [
            {"node_id": "A", "name": "comp-a", "type": "Model", "children": ["ghost"]}
        ],
    });
    let Ok(pipeline) = serde_json::from_value(ghost) else {
        panic!("that is a pipeline document");
    };
    broken.pipeline = pipeline;

    let Err(rejected) = accept(&broken) else {
        panic!("both are wrong, so this is not admitted");
    };
    assert!(matches!(rejected, Rejected::Pipeline(_)), "{rejected:?}");
}

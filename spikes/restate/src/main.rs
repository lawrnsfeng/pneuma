//! Restate spike — does a durable-execution engine cover what `pneuma-engine`
//! would do?
//!
//! Throwaway code. Its only product is `VERDICT.md`; correctness of the spike
//! itself matters only insofar as the findings are trustworthy.
//!
//! The interpreter deliberately reuses `pneuma-core` — `Pipeline`, `resolve`,
//! `StepRegistry`, `evaluate` — because that is the part of the port that
//! survives whatever this spike concludes. The question here is narrower:
//! whether the *execution* layer can be Restate's rather than ours.

use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{routing::post, Json as AxumJson, Router};
use pneuma_core::{
    condition::Condition,
    evaluator::evaluate,
    ids::NodeId,
    node::Pipeline,
    resolver::resolve,
    step::{Step, StepRegistry},
};
use restate_sdk::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

// --- the real corpus, same fixtures pneuma-core is tested against ----------

const CORPUS: &[(&str, &str)] = &[
    (
        "pipeline_params",
        include_str!("../../../crates/pneuma-core/tests/fixtures/pipelines/pipeline_params.yaml"),
    ),
    (
        "pipeline1",
        include_str!("../../../crates/pneuma-core/tests/fixtures/pipelines/pipeline1.yaml"),
    ),
    (
        "pipeline_condition_list",
        include_str!(
            "../../../crates/pneuma-core/tests/fixtures/pipelines/pipeline_condition_list.yaml"
        ),
    ),
    (
        "pipeline_nested_list_dict",
        include_str!(
            "../../../crates/pneuma-core/tests/fixtures/pipelines/pipeline_nested_list_dict.yaml"
        ),
    ),
];

fn load(pipeline: &str) -> Result<Pipeline, HandlerError> {
    let yaml = CORPUS
        .iter()
        .find(|(name, _)| *name == pipeline)
        .map(|(_, yaml)| *yaml)
        .ok_or_else(|| TerminalError::new(format!("unknown pipeline {pipeline}")))?;
    serde_yaml::from_str(yaml)
        .map_err(|e| TerminalError::new(format!("parse {pipeline}: {e}")).into())
}

// --- request / response ----------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunRequest {
    pipeline: String,
    #[serde(default)]
    tenant_id: String,
    #[serde(default)]
    input: Value,
    /// Bytes of filler the stub component should append to each output, for
    /// the payload-size probe.
    #[serde(default)]
    payload_padding: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct RunReport {
    pipeline: String,
    tenant_id: String,
    steps_executed: usize,
    model_calls: usize,
    output_bytes: usize,
    output: Value,
}

// --- the interpreter -------------------------------------------------------

struct Runner;

/// Mutable bookkeeping threaded through the walk. Note what is *absent*: any
/// barrier table, refcount, or `$addToSet`. Inside one durable handler a
/// fan-in is just "have all prerequisites appeared in this map yet", because
/// Restate replays the handler rather than distributing its state.
#[derive(Default)]
struct Trace {
    steps_executed: usize,
    model_calls: usize,
}

#[workflow]
impl Runner {
    #[handler]
    async fn run(
        &self,
        ctx: WorkflowContext<'_>,
        req: Json<RunRequest>,
    ) -> Result<Json<RunReport>, HandlerError> {
        let req = req.into_inner();
        let pipeline = load(&req.pipeline)?;
        let registry = resolve(&pipeline)
            .map_err(|e| TerminalError::new(format!("resolve {}: {e}", req.pipeline)))?;

        let scope: Vec<NodeId> = registry.iter().map(|s| s.node_id().clone()).collect();
        let starts: Vec<NodeId> = pipeline.start.node_ids().cloned().collect();

        let mut trace = Trace::default();
        let output = exec_subgraph(
            &ctx,
            &registry,
            &starts,
            &scope,
            req.input.clone(),
            &req,
            &mut trace,
        )
        .await?;

        let output_bytes = serde_json::to_vec(&output).map(|v| v.len()).unwrap_or(0);
        Ok(Json::from(RunReport {
            pipeline: req.pipeline,
            tenant_id: req.tenant_id,
            steps_executed: trace.steps_executed,
            model_calls: trace.model_calls,
            output_bytes,
            output,
        }))
    }
}

/// Executes the sub-graph reachable from `starts`, bounded to `scope`.
///
/// Boxed because it recurses: an aggregator re-enters this for its own
/// components, and Rust async fns cannot recurse without indirection.
fn exec_subgraph<'a>(
    ctx: &'a WorkflowContext<'a>,
    registry: &'a StepRegistry,
    starts: &'a [NodeId],
    scope: &'a [NodeId],
    input: Value,
    req: &'a RunRequest,
    trace: &'a mut Trace,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, HandlerError>> + Send + 'a>> {
    Box::pin(async move {
        // Outputs of steps completed within this scope.
        let mut done: HashMap<NodeId, Value> = HashMap::new();
        // How many prerequisites each in-scope step has already seen.
        let mut arrived: HashMap<NodeId, usize> = HashMap::new();
        let mut ready: Vec<(NodeId, Value)> =
            starts.iter().map(|id| (id.clone(), input.clone())).collect();
        let mut last = input.clone();

        while let Some((node_id, step_input)) = ready.pop() {
            if done.contains_key(&node_id) || !scope.contains(&node_id) {
                continue;
            }
            let Some(step) = registry.get(&node_id) else {
                continue;
            };

            let out = exec_step(ctx, registry, step, step_input.clone(), req, trace).await?;
            trace.steps_executed += 1;
            done.insert(node_id.clone(), out.clone());
            last = out.clone();

            // A conditional picks exactly one branch; everything else
            // continues to all its successors.
            let successors: Vec<NodeId> = match step {
                Step::Condition {
                    conditions,
                    children,
                    ..
                } => {
                    let taken = conditional_branch(conditions, &step_input, children)?;
                    taken.into_iter().collect()
                }
                _ => step.common().next_nodes.clone(),
            };

            for next in successors {
                let Some(next_step) = registry.get(&next) else {
                    continue;
                };
                let counter = arrived.entry(next.clone()).or_insert(0);
                *counter += 1;
                // The fan-in barrier, in full. No distributed coordination:
                // durable replay means this local count is already reliable.
                let needed = next_step.common().num_prerequisites.max(1) as usize;
                if *counter >= needed {
                    ready.push((next, out.clone()));
                }
            }
        }

        Ok(last)
    })
}

fn conditional_branch(
    conditions: &[Condition],
    input: &Value,
    children: &pneuma_core::node::ConditionalSuccessors,
) -> Result<Option<NodeId>, HandlerError> {
    // A malformed condition is terminal: retrying will not change the answer.
    let met = evaluate(conditions, input)
        .map_err(|e| TerminalError::new(format!("condition: {e}")))?;
    let branch = if met {
        &children.on_true
    } else {
        &children.on_false
    };
    Ok(branch.node_id().cloned())
}

async fn exec_step(
    ctx: &WorkflowContext<'_>,
    registry: &StepRegistry,
    step: &Step,
    input: Value,
    req: &RunRequest,
    trace: &mut Trace,
) -> Result<Value, HandlerError> {
    match step {
        Step::Model { common, .. } => {
            let name = common
                .name
                .as_deref()
                .unwrap_or_else(|| common.node_id.as_str())
                .to_string();
            trace.model_calls += 1;
            call_component(ctx, &name, input, req.payload_padding).await
        }

        Step::Condition { .. } => Ok(input),

        // Fan out over the input list, run the sub-graph per element, collect.
        // This is the case pneuma's own aggregation barrier exists for.
        Step::ListAggregator { refs, .. } => {
            let items = input.as_array().cloned().unwrap_or_else(|| vec![input.clone()]);
            let starts = vec![refs.start.first_node_id().clone()];
            let mut collected = Vec::with_capacity(items.len());
            for item in items {
                let out = exec_subgraph(
                    ctx,
                    registry,
                    &starts,
                    &refs.component_ids,
                    item,
                    req,
                    trace,
                )
                .await?;
                collected.push(out);
            }
            Ok(Value::Array(collected))
        }

        // Fan out over the declared start ids with the same input.
        Step::DictAggregator { refs, .. } => {
            let starts: Vec<NodeId> = refs.start.node_ids().cloned().collect();
            let mut collected = Vec::with_capacity(starts.len());
            for start in &starts {
                let out = exec_subgraph(
                    ctx,
                    registry,
                    std::slice::from_ref(start),
                    &refs.component_ids,
                    input.clone(),
                    req,
                    trace,
                )
                .await?;
                collected.push(out);
            }
            Ok(Value::Array(collected))
        }
    }
}

/// Calls the AI component over plain HTTP, wrapped in `ctx.run` so the result
/// is journalled and not repeated on replay.
///
/// The component is a bare HTTP service that has never heard of Restate —
/// this is the zero-SDK contract (differentiator #2) under test.
async fn call_component(
    ctx: &WorkflowContext<'_>,
    component: &str,
    input: Value,
    padding: usize,
) -> Result<Value, HandlerError> {
    let body = json!({ "jsonData": { "step_input": input, "padding": padding } });
    let component = component.to_string();

    let out = ctx
        .run(|| {
            let body = body.clone();
            let component = component.clone();
            async move {
                let client = reqwest::Client::new();
                let resp: Value = client
                    .post("http://127.0.0.1:9081/predict")
                    .header("x-component", component)
                    .json(&body)
                    .send()
                    .await?
                    .json()
                    .await?;
                Ok(Json::from(
                    resp.pointer("/jsonData/step_output").cloned().unwrap_or(Value::Null),
                ))
            }
        })
        .name(&format!("predict:{component}"))
        .await?
        .into_inner();

    Ok(out)
}

// --- probes ---------------------------------------------------------------

struct Probe;

#[service]
impl Probe {
    /// Journals a large value with **no HTTP involved**, to separate a
    /// Restate journal limit from a limit in the spike's own reqwest client.
    #[handler]
    async fn journal(&self, ctx: Context<'_>, bytes: u64) -> Result<String, HandlerError> {
        let n = bytes as usize;
        let out = ctx
            .run(|| async move { Ok(Json::from(json!({ "filler": "x".repeat(n) }))) })
            .name("journal_probe")
            .await?
            .into_inner();
        let len = serde_json::to_vec(&out).map(|v| v.len()).unwrap_or(0);
        Ok(format!("journalled {len} bytes"))
    }

    /// Fan-out width probe: N durable sleeps in one handler, to find any cap
    /// analogous to Temporal's 2,000 pending activities.
    #[handler]
    async fn fanout(&self, ctx: Context<'_>, n: u64) -> Result<String, HandlerError> {
        let mut futures = DurableFuturesUnordered::new();
        for _ in 0..n {
            futures.push(ctx.sleep(Duration::from_millis(1)));
        }
        let mut completed = 0u64;
        while let Some((_, r)) = futures.next().await? {
            r?;
            completed += 1;
        }
        Ok(format!("completed {completed} of {n}"))
    }
}

// --- the stubbed AI component ---------------------------------------------

/// How many times `/predict` has been entered, for the failure probes.
static ATTEMPTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Reports and resets the attempt counter, so a probe can count retries.
async fn attempts(AxumJson(body): AxumJson<Value>) -> AxumJson<Value> {
    use std::sync::atomic::Ordering;
    let seen = if body.get("reset").is_some() {
        ATTEMPTS.swap(0, Ordering::SeqCst)
    } else {
        ATTEMPTS.load(Ordering::SeqCst)
    };
    AxumJson(json!({ "attempts": seen }))
}

async fn predict(AxumJson(body): AxumJson<Value>) -> Result<AxumJson<Value>, axum::http::StatusCode> {
    use std::sync::atomic::Ordering;
    let seen = ATTEMPTS.fetch_add(1, Ordering::SeqCst);

    // Failure injection for the verdict's "failure semantics" gap. `fail_times`
    // in the step input makes this many attempts fail with 500 before
    // succeeding; a huge value never succeeds.
    let fail_times = body
        .pointer("/jsonData/step_input/fail_times")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if seen < fail_times {
        return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    }

    let input = body.pointer("/jsonData/step_input").cloned().unwrap_or(Value::Null);
    let padding = body
        .pointer("/jsonData/padding")
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize;

    // Preserve object keys so downstream conditions can still read them —
    // a real component transforms its input rather than burying it. Wrapping
    // opaquely was a harness artefact that made conditionals unreachable.
    // Pass data through unchanged (marking objects only), so the graph's own
    // mechanics are what is under test rather than the stub's transform. A
    // wrapping stub buried the keys conditions read, and turned the array a
    // ListAggregator needs into an object.
    let mut out = match &input {
        Value::Object(map) => {
            let mut m = map.clone();
            m.insert("_seen".into(), Value::Bool(true));
            Value::Object(m)
        }
        other => other.clone(),
    };
    if padding > 0 {
        out["filler"] = Value::String("x".repeat(padding));
    }
    Ok(AxumJson(json!({ "jsonData": { "step_output": out } })))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Stub component, on its own port, deliberately Restate-unaware.
    tokio::spawn(async {
        let app = Router::new().route("/predict", post(predict))
        .route("/attempts", axum::routing::post(attempts));
        let listener = tokio::net::TcpListener::bind("0.0.0.0:9081").await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let _ = Arc::new(());
    HttpServer::new(Endpoint::builder().bind(Runner).bind(Probe).build())
        .listen_and_serve("0.0.0.0:9080".parse().unwrap())
        .await;
}

//! Drives the barrier against a real MongoDB, concurrently.
//!
//! This is the test the crate exists for. The original's barrier lets two
//! children both decide they completed it — reproduced in 200 of 200 trials
//! against `mongo:5.0.28` (see `CONCURRENCY-AND-DIRECTION.md`). The replacement
//! must not, and asserting that requires actual concurrency, not a sequence.
//!
//! Requires `PNEUMA_TEST_MONGO_URL`, and fails rather than skips without it:
//! a concurrency test that silently does not run is worse than none.
//!
//! ```sh
//! docker run -d --name pn-mongo -p 27018:27017 mongo:5.0.28
//! PNEUMA_TEST_MONGO_URL=mongodb://127.0.0.1:27018 \
//!     cargo test -p pneuma-store --test barrier
//! ```

use mongodb::bson::{doc, Document};
use mongodb::{Client, Collection};
use pneuma_store::{BarrierStore, RefPath};

const TRIALS: usize = 200;
const CHILDREN: usize = 2;

async fn runs_collection() -> Collection<Document> {
    let Ok(url) = std::env::var("PNEUMA_TEST_MONGO_URL") else {
        panic!(
            "PNEUMA_TEST_MONGO_URL is not set. This test establishes that the \
             barrier is safe under concurrency; skipping it would report \
             success for work it did not do."
        );
    };
    let Ok(client) = Client::with_uri_str(&url).await else {
        panic!("could not connect to {url}");
    };
    let runs: Collection<Document> = client.database("pneuma_test").collection("runs");
    // Start clean. A previous run that panicked leaves its documents behind,
    // and since nothing makes `run_id` unique, the next run inserts a second
    // document for the same id -- `find_one_and_update` then matches the older
    // one, which already has both children, and every trial "completes" twice.
    // Found by watching a restored-from-mutation run fail for that reason.
    if let Err(error) = runs.drop().await {
        panic!("could not clear the test collection: {error}");
    }
    runs
}

fn path(value: &str) -> RefPath {
    match RefPath::new(value) {
        Ok(path) => path,
        Err(error) => panic!("{value} should encode: {error}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exactly_one_concurrent_arrival_completes_the_barrier() {
    let runs = runs_collection().await;
    let store = BarrierStore::new(runs.clone());
    let parent = path("run1.parent");

    let mut completions = 0usize;
    for trial in 0..TRIALS {
        let run_id = format!("run-{trial}");
        if let Err(error) = runs
            .insert_one(doc! { "run_id": &run_id, "refcounts": {} })
            .await
        {
            panic!("could not seed {run_id}: {error}");
        }

        // All children arrive at once. Each decides from the set its own
        // atomic update returned -- never from a later read.
        let arrivals = (0..CHILDREN).map(|child| {
            let store = store.clone();
            let parent = parent.clone();
            let run_id = run_id.clone();
            async move {
                let child = path(&format!("run1.child{child}"));
                match store.arrive(&run_id, &child, &parent).await {
                    Ok(arrival) => arrival.completes(CHILDREN),
                    Err(error) => panic!("arrive failed: {error}"),
                }
            }
        });

        let decided: Vec<bool> = futures::future::join_all(arrivals).await;
        let winners = decided.iter().filter(|completed| **completed).count();
        assert_eq!(
            winners, 1,
            "trial {trial}: exactly one arrival must complete the barrier, got {winners}"
        );
        completions += winners;
    }

    assert_eq!(completions, TRIALS, "one completion per trial");

    // Redelivery: a child that already arrived must not count twice, or a
    // barrier of two could be completed by one child arriving twice.
    let run_id = "redelivery";
    if let Err(error) = runs
        .insert_one(doc! { "run_id": run_id, "refcounts": {} })
        .await
    {
        panic!("could not seed the redelivery run: {error}");
    }
    let child = path("run1.child0");
    let Ok(first) = store.arrive(run_id, &child, &parent).await else {
        panic!("arrive failed");
    };
    assert!(first.newly_arrived);
    for _ in 0..3 {
        let Ok(arrival) = store.arrive(run_id, &child, &parent).await else {
            panic!("arrive failed");
        };
        assert_eq!(arrival.count(), 1, "$addToSet must not double-count");
        assert!(!arrival.newly_arrived, "a repeat added nothing");
        assert!(!arrival.completes(CHILDREN));
    }

    // The case a width-only check misses: a barrier already at full width,
    // then a redelivered arrival for a child that is already in the set. The
    // set is unchanged and complete, so `completes` must be false or the
    // aggregation fires twice -- the very defect this module exists to prevent,
    // reached by redelivery rather than by concurrency.
    let full = "already-full";
    if let Err(error) = runs
        .insert_one(doc! { "run_id": full, "refcounts": {} })
        .await
    {
        panic!("could not seed {full}: {error}");
    }
    let mut completions = 0;
    for index in 0..CHILDREN {
        let child = path(&format!("run1.child{index}"));
        let Ok(arrival) = store.arrive(full, &child, &parent).await else {
            panic!("arrive failed");
        };
        if arrival.completes(CHILDREN) {
            completions += 1;
        }
    }
    assert_eq!(completions, 1, "one completion while filling the barrier");
    for index in 0..CHILDREN {
        let child = path(&format!("run1.child{index}"));
        let Ok(redelivered) = store.arrive(full, &child, &parent).await else {
            panic!("arrive failed");
        };
        assert_eq!(redelivered.count(), CHILDREN, "the set is still full");
        assert!(
            !redelivered.completes(CHILDREN),
            "a redelivery must not complete an already-full barrier"
        );
    }

    // The prerequisite barrier is the same operation on the other map. The
    // original writes them as two functions and only one caller uses the
    // returned value; here there is one code path, so they cannot diverge.
    let run_id = "prerequisites";
    if let Err(error) = runs
        .insert_one(doc! { "run_id": run_id, "completed_prerequisites": {} })
        .await
    {
        panic!("could not seed the prerequisite run: {error}");
    }
    let node = path("run1.join");
    let Ok(first) = store.complete_prerequisite(run_id, &node, "run1.a").await else {
        panic!("complete_prerequisite failed");
    };
    assert_eq!(first.count(), 1);
    assert!(!first.completes(2));
    let Ok(second) = store.complete_prerequisite(run_id, &node, "run1.b").await else {
        panic!("complete_prerequisite failed");
    };
    assert!(
        second.completes(2),
        "the second prerequisite completes the join"
    );
    // Idempotent on the same map too.
    let Ok(repeat) = store.complete_prerequisite(run_id, &node, "run1.b").await else {
        panic!("complete_prerequisite failed");
    };
    assert_eq!(repeat.count(), 2, "$addToSet does not double-count");

    // The two barriers are separate maps: arriving at one does not advance the
    // other, even for the same run.
    let Ok(unaffected) = store.arrive(run_id, &path("run1.a"), &node).await else {
        panic!("arrive failed");
    };
    assert_eq!(
        unaffected.count(),
        1,
        "refcounts is a different map from completed_prerequisites"
    );

    // --- the expected-child count, under concurrency and redelivery ----------
    //
    // The original computes an absolute value from a stale read and $sets it:
    // two conditionals both read 5, both write 4, answer is 3. Measured 200/200
    // wrong. A bare $inc fixes that but double-decrements on redelivery, and
    // the call site is not redelivery-gated. Recording WHICH children were
    // skipped is idempotent and composable at once.
    const START: i32 = 5;
    let step = match pneuma_store::StepId::new("agg") {
        Ok(id) => id,
        Err(error) => panic!("should build: {error}"),
    };
    for trial in 0..50 {
        let run_id = format!("count-{trial}");
        if let Err(error) = runs
            .insert_one(doc! {
                "run_id": &run_id,
                "state": { "agg": { "num_children": START } }
            })
            .await
        {
            panic!("could not seed {run_id}: {error}");
        }

        // Two different children skipped, concurrently.
        let skips = ["skip-a", "skip-b"].map(|node| {
            let store = store.clone();
            let step = step.clone();
            let run_id = run_id.clone();
            async move {
                match store.skip_child(&run_id, &step, None, node).await {
                    Ok(remaining) => remaining,
                    Err(error) => panic!("skip_child failed: {error}"),
                }
            }
        });
        let _ = futures::future::join_all(skips).await;

        let Ok(remaining) = store.skip_child(&run_id, &step, None, "skip-a").await else {
            panic!("skip_child failed");
        };
        assert_eq!(
            remaining,
            i64::from(START) - 2,
            "trial {trial}: both skips land, and repeating one changes nothing"
        );
    }

    // Redelivery: the same skip applied ten times is still one skip. Under a
    // bare $inc this would read 5 - 10.
    let run_id = "redelivered-skip";
    if let Err(error) = runs
        .insert_one(doc! {
            "run_id": run_id,
            "state": { "agg": { "num_children": START } }
        })
        .await
    {
        panic!("could not seed {run_id}: {error}");
    }
    for _ in 0..10 {
        let Ok(remaining) = store.skip_child(run_id, &step, None, "same").await else {
            panic!("skip_child failed");
        };
        assert_eq!(
            remaining,
            i64::from(START) - 1,
            "a repeated skip must not decrement again"
        );
    }

    // The public reader agrees with what skip_child returned, and is the only
    // way to ask -- the deduction lives in the skip set, not in num_children.
    let Ok(remaining) = store.expected_children(run_id, &step, None).await else {
        panic!("expected_children failed");
    };
    assert_eq!(
        remaining,
        i64::from(START) - 1,
        "the reader subtracts the recorded skips"
    );
    let Err(error) = store.expected_children("absent", &step, None).await else {
        panic!("reading a missing run should fail");
    };
    assert!(error.to_string().contains("absent"));

    // A dict aggregator stores num_children as a scalar, which is the shape
    // the skip path actually serves.
    let scalar = "scalar-count";
    if let Err(error) = runs
        .insert_one(doc! {
            "run_id": scalar,
            "state": { "agg": { "num_children": 4_i32 } }
        })
        .await
    {
        panic!("could not seed {scalar}: {error}");
    }
    let Ok(remaining) = store.skip_child(scalar, &step, None, "gone").await else {
        panic!("skip_child failed against a scalar num_children");
    };
    assert_eq!(remaining, 3, "a scalar count is read and deducted from");

    // A missing run is an error here too, not a silent zero.
    let Err(error) = store.skip_child("absent", &step, None, "x").await else {
        panic!("skipping in a missing run should fail");
    };
    assert!(error.to_string().contains("absent"));

    // A run that does not exist is an error, not a silent empty arrival.
    let Err(error) = store.arrive("absent", &child, &parent).await else {
        panic!("arriving at a missing run should fail");
    };
    assert!(error.to_string().contains("absent"), "names the run");
}

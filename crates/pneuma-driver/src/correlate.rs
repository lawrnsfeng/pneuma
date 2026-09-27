//! Matching a result that arrives later to the call that is waiting for it.
//!
//! Pure, and the only interesting state this crate has. Everything about
//! *when* to call is `pneuma-interpreter`'s; everything about *how* to publish
//! is the transport's; what is left is a table.
//!
//! # One outstanding call per run, and why that is not the key
//!
//! `pneuma_runner::drive` awaits each call before asking for the next task, so
//! a single run has at most one call in flight and `run_id` alone would
//! identify it. The node is in the key anyway, because a result that arrives
//! *late* — after its call was abandoned and the run moved on — would
//! otherwise be handed to whatever call is outstanding now, which is a
//! different step's output delivered as this one's. That failure is silent and
//! produces a wrong answer rather than an error.

use std::collections::BTreeMap;

use serde_json::Value;
use tokio::sync::{oneshot, Mutex};

/// Which call a result belongs to.
///
/// Owned strings rather than borrowed: the key outlives the dispatch that made
/// it, by exactly as long as the call takes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CallKey {
    /// The run the call belongs to.
    pub run_id: String,
    /// The step within it.
    pub node_id: String,
}

impl CallKey {
    /// The key for one step of one run.
    pub fn new(run_id: impl Into<String>, node_id: impl Into<String>) -> Self {
        CallKey {
            run_id: run_id.into(),
            node_id: node_id.into(),
        }
    }
}

/// A call is already outstanding under that key.
///
/// Refused rather than replaced, and the distinction is the whole reason this
/// returns a `Result`. Replacing the waiter drops the first one's sender, so
/// the call that registered it never hears anything and waits for ever — a run
/// wedged with no error anywhere. It is reachable: the ingress is not
/// acknowledged until a run completes, so a redelivery while the first attempt
/// is still running starts a second `Execution` for the same `run_id`.
/// Refusing turns that into "this run is already being driven here", which the
/// caller can answer by leaving the message alone.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("a call for {} step {} is already outstanding", .key.run_id, .key.node_id)]
pub struct Occupied {
    /// The key that was already taken.
    pub key: CallKey,
}

/// What became of a delivered result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivered {
    /// Handed to the call that was waiting for it.
    Taken,
    /// Nobody here is waiting for it.
    ///
    /// The ordinary case, not an error: the result subject is shared and every
    /// replica sees every result, so most of them belong to runs another
    /// replica is driving.
    Unclaimed,
    /// Somebody was waiting and has stopped.
    ///
    /// Distinct from [`Delivered::Unclaimed`] because it means something here
    /// gave up — a cancelled run, a `drive` that returned early — and a result
    /// arriving for it is worth a log line, where an unclaimed one is worth
    /// nothing.
    Abandoned,
}

/// Calls waiting for their results.
///
/// A `tokio::sync::Mutex` rather than `std::sync::Mutex`, and not because the
/// guard is ever held across an `await` — it is not. `std`'s mutex poisons on a
/// panic, and the only honest handling of that here is
/// `poisoned.into_inner()`: the map is a plain table of senders with no
/// invariant across two operations, and a controller that stopped matching
/// results because one run panicked would wedge every other run on the replica.
/// But that recovery arm cannot be reached from outside this module, so it
/// would be an untestable branch defending against a state that cannot arise.
/// A mutex that does not poison removes the question rather than answering it
/// where nobody can check.
#[derive(Debug, Default)]
pub struct Pending {
    waiting: Mutex<BTreeMap<CallKey, oneshot::Sender<Value>>>,
}

impl Pending {
    /// An empty table.
    pub fn new() -> Self {
        Pending::default()
    }

    /// Registers a call, returning what to await for its result.
    ///
    /// Fails if one is already outstanding under that key — see [`Occupied`].
    pub async fn register(&self, key: CallKey) -> Result<oneshot::Receiver<Value>, Occupied> {
        let (sender, receiver) = oneshot::channel();
        let mut waiting = self.waiting.lock().await;
        if waiting.contains_key(&key) {
            return Err(Occupied { key });
        }
        waiting.insert(key, sender);
        Ok(receiver)
    }

    /// Gives up on a call, so a late result for it is [`Delivered::Abandoned`].
    ///
    /// Called when a run ends for any reason. Without it the table grows by one
    /// entry per abandoned call for the life of the process, and a replica that
    /// has driven a million runs is holding a million senders nobody will ever
    /// use.
    pub async fn forget(&self, key: &CallKey) -> bool {
        self.waiting.lock().await.remove(key).is_some()
    }

    /// Hands a result to whoever is waiting for it.
    pub async fn deliver(&self, key: &CallKey, result: Value) -> Delivered {
        let taken = self.waiting.lock().await.remove(key);
        match taken {
            None => Delivered::Unclaimed,
            // `send` fails when the receiver has been dropped, which is a
            // caller that stopped waiting -- a cancelled run, or a `drive` that
            // returned early on another step's failure.
            Some(sender) => match sender.send(result) {
                Ok(()) => Delivered::Taken,
                Err(_) => Delivered::Abandoned,
            },
        }
    }

    /// How many calls are outstanding.
    pub async fn outstanding(&self) -> usize {
        self.waiting.lock().await.len()
    }
}

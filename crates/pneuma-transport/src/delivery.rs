//! A message that cannot be silently dropped.
//!
//! # The defect this type exists to delete
//!
//! the original opens
//! `handleMsg` with a deferred delete:
//!
//! ```go
//! defer func() {
//!     meta, err := msg.Metadata()
//!     if err != nil {
//!         log.Printf("Error getting message metadata: %v\n", err)
//!     } else {
//!         err = handler.JetStream.DeleteMsg(meta.Stream, meta.Sequence.Stream)
//!         ...
//!     }
//! }()
//! ```
//!
//! `defer` runs on *every* path out of the function, including the ones that
//! returned an error. The malformed-body path does record itself — the
//! `json.Unmarshal` failure the original calls `SaveMsgToDLQ`
//! before returning — but the later error returns do not: a failure to send an
//! event or a result deletes the message with nothing anywhere recording that
//! the work was dropped. The bug is not that somebody wrote the wrong line —
//! it is that the right line and the wrong line look identical, and nothing in
//! the type system distinguishes them.
//!
//! Here, settling **consumes** the delivery, so the compiler tracks it; the
//! type is `#[must_use]`, so ignoring one is a warning; and a delivery that is
//! dropped without being settled is *returned to the broker* rather than
//! consumed. Every path out of a handler is covered, including the panicking
//! one, and the default is the safe direction.
//!
//! # Why the default is requeue, and what bounds it
//!
//! Returning the message is only safe while something limits how many times it
//! comes back. On a RabbitMQ quorum queue that thing is `x-delivery-limit`,
//! which dead-letters a message after N deliveries — so the queue declaration
//! sets it explicitly rather than relying on a server default, because the
//! default has changed between RabbitMQ releases and a poison message with no
//! limit is an infinite loop that looks like throughput.
//!
//! # Why settling does not `await`
//!
//! `Drop` is not async and cannot be. The settlement is therefore *posted*
//! rather than performed: [`Settle::settle`] is synchronous and infallible from
//! the caller's side, and whatever implements it is responsible for getting the
//! acknowledgement to the broker. An unbounded channel drained by the consumer
//! task is the intended implementation, which is why the trait says nothing
//! about failure — there is nothing a `Drop` could do with an error anyway, and
//! a trait that returned one would be inviting callers to ignore it in exactly
//! the place they cannot handle it.
//!
//! That is also why [`Settle::settle`] must not panic, and why the drop path
//! contains it if it does. A delivery is dropped *while unwinding* on the
//! handler-panicked path, and a panic inside a `Drop` that is already unwinding
//! is a double panic, which aborts the process. So the one type whose whole
//! purpose is to make a handler panic survivable would turn it into an
//! immediate `SIGABRT` — because a channel send in a shutdown race said
//! `.expect("consumer alive")`.

use std::sync::Arc;

/// How a delivery ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Handled. The broker may forget it.
    Ack,
    /// Not handled.
    Nack {
        /// Whether the broker should deliver it again.
        ///
        /// `false` is what sends it to the dead-letter queue, which is the
        /// right answer for a message that will fail the same way next time —
        /// a body that does not parse does not parse on the third attempt
        /// either, and retrying it is how a malformed message becomes a loop.
        requeue: bool,
    },
}

/// Somewhere a settlement can be posted without awaiting.
///
/// Synchronous and infallible because [`Delivery`]'s `Drop` calls it, and
/// `Drop` can neither await nor report. Implementations are expected to hand
/// the settlement to a task that does the awaiting.
///
/// # Contract
///
/// **An implementation must not panic.** A settlement posted from a channel
/// whose receiver has already shut down is the ordinary case at the end of a
/// process's life, and `send(..).expect(..)` there is a panic inside a `Drop`
/// that may already be unwinding — a double panic, which aborts. Swallow the
/// failure and log it. The drop path guards against this anyway (see
/// [`Delivery`]), but a guard is not a licence: a panicking implementation
/// still loses the settlement it was asked to post.
pub trait Settle: Send + Sync {
    /// Records how the delivery with this tag ended.
    fn settle(&self, tag: u64, disposition: Disposition);
}

/// A message taken from a broker, which must be settled.
///
/// Settling consumes it, so the compiler knows whether it was; dropping it
/// unsettled returns it to the broker rather than losing it.
#[must_use = "a delivery must be acked or nacked; dropping one returns it to the broker"]
#[derive(Debug)]
pub struct Delivery<S: Settle> {
    tag: u64,
    body: Vec<u8>,
    settler: Arc<S>,
    settled: bool,
}

impl<S: Settle> Delivery<S> {
    /// Wraps a message the broker just handed over.
    pub fn new(tag: u64, body: Vec<u8>, settler: Arc<S>) -> Self {
        Delivery {
            tag,
            body,
            settler,
            settled: false,
        }
    }

    /// The broker's identifier for this delivery.
    pub fn tag(&self) -> u64 {
        self.tag
    }

    /// What was sent.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// Handled. Consumes the delivery.
    pub fn ack(mut self) {
        self.finish(Disposition::Ack);
    }

    /// Not handled. Consumes the delivery.
    ///
    /// `requeue: false` is the dead-letter path, and is what a permanent
    /// failure deserves: a message the handler will reject the same way every
    /// time costs a redelivery loop and buys nothing.
    pub fn nack(mut self, requeue: bool) {
        self.finish(Disposition::Nack { requeue });
    }

    /// Posts the settlement once, whichever way it was reached.
    fn finish(&mut self, disposition: Disposition) {
        self.settled = true;
        self.settler.settle(self.tag, disposition);
    }
}

impl<S: Settle> Drop for Delivery<S> {
    fn drop(&mut self) {
        // Reached by every path that did not settle: an early `return`, a `?`,
        // a panic unwinding through the handler. All of them mean the work was
        // not done, so all of them return the message.
        if !self.settled {
            // Contained rather than propagated. This runs while unwinding on
            // the handler-panicked path, and a panic escaping a `Drop` that is
            // already unwinding aborts the process -- so a `Settle` that broke
            // its "must not panic" contract would turn the one failure this
            // type exists to survive into a `SIGABRT`. The panic is swallowed
            // here and nowhere else: `ack` and `nack` are called from ordinary
            // code, where a panicking settler should be as loud as any other
            // bug.
            let settled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.finish(Disposition::Nack { requeue: true });
            }));
            drop(settled);
        }
    }
}

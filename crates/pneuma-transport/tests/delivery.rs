//! What happens to a message on every path out of a handler.
//!
//! The defect being deleted is the original executor's `defer DeleteMsg`,
//! which deletes the message on the failing paths too — so failed work is
//! discarded as if it had succeeded. The tests below are that bug, written as
//! the shapes it takes: an early return, a `?`, and a panic.

use std::sync::{Arc, Mutex};

use pneuma_transport::{Delivery, Disposition, Settle};

/// A broker that only remembers what it was told.
#[derive(Debug, Default)]
struct Recorder(Mutex<Vec<(u64, Disposition)>>);

impl Settle for Recorder {
    fn settle(&self, tag: u64, disposition: Disposition) {
        match self.0.lock() {
            Ok(mut seen) => seen.push((tag, disposition)),
            Err(poisoned) => poisoned.into_inner().push((tag, disposition)),
        }
    }
}

impl Recorder {
    fn seen(&self) -> Vec<(u64, Disposition)> {
        match self.0.lock() {
            Ok(seen) => seen.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }
}

fn delivery(recorder: &Arc<Recorder>) -> Delivery<Recorder> {
    Delivery::new(7, b"{\"job\":1}".to_vec(), Arc::clone(recorder))
}

#[test]
fn a_delivery_carries_what_the_broker_sent() {
    let recorder = Arc::new(Recorder::default());
    let message = delivery(&recorder);
    assert_eq!(message.tag(), 7);
    assert_eq!(message.body(), b"{\"job\":1}");
    message.ack();
}

#[test]
fn settling_happens_once_and_says_which_way() {
    for (settle, expected) in [
        (
            Box::new(Delivery::ack) as Box<dyn Fn(Delivery<Recorder>)>,
            Disposition::Ack,
        ),
        (
            Box::new(|message: Delivery<Recorder>| message.nack(true)),
            Disposition::Nack { requeue: true },
        ),
        (
            Box::new(|message: Delivery<Recorder>| message.nack(false)),
            Disposition::Nack { requeue: false },
        ),
    ] {
        let recorder = Arc::new(Recorder::default());
        settle(delivery(&recorder));
        // Once, not twice: settling consumes the delivery, so `Drop` runs
        // immediately afterwards and must not post a second answer.
        assert_eq!(recorder.seen(), vec![(7, expected)]);
    }
}

#[test]
fn a_delivery_that_falls_out_of_scope_goes_back_to_the_broker() {
    // The `defer DeleteMsg` bug, as an early return. There, the message is
    // deleted and the work is gone; here it is returned and retried.
    fn handle(message: Delivery<Recorder>, ok: bool) {
        if !ok {
            return;
        }
        message.ack();
    }

    let recorder = Arc::new(Recorder::default());
    handle(delivery(&recorder), false);
    assert_eq!(
        recorder.seen(),
        vec![(7, Disposition::Nack { requeue: true })]
    );

    let recorder = Arc::new(Recorder::default());
    handle(delivery(&recorder), true);
    assert_eq!(recorder.seen(), vec![(7, Disposition::Ack)]);
}

#[test]
fn a_panicking_handler_returns_the_message_too() {
    // The path no `defer`, `finally` or explicit call covers reliably, and the
    // one a reviewer never checks.
    let recorder = Arc::new(Recorder::default());
    let taken = Arc::clone(&recorder);
    let panicked = std::panic::catch_unwind(move || {
        let _message = delivery(&taken);
        panic!("the handler blew up");
    });
    assert!(panicked.is_err(), "the panic is not swallowed");
    assert_eq!(
        recorder.seen(),
        vec![(7, Disposition::Nack { requeue: true })],
        "unwinding still settles it"
    );
}

/// A settler that breaks its contract.
struct Panicking;

impl Settle for Panicking {
    fn settle(&self, _tag: u64, _disposition: Disposition) {
        panic!("the consumer task is gone");
    }
}

#[test]
fn a_settler_that_panics_while_unwinding_does_not_abort_the_process() {
    // The failure this contains: a channel send in a shutdown race spelled
    // `.expect("consumer alive")`, reached from a `Drop` that is already
    // unwinding because the handler panicked. Two panics at once abort -- so
    // the one type whose purpose is to make a handler panic survivable would
    // turn it into a `SIGABRT`. Reaching the assertion below at all is the
    // whole test: an abort takes the harness with it.
    let panicked = std::panic::catch_unwind(|| {
        let _message = Delivery::new(7, Vec::new(), Arc::new(Panicking));
        panic!("the handler blew up");
    });
    assert!(panicked.is_err(), "the handler's panic still propagates");
}

#[test]
fn a_settler_that_panics_on_an_explicit_ack_is_as_loud_as_any_other_bug() {
    // Contained on the drop path and nowhere else. `ack` is called from
    // ordinary code, where swallowing a broken settler would hide it.
    let panicked = std::panic::catch_unwind(|| {
        Delivery::new(7, Vec::new(), Arc::new(Panicking)).ack();
    });
    assert!(panicked.is_err(), "not swallowed here");
}

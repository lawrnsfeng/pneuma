//! Turning an outcome into a settlement, and a queue into a loop.
//!
//! # The settlement table is the whole of the mapping
//!
//! Three outcomes, three answers to the broker, and the correspondence is
//! exact: handled means acknowledge, wrong-and-wrong-next-time means
//! dead-letter, not-now means return it. [`settle`] is that table and nothing
//! else, which is what lets it be checked without a broker.
//!
//! # The loop is three lines because the decision is not in it
//!
//! `pneuma_transport::decide` is the reconnect rule, and it is pure. What is left
//! here is: run until the consumer stops, say why it stopped, and do what the
//! rule says. The branches that only a misbehaving broker reaches are tested
//! over there, against an enum.

use std::time::Instant;

use pneuma_transport::{Consuming, Delivery, Disposition, Event, Settle};

use crate::handle::Outcome;

/// Settles `delivery` the way `outcome` says.
///
/// Consumes the delivery, so the compiler knows it was settled — and a path
/// that forgot would return the message rather than dropping it, which is
/// `pneuma_transport::delivery`'s whole design.
pub fn settle<S: Settle>(delivery: Delivery<S>, outcome: &Outcome) -> Disposition {
    match outcome {
        Outcome::Handled => {
            delivery.ack();
            Disposition::Ack
        }
        // Wrong, and wrong next time. Retrying is how a bad message becomes a
        // loop, and the dead-letter queue is where a person can find it.
        Outcome::Rejected(_) => {
            delivery.nack(false);
            Disposition::Nack { requeue: false }
        }
        // Not now. The queue's `x-delivery-limit` is what stops this being
        // unbounded, which is why that argument is set explicitly rather than
        // left to the broker's version.
        Outcome::Retry(_) => {
            delivery.nack(true);
            Disposition::Nack { requeue: true }
        }
    }
}

/// Consumes until the broker stops answering, and says why it stopped.
///
/// Returns the [`Event`] `pneuma_transport::decide` takes, carrying how long the
/// connection lasted — which is what tells a broker that restarted apart from
/// one that is flapping, and they need different answers.
pub async fn pump<H, F>(consuming: &mut Consuming, up: Instant, mut handle: H) -> Event
where
    H: FnMut(Vec<u8>) -> F,
    F: std::future::Future<Output = Outcome>,
{
    loop {
        let delivery = match consuming.next().await {
            Err(error) => return dropped(up, error.to_string()),
            Ok(None) => return dropped(up, "the consumer was cancelled".to_owned()),
            Ok(Some(delivery)) => delivery,
        };
        let outcome = handle(delivery.body().to_vec()).await;
        settle(delivery, &outcome);
    }
}

/// The event for a consumer that has stopped.
fn dropped(up: Instant, why: String) -> Event {
    Event::Dropped {
        after: up.elapsed(),
        why,
    }
}

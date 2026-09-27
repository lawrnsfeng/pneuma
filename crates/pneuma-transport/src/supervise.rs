//! Staying connected, decided without a clock or a socket.
//!
//! The reconnect supervisor is the highest coverage risk in the whole port,
//! and for a reason that has nothing to do with effort: every interesting
//! branch is reached by a broker misbehaving, which a test cannot arrange on
//! demand. Split, the decision is a total function over an enum and the part
//! that cannot be tested is three lines with no branches in it.
//!
//! # What the original does, and the one thing changed
//!
//! `RabbitMQReceiver.run` loops for ever: connect, consume, and on any
//! exception log and `asyncio.sleep(RECONNECT_DELAY)` — a **fixed** five
//! seconds (the original -- the log at 170, the
//! sleep at 174 -- and the original). The loop itself is kept, because its comment
//! records a real failure it was written for: `connect_robust` restores the
//! TCP connection but not the consumer, so a cancelled `Channel.Open` leaves a
//! process that is connected and consuming nothing.
//!
//! The fixed delay is not kept. A broker that has just restarted is a broker
//! every consumer is about to reconnect to at once, and a constant interval is
//! what synchronises them; the delay here doubles to a ceiling instead. It is
//! deliberately **not** jittered: jitter would need a random source, which
//! this repository's determinism gate exists to keep out of testable code, and
//! the ceiling plus staggered failure times is most of what jitter buys.
//!
//! # The two bugs this shape is guarding against
//!
//! **Never resetting the attempt counter.** A consumer that has been up for a
//! week, whose broker restarts, then waits the *maximum* backoff before trying
//! — because the counter still holds the failures from its first startup.
//!
//! **Resetting it on connection rather than on survival.** The obvious cure for
//! the first bug is to zero the counter the moment the consumer attaches, and
//! that reintroduces the fixed delay it was meant to remove. A broker that
//! accepts the connection and then closes the channel — which is exactly the
//! failure the original's loop was written for — produces
//! `Up, Dropped, Up, Dropped, …`, and with the counter zeroed at every `Up` the
//! attempt count never exceeds one. The ceiling is unreachable, the wait is the
//! floor for ever, and every consumer of a flapping broker retries in lockstep.
//!
//! So the reset is gated on **survival**, not attachment: [`Event::Dropped`]
//! carries how long the connection lasted, and only a connection that lasted
//! at least [`Backoff::stable`] starts a fresh episode. A flap is a
//! continuation of the previous one.

use std::time::Duration;

/// Why a backoff is not one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BackoffError {
    /// The first wait is zero, so nothing ever waits.
    ///
    /// Refused rather than tolerated: with a zero floor every delay is zero,
    /// however many failures there have been, and the supervisor becomes an
    /// unbounded reconnect loop against a broker that is down -- which is the
    /// load a backoff exists to prevent, arrived at by configuring one.
    #[error("the first wait must not be zero; every delay is a multiple of it")]
    NoFirstWait,

    /// The ceiling is below the floor.
    ///
    /// Not clamped silently. `max < first` collapses every delay to `max`, so
    /// a deployment that meant to raise the ceiling and typed it into the wrong
    /// variable would get a *shorter* wait than the default and no sign of it.
    #[error("the ceiling {max:?} is below the first wait {first:?}")]
    CeilingBelowFloor {
        /// The first wait.
        first: Duration,
        /// The ceiling that is below it.
        max: Duration,
    },
}

/// How long to wait before the first retry, how long to grow to, and how long
/// a connection must last to count as having worked.
///
/// The fields are private and [`Backoff::new`] checks them, because two of the
/// three ways of filling them in wrongly fail silently -- see [`BackoffError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    first: Duration,
    max: Duration,
    stable: Duration,
}

impl Backoff {
    /// The original's five seconds, as a floor, growing to a minute.
    ///
    /// The floor is what `RABBITMQ_RECEIVER_RECONNECT_DELAY` defaults to,
    /// so a broker that blips is
    /// recovered from exactly as quickly as it is today; the ceiling is what
    /// stops a broker that is *gone* from being retried thousands of times an
    /// hour by every consumer at once.
    ///
    /// `stable` is one minute: a connection that carried messages for a minute
    /// worked, and one that lasted less than that was a flap.
    pub const DEFAULT: Backoff = Backoff {
        first: Duration::from_secs(5),
        max: Duration::from_secs(60),
        stable: Duration::from_secs(60),
    };

    /// Builds a backoff, refusing the two settings that fail quietly.
    pub fn new(first: Duration, max: Duration, stable: Duration) -> Result<Self, BackoffError> {
        if first.is_zero() {
            return Err(BackoffError::NoFirstWait);
        }
        if max < first {
            return Err(BackoffError::CeilingBelowFloor { first, max });
        }
        Ok(Backoff { first, max, stable })
    }

    /// The wait after the first failure.
    pub fn first(self) -> Duration {
        self.first
    }

    /// The longest wait, however many failures there have been.
    ///
    /// A ceiling rather than a limit on attempts: a consumer that gave up would
    /// need something to notice it had, and there is nothing above it that
    /// would. Waiting a bounded interval for ever is the honest behaviour for a
    /// process whose whole job is to be attached to a broker.
    pub fn max(self) -> Duration {
        self.max
    }

    /// How long a connection must last before losing it starts a fresh episode.
    pub fn stable(self) -> Duration {
        self.stable
    }
}

/// What just happened to the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The connection was established and the consumer is attached.
    Up,
    /// Connecting failed.
    ConnectFailed(String),
    /// The consumer stopped on its own.
    ///
    /// Distinct from [`Event::ConnectFailed`] because it is the failure the
    /// original's loop was actually written for: the socket is fine and the
    /// consumer is gone, which no TCP-level reconnection notices.
    Dropped {
        /// How long the connection lasted.
        ///
        /// Carried because this is what tells a broker that restarted apart
        /// from a broker that is flapping, and they need different answers: the
        /// first should be retried immediately, the second should not be
        /// retried at the same interval for ever.
        after: Duration,
        /// What went wrong, for the log.
        why: String,
    },
    /// The process was asked to stop.
    Shutdown,
}

/// What to do about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Attach the consumer; the connection is up.
    Consume,
    /// Wait, then connect again.
    Reconnect {
        /// How long to wait first.
        after: Duration,
        /// Why, for the log. A reconnect loop with no reason in it is a
        /// process that reports being busy and never says what about.
        why: String,
    },
    /// Stop.
    Stop,
}

/// A decision, and the attempt counter to carry into the next one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    /// What to do now.
    pub action: Action,
    /// How many consecutive failures there have now been.
    pub attempt: u32,
}

/// How long to wait after `attempt` consecutive failures.
///
/// `first * 2^(attempt - 1)`, capped at `max`. Attempt 0 has not failed yet and
/// waits nothing, which keeps the function total rather than making the caller
/// promise never to ask.
///
/// Saturating throughout: the doubling is computed on `u32` seconds and
/// saturates rather than wrapping, because a wrap would turn the longest wait
/// into the shortest one — a backoff that becomes a hot loop after enough
/// failures is worse than no backoff at all.
pub fn delay(backoff: Backoff, attempt: u32) -> Duration {
    if attempt == 0 {
        return Duration::ZERO;
    }
    // Clamped before the shift: `1u32 << 32` is undefined, and an attempt count
    // that high belongs to a broker that has been unreachable for weeks. 31
    // doublings already exceeds any ceiling anyone would configure, so the
    // clamp changes nothing a deployment can observe -- it only keeps the
    // arithmetic total.
    let doublings = (attempt - 1).min(31);
    let grown = backoff.first.saturating_mul(1_u32 << doublings);
    grown.min(backoff.max)
}

/// Decides what to do next. Pure.
///
/// The attempt counter is returned rather than mutated so that the whole rule
/// is one function of its inputs — which is what lets "does a successful
/// connection reset the backoff?" be a test instead of a code review.
pub fn decide(event: &Event, attempt: u32, backoff: Backoff) -> Step {
    match event {
        // Attaching is not surviving, so the counter is carried, not cleared.
        // Clearing it here is the flap: a broker that accepts the connection
        // and closes the channel gives `Up, Dropped, Up, Dropped, ...`, and a
        // counter zeroed at every `Up` never exceeds one.
        Event::Up => Step {
            action: Action::Consume,
            attempt,
        },
        Event::Shutdown => Step {
            action: Action::Stop,
            attempt,
        },
        Event::ConnectFailed(why) => Step {
            action: reconnect(backoff, next(attempt), why),
            attempt: next(attempt),
        },
        // The reset, gated on survival. A connection that lasted long enough
        // to have worked starts a fresh episode, so the consumer that ran for
        // a week and then lost its broker retries in `first` rather than in
        // `max`; one that lasted less than that is the same episode continuing,
        // and keeps growing.
        Event::Dropped { after, why } => {
            let attempt = if *after >= backoff.stable {
                1
            } else {
                next(attempt)
            };
            Step {
                action: reconnect(backoff, attempt, why),
                attempt,
            }
        }
    }
}

/// The next attempt count.
///
/// Saturating rather than wrapping, for the same reason [`delay`] saturates: a
/// counter that wrapped to zero would silently reset the backoff of a broker
/// that has never once answered.
fn next(attempt: u32) -> u32 {
    attempt.saturating_add(1)
}

/// The reconnect action for an attempt, with its reason.
fn reconnect(backoff: Backoff, attempt: u32, why: &str) -> Action {
    Action::Reconnect {
        after: delay(backoff, attempt),
        why: why.to_owned(),
    }
}

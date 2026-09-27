# pneuma — broker portability by design

> Answers: can NATS be swapped for RabbitMQ/SQS/Kafka without rearchitecting? Yes, and the reason is a byproduct of an earlier correctness decision, not new work — with one honest exception.

## The crux insight

Idempotency (`execute_once`), fairness (the `SKIP LOCKED` + window-function dispatch scheduler), and survivability's retry decision (the Postgres `lease_expires_at` + sweeper) were all pushed into Postgres, not the broker, when fixing correctness bugs — not in pursuit of portability. That decision means none of them depend on broker-native features: `Nats-Msg-Id` dedup, RabbitMQ priority queues, SQS FIFO ordering, Kafka's transactional EOS are all optional accelerants the substrate never actually required. **The broker's real job shrinks to: reliably deliver an opaque payload at least once, with some way to acknowledge receipt.** That's a small, genuinely universal surface — much smaller than a design that mirrored NATS's full feature set as "the trait" would have needed.

## The trait

```rust
#[async_trait]
trait Consume: Send + Sync {
    async fn next_batch(&mut self, max: usize, wait: Duration) -> Vec<Delivery>;
}

struct Delivery { payload: Bytes, receipt: OpaqueReceiptToken, delivery_count: u32 }

#[async_trait]
trait Ack: Send + Sync {
    async fn ack(&self, receipt: OpaqueReceiptToken) -> Result<()>;
    async fn nak(&self, receipt: OpaqueReceiptToken, delay: Option<Duration>) -> Result<()>;
}

// optional, best-effort — never load-bearing for correctness
#[async_trait]
trait ExtendableAck: Ack {
    async fn extend(&self, receipt: OpaqueReceiptToken, extra: Duration) -> Result<()>;
}
```

## Capability matrix

| | NATS JetStream | RabbitMQ | SQS | Kafka |
|---|---|---|---|---|
| Consume shape | pull `Fetch()` | push + `prefetch_count` | pull `ReceiveMessage` (long-poll) | consumer-group poll |
| Ack | `ack()`/`nak(delay)`/`term()` | `basic.ack`/`basic.nack` | `DeleteMessage` | **offset commit — no per-message concept** |
| Native dedup | `Nats-Msg-Id` + window | none (plugin only) | FIFO only, 5-min window; standard: none | producer-side EOS only, not consumer-facing |
| Ack extension | `InProgress()` | none (channel-level `consumer_timeout` only) | `ChangeMessageVisibility` | no equivalent |
| Local ordering | per-subject | per-queue | FIFO only; standard: none | per-key |
| KEDA scaler | native | native | native | native, most mature |

## Why the gaps don't leak into correctness

None of the missing cells are things pneuma's correctness ever depended on:

- **No native dedup** → irrelevant; `execute_once` (pillar 1) was never broker-assisted.
- **No ack extension** → irrelevant; the Postgres lease decides staleness, the broker's own redelivery timeout is a secondary, best-effort backstop, never the source of truth.
- **No ordering** (SQS standard, worst case) → irrelevant, and worth being precise about *why*: the combiner/barrier design is set-based, not order-dependent (`PREGEL-NOTES.md`) — arrival order into a barrier was never meaningful — and a stale attempt's result is rejected by the idempotency key, not by delivery order. SQS's cheapest, highest-throughput mode is fully compatible, not a degraded fallback.
- **Fairness (pillar 4)** never queries the broker at all — it's entirely a Postgres outbox scheduler. Broker choice has zero effect on it.
- **KEDA scaler availability is uniform** — all four brokers have mature, native KEDA scalers. Not a differentiator; confirmed positively rather than assumed.

## The one honest exception: Kafka is not a thin adapter

Everywhere else, swapping brokers means implementing `Consume`/`Ack` against a different client library. Kafka is structurally different: **it has no per-message ack/nak/redelivery primitive.** Consumers commit *offsets* — a log position — not per-message acknowledgments; there's no "nak this one, keep the rest."

A Kafka adapter has to build, not just wrap:
- **Offset commit as a checkpoint of safe hand-off**, decoupled from work success — commit past a message once it's durably recorded in the Postgres outbox, never once the AI model call actually succeeds (that's still the lease/retry machinery's job).
- **A hand-rolled retry topic for the `nak` equivalent** — no native requeue exists; a failed message is explicitly re-published to `subject.retry`, consumed separately, dead-lettered after N attempts.

State this plainly rather than discover it mid-implementation: three of four brokers are "implement the trait." Kafka is "implement a different consumption strategy that happens to expose the same trait at the top." Budget accordingly if Kafka support is ever actually needed.

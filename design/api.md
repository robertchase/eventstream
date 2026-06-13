# Event Streaming API

## Purpose

An internal application event bus for microservices. Services publish domain
events to streams; other services consume them via durable, named cursors.

Pull-based, HTTP/REST + JSON. Intentionally simple — Kafka-style guarantees are
out of scope where they conflict with API simplicity.

## Mental model

Three nouns:

- **Stream** — an append-only log. Producers POST events to it.
- **Subscription** — a named cursor on a stream. Remembers where it has read up
  to. Multiple workers sharing one subscription split the work. Two
  subscriptions on the same stream each see every event.
- **Event** — a single record: id, key, payload, timestamp.

One lifecycle: **publish → pull → ack**.

Pulling an event leases it to the caller. Acking releases the cursor forward.
Not acking within the lease window causes redelivery to another worker.

That is the whole model. Partitioning, offset tracking, consumer-group
rebalancing, and replay mechanics are server concerns and do not appear in the
API.

## Endpoints

```
POST /v1/streams/{stream}/events            publish one event
POST /v1/subscriptions                    create a subscription
GET  /v1/subscriptions/{sub}/pull         fetch one event (long-poll)
POST /v1/subscriptions/{sub}/ack/{id}     ack an event
```

### Publish

```
POST /v1/streams/orders/events
Idempotency-Key: <uuid>          # optional; dedupes within retention window
Content-Type: application/json

{ "key": "order-123", "payload": { ... } }

→ 200 { "id": "evt_01J..." }
```

`key` is optional. It is stored with the event and returned on pull, so
consumers that care about grouping can read it.

> **Not yet a delivery guarantee.** The intent is that events sharing a key
> are delivered to the same worker in order, but the current Redis Streams
> backend does not implement key affinity — a consumer group hands each
> pending entry to whichever worker pulls next, regardless of key. So today
> `key` is metadata only. Honoring same-key→same-worker ordering needs
> key-hashed sub-streams (a partition count knob) and is deferred; see
> "Per-key ordering" under *Deliberately out of scope*. With a single
> worker per subscription, ordering is the stream's natural append order.

When `deliver_at` (absolute ISO 8601 timestamp) is set and in the future,
the event is queued for delivery at that time:

```
POST /v1/streams/orders/events
{ "key": "...", "payload": {...}, "deliver_at": "2026-05-26T10:00:00Z" }

→ 202 { "schedule_id": "sch_01J..." }
```

The response returns a `schedule_id` (used for cancel/list) instead of `id`.
At delivery time the event is assigned a normal event `id`; the
`schedule_id` becomes invalid. Delivery is a lower bound only — see
"Scheduled delivery is a lower bound" under Design decisions.

### Create subscription

```
POST /v1/subscriptions
{ "name": "billing-worker", "stream": "orders" }

→ 201 { "name": "billing-worker", "stream": "orders" }
```

Subscriptions are durable and idempotent to create. A new subscription
starts at the current tail of the stream — only events published after
creation are delivered.

### Pull

```
GET /v1/subscriptions/billing-worker/pull?wait=30s

→ 200 { "id": "evt_01J...", "key": "order-123", "payload": {...}, "ts": "..." }
→ 204  (nothing available within the wait window)
```

One event per call. The response *is* the event — no envelope.

The event is leased to the caller for a server-defined window (default ~30s).
If not acked within that window, it becomes available to other workers.

### Ack

```
POST /v1/subscriptions/billing-worker/ack/evt_01J...

→ 204
```

Releases the lease and advances the subscription cursor past this event.

## Admin endpoints

For stream and subscription management, plus inspection and cancellation of
scheduled events.

```
POST   /v1/streams                          create a stream
GET    /v1/streams                          list streams
GET    /v1/streams/{stream}                  inspect stream
DELETE /v1/streams/{stream}                  delete stream

GET    /v1/subscriptions                   list subscriptions (?stream= filter)
GET    /v1/subscriptions/{sub}             inspect: lag, in_flight, oldest_unacked_age
DELETE /v1/subscriptions/{sub}             delete subscription

GET    /v1/streams/{stream}/scheduled        list pending scheduled events
DELETE /v1/streams/{stream}/scheduled/{sid}  cancel a scheduled event

GET    /v1/subscriptions/{sub}/dlq         peek dead events
DELETE /v1/subscriptions/{sub}/dlq         purge the DLQ
DELETE /v1/subscriptions/{sub}/dlq/{id}    drop one dead event
```

Subscription inspection exposes only scalars (counts, ages), never cursors
or offsets — same stance as the consumer API.

## Design decisions

### One event per pull

Pulling returns a single event, not a batch. Removes the response envelope, the
batch-size knob, and any partial-failure handling on ack. Concurrency comes
from adding workers.

Tradeoff: per-event HTTP round-trip cost. Acceptable for an internal app event
bus where consumers do real work per event. Would not fit an analytics
ingestion use case.

### Implicit leases, no cursors in the API

Clients never see offsets or cursor tokens. Pull leases; ack releases. Lease
duration and redelivery are server-controlled. This means the server can change
its internal offset/partition representation without breaking clients.

### Implicit subscription membership

There is no explicit join/heartbeat endpoint. Activity on `pull` is the
heartbeat. Inactive workers time out and their leased events become available
to others. Trades a small risk of redelivery thunder for a much simpler API.

### Poison events go to a server-side DLQ

After a server-configured number of redeliveries, an event is moved out of
the subscription's pending set into a per-subscription dead-letter store.
From the consumer's perspective, events stop appearing after N failed
leases — the consumer does not need to track attempt counts or implement
its own poison handling. The DLQ and its peek/drop/purge admin endpoints
are in v1. Redeliver from DLQ is deferred.

### Scheduled delivery is a lower bound

`deliver_at` guarantees an event will not be delivered *earlier* than the
given time. It does not bound how late delivery may be. Two sources of
delay stack:

- The server sweeps the schedule store on a periodic loop; the sweep
  cadence sets a floor on jitter.
- Once swept, the event is appended to the stream and queued like any other
  event. Pull-side latency depends on consumer activity.

The semantic is "deliver no earlier than X, eventually." Consumers that
need tighter timing guarantees should not use this feature.

### Idempotency-Key on publish, optional

At-least-once producers add the header to get effectively-once delivery. Not a
separate concept or endpoint — just a standard HTTP idempotency header.

## Deliberately out of scope (for v1)

These are reasonable extensions but each adds API surface; defer until a real
use case demands them.

- **Explicit replay / seek.** Would be `POST /subscriptions/{sub}/seek` with a
  timestamp or event id. Add when first consumer needs to reprocess history.
- **Non-default starting position.** New subscriptions start at the current
  tail. Starting from the earliest retained event, a timestamp, or a specific
  event id is deferred — same shape as seek, just at create time.
- **Schema enforcement.** Streams currently accept any JSON payload. A future
  version could register a JSON Schema per stream and validate on publish.
- **Redeliver from DLQ.** Admin can peek/drop/purge dead events but not
  redeliver them. Redeliver would re-publish into the stream via the
  normal path, minting a new event id.
- **Pause / resume delivery.** Admin endpoint(s) to halt and resume
  delivery. Scope (global for maintenance, per-stream, or per-subscription)
  TBD when the use case clarifies. Publish behavior while paused (accept
  and queue vs reject) is also TBD.
- **Per-key ordering.** `key` is stored but not yet acted on (see *Publish*).
  Honoring same-key→same-worker in-order delivery means hashing the key to
  one of N sub-streams per subscription, which adds a partition-count knob.
  Deferred until a consumer needs it. Global ordering within a stream is not
  offered either way.
- **Multi-tenancy.** Single global namespace. Add `/v1/tenants/{tenant}/...`
  prefix later if needed.
- **Auth model.** TBD. Likely token-based with per-stream and per-subscription
  ACLs.

## Open questions

- Lease duration: fixed default, per-subscription setting, or per-pull
  parameter?
- Max redelivery count before an event is dropped or DLQ'd?
- Subscription retention: do subscriptions expire if unused for N days?
- Event id format: ULID, UUIDv7, or opaque server-assigned string?

"""Event operations: publish, pull, and ack.

``pull`` enforces lease-and-redeliver semantics: every call first tries to
claim a pending entry that has been idle longer than the subscription's
lease window — that's how unacked events reach another worker — and only
falls back to reading a fresh entry if nothing is idle. The lease window
and the redelivery cap are per-subscription (see
``subscriptions.config``); ``CONFIG`` provides the defaults a sub inherits
when it doesn't override.

``ack`` accepts an optional ``outcome`` (plus ``data``) that drives the
jobs engine: when a worker reports a result, the bus looks up the
job-routing map written at emit time and calls :func:`jobs.handle_ack`. A
bare ack (no outcome) preserves today's behavior. The jobs module is
imported lazily inside ``ack`` to avoid a circular import.

These functions are CLI-only today but could be promoted to meander handlers
later. Do **not** add ``from __future__ import annotations`` — see
``logic/streams.py`` for why.
"""

import json
import os
import socket
from datetime import UTC, datetime

import redis.asyncio as redis

from eventstream import config as CONFIG
from eventstream.logic import backend, dlq, streams, subscriptions


async def publish(stream: str, name: str, payload: dict) -> str:
    """Append an event named ``name`` to ``stream``; return its id.

    ``name`` is the event type — a single stream can carry many kinds, and
    consumers switch on it. Required. The stream is created on first publish.
    """
    fields: dict[str, str] = {"name": name, "payload": json.dumps(payload)}
    event_id = await backend.client().xadd(streams.key(stream), fields)
    await streams.register(stream)
    return event_id


async def pull(subscription: str, *, wait: float | None = None) -> dict | None:
    """Long-poll one event for ``subscription``; return ``None`` on timeout.

    Tries to reclaim an idle pending entry first (older than the
    subscription's lease window), then falls back to a fresh read. If a
    reclaimed event has exceeded the subscription's redelivery cap, it is
    moved to the DLQ and ``None`` is returned. The returned event carries
    ``delivery_count`` (1 on first delivery, 2+ on reclaim).
    """
    if wait is None:
        wait = CONFIG.pull_wait_seconds
    cfg = await subscriptions.config(subscription)
    stream_key = streams.key(cfg["stream"])
    lease_ms = int(cfg["lease_seconds"] * 1000)
    max_deliveries = cfg["max_deliveries"]
    client = backend.client()
    consumer = _consumer()

    reclaimed = await _try_reclaim(client, stream_key, subscription, consumer, lease_ms)
    if reclaimed is not None:
        if reclaimed["delivery_count"] > max_deliveries:
            await dlq.move(subscription, cfg["stream"], reclaimed)
            return None
        return reclaimed

    block = None if wait <= 0 else int(wait * 1000)
    result = await client.xreadgroup(
        subscription, consumer, {stream_key: ">"}, count=1, block=block
    )
    if not result:
        return None
    _, entries = result[0]
    event_id, fields = entries[0]
    return _to_event(event_id, fields, delivery_count=1)


async def ack(
    subscription: str,
    event_id: str,
    *,
    outcome: str | None = None,
    data: dict | None = None,
) -> dict | None:
    """Acknowledge ``event_id``, releasing its lease and advancing the cursor.

    With ``outcome`` set, also drive the workflow engine: look up the
    emit→job map written at publish time and call :func:`jobs.handle_ack`.
    Returns the updated job dict if a job advance happened, or ``None``.

    A bare ack (no ``outcome``) behaves exactly as before: lease released,
    cursor advanced, no jobs side effect.
    """
    advanced = None
    if outcome is not None:
        # Lazy import to avoid the events ↔ jobs circular dependency.
        from eventstream.logic import jobs as jobs_mod

        advanced = await jobs_mod.handle_ack(event_id, outcome, data or {})
    stream = await subscriptions.stream_of(subscription)
    await backend.client().xack(streams.key(stream), subscription, event_id)
    return advanced


async def _try_reclaim(
    client: redis.Redis,
    stream_key: str,
    subscription: str,
    consumer: str,
    min_idle_ms: int,
) -> dict | None:
    """Claim one pending entry idle longer than the lease, if any exists."""
    _next, claimed, _deleted = await client.xautoclaim(
        stream_key,
        subscription,
        consumer,
        min_idle_time=min_idle_ms,
        start_id="0-0",
        count=1,
    )
    if not claimed:
        return None
    event_id, fields = claimed[0]
    delivery_count = await _delivery_count(client, stream_key, subscription, event_id)
    return _to_event(event_id, fields, delivery_count=delivery_count)


async def _delivery_count(
    client: redis.Redis,
    stream_key: str,
    subscription: str,
    event_id: str,
) -> int:
    """Return how many times this pending entry has been delivered."""
    pending = await client.xpending_range(
        stream_key, subscription, event_id, event_id, count=1
    )
    return int(pending[0]["times_delivered"]) if pending else 1


def _consumer() -> str:
    """Return a consumer name identifying this process within a group."""
    return f"{socket.gethostname()}:{os.getpid()}"


def _to_event(event_id: str, fields: dict, *, delivery_count: int) -> dict:
    """Build an event dict from a Redis stream entry."""
    ms = int(event_id.split("-", 1)[0])
    return {
        "id": event_id,
        "name": fields.get("name", ""),
        "payload": json.loads(fields["payload"]),
        "ts": datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat(),
        "delivery_count": delivery_count,
    }

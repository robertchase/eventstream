"""Event operations: publish, pull, and ack.

``pull`` enforces lease-and-redeliver semantics: every call first tries to
claim a pending entry that has been idle longer than the lease window
(``CONFIG.lease_seconds``) — that's how unacked events reach another worker
— and only falls back to reading a fresh entry if nothing is idle.
"""

from __future__ import annotations

import json
import os
import socket
from datetime import UTC, datetime

import redis.asyncio as redis

from eventstream import config as CONFIG
from eventstream.logic import backend, streams, subscriptions


async def publish(stream: str, payload: dict, *, key: str | None = None) -> str:
    """Append an event to ``stream`` and return its server-assigned id.

    The stream is created on first publish. ``key`` is stored alongside the
    event for future per-key ordering; it is optional.
    """
    fields: dict[str, str] = {"payload": json.dumps(payload)}
    if key is not None:
        fields["key"] = key
    event_id = await backend.client().xadd(streams.key(stream), fields)
    await streams.register(stream)
    return event_id


async def pull(subscription: str, *, wait: float | None = None) -> dict | None:
    """Long-poll one event for ``subscription``; return ``None`` on timeout.

    Tries to reclaim an idle pending entry first (older than the lease window),
    then falls back to a fresh read. The returned event carries
    ``delivery_count`` (1 on first delivery, 2+ on reclaim).
    """
    if wait is None:
        wait = CONFIG.pull_wait_seconds
    stream = await subscriptions.stream_of(subscription)
    stream_key = streams.key(stream)
    client = backend.client()
    consumer = _consumer()

    reclaimed = await _try_reclaim(client, stream_key, subscription, consumer)
    if reclaimed is not None:
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


async def ack(subscription: str, event_id: str) -> None:
    """Acknowledge ``event_id``, releasing its lease and advancing the cursor."""
    stream = await subscriptions.stream_of(subscription)
    await backend.client().xack(streams.key(stream), subscription, event_id)


async def _try_reclaim(
    client: redis.Redis,
    stream_key: str,
    subscription: str,
    consumer: str,
) -> dict | None:
    """Claim one pending entry idle longer than the lease, if any exists."""
    min_idle_ms = int(CONFIG.lease_seconds * 1000)
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
    event: dict = {
        "id": event_id,
        "payload": json.loads(fields["payload"]),
        "ts": datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat(),
        "delivery_count": delivery_count,
    }
    if "key" in fields:
        event["key"] = fields["key"]
    return event

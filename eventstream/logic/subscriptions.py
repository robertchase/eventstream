"""Subscription operations: named, durable cursors over a stream."""

from __future__ import annotations

from redis.exceptions import ResponseError

from eventstream.logic import backend, streams
from eventstream.logic.exceptions import SubscriptionExists, SubscriptionNotFound

_REGISTRY = "eventstream:subscriptions"


async def create(name: str, stream: str) -> None:
    """Create a durable subscription on ``stream`` (idempotent).

    A new subscription starts at the current tail of the stream — only events
    published after creation are delivered. Re-creating with the same stream
    is a no-op; re-creating against a different stream is an error.
    """
    client = backend.client()
    existing = await client.hget(_REGISTRY, name)
    if existing is not None and existing != stream:
        raise SubscriptionExists(
            f"subscription {name!r} already exists on stream {existing!r}"
        )
    try:
        await client.xgroup_create(streams.key(stream), name, id="$", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise
    await client.hset(_REGISTRY, name, stream)
    await streams.register(stream)


async def stream_of(name: str) -> str:
    """Return the stream a subscription is bound to, or raise if unknown."""
    stream = await backend.client().hget(_REGISTRY, name)
    if stream is None:
        raise SubscriptionNotFound(f"subscription {name!r} does not exist")
    return stream


async def list_(stream: str | None = None) -> list[dict]:
    """List subscriptions as ``{name, stream}`` dicts, optional stream filter."""
    client = backend.client()
    names = sorted(await client.hkeys(_REGISTRY))
    if not names:
        return []
    bound = await client.hmget(_REGISTRY, names)
    return [
        {"name": name, "stream": s}
        for name, s in zip(names, bound, strict=True)
        if stream is None or s == stream
    ]


async def show(name: str) -> dict:
    """Summary stats for a subscription: lag, in-flight, oldest idle, cursor."""
    stream = await stream_of(name)
    stream_key = streams.key(stream)
    client = backend.client()

    groups = await client.xinfo_groups(stream_key)
    group = next((g for g in groups if g["name"] == name), None)
    if group is None:  # registered but not present in Redis — corrupted state
        raise SubscriptionNotFound(
            f"subscription {name!r} has no consumer group on stream {stream!r}"
        )

    oldest = await client.xpending_range(stream_key, name, "-", "+", count=1)
    oldest_idle_ms = int(oldest[0]["time_since_delivered"]) if oldest else 0

    return {
        "name": name,
        "stream": stream,
        "lag": int(group.get("lag", 0) or 0),
        "in_flight": int(group.get("pending", 0)),
        "oldest_idle_ms": oldest_idle_ms,
        "last_delivered_id": group.get("last-delivered-id"),
    }


async def pending(name: str, *, count: int = 10) -> list[dict]:
    """List pending (leased-but-unacked) entries for a subscription."""
    stream = await stream_of(name)
    entries = await backend.client().xpending_range(
        streams.key(stream), name, "-", "+", count=count
    )
    return [
        {
            "id": e["message_id"],
            "consumer": e["consumer"],
            "idle_ms": int(e["time_since_delivered"]),
            "delivery_count": int(e["times_delivered"]),
        }
        for e in entries
    ]

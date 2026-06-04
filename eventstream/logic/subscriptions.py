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
    items = await backend.client().hgetall(_REGISTRY)
    return [
        {"name": name, "stream": bound}
        for name, bound in sorted(items.items())
        if stream is None or bound == stream
    ]

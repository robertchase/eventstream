"""Stream operations: the append-only logs events are published to."""

from __future__ import annotations

from eventstream.logic import backend

_REGISTRY = "eventstream:streams"


def key(name: str) -> str:
    """Return the Redis key backing the named stream."""
    return f"stream:{name}"


async def register(name: str) -> None:
    """Record that a stream exists (idempotent)."""
    await backend.client().sadd(_REGISTRY, name)


async def list_() -> list[str]:
    """Return the names of all known streams, sorted."""
    return sorted(await backend.client().smembers(_REGISTRY))

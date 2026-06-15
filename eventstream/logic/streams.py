"""Stream operations: the append-only logs events are published to.

Functions in this module are registered as meander HTTP handlers. Do **not**
add ``from __future__ import annotations`` — it turns ``int`` and ``bool``
into strings in ``__annotations__``, which silently defeats meander's query
parameter type coercion (``?count=10`` arrives as ``"10"``).
"""

import json
from datetime import UTC, datetime

from eventstream.logic import backend
from eventstream.logic.exceptions import EventStreamError, StreamNotFound

_REGISTRY = "eventstream:streams"


class StreamHasSubscriptions(EventStreamError):
    """Refused: the stream still has subscriptions and cascade was not set."""


def key(name: str) -> str:
    """Return the Redis key backing the named stream."""
    return f"stream:{name}"


async def register(name: str) -> None:
    """Record that a stream exists (idempotent)."""
    await backend.client().sadd(_REGISTRY, name)


async def list_() -> list[str]:
    """Return the names of all known streams, sorted."""
    return sorted(await backend.client().smembers(_REGISTRY))


async def show(name: str) -> dict:
    """Return metadata for ``name``: length, id range, and group names."""
    await _require_exists(name)
    client = backend.client()
    info = await client.xinfo_stream(key(name))
    groups = await client.xinfo_groups(key(name))
    return {
        "name": name,
        "length": int(info.get("length", 0)),
        "first": _entry_meta(info.get("first-entry")),
        "last": _entry_meta(info.get("last-entry")),
        "groups": sorted(g["name"] for g in groups),
    }


async def peek(name: str, *, count: int = 10, reverse: bool = False) -> list[dict]:
    """Read up to ``count`` events from ``name`` without consuming them.

    Pure ``XRANGE`` / ``XREVRANGE`` — does not touch any consumer group.
    """
    await _require_exists(name)
    client = backend.client()
    if reverse:
        entries = await client.xrevrange(key(name), max="+", min="-", count=count)
    else:
        entries = await client.xrange(key(name), min="-", max="+", count=count)
    return [_event_from_entry(eid, fields) for eid, fields in entries]


async def truncate(name: str, *, keep: int = 0) -> int:
    """Discard events from ``name``, keeping at most ``keep`` newest.

    Destructive: drops events even if subscriptions haven't read them. The
    stream and its consumer groups remain. Returns the number removed.
    """
    await _require_exists(name)
    return await backend.client().xtrim(key(name), maxlen=keep, approximate=False)


async def delete(name: str, *, cascade: bool = False) -> None:
    """Delete a stream. Refused if it has subscriptions unless ``cascade``.

    With ``cascade``, every subscription on the stream is deleted first
    (consumer group, config, and DLQ), then the stream itself.
    """
    await _require_exists(name)
    # Lazy import: subscriptions imports streams, so this avoids a cycle.
    from eventstream.logic import subscriptions

    subs = await subscriptions.list_(name)
    if subs and not cascade:
        raise StreamHasSubscriptions(
            f"stream {name!r} has {len(subs)} subscription(s); "
            f"delete them or pass cascade"
        )
    for sub in subs:
        await subscriptions.delete(sub["name"])
    client = backend.client()
    await client.delete(key(name))
    await client.srem(_REGISTRY, name)


async def _require_exists(name: str) -> None:
    """Raise :class:`StreamNotFound` unless the stream is registered."""
    if not await backend.client().sismember(_REGISTRY, name):
        raise StreamNotFound(f"stream {name!r} does not exist")


def _entry_meta(entry: tuple | None) -> dict | None:
    """Return ``{id, ts}`` for a stream entry, or ``None`` if absent."""
    if not entry:
        return None
    event_id, _fields = entry
    return {"id": event_id, "ts": _ts_from_id(event_id)}


def _event_from_entry(event_id: str, fields: dict) -> dict:
    """Build an event dict for read-only inspection (no delivery count)."""
    event: dict = {
        "id": event_id,
        "payload": json.loads(fields["payload"]),
        "ts": _ts_from_id(event_id),
    }
    if "key" in fields:
        event["key"] = fields["key"]
    return event


def _ts_from_id(event_id: str) -> str:
    """Return the ISO timestamp encoded in a Redis stream id's ms prefix."""
    ms = int(event_id.split("-", 1)[0])
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat()

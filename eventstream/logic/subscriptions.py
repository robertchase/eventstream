"""Subscription operations: named, durable cursors over a stream.

Functions in this module are registered as meander HTTP handlers. Do **not**
add ``from __future__ import annotations`` — see ``logic/streams.py`` for
why.

Storage shape (per subscription)::

    eventstream:subscriptions               SET of subscription names
    eventstream:subscription:<name>         HASH
        stream            (required) the stream this sub is bound to
        lease_seconds     (optional) override of CONFIG.lease_seconds
        max_deliveries    (optional) override of CONFIG.max_deliveries

When a per-sub override is absent, the corresponding ``CONFIG`` default
applies. :func:`config` resolves the effective values.
"""

from redis.exceptions import ResponseError

from eventstream import config as CONFIG
from eventstream.logic import backend, streams
from eventstream.logic.exceptions import SubscriptionExists, SubscriptionNotFound

_INDEX = "eventstream:subscriptions"


def _key(name: str) -> str:
    """Return the Redis hash key holding a subscription's attributes."""
    return f"eventstream:subscription:{name}"


async def create(
    name: str,
    stream: str,
    *,
    lease_seconds: float | None = None,
    max_deliveries: int | None = None,
) -> None:
    """Create a durable subscription on ``stream`` (idempotent).

    A new subscription starts at the current tail of the stream — only events
    published after creation are delivered. Re-creating with the same stream
    is a no-op for the binding; pass ``lease_seconds`` or ``max_deliveries``
    only when creating new subs. Use :func:`set_` to change overrides on an
    existing subscription. Re-creating against a different stream is an
    error.
    """
    client = backend.client()
    existing = await client.hget(_key(name), "stream")
    if existing is not None and existing != stream:
        raise SubscriptionExists(
            f"subscription {name!r} already exists on stream {existing!r}"
        )
    try:
        await client.xgroup_create(streams.key(stream), name, id="$", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise

    fields: dict[str, str] = {"stream": stream}
    if lease_seconds is not None:
        fields["lease_seconds"] = str(lease_seconds)
    if max_deliveries is not None:
        fields["max_deliveries"] = str(max_deliveries)
    await client.hset(_key(name), mapping=fields)
    await client.sadd(_INDEX, name)
    await streams.register(stream)


async def delete(name: str) -> None:
    """Delete a subscription: its consumer group, config, and DLQ.

    Unconditional — nothing depends on a subscription. Dead-lettered events
    for this subscription are discarded along with it.
    """
    stream = await stream_of(name)  # raises SubscriptionNotFound if absent
    client = backend.client()
    # Destroy the consumer group on the stream (no-op if already gone).
    try:
        await client.xgroup_destroy(streams.key(stream), name)
    except ResponseError:
        pass
    await client.delete(_key(name))
    await client.delete(f"eventstream:dlq:{name}")  # mirrors dlq._key layout
    await client.srem(_INDEX, name)


async def set_(
    name: str,
    *,
    lease_seconds: float | None = None,
    max_deliveries: int | None = None,
) -> None:
    """Update an existing subscription's overrides.

    Pass only the fields to change; the stream binding is immutable. Setting
    a value to ``None`` is a no-op — :func:`unset` removes an override and
    reverts to the ``CONFIG`` default.
    """
    if lease_seconds is None and max_deliveries is None:
        await _require_exists(name)
        return
    await _require_exists(name)
    fields: dict[str, str] = {}
    if lease_seconds is not None:
        fields["lease_seconds"] = str(lease_seconds)
    if max_deliveries is not None:
        fields["max_deliveries"] = str(max_deliveries)
    await backend.client().hset(_key(name), mapping=fields)


async def unset(
    name: str,
    *,
    lease_seconds: bool = False,
    max_deliveries: bool = False,
) -> None:
    """Remove explicit overrides, reverting to ``CONFIG`` defaults."""
    await _require_exists(name)
    drop = []
    if lease_seconds:
        drop.append("lease_seconds")
    if max_deliveries:
        drop.append("max_deliveries")
    if drop:
        await backend.client().hdel(_key(name), *drop)


async def config(name: str) -> dict:
    """Return effective configuration: explicit overrides over CONFIG defaults.

    Result keys::

        name, stream, lease_seconds, max_deliveries,
        lease_seconds_explicit, max_deliveries_explicit
    """
    raw = await backend.client().hgetall(_key(name))
    if "stream" not in raw:
        raise SubscriptionNotFound(f"subscription {name!r} does not exist")
    has_lease = "lease_seconds" in raw
    has_max = "max_deliveries" in raw
    return {
        "name": name,
        "stream": raw["stream"],
        "lease_seconds": (
            float(raw["lease_seconds"]) if has_lease else CONFIG.lease_seconds
        ),
        "max_deliveries": (
            int(raw["max_deliveries"]) if has_max else CONFIG.max_deliveries
        ),
        "lease_seconds_explicit": has_lease,
        "max_deliveries_explicit": has_max,
    }


async def stream_of(name: str) -> str:
    """Return the stream a subscription is bound to, or raise if unknown."""
    stream = await backend.client().hget(_key(name), "stream")
    if stream is None:
        raise SubscriptionNotFound(f"subscription {name!r} does not exist")
    return stream


async def list_(stream: str | None = None) -> list[dict]:
    """List subscriptions as ``{name, stream}`` dicts, optional stream filter."""
    client = backend.client()
    names = sorted(await client.smembers(_INDEX))
    if not names:
        return []
    pipe = client.pipeline()
    for n in names:
        pipe.hget(_key(n), "stream")
    bound = await pipe.execute()
    return [
        {"name": n, "stream": s}
        for n, s in zip(names, bound, strict=True)
        if s is not None and (stream is None or s == stream)
    ]


async def show(name: str) -> dict:
    """Summary stats + effective config for a subscription."""
    cfg = await config(name)
    stream_key = streams.key(cfg["stream"])
    client = backend.client()

    groups = await client.xinfo_groups(stream_key)
    group = next((g for g in groups if g["name"] == name), None)
    if group is None:
        raise SubscriptionNotFound(
            f"subscription {name!r} has no consumer group on stream "
            f"{cfg['stream']!r}"
        )

    oldest = await client.xpending_range(stream_key, name, "-", "+", count=1)
    oldest_idle_ms = int(oldest[0]["time_since_delivered"]) if oldest else 0

    return {
        **cfg,
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


async def migrate() -> dict:
    """Convert pre-0.3 storage shape to the per-sub-hash shape.

    Old format had ``eventstream:subscriptions`` as a HASH of name → stream.
    New format uses that same key as a SET of names plus a per-sub HASH at
    ``eventstream:subscription:<name>`` for the stream and optional
    overrides. Safe to run multiple times — re-running on already-migrated
    data is a no-op.

    Returns ``{"migrated": int, "skipped": int, "reason": str | None}``.
    """
    client = backend.client()
    kind = await client.type(_INDEX)
    if kind == "set":
        return {"migrated": 0, "skipped": 0, "reason": "already migrated"}
    if kind == "none":
        return {"migrated": 0, "skipped": 0, "reason": "no data"}
    if kind != "hash":
        return {"migrated": 0, "skipped": 0, "reason": f"unexpected type {kind!r}"}

    old = await client.hgetall(_INDEX)
    if not old:
        await client.delete(_INDEX)
        return {"migrated": 0, "skipped": 0, "reason": "empty hash"}

    migrated = 0
    skipped = 0
    for name, stream in old.items():
        if await client.exists(_key(name)):
            skipped += 1
            continue
        await client.hset(_key(name), mapping={"stream": stream})
        migrated += 1
    await client.delete(_INDEX)
    await client.sadd(_INDEX, *old.keys())
    return {"migrated": migrated, "skipped": skipped, "reason": None}


async def _require_exists(name: str) -> None:
    """Raise :class:`SubscriptionNotFound` unless the sub is in the index."""
    if not await backend.client().sismember(_INDEX, name):
        raise SubscriptionNotFound(f"subscription {name!r} does not exist")

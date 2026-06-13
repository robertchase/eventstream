"""Dead-letter queue operations.

After an event has been redelivered more times than ``CONFIG.max_deliveries``,
the next ``pull`` moves it out of its stream's pending set and into a
per-subscription DLQ hash. The DLQ is keyed by event id; admins can peek,
drop, or purge dead entries. Redelivering from the DLQ is deferred per
``design/api.md``.

When the dead event was emitted by a job (it carries a ``_job`` tag), moving
it to the DLQ also feeds an ``error`` event to that job so a poison step
fails the workflow instead of leaving it stuck — see
:func:`eventstream.logic.jobs.handle_dead`.

Functions here are registered as meander HTTP handlers. Do **not** add
``from __future__ import annotations`` — see ``logic/streams.py`` for why.
"""

import json
import logging
from datetime import UTC, datetime

from eventstream.logic import backend, streams, subscriptions
from eventstream.logic.exceptions import EventNotFound

_log = logging.getLogger("eventstream.jobs")


def _key(subscription: str) -> str:
    """Return the Redis hash key backing a subscription's DLQ."""
    return f"eventstream:dlq:{subscription}"


async def move(subscription: str, stream: str, event: dict) -> None:
    """Move an event to the DLQ and ack it from the stream's consumer group.

    Called by :func:`eventstream.logic.events.pull` when a reclaimed event
    exceeds the redelivery cap. Not part of the public API.

    After the event is durably in the DLQ, if it was a job-step emit, notify
    the job so it can take its ``error`` transition. That notification is
    best-effort and isolated: a failure advancing the job is logged, never
    raised, so it can't break the (unrelated) consumer whose pull triggered
    the move.
    """
    blob = {
        "id": event["id"],
        "stream": stream,
        "key": event.get("key"),
        "payload": event["payload"],
        "ts": event["ts"],
        "delivery_count": event["delivery_count"],
        "dead_at": datetime.now(tz=UTC).isoformat(),
    }
    client = backend.client()
    await client.hset(_key(subscription), event["id"], json.dumps(blob))
    await client.xack(streams.key(stream), subscription, event["id"])

    # Lazy import to avoid the dlq ↔ jobs ↔ events import cycle.
    from eventstream.logic import jobs

    try:
        await jobs.handle_dead(event["id"])
    except Exception:  # noqa: BLE001 - the DLQ entry is durable; isolate failures
        _log.exception("failed to notify job of dead event %s", event["id"])


async def peek(subscription: str, *, count: int = 10) -> list[dict]:
    """Return up to ``count`` dead events for ``subscription``, oldest first."""
    await subscriptions.stream_of(subscription)
    client = backend.client()
    ids = sorted(await client.hkeys(_key(subscription)))[:count]
    if not ids:
        return []
    values = await client.hmget(_key(subscription), ids)
    return [json.loads(v) for v in values if v]


async def drop(subscription: str, event_id: str) -> None:
    """Remove one event from a subscription's DLQ."""
    await subscriptions.stream_of(subscription)
    removed = await backend.client().hdel(_key(subscription), event_id)
    if not removed:
        raise EventNotFound(
            f"event {event_id!r} is not in the DLQ for {subscription!r}"
        )


async def purge(subscription: str) -> None:
    """Remove every dead event for ``subscription``."""
    await subscriptions.stream_of(subscription)
    await backend.client().delete(_key(subscription))

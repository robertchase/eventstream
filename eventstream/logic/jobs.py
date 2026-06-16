"""Job lifecycle: create / get / list / advance / cancel + ack routing + timers.

The persistence wrapper around :mod:`eventstream.logic.engine`. The engine
runs in memory and produces a :class:`Recorder` describing side effects
(emitted events, scheduled timers, transitions); this module flushes them:
persists the new job state to Redis, publishes the recorded emits via
:mod:`eventstream.logic.events`, and writes the **emit-routing map** so a
worker's ack-with-outcome can find its way back to the right job.

Storage shape::

    eventstream:jobs                            SET of job ids
    eventstream:job:<id>                        HASH {workflow, version,
                                                       state, context,
                                                       status, created_at,
                                                       updated_at}
    eventstream:job:<id>:history                LIST of JSON transition records
    eventstream:emitted:<event_id>              HASH {job_id, state}
                                                  (TTL EMIT_MAP_TTL_SECONDS)
    eventstream:job_timers                      ZSET member=JSON({job_id,event,
                                                                 nonce})
                                                     score=fire_at_unix

``handle_ack`` is called from :func:`eventstream.logic.events.ack` when a
worker passes an ``outcome``. Stale acks (job has moved past the state it
was in when the emit was made) are silent no-ops; double-acks find no map
entry (because the first deleted it) and are likewise no-ops. The state
check + single-use map entry together provide the idempotency the design
calls for.

``tick`` fires due timers — call it periodically (e.g.
``eventstream jobs tick`` from cron, or wrap in a sweeper loop). When a
timer fires, the engine processes ``timer["event"]`` against the job's
current state, just like any other external event.

Functions in this module are registered as meander HTTP handlers. Do **not**
add ``from __future__ import annotations`` — see ``logic/streams.py`` for
why.
"""

import asyncio
import json
import logging
import secrets
import time
from datetime import UTC, datetime

from eventstream.logic import backend, engine, events, workflows
from eventstream.logic.exceptions import EventStreamError

_INDEX = "eventstream:jobs"
_TIMERS = "eventstream:job_timers"

_sweep_log = logging.getLogger("eventstream.jobs")

# Set by a running in-process sweeper (see ``sweep_forever``) so timer arming
# in the same process can wake it early. ``None`` when no sweeper runs here
# (e.g. a plain CLI ``jobs create``), in which case arming just persists the
# timer and any sweeper in another process picks it up on its own schedule.
_timer_armed: asyncio.Event | None = None

EMIT_MAP_TTL_SECONDS = 24 * 60 * 60  # 1 day; old entries auto-evict


class JobNotFound(EventStreamError):
    """A referenced job does not exist."""


class JobNotRunning(EventStreamError):
    """A mutation was attempted on a job that is no longer running."""


class JobRunning(EventStreamError):
    """A destructive operation was refused because the job is still running."""


def _job_key(job_id: str) -> str:
    return f"eventstream:job:{job_id}"


def _history_key(job_id: str) -> str:
    return f"eventstream:job:{job_id}:history"


def _emitted_key(event_id: str) -> str:
    return f"eventstream:emitted:{event_id}"


def _new_job_id() -> str:
    return f"job_{secrets.token_hex(8)}"


def _iso(epoch_seconds: int) -> str:
    """Render a unix timestamp as an ISO 8601 UTC string for ``$job.now``.

    Derived from the same tick used for the job's stored timestamps, so
    ``$job.now`` agrees with ``created_at``/``updated_at`` to the second.
    """
    return datetime.fromtimestamp(epoch_seconds, tz=UTC).isoformat()


async def create(
    workflow_name: str,
    context: dict | None = None,
    *,
    workflow_version: int | None = None,
) -> dict:
    """Create a new job, run its initial enter actions, and persist."""
    if context is None:
        context = {}
    wf = await workflows.get(workflow_name, version=workflow_version)
    ast = wf["ast"]
    initial_state = ast["initial"]

    job_id = _new_job_id()
    recorder = engine.Recorder(job_id)
    now = int(time.time())
    job_meta = {
        "workflow": ast["name"],
        "version": wf["version"],
        "now": _iso(now),
    }

    enter_refs = ast["states"][initial_state].get("enter", [])
    carry = engine._run_actions(
        enter_refs,
        ast,
        context,
        {"name": "_create"},
        recorder,
        initial=None,
        job=engine._job_scope(recorder, job_meta, initial_state),
    )
    state = initial_state
    if carry is not None:
        state = engine.step(
            ast, state, context, carry, recorder=recorder, job_meta=job_meta
        )

    status = "terminal" if ast["states"][state].get("terminal") else "running"

    await _persist_create(
        job_id, workflow_name, wf["version"], state, context, status, now, recorder
    )
    return await get(job_id)


async def get(job_id: str) -> dict:
    """Return a job's persisted state (no history)."""
    raw = await backend.client().hgetall(_job_key(job_id))
    if not raw:
        raise JobNotFound(f"job {job_id!r} does not exist")
    return {
        "id": job_id,
        "workflow": raw["workflow"],
        "version": int(raw["version"]),
        "state": raw["state"],
        "context": json.loads(raw["context"]),
        "status": raw["status"],
        "created_at": int(raw["created_at"]),
        "updated_at": int(raw["updated_at"]),
    }


async def history(job_id: str) -> list[dict]:
    """Return the full transition history for a job, oldest first."""
    client = backend.client()
    if not await client.exists(_job_key(job_id)):
        raise JobNotFound(f"job {job_id!r} does not exist")
    raw = await client.lrange(_history_key(job_id), 0, -1)
    return [json.loads(entry) for entry in raw]


async def list_(
    *, workflow: str | None = None, status: str | None = None
) -> list[dict]:
    """List jobs, optionally filtered by workflow name or status."""
    client = backend.client()
    ids = sorted(await client.smembers(_INDEX))
    result = []
    for job_id in ids:
        raw = await client.hgetall(_job_key(job_id))
        if not raw:
            continue
        if workflow and raw["workflow"] != workflow:
            continue
        if status and raw["status"] != status:
            continue
        result.append(
            {
                "id": job_id,
                "workflow": raw["workflow"],
                "version": int(raw["version"]),
                "state": raw["state"],
                "status": raw["status"],
                "created_at": int(raw["created_at"]),
                "updated_at": int(raw["updated_at"]),
            }
        )
    return result


async def advance(job_id: str, event_name: str, data: dict | None = None) -> dict:
    """Feed one external event into a job and persist the new state."""
    job = await get(job_id)
    if job["status"] != "running":
        raise JobNotRunning(f"job {job_id} is {job['status']!r}, cannot advance")
    wf = await workflows.get(job["workflow"], version=job["version"])
    ast = wf["ast"]

    context = job["context"]
    recorder = engine.Recorder(job_id)
    event = {"name": event_name, "data": data or {}}
    now = int(time.time())
    job_meta = {
        "workflow": job["workflow"],
        "version": job["version"],
        "now": _iso(now),
    }
    new_state = engine.step(
        ast, job["state"], context, event, recorder=recorder, job_meta=job_meta
    )

    status = "terminal" if ast["states"][new_state].get("terminal") else "running"
    await _persist_update(job_id, new_state, context, status, recorder, now)
    return await get(job_id)


async def cancel(job_id: str) -> None:
    """Mark a running job as cancelled. No-op on already-finished jobs."""
    job = await get(job_id)
    if job["status"] != "running":
        return
    now = int(time.time())
    await backend.client().hset(
        _job_key(job_id),
        mapping={"status": "cancelled", "updated_at": str(now)},
    )


async def delete(job_id: str, *, force: bool = False) -> None:
    """Remove a job: its state, history, and any pending timers.

    Running jobs are refused unless ``force`` — cancel first, or pass
    ``force=True`` to delete regardless. Emit-routing map entries are left
    to their TTL; an ack arriving for a deleted job is already a silent
    no-op in :func:`handle_ack`.
    """
    job = await get(job_id)
    if job["status"] == "running" and not force:
        raise JobRunning(f"job {job_id} is running; cancel it first or use force")
    client = backend.client()
    await client.delete(_job_key(job_id))
    await client.delete(_history_key(job_id))
    await client.srem(_INDEX, job_id)
    for member in await client.zrange(_TIMERS, 0, -1):
        try:
            entry = json.loads(member)
        except json.JSONDecodeError:
            continue
        if entry.get("job_id") == job_id:
            await client.zrem(_TIMERS, member)


async def _route(event_id: str, event_name: str, data: dict) -> dict | None:
    """Route an emitted event back to its job and advance it.

    Looks up the emit→job map written at publish time. If found and the job
    is still in the state recorded at emit time (and still running), advances
    it with ``event_name``. Missing entries (double-ack, expired, or not a
    job emit), stale entries (the FSM moved on), and non-running jobs are all
    silent no-ops. The map entry is single-use — deleted on the first match
    so a redelivered ack or a late DLQ notice finds nothing.
    Returns the updated job dict if an advance happened, else ``None``.
    """
    client = backend.client()
    raw = await client.hgetall(_emitted_key(event_id))
    if not raw:
        return None
    await client.delete(_emitted_key(event_id))

    try:
        job = await get(raw["job_id"])
    except JobNotFound:
        return None
    if job["state"] != raw["state"] or job["status"] != "running":
        return None

    try:
        return await advance(raw["job_id"], event_name, data)
    except JobNotRunning:
        return None  # raced with another writer; nothing to do


async def handle_ack(event_id: str, outcome: str, data: dict) -> dict | None:
    """Route a worker's ack-with-outcome through the engine, if applicable.

    Called from :func:`eventstream.logic.events.ack`. See :func:`_route` for
    the routing and idempotency rules. The bus side still XACKs the
    underlying event regardless of whether a job advance happened.
    """
    return await _route(event_id, outcome, data)


async def handle_dead(event_id: str) -> dict | None:
    """Route a DLQ'd job-step event into the job as an ``error`` event.

    Called from :func:`eventstream.logic.dlq.move` when a step event exhausts
    its redelivery cap. A workflow handles it like any other event — typically
    ``DEFAULT error failed`` — so a poison step fails the job instead of
    leaving it stuck forever. Non-job events and stale/finished jobs are
    silent no-ops (see :func:`_route`).
    """
    return await _route(
        event_id,
        "error",
        {"reason": "max_deliveries_exceeded", "event_id": event_id},
    )


async def sweep_forever(idle_interval: float, *, iterations: int | None = None) -> None:
    """Fire due timers, then sleep until the *next* timer is due — forever.

    Rather than polling on a fixed cadence, each pass fires everything due
    (:func:`tick`) and then sleeps exactly until the earliest pending timer's
    fire-time. Arming an earlier timer in this same process wakes the sleep
    immediately (see :func:`_maybe_wake_sweeper`); a later one leaves it
    waiting. When no timers are pending it waits up to ``idle_interval`` (a
    bounded re-check so timers armed by *other* processes — e.g. a CLI
    ``jobs create`` — are still noticed; in-process arming is always prompt).

    The loop never raises: a failing tick is logged and the loop continues,
    so this is safe to register as a meander background task (a raising task
    would cancel the server's task group). ``iterations`` bounds the loop for
    tests; ``None`` means run until cancelled.
    """
    global _timer_armed
    _timer_armed = asyncio.Event()
    count = 0
    while iterations is None or count < iterations:
        try:
            await tick()
        except Exception:  # noqa: BLE001 - a bad tick must not kill the sweeper
            _sweep_log.exception("timer sweep failed; continuing")
        count += 1
        if iterations is not None and count >= iterations:
            break
        await _wait_for_next(idle_interval)


async def _wait_for_next(idle_interval: float) -> None:
    """Block until the earliest pending timer is due, or woken early.

    Returns when (a) the next timer's fire-time arrives, (b) an earlier timer
    is armed in this process and signals :data:`_timer_armed`, or (c) — only
    when nothing is pending — ``idle_interval`` elapses. Clearing the event
    *before* reading the head avoids missing a wake armed concurrently.
    """
    _timer_armed.clear()
    next_at = await _next_timer_at()
    if next_at is None:
        timeout = idle_interval
    else:
        timeout = max(0.0, next_at - time.time())
    if timeout <= 0:
        return
    try:
        await asyncio.wait_for(_timer_armed.wait(), timeout=timeout)
    except TimeoutError:
        pass


async def _next_timer_at() -> float | None:
    """Fire-time (unix seconds) of the earliest pending timer, or ``None``."""
    head = await backend.client().zrange(_TIMERS, 0, 0, withscores=True)
    return float(head[0][1]) if head else None


async def _maybe_wake_sweeper(earliest_armed: int) -> None:
    """Wake an in-process sweeper if a just-armed timer is now the head.

    A no-op when no sweeper runs in this process, when one is already
    signalled, or when the armed timer sits behind an earlier pending one
    (the sweeper is already waiting on something at least as soon).
    """
    if _timer_armed is None or _timer_armed.is_set():
        return
    head = await _next_timer_at()
    if head is not None and earliest_armed <= head:
        _timer_armed.set()


async def tick() -> dict:
    """Sweep due timers; fire each as an event against its job.

    Returns ``{fired: N, dropped: M}`` where ``dropped`` counts timers whose
    job no longer exists or is no longer running (and the timer is removed).
    Call periodically — e.g. ``eventstream jobs tick`` from cron, or wrap
    in a sweeper loop.
    """
    client = backend.client()
    now = int(time.time())
    due = await client.zrangebyscore(_TIMERS, 0, now)
    fired = 0
    dropped = 0
    for member in due:
        try:
            entry = json.loads(member)
        except json.JSONDecodeError:
            await client.zrem(_TIMERS, member)
            dropped += 1
            continue
        try:
            await advance(entry["job_id"], entry["event"], {})
            fired += 1
        except (JobNotFound, JobNotRunning):
            dropped += 1
        await client.zrem(_TIMERS, member)
    return {"fired": fired, "dropped": dropped}


async def _persist_create(
    job_id, workflow_name, version, state, context, status, now, recorder
):
    client = backend.client()
    await client.hset(
        _job_key(job_id),
        mapping={
            "workflow": workflow_name,
            "version": str(version),
            "state": state,
            "context": json.dumps(context),
            "status": status,
            "created_at": str(now),
            "updated_at": str(now),
        },
    )
    await client.sadd(_INDEX, job_id)
    await _flush_side_effects(client, job_id, recorder, now, current_state=state)


async def _persist_update(job_id, state, context, status, recorder, now):
    client = backend.client()
    await client.hset(
        _job_key(job_id),
        mapping={
            "state": state,
            "context": json.dumps(context),
            "status": status,
            "updated_at": str(now),
        },
    )
    await _flush_side_effects(client, job_id, recorder, now, current_state=state)


async def _flush_side_effects(client, job_id, recorder, now, *, current_state):
    """Publish recorded emits, append history, register routing map, schedule timers."""
    for entry in recorder.journal:
        await client.rpush(_history_key(job_id), json.dumps({**entry, "ts": now}))

    for emit in recorder.emits:
        # The workflow's EMIT event-type becomes the event's name; _job is an
        # informational tag (routing back to the job is via the emit map below).
        payload = {"_job": job_id, **emit["payload"]}
        event_id = await events.publish(emit["stream"], emit["event_type"], payload)
        # Register the emit → job map so a worker's ack-with-outcome can
        # route back to this job in this state. Single-use, with a TTL so
        # never-acked emits don't accumulate forever.
        await client.hset(
            _emitted_key(event_id),
            mapping={"job_id": job_id, "state": current_state},
        )
        await client.expire(_emitted_key(event_id), EMIT_MAP_TTL_SECONDS)

    earliest_armed: int | None = None
    for timer in recorder.timers:
        fire_at = now + int(timer["delay_seconds"])
        member = json.dumps(
            {
                "job_id": job_id,
                "event": timer["event"],
                "nonce": secrets.token_hex(4),
            }
        )
        await client.zadd(_TIMERS, {member: fire_at})
        earliest_armed = (
            fire_at if earliest_armed is None else min(earliest_armed, fire_at)
        )

    if earliest_armed is not None:
        await _maybe_wake_sweeper(earliest_armed)

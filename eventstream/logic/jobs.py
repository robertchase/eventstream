"""Job lifecycle: create / get / list / advance / cancel.

This is the persistence wrapper around :mod:`eventstream.logic.engine`. The
engine runs in memory and produces a :class:`Recorder` describing side
effects (emitted events, scheduled timers, transitions); this module flushes
them: persists the new job state to Redis and publishes the recorded emits
to their streams via :mod:`eventstream.logic.events`.

Storage shape::

    eventstream:jobs                            SET of job ids
    eventstream:job:<id>                        HASH {workflow, version, state,
                                                       context, status,
                                                       created_at, updated_at}
    eventstream:job:<id>:history                LIST of JSON transition records

Timer firing is **not** implemented yet — timers are recorded by the engine
but the sweep that fires them is a separate landing. Same for the
ack-with-outcome plumbing that lets the bus drive job advances. For now,
``advance(id, event, data)`` is invoked manually via the CLI.

Functions in this module are registered as meander HTTP handlers. Do **not**
add ``from __future__ import annotations`` — see ``logic/streams.py`` for
why.
"""

import json
import secrets
import time

from eventstream.logic import backend, engine, events, workflows
from eventstream.logic.exceptions import EventStreamError

_INDEX = "eventstream:jobs"


class JobNotFound(EventStreamError):
    """A referenced job does not exist."""


class JobNotRunning(EventStreamError):
    """A mutation was attempted on a job that is no longer running."""


def _job_key(job_id: str) -> str:
    return f"eventstream:job:{job_id}"


def _history_key(job_id: str) -> str:
    return f"eventstream:job:{job_id}:history"


def _new_job_id() -> str:
    return f"job_{secrets.token_hex(8)}"


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

    # The job starts in `initial_state`. Run its enter actions, then if the
    # last enter action emitted, cascade through the engine on that carry.
    enter_refs = ast["states"][initial_state].get("enter", [])
    carry = None
    for ref in enter_refs:
        carry = engine._execute_action(
            ast["actions"][ref], ast, context, {"name": "_create"}, recorder
        )
    state = initial_state
    if carry is not None:
        state = engine.step(ast, state, context, carry, recorder=recorder)

    status = "terminal" if ast["states"][state].get("terminal") else "running"
    now = int(time.time())

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
    new_state = engine.step(ast, job["state"], context, event, recorder=recorder)

    status = "terminal" if ast["states"][new_state].get("terminal") else "running"
    await _persist_update(job_id, new_state, context, status, recorder)
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
    await _flush_side_effects(client, job_id, recorder, now)


async def _persist_update(job_id, state, context, status, recorder):
    client = backend.client()
    now = int(time.time())
    await client.hset(
        _job_key(job_id),
        mapping={
            "state": state,
            "context": json.dumps(context),
            "status": status,
            "updated_at": str(now),
        },
    )
    await _flush_side_effects(client, job_id, recorder, now)


async def _flush_side_effects(client, job_id, recorder, now):
    """Publish recorded emits and append history. Timers are not yet fired."""
    for trans in recorder.transitions:
        await client.rpush(_history_key(job_id), json.dumps({**trans, "ts": now}))
    for emit in recorder.emits:
        payload = {"_event": emit["event_type"], "_job": job_id, **emit["payload"]}
        await events.publish(emit["stream"], payload)

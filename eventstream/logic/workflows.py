"""Workflow registry: parse + store + retrieve workflow definitions.

Storage shape (per ``design/workflow-format.md``)::

    eventstream:workflows                       SET of names
    eventstream:workflow:<name>:versions        SORTED SET version → registered_ts
    eventstream:workflow:<name>:<version>       HASH {source, ast}

Each call to :func:`register` adds a new version of the workflow. There is no
in-place edit. :func:`get` returns the latest version unless one is named.

Functions in this module are registered as meander HTTP handlers. Do **not**
add ``from __future__ import annotations`` — see ``logic/streams.py`` for
why.
"""

import json
import time

from eventstream.logic import backend, workflow_parser
from eventstream.logic.exceptions import EventStreamError

_INDEX = "eventstream:workflows"


class WorkflowNotFound(EventStreamError):
    """A referenced workflow (or workflow version) does not exist."""


def _versions_key(name: str) -> str:
    return f"eventstream:workflow:{name}:versions"


def _payload_key(name: str, version: int) -> str:
    return f"eventstream:workflow:{name}:{version}"


async def register(source: str) -> dict:
    """Parse, validate, and store a workflow definition.

    Returns ``{name, version, registered_at}``. Raises
    :class:`workflow_parser.ParseError` on a bad source.
    """
    ast = workflow_parser.parse(source)
    name = ast["name"]
    client = backend.client()

    latest = await client.zrange(_versions_key(name), -1, -1, withscores=False)
    version = (int(latest[0]) + 1) if latest else 1

    registered_at = int(time.time())
    await client.hset(
        _payload_key(name, version),
        mapping={"source": source, "ast": json.dumps(ast)},
    )
    await client.zadd(_versions_key(name), {str(version): registered_at})
    await client.sadd(_INDEX, name)
    return {"name": name, "version": version, "registered_at": registered_at}


async def list_() -> list[dict]:
    """Return every workflow with its latest version."""
    client = backend.client()
    names = sorted(await client.smembers(_INDEX))
    if not names:
        return []
    pipe = client.pipeline()
    for name in names:
        pipe.zrange(_versions_key(name), -1, -1, withscores=False)
    versions = await pipe.execute()
    return [
        {"name": name, "latest_version": int(v[0]) if v else None}
        for name, v in zip(names, versions, strict=True)
    ]


async def get(name: str, version: int | None = None) -> dict:
    """Return a workflow's stored ``{name, version, source, ast}``.

    When ``version`` is None, returns the latest. Raises
    :class:`WorkflowNotFound` when the workflow or version is absent.
    """
    client = backend.client()
    if version is None:
        latest = await client.zrange(_versions_key(name), -1, -1, withscores=False)
        if not latest:
            raise WorkflowNotFound(f"workflow {name!r} does not exist")
        version = int(latest[0])

    raw = await client.hgetall(_payload_key(name, version))
    if "source" not in raw:
        raise WorkflowNotFound(f"workflow {name!r} has no version {version}")
    return {
        "name": name,
        "version": version,
        "source": raw["source"],
        "ast": json.loads(raw["ast"]),
    }


async def versions(name: str) -> list[int]:
    """Return all stored versions of a workflow, ascending."""
    client = backend.client()
    raw = await client.zrange(_versions_key(name), 0, -1, withscores=False)
    if not raw:
        if not await client.sismember(_INDEX, name):
            raise WorkflowNotFound(f"workflow {name!r} does not exist")
    return [int(v) for v in raw]


async def delete(name: str) -> None:
    """Remove every version of a workflow."""
    client = backend.client()
    if not await client.sismember(_INDEX, name):
        raise WorkflowNotFound(f"workflow {name!r} does not exist")
    raw = await client.zrange(_versions_key(name), 0, -1, withscores=False)
    pipe = client.pipeline()
    for v in raw:
        pipe.delete(_payload_key(name, int(v)))
    pipe.delete(_versions_key(name))
    pipe.srem(_INDEX, name)
    await pipe.execute()

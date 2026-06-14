"""HTTP write handlers for the ``/v1`` API.

Unlike the read routes (where a logic function's return value *is* the JSON),
writes need thin adapters: they shape the response to the ``design/api.md``
contract — ``{"id": …}`` on publish, ``201`` on create, ``204`` on ack and
empty pull — and bind the request body to logic kwargs.

This iteration covers the producer/consumer core four (publish, pull, ack,
create-subscription). Admin mutations over HTTP (dlq drop/purge, workflow
register/delete, job create/advance/cancel/delete) are a later step; the CLI
remains their path until then.

meander binds path captures + JSON-body keys to these handlers' parameters by
name, so the body shape is the parameter list. Do **not** add
``from __future__ import annotations`` — it disables meander's type coercion
(see ``logic/streams.py``).
"""

import meander

from eventstream.logic import events, subscriptions


async def publish_event(stream, payload: dict, key: str | None = None):
    """POST /v1/streams/{stream}/events — body ``{payload, key?}`` → ``{id}``."""
    event_id = await events.publish(stream, payload, key=key)
    return {"id": event_id}


async def pull_event(sub, wait: float = -1.0):
    """GET /v1/subscriptions/{sub}/pull?wait= — one event, or 204 on timeout.

    ``wait`` is seconds; omit it (sentinel ``-1``) to use the server default.
    """
    window = None if wait < 0 else wait
    event = await events.pull(sub, wait=window)
    if event is None:
        return meander.Response(code=204)
    return event


async def ack_event(
    sub, event_id, outcome: str | None = None, data: dict | None = None
):
    """POST /v1/subscriptions/{sub}/ack/{id} — body optional ``{outcome, data}``.

    With ``outcome`` set, drives the workflow engine (ack-with-outcome). Always
    returns 204 per the API contract; the job advance is a side effect.
    """
    await events.ack(sub, event_id, outcome=outcome, data=data)
    return meander.Response(code=204)


async def create_subscription(
    name,
    stream,
    lease_seconds: float | None = None,
    max_deliveries: int | None = None,
):
    """POST /v1/subscriptions — body ``{name, stream, lease_seconds?,
    max_deliveries?}``."""
    await subscriptions.create(
        name, stream, lease_seconds=lease_seconds, max_deliveries=max_deliveries
    )
    return meander.Response(content={"name": name, "stream": stream}, code=201)

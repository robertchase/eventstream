"""``eventstream ack`` — acknowledge a leased event, optionally with outcome."""

from __future__ import annotations

import json

import click

from eventstream.cli._run import coroutine
from eventstream.logic import events


@click.command()
@click.argument("subscription")
@click.argument("event_id")
@click.option(
    "--outcome",
    default=None,
    help="Workflow event name to advance a job with (ack-with-outcome).",
)
@click.option(
    "--data",
    default=None,
    help="JSON object passed as $event.data to the workflow handler.",
)
@coroutine
async def ack(
    subscription: str,
    event_id: str,
    outcome: str | None,
    data: str | None,
) -> None:
    """Acknowledge EVENT_ID on SUBSCRIPTION.

    With ``--outcome`` (and optional ``--data``), the bus looks up the
    job that emitted this event and drives ``jobs.advance(...)`` before
    XACKing. Bare ack (no outcome) behaves as before.
    """
    body = None
    if data is not None:
        if outcome is None:
            raise click.UsageError("--data requires --outcome")
        try:
            body = json.loads(data)
        except json.JSONDecodeError as exc:
            raise click.UsageError(f"--data is not valid JSON: {exc}") from exc
        if not isinstance(body, dict):
            raise click.UsageError("--data must be a JSON object")
    advanced = await events.ack(subscription, event_id, outcome=outcome, data=body)
    if advanced:
        click.echo(f"{advanced['id']}\t{advanced['state']}\t{advanced['status']}")

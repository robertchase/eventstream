"""``eventstream ack`` — acknowledge a leased event."""

from __future__ import annotations

import click

from eventstream.cli._run import coroutine
from eventstream.logic import events


@click.command()
@click.argument("subscription")
@click.argument("event_id")
@coroutine
async def ack(subscription: str, event_id: str) -> None:
    """Acknowledge EVENT_ID on SUBSCRIPTION."""
    await events.ack(subscription, event_id)

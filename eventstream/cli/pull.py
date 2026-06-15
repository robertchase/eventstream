"""``eventstream pull`` — long-poll one event from a subscription."""

from __future__ import annotations

import json

import click

from eventstream.cli._run import coroutine
from eventstream.logic import events


@click.command()
@click.argument("subscription")
@click.option("--wait", type=float, default=None, help="Long-poll seconds.")
@click.option("--ack", "auto_ack", is_flag=True, help="Ack right after pulling.")
@click.option("--json", "as_json", is_flag=True, help="Print the raw event as JSON.")
@coroutine
async def pull(
    subscription: str,
    wait: float | None,
    auto_ack: bool,
    as_json: bool,
) -> None:
    """Pull one event for SUBSCRIPTION, or report nothing within the window."""
    event = await events.pull(subscription, wait=wait)
    if event is None:
        click.echo("(no event)", err=True)
        return
    if auto_ack:
        await events.ack(subscription, event["id"])
    click.echo(json.dumps(event) if as_json else _format(event))


def _format(event: dict) -> str:
    """One-line human rendering of an event."""
    parts = [event["id"], event["name"]]
    if event.get("delivery_count", 1) > 1:
        parts.append(f"delivery={event['delivery_count']}")
    parts.append(json.dumps(event["payload"]))
    return "  ".join(parts)

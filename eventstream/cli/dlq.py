"""``eventstream dlq`` — dead-letter queue administration."""

from __future__ import annotations

import json

import click

from eventstream.cli._run import coroutine
from eventstream.logic import dlq as dlq_logic


@click.group()
def dlq() -> None:
    """Manage the dead-letter queue."""


@dlq.command()
@click.argument("subscription")
@click.option("--count", "-n", type=int, default=10, help="How many entries.")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON instead of text.")
@coroutine
async def peek(subscription: str, count: int, as_json: bool) -> None:
    """Show dead events for SUBSCRIPTION."""
    entries = await dlq_logic.peek(subscription, count=count)
    if as_json:
        click.echo(json.dumps(entries))
        return
    if not entries:
        click.echo("(no dead events)", err=True)
        return
    for entry in entries:
        parts = [entry["id"], f"delivery={entry['delivery_count']}"]
        if entry.get("key"):
            parts.append(f"key={entry['key']}")
        parts.append(json.dumps(entry["payload"]))
        click.echo("  ".join(parts))


@dlq.command()
@click.argument("subscription")
@click.argument("event_id")
@coroutine
async def drop(subscription: str, event_id: str) -> None:
    """Remove EVENT_ID from SUBSCRIPTION's DLQ."""
    await dlq_logic.drop(subscription, event_id)


@dlq.command()
@click.argument("subscription")
@coroutine
async def purge(subscription: str) -> None:
    """Remove every dead event for SUBSCRIPTION."""
    await dlq_logic.purge(subscription)

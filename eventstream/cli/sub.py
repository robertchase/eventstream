"""``eventstream sub`` — subscription administration."""

from __future__ import annotations

import click

from eventstream.cli._run import coroutine
from eventstream.logic import subscriptions


@click.group()
def sub() -> None:
    """Manage subscriptions."""


@sub.command()
@click.argument("name")
@click.option("--stream", required=True, help="Stream to subscribe to.")
@coroutine
async def create(name: str, stream: str) -> None:
    """Create durable subscription NAME on a stream."""
    await subscriptions.create(name, stream)


@sub.command(name="list")
@click.option("--stream", default=None, help="Filter by stream.")
@coroutine
async def list_(stream: str | None) -> None:
    """List subscriptions."""
    for item in await subscriptions.list_(stream):
        click.echo(f"{item['name']}\t{item['stream']}")

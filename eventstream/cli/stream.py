"""``eventstream stream`` — stream administration."""

from __future__ import annotations

import click

from eventstream.cli._run import coroutine
from eventstream.logic import streams


@click.group()
def stream() -> None:
    """Manage streams."""


@stream.command(name="list")
@coroutine
async def list_() -> None:
    """List known streams."""
    for name in await streams.list_():
        click.echo(name)

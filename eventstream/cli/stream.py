"""``eventstream stream`` — stream administration."""

from __future__ import annotations

import json

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


@stream.command()
@click.argument("name")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON instead of text.")
@coroutine
async def show(name: str, as_json: bool) -> None:
    """Show metadata for stream NAME."""
    info = await streams.show(name)
    if as_json:
        click.echo(json.dumps(info))
        return
    click.echo(f"stream:   {info['name']}")
    click.echo(f"length:   {info['length']}")
    click.echo(f"first:    {_format_entry(info['first'])}")
    click.echo(f"last:     {_format_entry(info['last'])}")
    click.echo(f"groups:   {', '.join(info['groups']) if info['groups'] else '—'}")


@stream.command()
@click.argument("name")
@click.option("--count", "-n", type=int, default=10, help="How many events.")
@click.option("--reverse", "-r", is_flag=True, help="Newest first.")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON instead of text.")
@coroutine
async def peek(name: str, count: int, reverse: bool, as_json: bool) -> None:
    """Read events from STREAM without consuming them."""
    events = await streams.peek(name, count=count, reverse=reverse)
    if as_json:
        click.echo(json.dumps(events))
        return
    for event in events:
        line = event["id"]
        if "key" in event:
            line += f"  key={event['key']}"
        line += f"  {json.dumps(event['payload'])}"
        click.echo(line)


@stream.command()
@click.argument("name")
@click.option(
    "--keep", type=int, default=0, help="Keep at most this many newest events."
)
@coroutine
async def truncate(name: str, keep: int) -> None:
    """Discard events from STREAM (keeping the newest --keep); prints removed."""
    removed = await streams.truncate(name, keep=keep)
    click.echo(f"removed {removed}")


@stream.command()
@click.argument("name")
@click.option(
    "--cascade",
    is_flag=True,
    help="Also delete subscriptions on the stream.",
)
@coroutine
async def delete(name: str, cascade: bool) -> None:
    """Delete STREAM. Refused if it has subscriptions unless --cascade."""
    await streams.delete(name, cascade=cascade)


def _format_entry(entry: dict | None) -> str:
    """Render a ``{id, ts}`` entry summary for human output."""
    if entry is None:
        return "—"
    return f"{entry['id']}  ({entry['ts']})"

"""``eventstream sub`` — subscription administration."""

from __future__ import annotations

import json

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


@sub.command()
@click.argument("name")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON instead of text.")
@coroutine
async def show(name: str, as_json: bool) -> None:
    """Show summary stats for subscription NAME."""
    info = await subscriptions.show(name)
    if as_json:
        click.echo(json.dumps(info))
        return
    idle = f"{info['oldest_idle_ms'] / 1000:.1f}s" if info["in_flight"] else "—"
    click.echo(f"subscription:    {info['name']}")
    click.echo(f"stream:          {info['stream']}")
    click.echo(f"lag:             {info['lag']}")
    click.echo(f"in_flight:       {info['in_flight']}")
    click.echo(f"oldest_idle:     {idle}")
    click.echo(f"last_delivered:  {info['last_delivered_id'] or '—'}")


@sub.command()
@click.argument("name")
@click.option("--count", "-n", type=int, default=10, help="How many entries.")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON instead of text.")
@coroutine
async def pending(name: str, count: int, as_json: bool) -> None:
    """List leased-but-unacked entries for subscription NAME."""
    entries = await subscriptions.pending(name, count=count)
    if as_json:
        click.echo(json.dumps(entries))
        return
    if not entries:
        click.echo("(no pending entries)", err=True)
        return
    click.echo(f"{'event_id':<22} {'consumer':<24} {'idle':>8}  delivery")
    for entry in entries:
        idle_s = entry["idle_ms"] / 1000
        click.echo(
            f"{entry['id']:<22} {entry['consumer']:<24} "
            f"{idle_s:>6.1f}s  {entry['delivery_count']}"
        )

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
@click.option(
    "--lease", type=float, default=None, help="Override lease window (seconds)."
)
@click.option(
    "--max-deliveries",
    type=int,
    default=None,
    help="Override redelivery cap before DLQ.",
)
@coroutine
async def create(
    name: str,
    stream: str,
    lease: float | None,
    max_deliveries: int | None,
) -> None:
    """Create durable subscription NAME on a stream."""
    await subscriptions.create(
        name, stream, lease_seconds=lease, max_deliveries=max_deliveries
    )


@sub.command(name="set")
@click.argument("name")
@click.option(
    "--lease", type=float, default=None, help="Override lease window (seconds)."
)
@click.option(
    "--max-deliveries",
    type=int,
    default=None,
    help="Override redelivery cap before DLQ.",
)
@coroutine
async def set_(name: str, lease: float | None, max_deliveries: int | None) -> None:
    """Set or change overrides on existing subscription NAME."""
    if lease is None and max_deliveries is None:
        raise click.UsageError("nothing to set — provide --lease or --max-deliveries")
    await subscriptions.set_(name, lease_seconds=lease, max_deliveries=max_deliveries)


@sub.command()
@click.argument("name")
@click.option("--lease", is_flag=True, help="Clear lease override (revert to default).")
@click.option(
    "--max-deliveries",
    is_flag=True,
    help="Clear max_deliveries override (revert to default).",
)
@coroutine
async def unset(name: str, lease: bool, max_deliveries: bool) -> None:
    """Clear overrides on subscription NAME."""
    if not (lease or max_deliveries):
        raise click.UsageError("nothing to unset — provide --lease or --max-deliveries")
    await subscriptions.unset(name, lease_seconds=lease, max_deliveries=max_deliveries)


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
    lease = _with_default(info["lease_seconds"], info["lease_seconds_explicit"])
    maxd = _with_default(info["max_deliveries"], info["max_deliveries_explicit"])
    click.echo(f"subscription:    {info['name']}")
    click.echo(f"stream:          {info['stream']}")
    click.echo(f"lease_seconds:   {lease}")
    click.echo(f"max_deliveries:  {maxd}")
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


@sub.command()
@click.argument("name")
@coroutine
async def delete(name: str) -> None:
    """Delete subscription NAME (its consumer group, config, and DLQ)."""
    await subscriptions.delete(name)


def _with_default(value: object, explicit: bool) -> str:
    """Format a config value, marking it ``(default)`` when not overridden."""
    return f"{value}" if explicit else f"{value}  (default)"

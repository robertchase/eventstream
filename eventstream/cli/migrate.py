"""``eventstream migrate`` — one-time data-shape upgrades.

Currently handles the 0.2 → 0.3 subscription storage shape change (HASH of
name→stream becomes SET-of-names + per-subscription HASHes). Safe to run
multiple times.
"""

from __future__ import annotations

import click

from eventstream.cli._run import coroutine
from eventstream.logic import subscriptions


@click.command()
@coroutine
async def migrate() -> None:
    """Migrate Redis data to the current eventstream storage shape."""
    result = await subscriptions.migrate()
    if result["reason"]:
        click.echo(f"subscriptions: {result['reason']}")
    else:
        click.echo(
            f"subscriptions: migrated {result['migrated']}, "
            f"skipped {result['skipped']}"
        )

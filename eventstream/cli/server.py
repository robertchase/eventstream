"""``eventstream server`` — run the HTTP server."""

from __future__ import annotations

import asyncio

import click

from eventstream import config as CONFIG
from eventstream.logic import apikeys
from eventstream.server import run


@click.command()
@click.option("--port", type=int, default=None, help="Override the bind port.")
def server(port: int | None) -> None:
    """Start the HTTP admin / API server (blocks)."""
    actual = port if port is not None else CONFIG.http_port
    click.echo(f"eventstream server listening on :{actual}", err=True)
    if CONFIG.auth:
        click.echo("auth: ON — /v1/* requires a bearer token", err=True)
        # Fail-closed: with auth on and no keys, every request is rejected.
        if not asyncio.run(apikeys.list_()):
            click.echo(
                "WARNING: no API keys exist; all /v1 requests will be "
                "rejected. Create one with `eventstream key create`.",
                err=True,
            )
    run(port=port)

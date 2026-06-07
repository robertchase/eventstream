"""``eventstream server`` — run the HTTP server."""

from __future__ import annotations

import click

from eventstream import config as CONFIG
from eventstream.server import run


@click.command()
@click.option("--port", type=int, default=None, help="Override the bind port.")
def server(port: int | None) -> None:
    """Start the HTTP admin / API server (blocks)."""
    actual = port if port is not None else CONFIG.http_port
    click.echo(f"eventstream server listening on :{actual}", err=True)
    run(port=port)

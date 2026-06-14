"""``eventstream key`` — API key (bearer token) administration.

Issuing and revoking keys is a control-plane operation: it runs over the
CLI's direct Redis access, never through the authenticated HTTP API.
"""

from __future__ import annotations

import click

from eventstream.cli._run import coroutine
from eventstream.logic import apikeys


@click.group()
def key() -> None:
    """Manage API keys."""


@key.command()
@click.option("--name", required=True, help="Human label for the key.")
@click.option(
    "--scope",
    "scopes",
    multiple=True,
    required=True,
    type=click.Choice(apikeys.VALID_SCOPES),
    help="Grant a scope (repeatable): read, write, admin.",
)
@coroutine
async def create(name: str, scopes: tuple[str, ...]) -> None:
    """Create a key and print its token ONCE (it is never retrievable again)."""
    result = await apikeys.create(name, list(scopes))
    click.echo(result["token"])


@key.command(name="list")
@coroutine
async def list_() -> None:
    """List keys (id, name, scopes, timestamps) — never the secret."""
    for k in await apikeys.list_():
        last = k["last_used_at"] or "-"
        click.echo(
            f"{k['keyid']}\t{k['name']}\t{','.join(k['scopes'])}\t"
            f"created={k['created_at']}\tlast_used={last}"
        )


@key.command()
@click.argument("keyid")
@coroutine
async def revoke(keyid: str) -> None:
    """Revoke (delete) the key with KEYID. Effective immediately."""
    await apikeys.revoke(keyid)

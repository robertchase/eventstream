"""``eventstream publish`` — publish an event to a stream."""

from __future__ import annotations

import json
from typing import IO

import click

from eventstream.cli._run import coroutine
from eventstream.logic import events


@click.command()
@click.argument("stream")
@click.argument("name")
@click.option("--payload", default=None, help="Inline JSON payload (object).")
@click.option(
    "--payload-file",
    type=click.File("r"),
    default=None,
    help="Read JSON payload from a file ('-' for stdin).",
)
@coroutine
async def publish(
    stream: str,
    name: str,
    payload: str | None,
    payload_file: IO[str] | None,
) -> None:
    """Publish one NAME event to STREAM and print its id."""
    data = _read_payload(payload, payload_file)
    click.echo(await events.publish(stream, name, data))


def _read_payload(payload: str | None, payload_file: IO[str] | None) -> dict:
    """Resolve the JSON payload from exactly one of the two sources."""
    if (payload is None) == (payload_file is None):
        raise click.UsageError("provide exactly one of --payload or --payload-file")
    text = payload if payload is not None else payload_file.read()
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise click.UsageError(f"payload is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise click.UsageError("payload must be a JSON object")
    return data

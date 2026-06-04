"""eventstream command-line interface.

Thin wrappers over :mod:`eventstream.logic`. Run with
``uv run -m eventstream.cli`` or the installed ``eventstream`` script.
"""

from __future__ import annotations

import click

from eventstream.cli import ack as ack_cmd
from eventstream.cli import publish as publish_cmd
from eventstream.cli import pull as pull_cmd
from eventstream.cli import stream as stream_cmd
from eventstream.cli import sub as sub_cmd
from eventstream.logic.exceptions import EventStreamError


class _Group(click.Group):
    """Group that renders domain errors as clean messages, not tracebacks."""

    def invoke(self, ctx: click.Context):
        try:
            return super().invoke(ctx)
        except EventStreamError as exc:
            raise click.ClickException(str(exc)) from exc


@click.group(cls=_Group)
def cli() -> None:
    """A pull-based application event bus."""


cli.add_command(publish_cmd.publish)
cli.add_command(pull_cmd.pull)
cli.add_command(ack_cmd.ack)
cli.add_command(stream_cmd.stream)
cli.add_command(sub_cmd.sub)

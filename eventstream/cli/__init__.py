"""eventstream command-line interface.

Thin wrappers over :mod:`eventstream.logic`. Run with
``uv run -m eventstream.cli`` or the installed ``eventstream`` script.
"""

from __future__ import annotations

import click

from eventstream.cli import ack as ack_cmd
from eventstream.cli import dlq as dlq_cmd
from eventstream.cli import jobs as jobs_cmd
from eventstream.cli import key as key_cmd
from eventstream.cli import migrate as migrate_cmd
from eventstream.cli import publish as publish_cmd
from eventstream.cli import pull as pull_cmd
from eventstream.cli import server as server_cmd
from eventstream.cli import stream as stream_cmd
from eventstream.cli import sub as sub_cmd
from eventstream.cli import workflow as workflow_cmd
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
cli.add_command(dlq_cmd.dlq)
cli.add_command(server_cmd.server)
cli.add_command(migrate_cmd.migrate)
cli.add_command(workflow_cmd.workflow)
cli.add_command(jobs_cmd.jobs_group, name="jobs")
cli.add_command(key_cmd.key)

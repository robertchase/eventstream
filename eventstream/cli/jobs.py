"""``eventstream jobs`` — job lifecycle administration."""

from __future__ import annotations

import json

import click

from eventstream.cli._run import coroutine
from eventstream.logic import jobs


@click.group()
def jobs_group() -> None:
    """Manage workflow jobs."""


@jobs_group.command()
@click.argument("workflow_name")
@click.option("--context", default=None, help="Initial context JSON object.")
@click.option(
    "--version", type=int, default=None, help="Pin a specific workflow version."
)
@coroutine
async def create(workflow_name: str, context: str | None, version: int | None) -> None:
    """Create a new job from WORKFLOW_NAME."""
    ctx = json.loads(context) if context else {}
    if not isinstance(ctx, dict):
        raise click.UsageError("--context must be a JSON object")
    job = await jobs.create(workflow_name, ctx, workflow_version=version)
    click.echo(job["id"])


@jobs_group.command(name="list")
@click.option("--workflow", default=None, help="Filter by workflow name.")
@click.option(
    "--status",
    type=click.Choice(["running", "terminal", "cancelled"]),
    default=None,
    help="Filter by status.",
)
@coroutine
async def list_(workflow: str | None, status: str | None) -> None:
    """List jobs."""
    for j in await jobs.list_(workflow=workflow, status=status):
        click.echo(
            f"{j['id']}\t{j['workflow']}\tv{j['version']}\t{j['state']}\t{j['status']}"
        )


@jobs_group.command()
@click.argument("job_id")
@click.option("--json", "as_json", is_flag=True, help="Emit JSON instead of text.")
@coroutine
async def show(job_id: str, as_json: bool) -> None:
    """Show a job's state and context."""
    job = await jobs.get(job_id)
    if as_json:
        click.echo(json.dumps(job))
        return
    click.echo(f"id:         {job['id']}")
    click.echo(f"workflow:   {job['workflow']}  v{job['version']}")
    click.echo(f"state:      {job['state']}")
    click.echo(f"status:     {job['status']}")
    click.echo(f"created_at: {job['created_at']}")
    click.echo(f"updated_at: {job['updated_at']}")
    click.echo("context:")
    for line in json.dumps(job["context"], indent=2).splitlines():
        click.echo(f"  {line}")


@jobs_group.command()
@click.argument("job_id")
@coroutine
async def history(job_id: str) -> None:
    """List a job's transition history."""
    for entry in await jobs.history(job_id):
        click.echo(
            f"{entry['ts']}\t{entry['from']}  --{entry['event']}-->  {entry['to']}"
        )


@jobs_group.command()
@click.argument("job_id")
@click.argument("event_name")
@click.option("--data", default=None, help="Event body as a JSON object.")
@coroutine
async def advance(job_id: str, event_name: str, data: str | None) -> None:
    """Feed EVENT_NAME into JOB_ID; persist the resulting transition(s)."""
    body = json.loads(data) if data else {}
    if not isinstance(body, dict):
        raise click.UsageError("--data must be a JSON object")
    job = await jobs.advance(job_id, event_name, body)
    click.echo(f"{job['state']}  ({job['status']})")


@jobs_group.command()
@click.argument("job_id")
@coroutine
async def cancel(job_id: str) -> None:
    """Cancel a running JOB_ID."""
    await jobs.cancel(job_id)


@jobs_group.command()
@click.argument("job_id")
@click.option("--force", is_flag=True, help="Delete even if the job is running.")
@coroutine
async def delete(job_id: str, force: bool) -> None:
    """Delete JOB_ID: its state, history, and pending timers."""
    await jobs.delete(job_id, force=force)


@jobs_group.command()
@coroutine
async def tick() -> None:
    """Fire any due timers (single sweep)."""
    result = await jobs.tick()
    click.echo(f"fired: {result['fired']}\tdropped: {result['dropped']}")

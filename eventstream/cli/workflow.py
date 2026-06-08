"""``eventstream workflow`` — register/list/show/delete workflow definitions."""

from __future__ import annotations

import json
import sys

import click

from eventstream.cli._run import coroutine
from eventstream.logic import workflows
from eventstream.logic.workflow_parser import ParseError


@click.group()
def workflow() -> None:
    """Manage workflow definitions."""


@workflow.command()
@click.argument("file", type=click.File("r"))
@coroutine
async def register(file) -> None:
    """Register the workflow at FILE; prints the assigned version."""
    source = file.read()
    try:
        result = await workflows.register(source)
    except ParseError as exc:
        click.echo(f"parse error: {exc}", err=True)
        sys.exit(2)
    click.echo(f"{result['name']} v{result['version']}")


@workflow.command(name="list")
@coroutine
async def list_() -> None:
    """List registered workflows with their latest version."""
    for item in await workflows.list_():
        version = item["latest_version"] or "-"
        click.echo(f"{item['name']}\tv{version}")


@workflow.command()
@click.argument("name")
@click.option("--version", type=int, default=None, help="Pick a specific version.")
@click.option("--source", is_flag=True, help="Print the original DSL source instead.")
@click.option("--json", "as_json", is_flag=True, help="Emit the AST as JSON.")
@coroutine
async def show(name: str, version: int | None, source: bool, as_json: bool) -> None:
    """Show a workflow's source or AST."""
    if source and as_json:
        raise click.UsageError("--source and --json are mutually exclusive")
    wf = await workflows.get(name, version=version)
    if source:
        click.echo(wf["source"], nl=False)
        return
    if as_json:
        click.echo(json.dumps(wf["ast"]))
        return
    _print_summary(wf)


@workflow.command()
@click.argument("name")
@coroutine
async def versions(name: str) -> None:
    """List all stored versions of NAME."""
    for v in await workflows.versions(name):
        click.echo(v)


@workflow.command()
@click.argument("name")
@coroutine
async def delete(name: str) -> None:
    """Delete every version of NAME."""
    await workflows.delete(name)


def _print_summary(wf: dict) -> None:
    """Human-readable summary of a workflow AST."""
    ast = wf["ast"]
    click.echo(f"workflow:    {ast['name']}  v{wf['version']}")
    if ast.get("description"):
        click.echo(f"description: {ast['description']}")
    click.echo(f"initial:     {ast['initial']}")
    if ast["defaults"]:
        click.echo("defaults:")
        for event, handler in ast["defaults"].items():
            goto = f" → {handler['goto']}" if handler.get("goto") else ""
            click.echo(f"  {event}{goto}  ({len(handler['do'])} actions)")
    click.echo(f"actions:     {len(ast['actions'])}")
    for name, action in ast["actions"].items():
        click.echo(f"  {name}: {action['type']}")
    click.echo(f"states:      {len(ast['states'])}")
    for name, state in ast["states"].items():
        if state.get("terminal"):
            click.echo(f"  {name}  [TERMINAL]")
        else:
            events = state.get("events", {})
            click.echo(f"  {name}  ({len(events)} events)")

"""HTML admin handlers. Each renders a Jinja2 template against the logic layer.

Handlers take ``is_hx`` from the :func:`hx_check` hook; when true, the
response is just the content fragment (HTMX swaps it in place). Otherwise
the page is wrapped in ``layout.html``.

Do **not** add ``from __future__ import annotations`` — these are meander
handlers and the future import disables meander's type coercion. See
``logic/streams.py`` for the gory detail.
"""

import meander

from eventstream.logic import dlq, jobs, streams, subscriptions, workflows
from eventstream.server import diagram
from eventstream.server.templating import render_page


async def index(is_hx: bool = False) -> meander.HTMLResponse:
    """Overview: lists of streams and subscriptions."""
    body = render_page(
        "index.html",
        fragment=is_hx,
        title="overview",
        streams=await streams.list_(),
        subscriptions=await subscriptions.list_(),
    )
    return meander.HTMLResponse(content=body)


async def stream_detail(name: str, is_hx: bool = False) -> meander.HTMLResponse:
    """Stream detail: metadata + ten most recent events."""
    body = render_page(
        "stream.html",
        fragment=is_hx,
        title=f"stream {name}",
        info=await streams.show(name),
        recent=await streams.peek(name, count=10, reverse=True),
    )
    return meander.HTMLResponse(content=body)


async def subscription_detail(name: str, is_hx: bool = False) -> meander.HTMLResponse:
    """Subscription detail: stats, pending entries, dead-letter entries."""
    body = render_page(
        "subscription.html",
        fragment=is_hx,
        title=f"subscription {name}",
        info=await subscriptions.show(name),
        pending=await subscriptions.pending(name, count=10),
        dead=await dlq.peek(name, count=10),
    )
    return meander.HTMLResponse(content=body)


async def workflow_list(is_hx: bool = False) -> meander.HTMLResponse:
    """List of registered workflows with their latest version."""
    body = render_page(
        "workflows.html",
        fragment=is_hx,
        title="workflows",
        workflows=await workflows.list_(),
    )
    return meander.HTMLResponse(content=body)


async def workflow_detail(
    name: str, is_hx: bool = False, version: int | None = None
) -> meander.HTMLResponse:
    """Workflow detail: definition + version picker + jobs running this workflow."""
    wf = await workflows.get(name, version=version)
    body = render_page(
        "workflow.html",
        fragment=is_hx,
        title=f"workflow {name}",
        wf=wf,
        requested_version=version,
        all_versions=await workflows.versions(name),
        jobs=await jobs.list_(workflow=name),
        diagram=diagram.to_nomnoml(wf["ast"]),
    )
    return meander.HTMLResponse(content=body)


async def job_list(
    is_hx: bool = False,
    workflow: str | None = None,
    status: str | None = None,
) -> meander.HTMLResponse:
    """Job list with workflow + status filters."""
    qs_parts = []
    if workflow:
        qs_parts.append(f"workflow={workflow}")
    if status:
        qs_parts.append(f"status={status}")
    qs = "?" + "&".join(qs_parts) if qs_parts else ""
    body = render_page(
        "jobs.html",
        fragment=is_hx,
        title="jobs",
        jobs=await jobs.list_(workflow=workflow, status=status),
        workflow=workflow,
        status=status,
        qs=qs,
    )
    return meander.HTMLResponse(content=body)


async def job_detail(job_id: str, is_hx: bool = False) -> meander.HTMLResponse:
    """Job detail: state + context + transition history + position diagram."""
    job = await jobs.get(job_id)
    wf = await workflows.get(job["workflow"], version=job["version"])
    current = job["state"] if job["status"] == "running" else None
    body = render_page(
        "job.html",
        fragment=is_hx,
        title=f"job {job_id}",
        job=job,
        history=await jobs.history(job_id),
        diagram=diagram.to_nomnoml(wf["ast"], current_state=current),
    )
    return meander.HTMLResponse(content=body)

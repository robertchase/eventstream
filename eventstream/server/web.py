"""HTML admin handlers. Each renders a Jinja2 template against the logic layer.

Handlers take ``is_hx`` from the :func:`hx_check` hook; when true, the
response is just the content fragment (HTMX swaps it in place). Otherwise
the page is wrapped in ``layout.html``.

Do **not** add ``from __future__ import annotations`` — these are meander
handlers and the future import disables meander's type coercion. See
``logic/streams.py`` for the gory detail.
"""

import meander

from eventstream.logic import dlq, streams, subscriptions
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

"""Route table: JSON API + HTML admin + static, registered on a meander server.

meander binds URL regex captures to handler parameters by position, so
``r"/v1/streams/([^/]+)$"`` is enough to make the capture flow into the
handler's first parameter — no path-args before hook needed.

Domain exception translation lives in :mod:`eventstream.server`, registered
with meander as a server-level ``exception_handler``. Handlers stay
HTTP-agnostic; they're the same functions the CLI calls.

A guard test in ``tests/test_server.py`` asserts every registered handler
has real-class annotations; if someone adds ``from __future__ import
annotations`` to a handler module, the test fails loudly (otherwise
meander silently stops coercing query parameters to int/bool).
"""

from __future__ import annotations

import meander

from eventstream.logic import dlq, jobs, streams, subscriptions, workflows
from eventstream.server import web
from eventstream.server.hooks import hx_check
from eventstream.server.static import serve_static


def register(server: meander.server.Server) -> None:
    """Add every eventstream route to ``server``."""
    # JSON API — bus.
    server.add_route(r"/v1/streams$", streams.list_)
    server.add_route(r"/v1/streams/([^/]+)$", streams.show)
    server.add_route(r"/v1/streams/([^/]+)/events$", streams.peek)
    server.add_route(r"/v1/subscriptions$", subscriptions.list_)
    server.add_route(r"/v1/subscriptions/([^/]+)$", subscriptions.show)
    server.add_route(r"/v1/subscriptions/([^/]+)/pending$", subscriptions.pending)
    server.add_route(r"/v1/subscriptions/([^/]+)/dlq$", dlq.peek)

    # JSON API — workflows & jobs.
    server.add_route(r"/v1/workflows$", workflows.list_)
    server.add_route(r"/v1/workflows/([^/]+)$", workflows.get)
    server.add_route(r"/v1/workflows/([^/]+)/versions$", workflows.versions)
    server.add_route(r"/v1/jobs$", jobs.list_)
    server.add_route(r"/v1/jobs/([^/]+)$", jobs.get)
    server.add_route(r"/v1/jobs/([^/]+)/history$", jobs.history)

    # HTML admin — fragment-or-page via the HX-Request header.
    server.add_route(r"/$", web.index, before=hx_check)
    server.add_route(r"/streams/([^/]+)$", web.stream_detail, before=hx_check)
    server.add_route(
        r"/subscriptions/([^/]+)$", web.subscription_detail, before=hx_check
    )
    server.add_route(r"/workflows$", web.workflow_list, before=hx_check)
    server.add_route(r"/workflows/([^/]+)$", web.workflow_detail, before=hx_check)
    server.add_route(r"/jobs$", web.job_list, before=hx_check)
    server.add_route(r"/jobs/([^/]+)$", web.job_detail, before=hx_check)

    # Static — CSS, vendored htmx.
    server.add_route(r"/static/(.+)$", serve_static)

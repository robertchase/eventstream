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

from eventstream import config as CONFIG
from eventstream.logic import dlq, jobs, streams, subscriptions, workflows
from eventstream.server import auth, web, writes
from eventstream.server.hooks import hx_check
from eventstream.server.static import serve_static


def register(server: meander.server.Server) -> None:
    """Add every eventstream route to ``server``."""

    def guard(scope: str):
        """A scope before-hook when auth is on; nothing when it's off."""
        return [auth.require(scope)] if CONFIG.auth else None

    # JSON API — bus. All reads today; write/admin guards attach with the
    # write-API work (see design/auth.md). Guarded only when EVENTSTREAM_AUTH.
    server.add_route(r"/v1/streams$", streams.list_, before=guard("read"))
    server.add_route(r"/v1/streams/([^/]+)$", streams.show, before=guard("read"))
    server.add_route(r"/v1/streams/([^/]+)/events$", streams.peek, before=guard("read"))
    server.add_route(r"/v1/subscriptions$", subscriptions.list_, before=guard("read"))
    server.add_route(
        r"/v1/subscriptions/([^/]+)$", subscriptions.show, before=guard("read")
    )
    server.add_route(
        r"/v1/subscriptions/([^/]+)/pending$",
        subscriptions.pending,
        before=guard("read"),
    )
    server.add_route(r"/v1/subscriptions/([^/]+)/dlq$", dlq.peek, before=guard("read"))

    # JSON API — workflows & jobs.
    server.add_route(r"/v1/workflows$", workflows.list_, before=guard("read"))
    server.add_route(r"/v1/workflows/([^/]+)$", workflows.get, before=guard("read"))
    server.add_route(
        r"/v1/workflows/([^/]+)/versions$", workflows.versions, before=guard("read")
    )
    server.add_route(r"/v1/jobs$", jobs.list_, before=guard("read"))
    server.add_route(r"/v1/jobs/([^/]+)$", jobs.get, before=guard("read"))
    server.add_route(r"/v1/jobs/([^/]+)/history$", jobs.history, before=guard("read"))

    # JSON API — writes (producer/consumer core four). publish/pull/ack need
    # the `write` scope; creating a subscription is `admin`.
    server.add_route(
        r"/v1/streams/([^/]+)/events$",
        writes.publish_event,
        method="POST",
        before=guard("write"),
    )
    server.add_route(
        r"/v1/subscriptions/([^/]+)/pull$",
        writes.pull_event,
        method="GET",
        before=guard("write"),
    )
    server.add_route(
        r"/v1/subscriptions/([^/]+)/ack/([^/]+)$",
        writes.ack_event,
        method="POST",
        before=guard("write"),
    )
    server.add_route(
        r"/v1/subscriptions$",
        writes.create_subscription,
        method="POST",
        before=guard("admin"),
    )

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

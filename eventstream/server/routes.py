"""Route table: JSON API + HTML admin + static, registered on a meander server.

Routes are added through the local :func:`register.add` helper, which both
registers the route and records it in :data:`CATALOG` — the single source the
``/endpoints`` reference page renders, so the page can never drift from what's
actually served.

meander binds URL regex captures to handler parameters by position. Domain
exception translation lives in :mod:`eventstream.server`; auth scope guards
attach only when ``CONFIG.auth`` (see ``design/auth.md``).

A guard test in ``tests/test_server.py`` asserts every registered handler has
real-class annotations; don't add ``from __future__ import annotations`` to a
handler module or meander stops coercing query params.
"""

from __future__ import annotations

import inspect
import re

import meander

from eventstream import config as CONFIG
from eventstream.logic import dlq, jobs, streams, subscriptions, workflows
from eventstream.server import auth, web, writes
from eventstream.server.hooks import hx_check
from eventstream.server.static import serve_static

#: Catalog of registered routes, rebuilt on each register(); read by the
#: /endpoints page. Each entry: {method, path, scope, kind, doc}.
CATALOG: list[dict] = []

_HTTP_VERBS = {"GET", "POST", "PUT", "PATCH", "DELETE"}
_GROUP_RE = re.compile(r"\([^)]*\)")


def _pretty_path(resource: str, handler) -> str:
    """Turn a route regex into a readable path, naming captures from params."""
    params = [
        p.name
        for p in inspect.signature(inspect.unwrap(handler)).parameters.values()
        if p.default is p.empty
        and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        and p.name != "is_hx"
    ]
    names = iter(params)
    path = _GROUP_RE.sub(lambda _m: "{" + next(names, "?") + "}", resource)
    return path.rstrip("$") or "/"


def _doc(handler) -> str:
    """First docstring line, minus any redundant leading ``VERB /path — ``."""
    text = (handler.__doc__ or "").strip()
    if not text:
        return ""
    line = text.splitlines()[0]
    if " — " in line and line.split()[0] in _HTTP_VERBS:
        line = line.split(" — ", 1)[1]
    return line.replace("``", "")  # drop RST inline-literal markup for HTML


def _kind(resource: str) -> str:
    if resource.startswith("/v1"):
        return "api"
    if resource.startswith("/static"):
        return "static"
    return "web"


def register(server: meander.server.Server) -> None:
    """Add every eventstream route to ``server`` and rebuild :data:`CATALOG`."""
    CATALOG.clear()

    def add(resource, handler, *, method="GET", scope=None, before=None):
        hooks: list = []
        if scope and CONFIG.auth:
            hooks.append(auth.require(scope))
        if before:
            hooks.append(before)
        server.add_route(resource, handler, method=method, before=hooks or None)
        CATALOG.append(
            {
                "method": method,
                "path": _pretty_path(resource, handler),
                "scope": scope,
                "kind": _kind(resource),
                "doc": _doc(handler),
            }
        )

    # JSON API — reads.
    add(r"/v1/streams$", streams.list_, scope="read")
    add(r"/v1/streams/([^/]+)$", streams.show, scope="read")
    add(r"/v1/streams/([^/]+)/events$", streams.peek, scope="read")
    add(r"/v1/subscriptions$", subscriptions.list_, scope="read")
    add(r"/v1/subscriptions/([^/]+)$", subscriptions.show, scope="read")
    add(r"/v1/subscriptions/([^/]+)/pending$", subscriptions.pending, scope="read")
    add(r"/v1/subscriptions/([^/]+)/dlq$", dlq.peek, scope="read")
    add(r"/v1/workflows$", workflows.list_, scope="read")
    add(r"/v1/workflows/([^/]+)$", workflows.get, scope="read")
    add(r"/v1/workflows/([^/]+)/versions$", workflows.versions, scope="read")
    add(r"/v1/jobs$", jobs.list_, scope="read")
    add(r"/v1/jobs/([^/]+)$", jobs.get, scope="read")
    add(r"/v1/jobs/([^/]+)/history$", jobs.history, scope="read")

    # JSON API — writes (producer/consumer core four).
    add(
        r"/v1/streams/([^/]+)/events$",
        writes.publish_event,
        method="POST",
        scope="write",
    )
    add(
        r"/v1/subscriptions/([^/]+)/pull$",
        writes.pull_event,
        method="GET",
        scope="write",
    )
    add(
        r"/v1/subscriptions/([^/]+)/ack/([^/]+)$",
        writes.ack_event,
        method="POST",
        scope="write",
    )
    add(r"/v1/subscriptions$", writes.create_subscription, method="POST", scope="admin")

    # HTML admin — fragment-or-page via the HX-Request header.
    add(r"/$", web.index, before=hx_check)
    add(r"/streams/([^/]+)$", web.stream_detail, before=hx_check)
    add(r"/subscriptions/([^/]+)$", web.subscription_detail, before=hx_check)
    add(r"/workflows$", web.workflow_list, before=hx_check)
    add(r"/workflows/([^/]+)$", web.workflow_detail, before=hx_check)
    add(r"/jobs$", web.job_list, before=hx_check)
    add(r"/jobs/([^/]+)$", web.job_detail, before=hx_check)
    add(r"/endpoints$", web.endpoints, before=hx_check)

    # Static — CSS, vendored htmx + nomnoml.
    add(r"/static/(.+)$", serve_static)

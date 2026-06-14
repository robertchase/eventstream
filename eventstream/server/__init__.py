"""HTTP server: meander routes over the existing logic layer.

Two route groups: ``/v1/*`` JSON API (per ``design/api.md``) and ``/`` HTML
admin viewer (per ``design/server.md``). See :func:`build` for the route
registration and :func:`run` for the bind-and-serve entry point.

Domain errors raised by the logic layer are translated to HTTP responses by
:func:`_on_exception`, registered with meander as a server-level
``exception_handler``. Handlers themselves stay HTTP-agnostic (and are the
same functions the CLI calls).
"""

from __future__ import annotations

import meander

from eventstream import config as CONFIG
from eventstream.logic import jobs
from eventstream.logic.apikeys import InsufficientScope, InvalidToken
from eventstream.logic.exceptions import (
    EventNotFound,
    EventStreamError,
    StreamNotFound,
    SubscriptionExists,
    SubscriptionNotFound,
)
from eventstream.logic.jobs import JobNotFound, JobNotRunning, JobRunning
from eventstream.logic.workflows import WorkflowNotFound
from eventstream.server.routes import register

_STATUS: dict[type[EventStreamError], int] = {
    StreamNotFound: 404,
    SubscriptionNotFound: 404,
    EventNotFound: 404,
    WorkflowNotFound: 404,
    JobNotFound: 404,
    SubscriptionExists: 409,
    JobNotRunning: 409,
    JobRunning: 409,
    InvalidToken: 401,
    InsufficientScope: 403,
}


def _on_exception(exc: Exception) -> meander.Response | None:
    """Map a domain exception to an HTTP response, or defer to meander.

    Returns ``None`` for anything that isn't an :class:`EventStreamError` so
    that meander applies its default 500-with-traceback behavior. Domain
    errors become clean text responses with the appropriate status code,
    and meander suppresses the error log because the exception was expected.
    """
    if isinstance(exc, EventStreamError):
        code = _STATUS.get(type(exc), 500)
        headers = {"WWW-Authenticate": "Bearer"} if code == 401 else {}
        return meander.Response(
            content=str(exc), code=code, content_type="text/plain", headers=headers
        )
    return None


def build(port: int | None = None) -> meander.server.Server:
    """Create a meander server with every eventstream route registered."""
    server = meander.add_server(
        port=port if port is not None else CONFIG.http_port,
        exception_handler=_on_exception,
    )
    register(server)
    return server


def run(*, port: int | None = None) -> None:
    """Build the server and block, serving requests forever.

    When ``CONFIG.sweep_interval`` is positive, also runs the job-timer
    sweeper as a meander background task so timers fire without a separate
    ``eventstream jobs sweep`` process.
    """
    build(port=port)
    if CONFIG.sweep_interval > 0:
        meander.add_task(lambda: jobs.sweep_forever(CONFIG.sweep_interval))
    meander.run()

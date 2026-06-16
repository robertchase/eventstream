"""Central configuration for eventstream.

Environment variables and their defaults live here. Import as::

    from eventstream import config as CONFIG

and read values as module attributes, e.g. ``CONFIG.redis_url``.
"""

from __future__ import annotations

import logging
import os

redis_url: str = os.environ.get("EVENTSTREAM_REDIS_URL", "redis://localhost:6379/0")
"""Redis connection URL. Override with ``EVENTSTREAM_REDIS_URL``."""

pull_wait_seconds: float = float(os.environ.get("EVENTSTREAM_PULL_WAIT", "30"))
"""Default long-poll window for ``pull``, in seconds."""

lease_seconds: float = float(os.environ.get("EVENTSTREAM_LEASE", "30"))
"""How long a pulled event is leased before another puller may reclaim it."""

max_deliveries: int = int(os.environ.get("EVENTSTREAM_MAX_DELIVERIES", "5"))
"""How many deliveries an event gets before being routed to the DLQ.

A value of ``N`` lets the event be handed to a worker up to ``N`` times; on
the (N+1)th reclaim it is moved to the per-subscription DLQ and acked off
the stream.
"""

http_host: str = os.environ.get("EVENTSTREAM_HOST", "127.0.0.1")
"""Host the HTTP server binds to."""

http_port: int = int(os.environ.get("EVENTSTREAM_PORT", "8080"))
"""Port the HTTP server binds to."""

auth: bool = os.environ.get("EVENTSTREAM_AUTH", "0").lower() in ("1", "true", "yes")
"""Whether the HTTP ``/v1/*`` API requires a bearer token.

Off by default (behavior unchanged). When on, every ``/v1/*`` route is
guarded by a scope check — see ``design/auth.md``. Manage keys with
``eventstream key``.
"""

sweep_interval: float = float(os.environ.get("EVENTSTREAM_SWEEP_INTERVAL", "1"))
"""How often (seconds) the HTTP server sweeps job timers in the background.

The in-server sweeper fires due job timers so workflows advance without a
separate ``eventstream jobs sweep`` process. On by default at a 1s cadence;
change the interval with ``EVENTSTREAM_SWEEP_INTERVAL``, or set it to ``0``
(equivalently, pass ``--no-sweep`` to ``eventstream server``) to disable the
in-server sweeper and run the standalone sweeper instead.
"""

log_level: str | None = os.environ.get("EVENTSTREAM_LOG_LEVEL") or None
"""Operational log level for the ``eventstream`` logger (e.g. ``INFO``).

Unset (the default) leaves logging unconfigured, so the engine's ``LOG``
directive and other library logs go nowhere on stderr. Set it to surface
them — workflow ``LOG`` lines are recorded to durable job history regardless
of this setting; this only controls whether they (and other internal logs)
also print as the process runs. Honored by the CLI and the HTTP server.
"""


def configure_logging() -> None:
    """Attach a stderr handler to the ``eventstream`` logger when configured.

    A no-op unless :data:`log_level` is set, so importing the library never
    touches global logging state. Scoped to the ``eventstream`` namespace so
    third-party loggers (redis, etc.) are left alone. Idempotent — safe to
    call from every CLI invocation and at server startup.
    """
    if not log_level:
        return
    logger = logging.getLogger("eventstream")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(log_level.upper())

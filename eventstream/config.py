"""Central configuration for eventstream.

Environment variables and their defaults live here. Import as::

    from eventstream import config as CONFIG

and read values as module attributes, e.g. ``CONFIG.redis_url``.
"""

from __future__ import annotations

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

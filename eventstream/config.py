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

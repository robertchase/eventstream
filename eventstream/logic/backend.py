"""Redis connection management — the single seam to the backing store.

Every logic module obtains its client from :func:`client`, so tests can
substitute a fake by patching this one function. The client is async; logic
functions ``await`` it and the CLI wraps each command in ``asyncio.run``.
"""

from __future__ import annotations

import redis.asyncio as redis

from eventstream import config as CONFIG

_client: redis.Redis | None = None


def client() -> redis.Redis:
    """Return a shared async Redis client, created on first use.

    ``socket_timeout=None`` is required for blocking commands (XREADGROUP with
    BLOCK, BLPOP, etc.) to wait their full window. redis-py 8.x changed the
    default to 5 s, which raises :class:`redis.exceptions.TimeoutError` on
    any long-poll instead of letting the server return an empty result on
    BLOCK expiry.
    """
    global _client
    if _client is None:
        _client = redis.from_url(
            CONFIG.redis_url,
            decode_responses=True,
            socket_timeout=None,
            socket_keepalive=True,
        )
    return _client

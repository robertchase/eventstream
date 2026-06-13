"""Fixtures for integration tests that run against a real Redis.

These tests are NOT run by the default ``uv run pytest`` (which is scoped to
``tests/`` via pyproject). Run them explicitly::

    uv run pytest integration-tests/

They connect to a real Redis — default ``redis://localhost:6379/15``,
overridable with ``EVENTSTREAM_TEST_REDIS_URL`` — and flush that database
around every test, so point it at a throwaway DB. When no Redis is
reachable, every test skips rather than fails.

Unlike ``tests/``, there is no fakeredis decode wrapper here: the whole
point is to exercise the real ``backend.client()`` (real ``decode_responses``,
real ``socket_timeout=None``, real stream semantics) and catch the things
fakeredis gets wrong.
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio
import redis.exceptions

from eventstream import config as CONFIG
from eventstream.logic import backend

_TEST_URL = os.environ.get("EVENTSTREAM_TEST_REDIS_URL", "redis://localhost:6379/15")


@pytest_asyncio.fixture(autouse=True)
async def real_redis(monkeypatch: pytest.MonkeyPatch):
    """Point the logic layer at a real test Redis, flushed around each test."""
    monkeypatch.setattr(CONFIG, "redis_url", _TEST_URL)
    backend._client = None  # force a rebuild against the test URL
    client = backend.client()
    try:
        await client.ping()
    except (OSError, redis.exceptions.RedisError):
        await client.aclose()
        backend._client = None
        pytest.skip(
            f"no Redis at {_TEST_URL} "
            "(set EVENTSTREAM_TEST_REDIS_URL to run integration tests)"
        )
    await client.flushdb()
    yield client
    try:
        await client.flushdb()
        await client.aclose()
    finally:
        backend._client = None

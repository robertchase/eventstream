"""Shared test fixtures.

Wraps :class:`fakeredis.FakeAsyncRedis` to work around its incomplete
``decode_responses`` support: ``hgetall`` and stream commands return ``bytes``
in fakeredis 2.36 even when ``decode_responses=True``. Real Redis decodes
them correctly, so this workaround is test-only.
"""

from __future__ import annotations

import asyncio
import functools
from typing import Any

import fakeredis
import pytest

from eventstream.logic import backend


def _decode(value: Any) -> Any:
    """Recursively convert ``bytes`` to ``str`` in fakeredis return values."""
    if isinstance(value, bytes):
        try:
            return value.decode()
        except UnicodeDecodeError:
            return value
    if isinstance(value, dict):
        return {_decode(k): _decode(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_decode(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_decode(v) for v in value)
    if isinstance(value, set):
        return {_decode(v) for v in value}
    return value


class _DecodingFake:
    """Proxy for :class:`fakeredis.FakeAsyncRedis` that decodes every result."""

    def __init__(self, inner: fakeredis.FakeAsyncRedis) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)
        if not callable(attr):
            return attr

        @functools.wraps(attr)
        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            result = attr(*args, **kwargs)
            if asyncio.iscoroutine(result):
                return _await_and_decode(result)
            return _decode(result)

        return _wrapped


async def _await_and_decode(coro: Any) -> Any:
    """Await ``coro`` and decode its result."""
    return _decode(await coro)


@pytest.fixture(autouse=True)
def fake_redis(monkeypatch: pytest.MonkeyPatch) -> _DecodingFake:
    """Back all logic with an in-memory fake async Redis for each test."""
    fake = _DecodingFake(fakeredis.FakeAsyncRedis(decode_responses=True))
    monkeypatch.setattr(backend, "client", lambda: fake)
    return fake

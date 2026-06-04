"""Async-to-sync bridge for click commands.

Click expects synchronous callbacks; logic functions are async. Decorate a
command body with :func:`coroutine` so click can invoke it via ``asyncio.run``.
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Awaitable, Callable
from typing import Any


def coroutine(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Any]:
    """Wrap an async callable so click can call it synchronously."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return asyncio.run(fn(*args, **kwargs))

    return wrapper

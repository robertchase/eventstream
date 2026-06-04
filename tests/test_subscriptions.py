"""Micro-tests for subscription management."""

from __future__ import annotations

import pytest

from eventstream.logic import subscriptions
from eventstream.logic.exceptions import SubscriptionExists, SubscriptionNotFound


async def test_create_is_idempotent_for_same_stream() -> None:
    await subscriptions.create("w", "orders")
    await subscriptions.create("w", "orders")
    assert await subscriptions.stream_of("w") == "orders"


async def test_create_conflicting_stream_raises() -> None:
    await subscriptions.create("w", "orders")
    with pytest.raises(SubscriptionExists):
        await subscriptions.create("w", "billing")


async def test_stream_of_unknown_raises() -> None:
    with pytest.raises(SubscriptionNotFound):
        await subscriptions.stream_of("ghost")


async def test_list_and_filter() -> None:
    await subscriptions.create("a", "orders")
    await subscriptions.create("b", "orders")
    await subscriptions.create("c", "billing")
    assert await subscriptions.list_() == [
        {"name": "a", "stream": "orders"},
        {"name": "b", "stream": "orders"},
        {"name": "c", "stream": "billing"},
    ]
    assert await subscriptions.list_("billing") == [{"name": "c", "stream": "billing"}]

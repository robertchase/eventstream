"""Micro-tests for event publish/pull/ack."""

from __future__ import annotations

import asyncio

import pytest

from eventstream import config as CONFIG
from eventstream.logic import events, subscriptions
from eventstream.logic.exceptions import SubscriptionNotFound


async def test_publish_returns_an_id() -> None:
    assert await events.publish("orders", "ev", {"n": 1})


async def test_pull_delivers_event_published_after_subscribe() -> None:
    await subscriptions.create("w", "orders")
    event_id = await events.publish("orders", "placed", {"n": 1})
    event = await events.pull("w", wait=0)
    assert event is not None
    assert event["id"] == event_id
    assert event["payload"] == {"n": 1}
    assert event["name"] == "placed"
    assert event["delivery_count"] == 1
    assert "ts" in event


async def test_pull_returns_none_when_empty() -> None:
    await subscriptions.create("w", "orders")
    assert await events.pull("w", wait=0) is None


async def test_subscription_starts_at_tail() -> None:
    await events.publish("orders", "ev", {"n": "before"})
    await subscriptions.create("w", "orders")
    assert await events.pull("w", wait=0) is None


async def test_ack_removes_event_from_pending() -> None:
    await subscriptions.create("w", "orders")
    event_id = await events.publish("orders", "ev", {"n": 1})
    await events.pull("w", wait=0)
    await events.ack("w", event_id)
    assert await events.pull("w", wait=0) is None


async def test_pull_unknown_subscription_raises() -> None:
    with pytest.raises(SubscriptionNotFound):
        await events.pull("ghost", wait=0)


async def test_unacked_event_not_redelivered_within_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A leased event stays leased — no other pull returns it before expiry."""
    monkeypatch.setattr(CONFIG, "lease_seconds", 60.0)
    await subscriptions.create("w", "orders")
    await events.publish("orders", "ev", {"n": 1})
    first = await events.pull("w", wait=0)
    assert first is not None
    # Same consumer pulling again: nothing new, and the leased one is not idle.
    assert await events.pull("w", wait=0) is None


async def test_unacked_event_redelivered_after_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Past the lease window, the next pull reclaims the unacked entry."""
    monkeypatch.setattr(CONFIG, "lease_seconds", 0.05)
    await subscriptions.create("w", "orders")
    event_id = await events.publish("orders", "ev", {"n": 1})
    first = await events.pull("w", wait=0)
    assert first is not None and first["id"] == event_id
    assert first["delivery_count"] == 1
    await asyncio.sleep(0.1)
    second = await events.pull("w", wait=0)
    assert second is not None and second["id"] == event_id
    assert second["delivery_count"] == 2


async def test_ack_after_reclaim_clears_the_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Acking a reclaimed event releases it just like the original delivery."""
    monkeypatch.setattr(CONFIG, "lease_seconds", 0.05)
    await subscriptions.create("w", "orders")
    event_id = await events.publish("orders", "ev", {"n": 1})
    await events.pull("w", wait=0)
    await asyncio.sleep(0.1)
    reclaimed = await events.pull("w", wait=0)
    assert reclaimed is not None
    await events.ack("w", event_id)
    await asyncio.sleep(0.1)
    assert await events.pull("w", wait=0) is None

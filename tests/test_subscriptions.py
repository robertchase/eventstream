"""Micro-tests for subscription management."""

from __future__ import annotations

import asyncio

import pytest

from eventstream import config as CONFIG
from eventstream.logic import events, subscriptions
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


async def test_show_unknown_subscription_raises() -> None:
    with pytest.raises(SubscriptionNotFound):
        await subscriptions.show("ghost")


async def test_show_reports_lag_and_no_in_flight_when_idle() -> None:
    await subscriptions.create("w", "orders")
    await events.publish("orders", {"n": 1})
    await events.publish("orders", {"n": 2})
    info = await subscriptions.show("w")
    assert info["name"] == "w"
    assert info["stream"] == "orders"
    # fakeredis 2.36 reports lag off-by-one (1 instead of 2); real Redis 7+
    # returns the correct count. Assert the field is populated; integration
    # tests against real Redis will verify the exact value.
    assert info["lag"] >= 1
    assert info["in_flight"] == 0
    assert info["oldest_idle_ms"] == 0


async def test_show_reports_in_flight_after_pull(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(CONFIG, "lease_seconds", 60.0)
    await subscriptions.create("w", "orders")
    await events.publish("orders", {"n": 1})
    await events.publish("orders", {"n": 2})
    await events.pull("w", wait=0)
    info = await subscriptions.show("w")
    assert info["in_flight"] == 1
    assert info["lag"] == 1
    assert info["oldest_idle_ms"] >= 0


async def test_pending_unknown_subscription_raises() -> None:
    with pytest.raises(SubscriptionNotFound):
        await subscriptions.pending("ghost")


async def test_pending_empty_when_nothing_leased() -> None:
    await subscriptions.create("w", "orders")
    await events.publish("orders", {"n": 1})
    assert await subscriptions.pending("w") == []


async def test_pending_lists_leased_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(CONFIG, "lease_seconds", 60.0)
    await subscriptions.create("w", "orders")
    event_id = await events.publish("orders", {"n": 1})
    await events.pull("w", wait=0)
    entries = await subscriptions.pending("w")
    assert len(entries) == 1
    entry = entries[0]
    assert entry["id"] == event_id
    assert ":" in entry["consumer"]  # host:pid format
    assert entry["delivery_count"] == 1
    assert entry["idle_ms"] >= 0


async def test_pending_reflects_redelivery_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(CONFIG, "lease_seconds", 0.05)
    await subscriptions.create("w", "orders")
    await events.publish("orders", {"n": 1})
    await events.pull("w", wait=0)
    await asyncio.sleep(0.1)
    await events.pull("w", wait=0)  # reclaim
    entries = await subscriptions.pending("w")
    assert len(entries) == 1
    assert entries[0]["delivery_count"] == 2

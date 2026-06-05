"""Micro-tests for the dead-letter queue."""

from __future__ import annotations

import asyncio

import pytest

from eventstream import config as CONFIG
from eventstream.logic import dlq, events, subscriptions
from eventstream.logic.exceptions import EventNotFound, SubscriptionNotFound


async def test_pull_does_not_dlq_within_max_deliveries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An event under the cap is returned, not DLQ'd."""
    monkeypatch.setattr(CONFIG, "lease_seconds", 0.05)
    monkeypatch.setattr(CONFIG, "max_deliveries", 2)
    await subscriptions.create("w", "orders")
    event_id = await events.publish("orders", {"n": 1})

    first = await events.pull("w", wait=0)
    assert first is not None and first["delivery_count"] == 1

    await asyncio.sleep(0.1)
    second = await events.pull("w", wait=0)
    assert second is not None and second["delivery_count"] == 2
    assert second["id"] == event_id

    assert await dlq.peek("w") == []


async def test_pull_moves_to_dlq_past_max_deliveries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The (max+1)th reclaim DLQs the event and pull returns None."""
    monkeypatch.setattr(CONFIG, "lease_seconds", 0.05)
    monkeypatch.setattr(CONFIG, "max_deliveries", 2)
    await subscriptions.create("w", "orders")
    event_id = await events.publish("orders", {"n": 1}, key="k")

    await events.pull("w", wait=0)  # delivery 1
    await asyncio.sleep(0.1)
    await events.pull("w", wait=0)  # delivery 2 (still under cap)
    await asyncio.sleep(0.1)

    third = await events.pull("w", wait=0)  # delivery 3 → DLQ, return None
    assert third is None

    dead = await dlq.peek("w")
    assert len(dead) == 1
    entry = dead[0]
    assert entry["id"] == event_id
    assert entry["stream"] == "orders"
    assert entry["key"] == "k"
    assert entry["payload"] == {"n": 1}
    assert entry["delivery_count"] == 3
    assert "dead_at" in entry


async def test_dead_event_does_not_redeliver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Once DLQ'd, the event is ack'd off the stream; pulls find nothing."""
    monkeypatch.setattr(CONFIG, "lease_seconds", 0.05)
    monkeypatch.setattr(CONFIG, "max_deliveries", 1)
    await subscriptions.create("w", "orders")
    await events.publish("orders", {"n": 1})

    await events.pull("w", wait=0)  # delivery 1
    await asyncio.sleep(0.1)
    assert await events.pull("w", wait=0) is None  # delivery 2 → DLQ
    await asyncio.sleep(0.1)
    assert await events.pull("w", wait=0) is None  # nothing left to reclaim


async def test_peek_unknown_subscription_raises() -> None:
    with pytest.raises(SubscriptionNotFound):
        await dlq.peek("ghost")


async def test_peek_empty_when_no_dead_events() -> None:
    await subscriptions.create("w", "orders")
    assert await dlq.peek("w") == []


async def test_peek_returns_oldest_first_and_respects_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(CONFIG, "lease_seconds", 0.05)
    monkeypatch.setattr(CONFIG, "max_deliveries", 1)
    await subscriptions.create("w", "orders")
    id_a = await events.publish("orders", {"n": "a"})
    id_b = await events.publish("orders", {"n": "b"})
    id_c = await events.publish("orders", {"n": "c"})
    await events.pull("w", wait=0)
    await events.pull("w", wait=0)
    await events.pull("w", wait=0)
    await asyncio.sleep(0.1)
    # Trigger DLQ moves
    for _ in range(3):
        await events.pull("w", wait=0)

    two = await dlq.peek("w", count=2)
    assert [e["id"] for e in two] == sorted([id_a, id_b, id_c])[:2]


async def test_drop_removes_one_dead_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(CONFIG, "lease_seconds", 0.05)
    monkeypatch.setattr(CONFIG, "max_deliveries", 1)
    await subscriptions.create("w", "orders")
    event_id = await events.publish("orders", {"n": 1})
    await events.pull("w", wait=0)
    await asyncio.sleep(0.1)
    await events.pull("w", wait=0)  # → DLQ

    await dlq.drop("w", event_id)
    assert await dlq.peek("w") == []


async def test_drop_missing_event_raises() -> None:
    await subscriptions.create("w", "orders")
    with pytest.raises(EventNotFound):
        await dlq.drop("w", "999-0")


async def test_purge_clears_dlq(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(CONFIG, "lease_seconds", 0.05)
    monkeypatch.setattr(CONFIG, "max_deliveries", 1)
    await subscriptions.create("w", "orders")
    await events.publish("orders", {"n": 1})
    await events.publish("orders", {"n": 2})
    await events.pull("w", wait=0)
    await events.pull("w", wait=0)
    await asyncio.sleep(0.1)
    await events.pull("w", wait=0)
    await events.pull("w", wait=0)
    assert len(await dlq.peek("w")) == 2

    await dlq.purge("w")
    assert await dlq.peek("w") == []

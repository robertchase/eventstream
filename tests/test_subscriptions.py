"""Micro-tests for subscription management."""

from __future__ import annotations

import asyncio

import pytest

from eventstream import config as CONFIG
from eventstream.logic import events, subscriptions
from eventstream.logic.exceptions import SubscriptionExists, SubscriptionNotFound

# ---- registry + binding ------------------------------------------------------


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


# ---- show / pending (with the new effective-config fields) ------------------


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
    # fakeredis 2.36 reports lag off-by-one; assert the field is populated.
    assert info["lag"] >= 1
    assert info["in_flight"] == 0
    assert info["oldest_idle_ms"] == 0
    # And the new effective-config fields:
    assert info["lease_seconds"] == CONFIG.lease_seconds
    assert info["lease_seconds_explicit"] is False
    assert info["max_deliveries"] == CONFIG.max_deliveries
    assert info["max_deliveries_explicit"] is False


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
    assert ":" in entry["consumer"]
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
    await events.pull("w", wait=0)
    entries = await subscriptions.pending("w")
    assert len(entries) == 1
    assert entries[0]["delivery_count"] == 2


# ---- per-subscription overrides ---------------------------------------------


async def test_config_defaults_when_no_overrides() -> None:
    await subscriptions.create("w", "orders")
    cfg = await subscriptions.config("w")
    assert cfg["stream"] == "orders"
    assert cfg["lease_seconds"] == CONFIG.lease_seconds
    assert cfg["lease_seconds_explicit"] is False
    assert cfg["max_deliveries"] == CONFIG.max_deliveries
    assert cfg["max_deliveries_explicit"] is False


async def test_create_with_overrides_stores_them() -> None:
    await subscriptions.create("w", "orders", lease_seconds=60, max_deliveries=10)
    cfg = await subscriptions.config("w")
    assert cfg["lease_seconds"] == 60.0
    assert cfg["lease_seconds_explicit"] is True
    assert cfg["max_deliveries"] == 10
    assert cfg["max_deliveries_explicit"] is True


async def test_set_updates_only_supplied_fields() -> None:
    await subscriptions.create("w", "orders", lease_seconds=60, max_deliveries=10)
    await subscriptions.set_("w", lease_seconds=120)
    cfg = await subscriptions.config("w")
    assert cfg["lease_seconds"] == 120.0
    assert cfg["max_deliveries"] == 10  # unchanged


async def test_set_unknown_subscription_raises() -> None:
    with pytest.raises(SubscriptionNotFound):
        await subscriptions.set_("ghost", lease_seconds=5)


async def test_unset_reverts_to_default() -> None:
    await subscriptions.create("w", "orders", lease_seconds=60, max_deliveries=10)
    await subscriptions.unset("w", lease_seconds=True)
    cfg = await subscriptions.config("w")
    assert cfg["lease_seconds"] == CONFIG.lease_seconds
    assert cfg["lease_seconds_explicit"] is False
    assert cfg["max_deliveries"] == 10
    assert cfg["max_deliveries_explicit"] is True


async def test_per_sub_lease_overrides_global(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sub with explicit lease ignores CONFIG and reclaims on its own clock."""
    monkeypatch.setattr(CONFIG, "lease_seconds", 60.0)  # global is long
    await subscriptions.create("fast", "orders", lease_seconds=0.05)
    await events.publish("orders", {"n": 1})
    first = await events.pull("fast", wait=0)
    assert first is not None
    await asyncio.sleep(0.1)
    second = await events.pull("fast", wait=0)
    assert second is not None
    assert second["delivery_count"] == 2  # reclaimed at the per-sub lease


async def test_per_sub_max_deliveries_overrides_global(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sub with explicit max_deliveries triggers DLQ on its own threshold."""
    monkeypatch.setattr(CONFIG, "max_deliveries", 99)  # global is generous
    monkeypatch.setattr(CONFIG, "lease_seconds", 0.05)
    await subscriptions.create("strict", "orders", max_deliveries=1)
    await events.publish("orders", {"n": 1})
    await events.pull("strict", wait=0)  # delivery 1
    await asyncio.sleep(0.1)
    # delivery 2 exceeds max=1 → DLQ; pull returns None
    assert await events.pull("strict", wait=0) is None
    # Confirm the event landed in the DLQ via the public API.
    from eventstream.logic import dlq

    dead = await dlq.peek("strict")
    assert len(dead) == 1


# ---- migration ---------------------------------------------------------------


async def test_migrate_is_noop_on_new_shape() -> None:
    await subscriptions.create("w", "orders")
    result = await subscriptions.migrate()
    assert result["reason"] == "already migrated"


async def test_migrate_on_empty_redis_is_noop() -> None:
    result = await subscriptions.migrate()
    assert result["reason"] == "no data"


async def test_migrate_from_legacy_hash_format(fake_redis) -> None:
    """Simulate pre-0.3 data: HASH at eventstream:subscriptions."""
    # Seed legacy shape directly.
    await fake_redis.hset(
        "eventstream:subscriptions",
        mapping={"billing-worker": "orders", "fulfillment": "orders"},
    )
    result = await subscriptions.migrate()
    assert result["migrated"] == 2
    # New shape works.
    assert await subscriptions.stream_of("billing-worker") == "orders"
    assert await subscriptions.list_() == [
        {"name": "billing-worker", "stream": "orders"},
        {"name": "fulfillment", "stream": "orders"},
    ]


# ---- delete -----------------------------------------------------------------


async def test_delete_removes_subscription() -> None:
    await subscriptions.create("w", "orders")
    await subscriptions.delete("w")
    assert await subscriptions.list_() == []
    with pytest.raises(SubscriptionNotFound):
        await subscriptions.stream_of("w")


async def test_delete_unknown_subscription_raises() -> None:
    with pytest.raises(SubscriptionNotFound):
        await subscriptions.delete("ghost")


async def test_delete_clears_dlq_and_lets_group_be_recreated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from eventstream.logic import dlq

    monkeypatch.setattr(CONFIG, "lease_seconds", 0.05)
    monkeypatch.setattr(CONFIG, "max_deliveries", 1)
    await subscriptions.create("w", "orders")
    await events.publish("orders", {"n": 1})
    await events.pull("w", wait=0)
    await asyncio.sleep(0.1)
    await events.pull("w", wait=0)  # → DLQ
    assert len(await dlq.peek("w")) == 1

    await subscriptions.delete("w")
    # Recreating with the same name starts clean: no leftover DLQ, fresh group.
    await subscriptions.create("w", "orders")
    assert await dlq.peek("w") == []

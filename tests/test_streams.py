"""Micro-tests for the stream registry and admin views."""

from __future__ import annotations

import pytest

from eventstream.logic import events, streams, subscriptions
from eventstream.logic.exceptions import StreamNotFound


async def test_publish_registers_stream() -> None:
    await events.publish("orders", {"n": 1})
    assert await streams.list_() == ["orders"]


async def test_list_is_sorted_and_deduped() -> None:
    await events.publish("orders", {"n": 1})
    await events.publish("orders", {"n": 2})
    await events.publish("billing", {"n": 3})
    assert await streams.list_() == ["billing", "orders"]


async def test_show_unknown_stream_raises() -> None:
    with pytest.raises(StreamNotFound):
        await streams.show("ghost")


async def test_show_empty_stream_has_no_entries() -> None:
    await subscriptions.create("w", "orders")  # creates empty stream via mkstream
    info = await streams.show("orders")
    assert info["name"] == "orders"
    assert info["length"] == 0
    assert info["first"] is None
    assert info["last"] is None
    assert info["groups"] == ["w"]


async def test_show_populated_stream_has_first_last_and_groups() -> None:
    await subscriptions.create("w", "orders")
    first_id = await events.publish("orders", {"n": 1})
    last_id = await events.publish("orders", {"n": 2})
    info = await streams.show("orders")
    assert info["length"] == 2
    assert info["first"]["id"] == first_id
    assert "T" in info["first"]["ts"]  # ISO timestamp
    assert info["last"]["id"] == last_id
    assert info["groups"] == ["w"]


async def test_peek_unknown_stream_raises() -> None:
    with pytest.raises(StreamNotFound):
        await streams.peek("ghost")


async def test_peek_returns_events_in_order_and_respects_count() -> None:
    await events.publish("orders", {"n": 1}, key="a")
    await events.publish("orders", {"n": 2})
    await events.publish("orders", {"n": 3})
    seen = await streams.peek("orders", count=2)
    assert [e["payload"] for e in seen] == [{"n": 1}, {"n": 2}]
    assert seen[0]["key"] == "a"
    assert "ts" in seen[0]


async def test_peek_reverse_yields_newest_first() -> None:
    await events.publish("orders", {"n": 1})
    await events.publish("orders", {"n": 2})
    await events.publish("orders", {"n": 3})
    seen = await streams.peek("orders", count=2, reverse=True)
    assert [e["payload"] for e in seen] == [{"n": 3}, {"n": 2}]


async def test_peek_does_not_consume_for_subscriptions() -> None:
    """peek bypasses consumer groups; a sub still sees its events afterward."""
    await subscriptions.create("w", "orders")
    await events.publish("orders", {"n": 1})
    assert len(await streams.peek("orders")) == 1
    event = await events.pull("w", wait=0)
    assert event is not None
    assert event["payload"] == {"n": 1}


# ---- truncate ---------------------------------------------------------------


async def test_truncate_removes_all_by_default() -> None:
    await events.publish("orders", {"n": 1})
    await events.publish("orders", {"n": 2})
    await events.publish("orders", {"n": 3})
    removed = await streams.truncate("orders")
    assert removed == 3
    assert await streams.peek("orders") == []
    # The stream itself still exists (registry + key remain).
    assert "orders" in await streams.list_()


async def test_truncate_keeps_newest() -> None:
    for n in range(5):
        await events.publish("orders", {"n": n})
    removed = await streams.truncate("orders", keep=2)
    assert removed == 3
    kept = await streams.peek("orders")
    assert [e["payload"]["n"] for e in kept] == [3, 4]


async def test_truncate_unknown_stream_raises() -> None:
    with pytest.raises(StreamNotFound):
        await streams.truncate("ghost")


# ---- delete (with cascade guard) --------------------------------------------


async def test_delete_stream_without_subscriptions() -> None:
    await events.publish("orders", {"n": 1})
    await streams.delete("orders")
    assert "orders" not in await streams.list_()


async def test_delete_stream_with_subscriptions_refused() -> None:
    await subscriptions.create("w", "orders")
    with pytest.raises(streams.StreamHasSubscriptions):
        await streams.delete("orders")
    # Nothing removed.
    assert "orders" in await streams.list_()
    assert await subscriptions.stream_of("w") == "orders"


async def test_delete_stream_cascade_removes_subscriptions() -> None:
    await subscriptions.create("a", "orders")
    await subscriptions.create("b", "orders")
    await streams.delete("orders", cascade=True)
    assert "orders" not in await streams.list_()
    assert await subscriptions.list_() == []


async def test_delete_unknown_stream_raises() -> None:
    with pytest.raises(StreamNotFound):
        await streams.delete("ghost")

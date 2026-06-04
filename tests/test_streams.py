"""Micro-tests for the stream registry."""

from __future__ import annotations

from eventstream.logic import events, streams


async def test_publish_registers_stream() -> None:
    await events.publish("orders", {"n": 1})
    assert await streams.list_() == ["orders"]


async def test_list_is_sorted_and_deduped() -> None:
    await events.publish("orders", {"n": 1})
    await events.publish("orders", {"n": 2})
    await events.publish("billing", {"n": 3})
    assert await streams.list_() == ["billing", "orders"]

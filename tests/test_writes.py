"""Micro-tests for the HTTP write adapter handlers.

Calls the handlers directly (the meander request plumbing is exercised live);
asserts the api.md-shaped responses and the Redis side effects.
"""

from __future__ import annotations

import meander
import pytest

from eventstream import config as CONFIG
from eventstream.logic import jobs, streams, subscriptions, workflows
from eventstream.server import writes


async def test_publish_returns_id_and_persists() -> None:
    res = await writes.publish_event("orders", {"n": 1}, key="k")
    assert list(res.keys()) == ["id"]
    seen = await streams.peek("orders")
    assert seen[0]["payload"] == {"n": 1}
    assert seen[0]["key"] == "k"


async def test_pull_returns_event_then_204() -> None:
    await subscriptions.create("w", "orders")
    await writes.publish_event("orders", {"n": 1})
    event = await writes.pull_event("w", wait=0)
    assert event["payload"] == {"n": 1}
    empty = await writes.pull_event("w", wait=0)
    assert isinstance(empty, meander.Response)
    assert empty.code == 204


async def test_ack_returns_204_and_clears_pending() -> None:
    await subscriptions.create("w", "orders")
    published = await writes.publish_event("orders", {"n": 1})
    await writes.pull_event("w", wait=0)
    result = await writes.ack_event("w", published["id"])
    assert isinstance(result, meander.Response)
    assert result.code == 204
    assert await subscriptions.pending("w") == []


async def test_create_subscription_returns_201() -> None:
    result = await writes.create_subscription("billing", "orders")
    assert result.code == 201
    assert await subscriptions.stream_of("billing") == "orders"


async def test_create_subscription_with_overrides() -> None:
    await writes.create_subscription(
        "billing", "orders", lease_seconds=60, max_deliveries=9
    )
    cfg = await subscriptions.config("billing")
    assert cfg["lease_seconds"] == 60.0
    assert cfg["max_deliveries"] == 9


async def test_create_subscription_conflict_raises() -> None:
    await writes.create_subscription("w", "orders")
    with pytest.raises(subscriptions.SubscriptionExists):
        await writes.create_subscription("w", "billing")


async def test_pull_unknown_subscription_raises() -> None:
    with pytest.raises(subscriptions.SubscriptionNotFound):
        await writes.pull_event("ghost", wait=0)


async def test_ack_with_outcome_drives_a_job(monkeypatch: pytest.MonkeyPatch) -> None:
    """A write-path ack carrying an outcome advances the emitting job."""
    monkeypatch.setattr(CONFIG, "lease_seconds", 60.0)
    source = """\
NAME    greet
INITIAL waiting

ACTION send
  EMIT outbound hello
  PAYLOAD job $job.id

STATE waiting
  ENTER
    ACTION send
  EVENT ok done

STATE done TERMINAL
"""
    await workflows.register(source)
    await subscriptions.create("worker", "outbound")
    job = await jobs.create("greet")
    event = await writes.pull_event("worker", wait=0)
    await writes.ack_event("worker", event["id"], outcome="ok", data={})
    assert (await jobs.get(job["id"]))["state"] == "done"

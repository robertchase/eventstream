"""Integration tests against a real Redis.

Each test here targets behavior the in-process fakeredis fake gets wrong or
cannot exercise — the bugs we hit by hand during development:

* ``XINFO GROUPS`` lag (fakeredis is off by one)
* ``decode_responses`` on ``hgetall`` / stream reads (fakeredis returns bytes)
* blocking ``XREADGROUP`` past redis-py 8.0's 5 s default ``socket_timeout``
* real ``XAUTOCLAIM`` idle-time lease reclaim
* the full job lifecycle over real streams, hashes, and sorted sets
"""

from __future__ import annotations

import asyncio
import time

import pytest

from eventstream import config as CONFIG
from eventstream.logic import apikeys, dlq, events, jobs, subscriptions, workflows

# ---- decode + lag (fakeredis gets these wrong) ------------------------------


async def test_decode_responses_returns_str_not_bytes() -> None:
    """Real client with decode_responses=True yields str/dict, no wrapper."""
    await subscriptions.create("w", "orders")
    await events.publish("orders", "order-placed", {"n": 1})
    event = await events.pull("w", wait=0)
    assert event is not None
    assert isinstance(event["id"], str)
    assert isinstance(event["name"], str) and event["name"] == "order-placed"
    assert event["payload"] == {"n": 1}
    # hgetall-backed read (subscription config) also comes back as str.
    cfg = await subscriptions.config("w")
    assert cfg["stream"] == "orders"


async def test_subscription_lag_is_exact() -> None:
    """Real Redis reports the true lag; fakeredis under-counts by one."""
    await subscriptions.create("w", "orders")
    await events.publish("orders", "ev", {"n": 1})
    await events.publish("orders", "ev", {"n": 2})
    await events.publish("orders", "ev", {"n": 3})
    info = await subscriptions.show("w")
    assert info["lag"] == 3


# ---- blocking pull past the socket timeout (redis-py 8.0 regression) --------


async def test_long_block_pull_returns_none_without_timeout() -> None:
    """A pull that blocks longer than redis-py's 5 s default socket_timeout
    must return None on lease expiry, not raise TimeoutError.

    This is the regression that motivated socket_timeout=None in backend.py;
    no in-process fake can exercise it.
    """
    await subscriptions.create("w", "orders")
    start = time.monotonic()
    result = await events.pull("w", wait=6)  # > the old 5 s socket default
    elapsed = time.monotonic() - start
    assert result is None
    assert elapsed >= 5.5  # proves it actually blocked past 5 s


# ---- real lease / redelivery timing -----------------------------------------


async def test_unacked_event_redelivered_after_real_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(CONFIG, "lease_seconds", 1.0)
    await subscriptions.create("w", "orders")
    event_id = await events.publish("orders", "ev", {"n": 1})

    first = await events.pull("w", wait=0)
    assert first is not None and first["id"] == event_id
    assert first["delivery_count"] == 1

    await asyncio.sleep(1.2)
    second = await events.pull("w", wait=0)
    assert second is not None and second["id"] == event_id
    assert second["delivery_count"] == 2


async def test_ack_clears_pending_on_real_redis() -> None:
    await subscriptions.create("w", "orders")
    event_id = await events.publish("orders", "ev", {"n": 1})
    await events.pull("w", wait=0)
    await events.ack("w", event_id)
    assert await subscriptions.pending("w") == []


# ---- full job lifecycle over real Redis -------------------------------------

_GREET = """\
NAME    greet
INITIAL waiting

ACTION send
  EMIT outbound hello
  PAYLOAD job $job.id
  PAYLOAD to  $context.to

STATE waiting
  ENTER
    ACTION send
  EVENT ok done

STATE done TERMINAL
"""

_STEP = """\
NAME    stepflow
INITIAL working

ACTION do-step
  EMIT tasks run
  PAYLOAD job $job.id

DEFAULT error failed

STATE working
  ENTER
    ACTION do-step
  EVENT done finished

STATE finished TERMINAL
STATE failed   TERMINAL
"""


async def test_job_advances_via_ack_with_outcome() -> None:
    await workflows.register(_GREET)
    await subscriptions.create("worker", "outbound")
    job = await jobs.create("greet", {"to": "alice"})

    event = await events.pull("worker", wait=0)
    assert event is not None
    assert event["name"] == "hello"
    assert event["payload"]["to"] == "alice"

    advanced = await events.ack("worker", event["id"], outcome="ok", data={})
    assert advanced is not None
    assert advanced["id"] == job["id"]
    assert advanced["state"] == "done"
    assert advanced["status"] == "terminal"
    assert await subscriptions.pending("worker") == []


async def test_dlqd_step_fails_job_on_real_redis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(CONFIG, "lease_seconds", 1.0)
    await workflows.register(_STEP)
    await subscriptions.create("runner", "tasks", max_deliveries=1)
    job = await jobs.create("stepflow")

    await events.pull("runner", wait=0)  # delivery 1
    await asyncio.sleep(1.2)
    assert await events.pull("runner", wait=0) is None  # delivery 2 → DLQ → error

    after = await jobs.get(job["id"])
    assert after["state"] == "failed"
    assert after["status"] == "terminal"
    assert len(await dlq.peek("runner")) == 1


async def test_apikey_roundtrip_on_real_redis() -> None:
    """create → verify (scope + hash) → revoke, against real Redis hashes."""
    created = await apikeys.create("svc", ["read", "write"])
    assert await apikeys.verify(created["token"], required="read") == "svc"
    assert await apikeys.verify(created["token"], required="write") == "svc"
    with pytest.raises(apikeys.InsufficientScope):
        await apikeys.verify(created["token"], required="admin")
    await apikeys.revoke(created["keyid"])
    with pytest.raises(apikeys.InvalidToken):
        await apikeys.verify(created["token"], required="read")


async def test_timer_fires_via_tick_on_real_redis() -> None:
    source = """\
NAME    timed
INITIAL waiting

ACTION arm
  TIMER 1s due

STATE waiting
  ENTER
    ACTION arm
  EVENT due done

STATE done TERMINAL
"""
    await workflows.register(source)
    job = await jobs.create("timed")
    assert (await jobs.tick())["fired"] == 0  # not due yet
    await asyncio.sleep(1.2)
    assert (await jobs.tick())["fired"] == 1
    assert (await jobs.get(job["id"]))["state"] == "done"

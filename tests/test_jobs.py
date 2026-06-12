"""Micro-tests for the job persistence layer."""

from __future__ import annotations

import pytest

from eventstream.logic import events, jobs, streams, subscriptions, workflows

_WF_SOURCE = """\
NAME    test-flow
INITIAL waiting

ACTION send
  EMIT outbound greet
  PAYLOAD who $context.target
  PAYLOAD job $job.id

ACTION mark
  SET acked yes

DEFAULT error failed

STATE waiting
  ENTER
    ACTION send
  EVENT success done
    ACTION mark

STATE done   TERMINAL
STATE failed TERMINAL
"""

_WF_WITH_TIMER = """\
NAME    timed-flow
INITIAL waiting

ACTION arm
  TIMER 1s timeout

STATE waiting
  ENTER
    ACTION arm
  EVENT timeout end

STATE end TERMINAL
"""


async def _register() -> None:
    await workflows.register(_WF_SOURCE)


# ---- create ----------------------------------------------------------------


async def test_create_persists_initial_state_and_runs_enter() -> None:
    await _register()
    job = await jobs.create("test-flow", {"target": "alice"})
    assert job["state"] == "waiting"
    assert job["status"] == "running"
    assert job["context"] == {"target": "alice"}
    # ENTER ran send → an event landed on `outbound`.
    events_on_stream = await streams.peek("outbound")
    assert len(events_on_stream) == 1
    payload = events_on_stream[0]["payload"]
    assert payload["_event"] == "greet"
    assert payload["who"] == "alice"
    assert payload["job"] == job["id"]


async def test_create_unknown_workflow_raises() -> None:
    with pytest.raises(workflows.WorkflowNotFound):
        await jobs.create("ghost")


# ---- get / list -----------------------------------------------------------


async def test_get_unknown_raises() -> None:
    with pytest.raises(jobs.JobNotFound):
        await jobs.get("job_doesnotexist")


async def test_list_returns_running_jobs() -> None:
    await _register()
    a = await jobs.create("test-flow", {"target": "a"})
    b = await jobs.create("test-flow", {"target": "b"})
    listing = await jobs.list_()
    ids = {j["id"] for j in listing}
    assert ids == {a["id"], b["id"]}


async def test_list_filters_by_workflow() -> None:
    await _register()
    await workflows.register(_WF_SOURCE.replace("test-flow", "other-flow"))
    j1 = await jobs.create("test-flow", {"target": "a"})
    await jobs.create("other-flow", {"target": "b"})
    filtered = await jobs.list_(workflow="test-flow")
    assert [j["id"] for j in filtered] == [j1["id"]]


# ---- advance ---------------------------------------------------------------


async def test_advance_to_terminal() -> None:
    await _register()
    job = await jobs.create("test-flow", {"target": "alice"})
    advanced = await jobs.advance(job["id"], "success", {})
    assert advanced["state"] == "done"
    assert advanced["status"] == "terminal"
    assert advanced["context"]["acked"] == "yes"


async def test_advance_unknown_event_quiesces() -> None:
    await _register()
    job = await jobs.create("test-flow", {"target": "alice"})
    advanced = await jobs.advance(job["id"], "no-handler-for-this", {})
    assert advanced["state"] == "waiting"
    assert advanced["status"] == "running"


async def test_advance_routes_error_through_default() -> None:
    await _register()
    job = await jobs.create("test-flow", {"target": "alice"})
    advanced = await jobs.advance(job["id"], "error", {})
    assert advanced["state"] == "failed"
    assert advanced["status"] == "terminal"


async def test_advance_terminal_job_raises() -> None:
    await _register()
    job = await jobs.create("test-flow", {"target": "alice"})
    await jobs.advance(job["id"], "success", {})
    with pytest.raises(jobs.JobNotRunning):
        await jobs.advance(job["id"], "success", {})


# ---- history --------------------------------------------------------------


async def test_history_records_transitions() -> None:
    await _register()
    job = await jobs.create("test-flow", {"target": "alice"})
    await jobs.advance(job["id"], "success", {})
    hist = await jobs.history(job["id"])
    assert len(hist) == 1
    assert hist[0]["from"] == "waiting"
    assert hist[0]["event"] == "success"
    assert hist[0]["to"] == "done"


# ---- cancel ---------------------------------------------------------------


async def test_cancel_marks_running_job() -> None:
    await _register()
    job = await jobs.create("test-flow", {"target": "alice"})
    await jobs.cancel(job["id"])
    after = await jobs.get(job["id"])
    assert after["status"] == "cancelled"
    with pytest.raises(jobs.JobNotRunning):
        await jobs.advance(job["id"], "success", {})


async def test_delete_removes_finished_job() -> None:
    await _register()
    job = await jobs.create("test-flow", {"target": "alice"})
    await jobs.advance(job["id"], "success", {})
    await jobs.delete(job["id"])
    with pytest.raises(jobs.JobNotFound):
        await jobs.get(job["id"])
    with pytest.raises(jobs.JobNotFound):
        await jobs.history(job["id"])
    assert await jobs.list_() == []


async def test_delete_running_job_refused_without_force() -> None:
    await _register()
    job = await jobs.create("test-flow", {"target": "alice"})
    with pytest.raises(jobs.JobRunning):
        await jobs.delete(job["id"])
    # Still there.
    assert (await jobs.get(job["id"]))["status"] == "running"


async def test_delete_running_job_with_force() -> None:
    await _register()
    job = await jobs.create("test-flow", {"target": "alice"})
    await jobs.delete(job["id"], force=True)
    with pytest.raises(jobs.JobNotFound):
        await jobs.get(job["id"])


async def test_delete_cancelled_job_allowed() -> None:
    await _register()
    job = await jobs.create("test-flow", {"target": "alice"})
    await jobs.cancel(job["id"])
    await jobs.delete(job["id"])
    with pytest.raises(jobs.JobNotFound):
        await jobs.get(job["id"])


async def test_delete_unknown_job_raises() -> None:
    with pytest.raises(jobs.JobNotFound):
        await jobs.delete("job_ghost")


async def test_delete_cleans_pending_timers() -> None:
    await workflows.register(_WF_WITH_TIMER)
    job = await jobs.create("timed-flow")
    await jobs.delete(job["id"], force=True)
    # The timer must not fire (and not even count as dropped — it's gone).
    import asyncio

    await asyncio.sleep(1.2)
    assert await jobs.tick() == {"fired": 0, "dropped": 0}


async def test_cancel_terminal_is_noop() -> None:
    await _register()
    job = await jobs.create("test-flow", {"target": "alice"})
    await jobs.advance(job["id"], "success", {})
    await jobs.cancel(job["id"])
    after = await jobs.get(job["id"])
    assert after["status"] == "terminal"  # stays terminal


# ---- bus integration: enter emits land on the worker's subscription -------


async def test_worker_can_consume_what_create_emitted() -> None:
    """A subscription on `outbound` receives the greet emit, payload tagged."""
    await _register()
    await subscriptions.create("worker", "outbound")
    job = await jobs.create("test-flow", {"target": "alice"})
    event = await events.pull("worker", wait=0)
    assert event is not None
    assert event["payload"]["_event"] == "greet"
    assert event["payload"]["_job"] == job["id"]
    assert event["payload"]["who"] == "alice"


# ---- ack-with-outcome routing --------------------------------------------


async def test_ack_with_outcome_advances_the_job() -> None:
    await _register()
    await subscriptions.create("worker", "outbound")
    job = await jobs.create("test-flow", {"target": "alice"})
    event = await events.pull("worker", wait=0)
    advanced = await events.ack("worker", event["id"], outcome="success", data={})
    assert advanced is not None
    assert advanced["id"] == job["id"]
    assert advanced["state"] == "done"
    assert advanced["status"] == "terminal"
    # And the underlying bus event is XACKed:
    assert await subscriptions.pending("worker") == []


async def test_ack_without_outcome_does_not_advance() -> None:
    await _register()
    await subscriptions.create("worker", "outbound")
    job = await jobs.create("test-flow", {"target": "alice"})
    event = await events.pull("worker", wait=0)
    result = await events.ack("worker", event["id"])  # bare ack
    assert result is None
    # Job hasn't moved.
    fresh = await jobs.get(job["id"])
    assert fresh["state"] == "waiting"


async def test_double_ack_with_outcome_is_idempotent() -> None:
    """Second ack-with-outcome finds the map entry gone; no double advance."""
    await _register()
    await subscriptions.create("worker", "outbound")
    await jobs.create("test-flow", {"target": "alice"})
    event = await events.pull("worker", wait=0)
    first = await events.ack("worker", event["id"], outcome="success", data={})
    assert first["state"] == "done"
    # Reset the job to "waiting" to ensure a second ack would NOT advance.
    # (Since the job is terminal anyway, advance would also raise, but the
    # handle_ack path silently no-ops on missing map entry — that's what
    # we want to verify.)
    second = await events.ack("worker", event["id"], outcome="success", data={})
    assert second is None


async def test_stale_ack_does_not_advance() -> None:
    """Ack arrives after the job has already moved past the emit state."""
    await _register()
    await subscriptions.create("worker", "outbound")
    job = await jobs.create("test-flow", {"target": "alice"})
    event = await events.pull("worker", wait=0)
    # Move the job past `waiting` via a separate path (the error→default).
    await jobs.advance(job["id"], "error", {})
    fresh = await jobs.get(job["id"])
    assert fresh["state"] == "failed"
    # Now the worker's stale ack arrives; state no longer matches.
    result = await events.ack("worker", event["id"], outcome="success", data={})
    assert result is None
    # Job stayed at `failed`.
    final = await jobs.get(job["id"])
    assert final["state"] == "failed"


# ---- timers --------------------------------------------------------------


async def test_tick_fires_due_timers() -> None:
    await workflows.register(_WF_WITH_TIMER)
    job = await jobs.create("timed-flow")
    assert job["state"] == "waiting"
    # Timer was scheduled with delay 1s; without waiting it shouldn't fire.
    result = await jobs.tick()
    assert result == {"fired": 0, "dropped": 0}
    assert (await jobs.get(job["id"]))["state"] == "waiting"

    # Sleep past the delay, then sweep.
    import asyncio

    await asyncio.sleep(1.2)
    result = await jobs.tick()
    assert result["fired"] == 1
    assert (await jobs.get(job["id"]))["state"] == "end"


async def test_tick_drops_timers_for_terminal_jobs() -> None:
    await workflows.register(_WF_WITH_TIMER)
    job = await jobs.create("timed-flow")
    # Force the job past the state so the timer is stale when it fires.
    await jobs.advance(job["id"], "timeout", {})
    assert (await jobs.get(job["id"]))["state"] == "end"
    import asyncio

    await asyncio.sleep(1.2)
    result = await jobs.tick()
    assert result["fired"] == 0
    assert result["dropped"] == 1

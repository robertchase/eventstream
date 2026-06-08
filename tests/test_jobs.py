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

"""Micro-tests for the job persistence layer."""

from __future__ import annotations

import asyncio

import pytest

from eventstream import config as CONFIG
from eventstream.logic import (
    backend,
    dlq,
    events,
    jobs,
    streams,
    subscriptions,
    workflows,
)


@pytest.fixture(autouse=True)
def _reset_timer_armed():
    """Isolate the module-global sweeper signal between tests."""
    saved = jobs._timer_armed
    jobs._timer_armed = None
    yield
    jobs._timer_armed = saved


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

_WF_WITH_STEP = """\
NAME    step-flow
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
    assert events_on_stream[0]["name"] == "greet"
    payload = events_on_stream[0]["payload"]
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
    assert hist[0]["kind"] == "transition"
    assert hist[0]["from"] == "waiting"
    assert hist[0]["event"] == "success"
    assert hist[0]["to"] == "done"


_WF_WITH_LOG = """\
NAME    log-flow
INITIAL waiting

ACTION announce
  LOG entered waiting

ACTION wrap-up
  LOG wrapping up

STATE waiting
  ENTER
    ACTION announce
  EVENT go done
    ACTION wrap-up

STATE done TERMINAL
"""


async def test_log_lines_recorded_in_history_in_order() -> None:
    """LOG output is durable: it lands in job history, interleaved with the
    transition and tagged with the state it ran in."""
    await workflows.register(_WF_WITH_LOG)
    job = await jobs.create("log-flow")

    # The initial ENTER's LOG is recorded immediately, before any transition.
    hist = await jobs.history(job["id"])
    assert len(hist) == 1
    assert hist[0]["kind"] == "log"
    assert hist[0]["message"] == "entered waiting"
    assert hist[0]["state"] == "waiting"

    await jobs.advance(job["id"], "go", {})
    hist = await jobs.history(job["id"])
    assert [(e["kind"], e.get("message") or e.get("event")) for e in hist] == [
        ("log", "entered waiting"),
        ("log", "wrapping up"),  # LOG in the `do` runs before the transition
        ("transition", "go"),
    ]
    assert hist[-1]["from"] == "waiting" and hist[-1]["to"] == "done"


_WF_INLINE_LOG = """\
NAME    inline-log-flow
INITIAL working

ACTION fan
  EMIT internal relay

STATE working
  EVENT step working
    ACTION fan
    LOG handled step, relaying
  EVENT relay done

STATE done TERMINAL
"""


async def test_inline_log_under_event_runs_and_is_transparent() -> None:
    """An inline LOG under EVENT records to history and leaves the carry
    intact, so the preceding EMIT's event still cascades."""
    await workflows.register(_WF_INLINE_LOG)
    job = await jobs.create("inline-log-flow")
    advanced = await jobs.advance(job["id"], "step", {})

    # `fan` emitted `relay`; the inline LOG didn't clear the carry, so `relay`
    # cascaded working -> done.
    assert advanced["state"] == "done"
    hist = await jobs.history(job["id"])
    assert [(e["kind"], e.get("message") or e.get("event")) for e in hist] == [
        ("log", "handled step, relaying"),
        ("transition", "step"),
        ("transition", "relay"),
    ]
    assert hist[0]["state"] == "working"


_WF_INLINE_LOG_ENTER = """\
NAME    enter-log-flow
INITIAL waiting

STATE waiting
  ENTER
    LOG job created, waiting
  EVENT go done

STATE done TERMINAL
"""


async def test_inline_log_in_initial_enter_records_to_history() -> None:
    """An inline LOG in the initial ENTER runs at create time and is durable."""
    await workflows.register(_WF_INLINE_LOG_ENTER)
    job = await jobs.create("enter-log-flow")
    hist = await jobs.history(job["id"])
    assert len(hist) == 1
    assert hist[0]["kind"] == "log"
    assert hist[0]["message"] == "job created, waiting"
    assert hist[0]["state"] == "waiting"


_WF_MULTI_STATEMENT = """\
NAME    multi-flow
INITIAL waiting

ACTION kickoff
  SET status started
  EMIT outbound begin
  PAYLOAD job $job.id
  LOG kicked off

STATE waiting
  ENTER
    ACTION kickoff
  EVENT done end

STATE end TERMINAL
"""


async def test_multi_statement_action_persists_emits_and_logs() -> None:
    """One ACTION with SET + EMIT + LOG: context persists, the event is
    published, and the log lands in history — all in order."""
    await workflows.register(_WF_MULTI_STATEMENT)
    await subscriptions.create("worker", "outbound")
    job = await jobs.create("multi-flow")

    assert (await jobs.get(job["id"]))["context"] == {"status": "started"}
    event = await events.pull("worker", wait=0)
    assert event["name"] == "begin"
    assert event["payload"]["_job"] == job["id"]
    hist = await jobs.history(job["id"])
    assert hist[-1] == {
        "kind": "log",
        "message": "kicked off",
        "state": "waiting",
        "ts": hist[-1]["ts"],
    }


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

    await asyncio.sleep(1.2)
    assert await jobs.tick() == {"fired": 0, "dropped": 0}


# ---- DLQ → job error wiring ------------------------------------------------


async def test_dlqd_step_fails_the_job(monkeypatch: pytest.MonkeyPatch) -> None:
    """A step event that exhausts redelivery moves to the DLQ and the job
    takes its DEFAULT error transition instead of hanging."""
    monkeypatch.setattr(CONFIG, "lease_seconds", 0.05)
    await workflows.register(_WF_WITH_STEP)
    await subscriptions.create("runner", "tasks", max_deliveries=1)
    job = await jobs.create("step-flow")
    assert job["state"] == "working"

    await events.pull("runner", wait=0)  # delivery 1
    await asyncio.sleep(0.1)
    assert await events.pull("runner", wait=0) is None  # delivery 2 → DLQ → error

    after = await jobs.get(job["id"])
    assert after["state"] == "failed"
    assert after["status"] == "terminal"
    # The dead event is also durably in the DLQ.
    assert len(await dlq.peek("runner")) == 1


async def test_dead_event_for_finished_job_is_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the job already moved past the emitting state, the DLQ notice is a
    no-op (the job is not advanced again)."""
    monkeypatch.setattr(CONFIG, "lease_seconds", 0.05)
    await workflows.register(_WF_WITH_STEP)
    await subscriptions.create("runner", "tasks", max_deliveries=1)
    job = await jobs.create("step-flow")

    await events.pull("runner", wait=0)  # delivery 1
    await jobs.advance(job["id"], "done")  # job → finished (terminal)
    await asyncio.sleep(0.1)
    await events.pull("runner", wait=0)  # delivery 2 → DLQ → handle_dead no-ops

    after = await jobs.get(job["id"])
    assert after["state"] == "finished"  # unchanged by the dead notice


# ---- sweeper ---------------------------------------------------------------


async def test_sweep_forever_runs_bounded_iterations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    async def fake_tick() -> dict:
        calls.append(1)
        return {"fired": 0, "dropped": 0}

    monkeypatch.setattr(jobs, "tick", fake_tick)
    await jobs.sweep_forever(0, iterations=3)
    assert len(calls) == 3


async def test_sweep_forever_survives_a_failing_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []

    async def flaky_tick() -> dict:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("boom")
        return {"fired": 0, "dropped": 0}

    monkeypatch.setattr(jobs, "tick", flaky_tick)
    await jobs.sweep_forever(0, iterations=2)
    assert len(calls) == 2  # the loop kept going after the error


async def test_sweep_forever_fires_a_real_timer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await workflows.register(_WF_WITH_TIMER)
    job = await jobs.create("timed-flow")
    await asyncio.sleep(1.2)
    await jobs.sweep_forever(0, iterations=1)
    assert (await jobs.get(job["id"]))["state"] == "end"


async def test_next_timer_at_reports_the_head() -> None:
    assert await jobs._next_timer_at() is None
    await workflows.register(_WF_WITH_TIMER)
    await jobs.create("timed-flow")  # ENTER arms TIMER 1s
    head = await jobs._next_timer_at()
    assert head is not None and head > 0


async def test_arming_head_timer_wakes_in_process_sweeper() -> None:
    """A timer that becomes the head signals an in-process sweeper."""
    jobs._timer_armed = asyncio.Event()
    await workflows.register(_WF_WITH_TIMER)
    await jobs.create("timed-flow")
    assert jobs._timer_armed.is_set()


async def test_arming_later_timer_leaves_sweeper_waiting() -> None:
    """A timer behind an earlier pending one must not wake the sweeper."""
    jobs._timer_armed = asyncio.Event()
    # An earlier timer already sits at the head (score 1 → epoch 1970).
    await backend.client().zadd(
        jobs._TIMERS, {'{"job_id": "x", "event": "e", "nonce": "n"}': 1}
    )
    await workflows.register(_WF_WITH_TIMER)
    await jobs.create("timed-flow")  # arms now+1s, far behind the head
    assert not jobs._timer_armed.is_set()


async def test_arming_timer_without_sweeper_is_a_noop() -> None:
    """No sweeper in this process → arming just persists; no crash."""
    assert jobs._timer_armed is None  # the autouse fixture cleared it
    await workflows.register(_WF_WITH_TIMER)
    await jobs.create("timed-flow")
    assert jobs._timer_armed is None


async def test_running_sweeper_wakes_for_an_in_process_timer() -> None:
    """End to end: a sweeper parked on a long idle wait still fires a timer
    armed after it started, because arming wakes it."""
    await workflows.register(_WF_WITH_TIMER)  # ENTER arms TIMER 1s → end
    # Long idle interval: without the wake, the sweeper would sleep ~1000s and
    # the timer would never fire within this test.
    task = asyncio.create_task(jobs.sweep_forever(1000.0))
    await asyncio.sleep(0.05)  # let the sweeper reach its idle wait
    job = await jobs.create("timed-flow")  # arms the timer → wakes the sweeper
    await asyncio.sleep(1.3)  # 1s timer + margin
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (await jobs.get(job["id"]))["state"] == "end"


async def test_cancel_terminal_is_noop() -> None:
    await _register()
    job = await jobs.create("test-flow", {"target": "alice"})
    await jobs.advance(job["id"], "success", {})
    await jobs.cancel(job["id"])
    after = await jobs.get(job["id"])
    assert after["status"] == "terminal"  # stays terminal


# ---- bus integration: enter emits land on the worker's subscription -------


async def test_worker_can_consume_what_create_emitted() -> None:
    """A subscription on `outbound` receives the greet emit, job tagged."""
    await _register()
    await subscriptions.create("worker", "outbound")
    job = await jobs.create("test-flow", {"target": "alice"})
    event = await events.pull("worker", wait=0)
    assert event is not None
    assert event["name"] == "greet"
    assert event["payload"]["_job"] == job["id"]
    assert event["payload"]["who"] == "alice"


_WF_JOB_SCOPE = """\
NAME    scope-flow
INITIAL waiting

ACTION snapshot
  EMIT outbound snap
  PAYLOAD wf  $job.workflow
  PAYLOAD ver $job.version
  PAYLOAD st  $job.state
  PAYLOAD at  $job.now

STATE waiting
  ENTER
    ACTION snapshot

STATE done TERMINAL
"""


async def test_create_emits_expose_full_job_scope() -> None:
    """create() wires workflow/version/state/now into the `$job` scope."""
    await workflows.register(_WF_JOB_SCOPE)
    job = await jobs.create("scope-flow")
    snap = (await streams.peek("outbound"))[0]["payload"]
    assert snap["wf"] == "scope-flow"
    assert snap["ver"] == 1
    assert snap["st"] == "waiting"
    assert isinstance(snap["at"], str) and "T" in snap["at"]
    assert snap["_job"] == job["id"]


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

    await asyncio.sleep(1.2)
    result = await jobs.tick()
    assert result["fired"] == 0
    assert result["dropped"] == 1

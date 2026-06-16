"""Micro-tests for the pure FSM engine — no Redis, no jobs persistence."""

from __future__ import annotations

import pytest

from eventstream.logic.engine import (
    CascadeBudgetExceeded,
    MissingReference,
    Recorder,
    _interpolate,
    _resolve_path,
    step,
)
from eventstream.logic.workflow_parser import parse

# Some compact workflow fixtures.


def _wf(text: str) -> dict:
    """Parse and return the AST."""
    return parse(text)


_ECHO = _wf("""\
NAME w
INITIAL s

ACTION record
  SET txn $event.data.txn_id

STATE s
  EVENT success done
    ACTION record

STATE done TERMINAL
""")


_LOOP = _wf("""\
NAME w
INITIAL s

ACTION emit-tick
  EMIT timer-stream tick
  PAYLOAD seq $context.seq

STATE s
  ENTER
    ACTION emit-tick
  EVENT tick s
    ACTION emit-tick

STATE done TERMINAL
""")


# ---- resolution + interpolation --------------------------------------------


def test_resolve_path_walks_dotted() -> None:
    job = {"id": "j_1"}
    ctx = {"order": {"total": 4200}}
    evt = {"name": "x", "data": {"y": "z"}}
    assert _resolve_path("job.id", job, ctx, evt) == "j_1"
    assert _resolve_path("context.order.total", job, ctx, evt) == 4200
    assert _resolve_path("event.data.y", job, ctx, evt) == "z"


def test_resolve_path_missing_raises() -> None:
    with pytest.raises(MissingReference):
        _resolve_path("context.missing.key", {}, {}, {"name": "x", "data": {}})


def test_resolve_path_unknown_root_raises() -> None:
    with pytest.raises(MissingReference):
        _resolve_path("nope.x", {}, {}, {"name": "x", "data": {}})


def test_interpolate_replaces_refs() -> None:
    out = _interpolate(
        "order $context.id total $context.order.total",
        {"id": "j"},
        {"id": "ord-1", "order": {"total": 99}},
        {"name": "x", "data": {}},
    )
    assert out == "order ord-1 total 99"


def test_interpolate_leaves_unresolvable_as_is() -> None:
    out = _interpolate(
        "missing $context.nope!", {"id": "j"}, {}, {"name": "x", "data": {}}
    )
    assert out == "missing $context.nope!"


# ---- step semantics --------------------------------------------------------


def test_step_quiesces_with_no_matching_handler() -> None:
    context = {}
    rec = Recorder("j_1")
    final = step(_ECHO, "s", context, {"name": "ignored", "data": {}}, recorder=rec)
    assert final == "s"  # no transition
    assert rec.transitions == []


def test_step_runs_handler_and_transitions_to_terminal() -> None:
    context = {}
    rec = Recorder("j_1")
    final = step(
        _ECHO,
        "s",
        context,
        {"name": "success", "data": {"txn_id": "tx-7"}},
        recorder=rec,
    )
    assert final == "done"
    assert context == {"txn": "tx-7"}
    assert rec.transitions == [{"from": "s", "event": "success", "to": "done"}]


def test_cascade_budget_exceeded_on_infinite_loop() -> None:
    context = {"seq": 1}
    rec = Recorder("j_1")
    # Same workflow, but the loop has no exit — every tick re-enters s
    # which re-emits tick → tick → tick → ...
    with pytest.raises(CascadeBudgetExceeded):
        step(_LOOP, "s", context, {"name": "tick", "data": {}}, recorder=rec)


def test_set_action_updates_context_in_place() -> None:
    context = {}
    rec = Recorder("j_1")
    step(
        _ECHO,
        "s",
        context,
        {"name": "success", "data": {"txn_id": "tx-x"}},
        recorder=rec,
    )
    assert context["txn"] == "tx-x"


def test_default_handler_fires_when_state_misses() -> None:
    wf = _wf("""\
NAME w
INITIAL s

ACTION mark-err
  SET err yes

DEFAULT error failed
  ACTION mark-err

STATE s
  EVENT done end

STATE end    TERMINAL
STATE failed TERMINAL
""")
    context = {}
    rec = Recorder("j_1")
    final = step(wf, "s", context, {"name": "error", "data": {}}, recorder=rec)
    assert final == "failed"
    assert context == {"err": "yes"}


# ---- carry rule ------------------------------------------------------------


def test_carry_rule_drops_when_last_action_is_set() -> None:
    """An emit followed by a set in the same sequence → carry is None."""
    wf = _wf("""\
NAME w
INITIAL s

ACTION emit-x
  EMIT places x
  PAYLOAD k v

ACTION set-y
  SET y z

STATE s
  EVENT go s
    ACTION emit-x
    ACTION set-y

STATE done TERMINAL
""")
    context = {}
    rec = Recorder("j_1")
    final = step(wf, "s", context, {"name": "go", "data": {}}, recorder=rec)
    # Single transition; no cascade because the final action of `do` was SET.
    assert final == "s"
    assert len(rec.emits) == 1
    assert context == {"y": "z"}


def test_log_is_transparent_to_the_carry() -> None:
    """A LOG after an EMIT does not clear the carry — the emit still cascades."""
    wf = _wf("""\
NAME w
INITIAL s

ACTION fan
  EMIT places relay

ACTION note
  LOG handled go

STATE s
  EVENT go s
    ACTION fan
    ACTION note
  EVENT relay done

STATE done TERMINAL
""")
    rec = Recorder("j_1")
    final = step(wf, "s", {}, {"name": "go", "data": {}}, recorder=rec)
    # `fan` emitted `relay`; the trailing LOG left the carry intact, so `relay`
    # cascaded and drove the second transition to `done`.
    assert final == "done"
    assert len(rec.emits) == 1
    assert rec.logs == ["handled go"]


# ---- Recorder shape --------------------------------------------------------


def test_recorder_captures_log_and_timer() -> None:
    wf = _wf("""\
NAME w
INITIAL s

ACTION shout
  LOG hello $context.who

ACTION wait
  TIMER 30s timeout

STATE s
  EVENT a s
    ACTION shout
  EVENT b s
    ACTION wait

STATE done TERMINAL
""")
    context = {"who": "world"}
    rec = Recorder("j_X")
    step(wf, "s", context, {"name": "a", "data": {}}, recorder=rec)
    assert rec.logs == ["hello world"]
    step(wf, "s", context, {"name": "b", "data": {}}, recorder=rec)
    assert rec.timers == [{"event": "timeout", "delay_seconds": 30}]


def test_journal_orders_logs_and_transitions_with_state() -> None:
    """The journal records logs (tagged with their state) and transitions in
    execution order; `logs`/`transitions` remain as filtered views."""
    wf = _wf("""\
NAME w
INITIAL s

ACTION note
  LOG working

STATE s
  EVENT go done
    ACTION note

STATE done TERMINAL
""")
    rec = Recorder("j_1")
    step(wf, "s", {}, {"name": "go", "data": {}}, recorder=rec)
    assert rec.journal == [
        {"kind": "log", "message": "working", "state": "s"},
        {"kind": "transition", "from": "s", "event": "go", "to": "done"},
    ]
    assert rec.logs == ["working"]
    assert rec.transitions == [{"from": "s", "event": "go", "to": "done"}]


# ---- $job scope ------------------------------------------------------------

_SCOPE = _wf("""\
NAME flow
INITIAL start

ACTION snap-do
  EMIT audit do
  PAYLOAD st $job.state

ACTION snap-enter
  EMIT audit enter
  PAYLOAD id  $job.id
  PAYLOAD wf  $job.workflow
  PAYLOAD ver $job.version
  PAYLOAD st  $job.state
  PAYLOAD at  $job.now

STATE start
  EVENT go next
    ACTION snap-do

STATE next
  ENTER
    ACTION snap-enter

STATE done TERMINAL
""")


def test_step_exposes_full_job_scope_with_state_per_phase() -> None:
    """`$job.*` resolves; state is the source for `do`, the target for `enter`."""
    rec = Recorder("j_1")
    final = step(
        _SCOPE,
        "start",
        {},
        {"name": "go", "data": {}},
        recorder=rec,
        job_meta={"workflow": "flow", "version": 3, "now": "2026-06-15T00:00:00+00:00"},
    )
    assert final == "next"
    do_emit, enter_emit = rec.emits
    assert do_emit["payload"]["st"] == "start"  # `do` runs in the source state
    assert enter_emit["payload"] == {
        "id": "j_1",  # falls back to the recorder's job id
        "wf": "flow",
        "ver": 3,
        "st": "next",  # `enter` runs in the target state
        "at": "2026-06-15T00:00:00+00:00",
    }


def test_job_field_absent_without_meta_raises() -> None:
    """Without job_meta only `$job.id` is in scope; other fields fail the step."""
    rec = Recorder("j_1")
    with pytest.raises(MissingReference):
        step(_SCOPE, "start", {}, {"name": "go", "data": {}}, recorder=rec)

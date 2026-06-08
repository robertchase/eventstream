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

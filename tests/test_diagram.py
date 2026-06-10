"""Micro-tests for the AST → nomnoml converter."""

from __future__ import annotations

from eventstream.logic.workflow_parser import parse
from eventstream.server.diagram import to_nomnoml

_ZWSP = "​"

_SOURCE = """\
NAME    orders
INITIAL charging

ACTION charge
  EMIT payments charge
  PAYLOAD job $job.id

DEFAULT error failed

STATE charging
  ENTER
    ACTION charge
  EVENT success shipping
  EVENT declined cancelled
  EVENT retry

STATE shipping
  EVENT success done

STATE cancelled TERMINAL
STATE done      TERMINAL
STATE failed    TERMINAL
"""


def test_start_edge_points_at_initial() -> None:
    out = to_nomnoml(parse(_SOURCE))
    assert f"[<start> {_ZWSP}start] -> [charging]" in out


def test_transitions_become_label_nodes_on_the_edge() -> None:
    out = to_nomnoml(parse(_SOURCE))
    # First occurrence of an event gets one zero-width-space suffix.
    assert f"[charging] - [<ev> success{_ZWSP}]" in out
    assert f"[<ev> success{_ZWSP}] -> [shipping]" in out
    assert f"[charging] - [<ev> declined{_ZWSP}]" in out
    assert f"[<ev> declined{_ZWSP}] -> [cancelled]" in out


def test_repeated_event_names_get_distinct_label_nodes() -> None:
    """Two `success` events must not merge into one diagram node."""
    out = to_nomnoml(parse(_SOURCE))
    assert f"[<ev> success{_ZWSP}] -> [shipping]" in out
    assert f"[<ev> success{_ZWSP}{_ZWSP}] -> [done]" in out


def test_goto_less_event_loops_back_to_its_state() -> None:
    out = to_nomnoml(parse(_SOURCE))
    assert f"[charging] - [<ev> retry{_ZWSP}]" in out
    assert f"[<ev> retry{_ZWSP}] -> [charging]" in out


def test_event_named_like_a_state_does_not_merge() -> None:
    """An event named `waiting` must not collide with the state `waiting`."""
    source = """\
NAME    w
INITIAL waiting

ACTION a
  SET k v

STATE waiting
  EVENT waiting end

STATE end TERMINAL
"""
    out = to_nomnoml(parse(source))
    assert f"[waiting] - [<ev> waiting{_ZWSP}]" in out
    assert f"[<ev> waiting{_ZWSP}] -> [end]" in out


def test_terminal_states_get_terminal_style() -> None:
    out = to_nomnoml(parse(_SOURCE))
    assert "[<terminal> done]" in out
    assert "[<terminal> cancelled]" in out
    assert "[<terminal> failed]" in out


def test_terminal_declarations_precede_edges() -> None:
    """nomnoml styles a node from its first appearance, so the <terminal>
    declaration must come before any edge that references the node."""
    out = to_nomnoml(parse(_SOURCE))
    assert out.index("[<terminal> done]") < out.index("-> [done]")
    assert out.index("[<terminal> cancelled]") < out.index("-> [cancelled]")


def test_default_becomes_dashed_edges_from_pseudo_node() -> None:
    out = to_nomnoml(parse(_SOURCE))
    assert f"[<note> {_ZWSP}DEFAULT] -- [<ev> error{_ZWSP}]" in out
    assert f"[<ev> error{_ZWSP}] --> [failed]" in out


def test_current_state_is_highlighted_and_declared_before_edges() -> None:
    out = to_nomnoml(parse(_SOURCE), current_state="charging")
    assert "[<current> charging]" in out
    assert out.index("[<current> charging]") < out.index("[charging] -")


def test_no_current_marker_by_default() -> None:
    out = to_nomnoml(parse(_SOURCE))
    assert "<current>" not in out


def test_terminal_current_state_keeps_terminal_style() -> None:
    """A finished job's state stays grey; <current> is for running jobs."""
    out = to_nomnoml(parse(_SOURCE), current_state="done")
    assert "<current>" not in out
    assert "[<terminal> done]" in out

"""Micro-tests for the AST → nomnoml converter."""

from __future__ import annotations

from eventstream.logic.workflow_parser import parse
from eventstream.server.diagram import to_nomnoml

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
    assert "[<start> start] -> [charging]" in out


def test_transitions_become_labeled_edges() -> None:
    out = to_nomnoml(parse(_SOURCE))
    assert "[charging] -> success [shipping]" in out
    assert "[charging] -> declined [cancelled]" in out
    assert "[shipping] -> success [done]" in out


def test_goto_less_event_becomes_self_loop() -> None:
    out = to_nomnoml(parse(_SOURCE))
    assert "[charging] -> retry [charging]" in out


def test_terminal_states_get_terminal_style() -> None:
    out = to_nomnoml(parse(_SOURCE))
    assert "[<terminal> done]" in out
    assert "[<terminal> cancelled]" in out
    assert "[<terminal> failed]" in out


def test_terminal_declarations_precede_edges() -> None:
    """nomnoml styles a node from its first appearance, so the <terminal>
    declaration must come before any edge that references the node."""
    out = to_nomnoml(parse(_SOURCE))
    assert out.index("[<terminal> done]") < out.index("success [done]")
    assert out.index("[<terminal> cancelled]") < out.index("declined [cancelled]")


def test_default_becomes_dashed_edge_from_pseudo_node() -> None:
    out = to_nomnoml(parse(_SOURCE))
    assert "[<note> DEFAULT] --> error [failed]" in out


def test_current_state_is_highlighted_and_declared_before_edges() -> None:
    out = to_nomnoml(parse(_SOURCE), current_state="charging")
    assert "[<current> charging]" in out
    assert out.index("[<current> charging]") < out.index("[charging] ->")


def test_no_current_marker_by_default() -> None:
    out = to_nomnoml(parse(_SOURCE))
    assert "<current>" not in out


def test_terminal_current_state_keeps_terminal_style() -> None:
    """A finished job's state stays grey; <current> is for running jobs."""
    out = to_nomnoml(parse(_SOURCE), current_state="done")
    assert "<current>" not in out
    assert "[<terminal> done]" in out

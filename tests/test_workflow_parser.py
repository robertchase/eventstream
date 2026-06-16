"""Micro-tests for the .flow workflow DSL parser."""

from __future__ import annotations

import pytest

from eventstream.logic.workflow_parser import ParseError, parse

_MINIMAL = """\
NAME    w
INITIAL start

ACTION noop
  SET k v

STATE start
  ENTER
    ACTION noop
  EVENT done end

STATE end TERMINAL
"""


def test_minimal_parses() -> None:
    wf = parse(_MINIMAL)
    assert wf["name"] == "w"
    assert wf["initial"] == "start"
    assert wf["description"] is None
    assert "noop" in wf["actions"]
    assert wf["actions"]["noop"]["type"] == "set"
    assert wf["actions"]["noop"]["fields"] == {"k": "v"}
    assert wf["states"]["start"]["enter"] == ["noop"]
    assert wf["states"]["start"]["events"]["done"] == {"do": [], "goto": "end"}
    assert wf["states"]["end"] == {"terminal": True}


def test_full_example_parses() -> None:
    """The canonical example from design/workflow-format.md."""
    source = """\
NAME        order-fulfillment
INITIAL     charging
DESCRIPTION Charges card, ships order, notifies on failure

ACTION charge-card
  EMIT payments charge
  PAYLOAD job    $job.id
  PAYLOAD amount $context.order.total

ACTION record-charge
  SET txn_id     $event.data.txn_id
  SET charged_at $job.now

ACTION ship-order
  EMIT fulfillment ship
  PAYLOAD job     $job.id
  PAYLOAD address $context.customer.address

ACTION notify-customer-decline
  EMIT notifications notify
  PAYLOAD type email
  PAYLOAD to   $context.customer.email

DEFAULT error failed

STATE charging
  ENTER
    ACTION charge-card
  EVENT success shipping
    ACTION record-charge
  EVENT declined cancelled

STATE shipping
  ENTER
    ACTION ship-order
  EVENT success done

STATE cancelled
  ENTER
    ACTION notify-customer-decline
  EVENT success failed

STATE done   TERMINAL
STATE failed TERMINAL
"""
    wf = parse(source)
    assert wf["name"] == "order-fulfillment"
    assert wf["description"] == "Charges card, ships order, notifies on failure"
    assert wf["initial"] == "charging"
    assert wf["defaults"]["error"] == {"do": [], "goto": "failed"}
    # Emit action with payload references:
    charge = wf["actions"]["charge-card"]
    assert charge["type"] == "emit"
    assert charge["stream"] == "payments"
    assert charge["event"] == "charge"
    assert charge["payload"] == {
        "job": {"ref": "job.id"},
        "amount": {"ref": "context.order.total"},
    }
    # Set action with multiple fields:
    record = wf["actions"]["record-charge"]
    assert record["type"] == "set"
    assert record["fields"] == {
        "txn_id": {"ref": "event.data.txn_id"},
        "charged_at": {"ref": "job.now"},
    }
    # State with multiple events:
    charging = wf["states"]["charging"]
    assert charging["enter"] == ["charge-card"]
    assert charging["events"]["success"] == {
        "do": ["record-charge"],
        "goto": "shipping",
    }
    assert charging["events"]["declined"] == {"do": [], "goto": "cancelled"}
    # Terminal states:
    assert wf["states"]["done"]["terminal"]
    assert wf["states"]["failed"]["terminal"]


# ---- non-emit action types --------------------------------------------------


def test_log_action() -> None:
    wf = parse(
        "NAME w\nINITIAL s\n"
        "ACTION speak\n  LOG hello $context.user\n"
        "STATE s TERMINAL\n"
    )
    assert wf["actions"]["speak"] == {
        "type": "log",
        "message": "hello $context.user",
    }


def test_timer_action_parses_duration() -> None:
    wf = parse(
        "NAME w\nINITIAL s\n" "ACTION wait\n  TIMER 10m timeout\n" "STATE s TERMINAL\n"
    )
    assert wf["actions"]["wait"] == {
        "type": "timer",
        "delay_seconds": 600,
        "event": "timeout",
    }


def test_timer_unit_suffixes() -> None:
    def dur(s: str) -> int:
        wf = parse(f"NAME w\nINITIAL s\nACTION a\n  TIMER {s} t\nSTATE s TERMINAL\n")
        return wf["actions"]["a"]["delay_seconds"]

    assert dur("45s") == 45
    assert dur("2m") == 120
    assert dur("3h") == 10800
    assert dur("1d") == 86400


def test_bad_duration_fails() -> None:
    with pytest.raises(ParseError, match="bad duration"):
        parse("NAME w\nINITIAL s\nACTION a\n" "  TIMER 30 t\n" "STATE s TERMINAL\n")


# ---- value tokenization ------------------------------------------------------


def test_set_value_can_contain_spaces() -> None:
    wf = parse(
        "NAME w\nINITIAL s\n"
        "ACTION a\n  SET label A multi word value\n"
        "STATE s TERMINAL\n"
    )
    assert wf["actions"]["a"]["fields"] == {"label": "A multi word value"}


def test_dollar_prefix_means_reference() -> None:
    wf = parse(
        "NAME w\nINITIAL s\n"
        "ACTION a\n  SET copy $context.thing\n"
        "STATE s TERMINAL\n"
    )
    assert wf["actions"]["a"]["fields"]["copy"] == {"ref": "context.thing"}


# ---- comments and whitespace ------------------------------------------------


def test_comments_and_blank_lines_ignored() -> None:
    wf = parse(
        "# header\n\n"
        "NAME w  # trailing comment\n"
        "\n"
        "INITIAL s\n"
        "ACTION a\n  SET k v\n"
        "STATE s TERMINAL\n"
    )
    assert wf["name"] == "w"


# ---- structural errors -----------------------------------------------------


def test_missing_name() -> None:
    with pytest.raises(ParseError, match="NAME is required"):
        parse("INITIAL s\nSTATE s TERMINAL\n")


def test_missing_initial() -> None:
    with pytest.raises(ParseError, match="INITIAL is required"):
        parse("NAME w\nSTATE s TERMINAL\n")


def test_initial_must_exist() -> None:
    with pytest.raises(ParseError, match="INITIAL state 'ghost'"):
        parse("NAME w\nINITIAL ghost\nSTATE s TERMINAL\n")


def test_event_target_must_exist() -> None:
    with pytest.raises(ParseError, match="transitions to undefined state 'ghost'"):
        parse(
            "NAME w\nINITIAL s\n"
            "ACTION a\n  SET k v\n"
            "STATE s\n  EVENT next ghost\n"
        )


def test_action_reference_must_exist() -> None:
    with pytest.raises(ParseError, match="undefined action 'ghost'"):
        parse(
            "NAME w\nINITIAL s\n"
            "ACTION real\n  SET k v\n"
            "STATE s\n  ENTER\n    ACTION ghost\n  EVENT done end\n"
            "STATE end TERMINAL\n"
        )


def test_action_with_no_operation_fails() -> None:
    with pytest.raises(ParseError, match="ACTION 'empty' has no operation"):
        parse("NAME w\nINITIAL s\n" "ACTION empty\n" "STATE s TERMINAL\n")


def test_mixed_op_types_in_action_fails() -> None:
    with pytest.raises(ParseError, match="cannot add EMIT"):
        parse(
            "NAME w\nINITIAL s\n"
            "ACTION mix\n"
            "  SET k v\n"
            "  EMIT a b\n"
            "STATE s TERMINAL\n"
        )


def test_payload_outside_emit_fails() -> None:
    with pytest.raises(ParseError, match="PAYLOAD is only valid inside an EMIT"):
        parse(
            "NAME w\nINITIAL s\n"
            "ACTION mix\n"
            "  SET k v\n"
            "  PAYLOAD x y\n"
            "STATE s TERMINAL\n"
        )


def test_enter_outside_state_fails() -> None:
    with pytest.raises(ParseError, match="ENTER outside STATE block"):
        parse("NAME w\nINITIAL s\nENTER\nSTATE s TERMINAL\n")


def test_terminal_state_cannot_have_events() -> None:
    with pytest.raises(ParseError, match="EVENT not allowed in TERMINAL state"):
        parse("NAME w\nINITIAL s\n" "STATE s TERMINAL\n" "  EVENT click x\n")


def test_duplicate_state_fails() -> None:
    with pytest.raises(ParseError, match="STATE 's' already defined"):
        parse(
            "NAME w\nINITIAL s\n"
            "ACTION a\n  SET k v\n"
            "STATE s\n  EVENT done end\n"
            "STATE end TERMINAL\n"
            "STATE s TERMINAL\n"
        )


def test_duplicate_action_fails() -> None:
    with pytest.raises(ParseError, match="ACTION 'dup' already defined"):
        parse(
            "NAME w\nINITIAL s\n"
            "ACTION dup\n  SET k v\n"
            "ACTION dup\n  SET k v\n"
            "STATE s TERMINAL\n"
        )


def test_bad_ref_root_fails() -> None:
    with pytest.raises(ParseError, match="must start with"):
        parse("NAME w\nINITIAL s\n" "ACTION a\n  SET k $other.x\n" "STATE s TERMINAL\n")


def test_unknown_directive_fails() -> None:
    with pytest.raises(ParseError, match="unknown directive 'WAT'"):
        parse("NAME w\nWAT\n")


def test_error_carries_line_number() -> None:
    with pytest.raises(ParseError) as exc_info:
        parse("NAME w\n\n# blank above\nWAT\n")
    assert exc_info.value.line_no == 4


# ---- DEFAULT ----------------------------------------------------------------


def test_default_with_bare_transition() -> None:
    wf = parse(
        "NAME w\nINITIAL s\n"
        "ACTION a\n  SET k v\n"
        "DEFAULT error end\n"
        "STATE s\n  EVENT done end\n"
        "STATE end TERMINAL\n"
    )
    assert wf["defaults"]["error"] == {"do": [], "goto": "end"}


def test_default_with_actions_and_transition() -> None:
    wf = parse(
        "NAME w\nINITIAL s\n"
        "ACTION log-it\n  LOG dead\n"
        "DEFAULT error end\n"
        "  ACTION log-it\n"
        "STATE s\n  EVENT done end\n"
        "STATE end TERMINAL\n"
    )
    assert wf["defaults"]["error"] == {"do": ["log-it"], "goto": "end"}


# ---- inline LOG (no named ACTION) -------------------------------------------


def test_inline_log_under_event() -> None:
    """A bare LOG under EVENT becomes an inline log action in the do-list."""
    wf = parse(
        "NAME w\nINITIAL s\n"
        "STATE s\n"
        "  EVENT go end\n"
        "    LOG handling $event.data.id\n"
        "STATE end TERMINAL\n"
    )
    assert wf["states"]["s"]["events"]["go"] == {
        "do": [{"type": "log", "message": "handling $event.data.id"}],
        "goto": "end",
    }


def test_inline_log_under_default() -> None:
    wf = parse(
        "NAME w\nINITIAL s\n"
        "DEFAULT error end\n"
        "  LOG unhandled $event.name\n"
        "STATE s\n  EVENT go end\n"
        "STATE end TERMINAL\n"
    )
    assert wf["defaults"]["error"] == {
        "do": [{"type": "log", "message": "unhandled $event.name"}],
        "goto": "end",
    }


def test_inline_log_mixes_with_action_refs_in_order() -> None:
    wf = parse(
        "NAME w\nINITIAL s\n"
        "ACTION fan\n  EMIT out go\n"
        "STATE s\n"
        "  EVENT go s\n"
        "    ACTION fan\n"
        "    LOG fanned out\n"
        "STATE end TERMINAL\n"
    )
    assert wf["states"]["s"]["events"]["go"]["do"] == [
        "fan",
        {"type": "log", "message": "fanned out"},
    ]


def test_inline_log_without_transition() -> None:
    """No next-state is fine — the handler just logs and stays put."""
    wf = parse(
        "NAME w\nINITIAL s\n"
        "STATE s\n"
        "  EVENT ping\n"
        "    LOG pinged\n"
        "STATE end TERMINAL\n"
    )
    assert wf["states"]["s"]["events"]["ping"] == {
        "do": [{"type": "log", "message": "pinged"}],
        "goto": None,
    }


def test_inline_log_under_enter_is_rejected() -> None:
    with pytest.raises(ParseError, match="inline LOG is only allowed under EVENT"):
        parse(
            "NAME w\nINITIAL s\n"
            "STATE s\n"
            "  ENTER\n"
            "    LOG starting\n"
            "STATE end TERMINAL\n"
        )

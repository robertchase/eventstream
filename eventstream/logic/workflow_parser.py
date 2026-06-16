"""Parse the ``.flow`` workflow DSL into an AST dict.

See ``design/workflow-format.md`` for the language spec. This module is
purely syntactic + structural: it builds the AST and runs cross-reference
validation, but the runtime FSM engine lives elsewhere.

Functions in this module are registered as meander HTTP handlers. Do **not**
add ``from __future__ import annotations`` — see ``logic/streams.py`` for
why.
"""

import re

_DURATION_RE = re.compile(r"^(\d+)([smhd])$")
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_VALID_REF_ROOTS = ("job", "context", "event")


class ParseError(Exception):
    """A malformed ``.flow`` file. Carries the offending line number."""

    def __init__(self, line_no: int, message: str) -> None:
        prefix = f"line {line_no}: " if line_no else ""
        super().__init__(f"{prefix}{message}")
        self.line_no = line_no
        self.message = message


def parse(text: str) -> dict:
    """Convert ``.flow`` text into a validated workflow AST."""
    workflow = _build(text)
    validate(workflow)
    return workflow


def validate(workflow: dict) -> None:
    """Walk the AST and raise :class:`ParseError` on any referential issue."""
    errors: list[str] = []

    if not workflow["name"]:
        errors.append("NAME is required")
    if not workflow["initial"]:
        errors.append("INITIAL is required")
    elif workflow["initial"] not in workflow["states"]:
        errors.append(f"INITIAL state {workflow['initial']!r} is not defined")
    if not workflow["states"]:
        errors.append("at least one STATE is required")

    actions = workflow["actions"]
    states = workflow["states"]

    for state_name, state in states.items():
        if state.get("terminal"):
            continue
        for ref in state.get("enter", []):
            if isinstance(ref, str) and ref not in actions:
                errors.append(
                    f"STATE {state_name!r} ENTER references "
                    f"undefined action {ref!r}"
                )
        for ref in state.get("exit", []):
            if isinstance(ref, str) and ref not in actions:
                errors.append(
                    f"STATE {state_name!r} EXIT references " f"undefined action {ref!r}"
                )
        for event_name, handler in state.get("events", {}).items():
            target = handler.get("goto")
            if target is not None and target not in states:
                errors.append(
                    f"STATE {state_name!r} EVENT {event_name!r} "
                    f"transitions to undefined state {target!r}"
                )
            for ref in handler["do"]:
                if isinstance(ref, str) and ref not in actions:
                    errors.append(
                        f"STATE {state_name!r} EVENT {event_name!r} "
                        f"references undefined action {ref!r}"
                    )

    for event_name, handler in workflow["defaults"].items():
        target = handler.get("goto")
        if target is not None and target not in states:
            errors.append(
                f"DEFAULT {event_name!r} transitions to " f"undefined state {target!r}"
            )
        for ref in handler["do"]:
            if isinstance(ref, str) and ref not in actions:
                errors.append(
                    f"DEFAULT {event_name!r} references " f"undefined action {ref!r}"
                )

    for name, action in actions.items():
        if action.get("type") is None:
            errors.append(
                f"ACTION {name!r} has no operation " f"(EMIT / SET / LOG / TIMER)"
            )
            continue
        for path in _refs_in_action(action):
            root = path.split(".", 1)[0]
            if root not in _VALID_REF_ROOTS:
                errors.append(
                    f"ACTION {name!r} reference '${path}': "
                    f"path must start with $job. / $context. / $event."
                )

    if errors:
        raise ParseError(0, "; ".join(errors))


def _build(text: str) -> dict:
    """Lex line-by-line and assemble the AST. No cross-checks here."""
    workflow: dict = {
        "name": None,
        "description": None,
        "initial": None,
        "defaults": {},
        "actions": {},
        "states": {},
    }
    cursor = _Cursor()

    for line_no, raw in enumerate(text.splitlines(), 1):
        stripped = raw.split("#", 1)[0].rstrip()
        if not stripped.strip():
            continue
        tokens = stripped.split()
        directive = tokens[0]
        _dispatch(workflow, cursor, line_no, directive, tokens, stripped)

    return workflow


class _Cursor:
    """Tracks which block the parser is currently inside."""

    def __init__(self) -> None:
        self.state: str | None = None
        # Most-recently-seen STATE name (including terminal ones); used for
        # better error messages when ENTER/EXIT/EVENT lands in a terminal:
        self.last_state: str | None = None
        # The list to append actions to inside ENTER/EXIT/EVENT/DEFAULT —
        # ACTION-name refs plus any inline LOG action dicts:
        self.handler_do: list | None = None
        # The action dict currently being defined at top level:
        self.action: dict | None = None
        # True once any STATE or DEFAULT has been seen; after that, any new
        # top-level ACTION (a definition, not a reference) is an error:
        self.structure_seen: bool = False

    def close_all(self) -> None:
        self.state = None
        self.handler_do = None
        self.action = None


def _dispatch(
    workflow: dict,
    cur: _Cursor,
    line_no: int,
    directive: str,
    tokens: list[str],
    full_line: str,
) -> None:
    """Dispatch one parsed line to the right handler."""
    # ACTION is special: inside a state handler scope it's a reference,
    # otherwise it's a top-level definition.
    if directive == "ACTION" and cur.handler_do is not None:
        _do_action_ref(cur, line_no, tokens)
        return

    if directive == "NAME":
        _expect_arg_count(line_no, tokens, 2, "NAME")
        cur.close_all()
        if workflow["name"] is not None:
            raise ParseError(line_no, "NAME already set")
        workflow["name"] = tokens[1]
    elif directive == "INITIAL":
        _expect_arg_count(line_no, tokens, 2, "INITIAL")
        cur.close_all()
        if workflow["initial"] is not None:
            raise ParseError(line_no, "INITIAL already set")
        workflow["initial"] = tokens[1]
    elif directive == "DESCRIPTION":
        cur.close_all()
        if workflow["description"] is not None:
            raise ParseError(line_no, "DESCRIPTION already set")
        workflow["description"] = _rest_of_line(full_line, after=1)
    elif directive == "STATE":
        cur.close_all()
        cur.structure_seen = True
        _do_state(workflow, cur, line_no, tokens)
    elif directive == "ACTION":
        if cur.structure_seen:
            raise ParseError(
                line_no,
                "ACTION definitions must precede STATE and DEFAULT directives",
            )
        cur.close_all()
        _do_action_def(workflow, cur, line_no, tokens)
    elif directive == "DEFAULT":
        cur.close_all()
        cur.structure_seen = True
        _do_default(workflow, cur, line_no, tokens)
    elif directive == "ENTER":
        _expect_arg_count(line_no, tokens, 1, "ENTER")
        _do_enter_exit(workflow, cur, line_no, "enter")
    elif directive == "EXIT":
        _expect_arg_count(line_no, tokens, 1, "EXIT")
        _do_enter_exit(workflow, cur, line_no, "exit")
    elif directive == "EVENT":
        _do_event(workflow, cur, line_no, tokens)
    elif directive == "EMIT":
        _do_emit(cur, line_no, tokens)
    elif directive == "SET":
        _do_set(cur, line_no, tokens, full_line)
    elif directive == "LOG":
        _do_log(cur, line_no, tokens, full_line)
    elif directive == "TIMER":
        _do_timer(cur, line_no, tokens)
    elif directive == "PAYLOAD":
        _do_payload(cur, line_no, tokens, full_line)
    else:
        raise ParseError(line_no, f"unknown directive {directive!r}")


def _do_state(workflow, cur, line_no, tokens):
    if len(tokens) < 2:
        raise ParseError(line_no, "STATE requires a name")
    name = tokens[1]
    terminal = False
    if len(tokens) == 3:
        if tokens[2] != "TERMINAL":
            raise ParseError(
                line_no, f"unexpected token {tokens[2]!r} after STATE name"
            )
        terminal = True
    elif len(tokens) > 3:
        raise ParseError(line_no, "extra tokens on STATE line")
    if name in workflow["states"]:
        raise ParseError(line_no, f"STATE {name!r} already defined")
    cur.last_state = name
    if terminal:
        workflow["states"][name] = {"terminal": True}
        cur.state = None
    else:
        workflow["states"][name] = {"enter": [], "exit": [], "events": {}}
        cur.state = name


def _do_action_def(workflow, cur, line_no, tokens):
    if len(tokens) != 2:
        raise ParseError(line_no, "ACTION definition takes one name")
    name = tokens[1]
    if name in workflow["actions"]:
        raise ParseError(line_no, f"ACTION {name!r} already defined")
    cur.action = {}
    workflow["actions"][name] = cur.action


def _do_action_ref(cur, line_no, tokens):
    if len(tokens) != 2:
        raise ParseError(line_no, "ACTION reference takes one name")
    cur.handler_do.append(tokens[1])


def _do_default(workflow, cur, line_no, tokens):
    if len(tokens) < 2 or len(tokens) > 3:
        raise ParseError(line_no, "DEFAULT requires <event> [<next-state>]")
    event_name = tokens[1]
    if event_name in workflow["defaults"]:
        raise ParseError(line_no, f"DEFAULT {event_name!r} already defined")
    goto = tokens[2] if len(tokens) == 3 else None
    handler = {"do": [], "goto": goto}
    workflow["defaults"][event_name] = handler
    cur.handler_do = handler["do"]
    cur.action = None


def _do_enter_exit(workflow, cur, line_no, kind):
    if cur.state is None:
        if cur.last_state and workflow["states"][cur.last_state].get("terminal"):
            raise ParseError(line_no, f"{kind.upper()} not allowed in TERMINAL state")
        raise ParseError(line_no, f"{kind.upper()} outside STATE block")
    cur.handler_do = workflow["states"][cur.state][kind]
    cur.action = None


def _do_event(workflow, cur, line_no, tokens):
    if cur.state is None:
        if cur.last_state and workflow["states"][cur.last_state].get("terminal"):
            raise ParseError(line_no, "EVENT not allowed in TERMINAL state")
        raise ParseError(line_no, "EVENT outside STATE block")
    if len(tokens) < 2 or len(tokens) > 3:
        raise ParseError(line_no, "EVENT requires <name> [<next-state>]")
    state = workflow["states"][cur.state]
    name = tokens[1]
    if name in state["events"]:
        raise ParseError(
            line_no, f"EVENT {name!r} already defined in STATE {cur.state!r}"
        )
    goto = tokens[2] if len(tokens) == 3 else None
    handler = {"do": [], "goto": goto}
    state["events"][name] = handler
    cur.handler_do = handler["do"]
    cur.action = None


def _do_emit(cur, line_no, tokens):
    action = _require_action(cur, line_no, "EMIT")
    if action.get("type") is not None:
        raise ParseError(
            line_no, f"ACTION already has type {action['type']!r}; cannot add EMIT"
        )
    if len(tokens) != 3:
        raise ParseError(line_no, "EMIT requires <stream> <event>")
    action["type"] = "emit"
    action["stream"] = tokens[1]
    action["event"] = tokens[2]
    action["payload"] = {}


def _do_set(cur, line_no, tokens, full_line):
    action = _require_action(cur, line_no, "SET")
    existing = action.get("type")
    if existing is None:
        action["type"] = "set"
        action["fields"] = {}
    elif existing != "set":
        raise ParseError(line_no, f"SET in {existing!r} action")
    if len(tokens) < 3:
        raise ParseError(line_no, "SET requires <key> <value>")
    key = tokens[1]
    value = _rest_of_line(full_line, after=2)
    action["fields"][key] = _parse_value(value)


def _do_log(cur, line_no, tokens, full_line):
    if len(tokens) < 2:
        raise ParseError(line_no, "LOG requires a message")
    message = _rest_of_line(full_line, after=1)

    # Inline LOG: a bare LOG line directly inside any handler block
    # (ENTER/EXIT/EVENT/DEFAULT), with no named ACTION. It becomes an
    # anonymous log action in the handler's do-list. Other ops
    # (EMIT/SET/TIMER) still require an ACTION.
    if cur.action is None and cur.handler_do is not None:
        cur.handler_do.append({"type": "log", "message": message})
        return

    action = _require_action(cur, line_no, "LOG")
    if action.get("type") is not None:
        raise ParseError(
            line_no, f"ACTION already has type {action['type']!r}; cannot add LOG"
        )
    action["type"] = "log"
    action["message"] = message


def _do_timer(cur, line_no, tokens):
    action = _require_action(cur, line_no, "TIMER")
    if action.get("type") is not None:
        raise ParseError(
            line_no, f"ACTION already has type {action['type']!r}; cannot add TIMER"
        )
    if len(tokens) != 3:
        raise ParseError(line_no, "TIMER requires <duration> <event>")
    action["type"] = "timer"
    action["delay_seconds"] = _parse_duration(line_no, tokens[1])
    action["event"] = tokens[2]


def _do_payload(cur, line_no, tokens, full_line):
    action = _require_action(cur, line_no, "PAYLOAD")
    if action.get("type") != "emit":
        raise ParseError(line_no, "PAYLOAD is only valid inside an EMIT action")
    if len(tokens) < 3:
        raise ParseError(line_no, "PAYLOAD requires <key> <value>")
    key = tokens[1]
    value = _rest_of_line(full_line, after=2)
    action["payload"][key] = _parse_value(value)


def _require_action(cur, line_no, directive):
    if cur.action is None:
        raise ParseError(line_no, f"{directive} outside ACTION block")
    return cur.action


def _expect_arg_count(line_no, tokens, expected, directive):
    if len(tokens) != expected:
        raise ParseError(line_no, f"{directive} expects {expected - 1} argument(s)")


def _rest_of_line(line: str, *, after: int) -> str:
    """Return everything after the first ``after`` whitespace tokens."""
    parts = line.split(maxsplit=after)
    return parts[after].strip() if len(parts) > after else ""


def _parse_value(value: str):
    """Return ``{"ref": path}`` for ``$``-prefixed values; otherwise literal."""
    if value.startswith("$"):
        return {"ref": value[1:]}
    return value


def _parse_duration(line_no: int, dur: str) -> int:
    match = _DURATION_RE.match(dur)
    if not match:
        raise ParseError(line_no, f"bad duration {dur!r}; expected NUMBER+s/m/h/d")
    return int(match.group(1)) * _UNIT_SECONDS[match.group(2)]


def _refs_in_action(action: dict):
    """Yield every reference path used by an action's payload/fields/message."""
    if action["type"] == "emit":
        for v in action["payload"].values():
            if isinstance(v, dict) and "ref" in v:
                yield v["ref"]
    elif action["type"] == "set":
        for v in action["fields"].values():
            if isinstance(v, dict) and "ref" in v:
                yield v["ref"]
    # LOG and TIMER references would be in the message/event name; deferred.

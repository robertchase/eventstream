"""Pure FSM engine for workflow execution.

Takes a workflow AST + current state + context + a triggering event and
walks the FSM until it quiesces or reaches a terminal state. All side
effects (emitted events, scheduled timers, log lines, recorded transitions)
are collected on a :class:`Recorder` that the persistence layer flushes
afterward. No I/O happens here.

Evaluation semantics (per ``design/workflow-format.md``):

* Run the matched handler's action sequence in order. Track the *carry*
  event — the carry equals whatever the last carrying action returned. EMIT
  returns ``{"name", "data"}``; SET / TIMER return ``None`` (clearing it).
  LOG is transparent: it runs for its side effect but leaves the carry
  alone, so a log line anywhere in the sequence never changes which event
  cascades next.
* On transition, run ``S.exit``, switch state, run ``T.enter``. The carry
  is updated by every non-LOG action that ran, so it ends up being the last
  such action's emit across the whole ``do → exit → enter`` chain.
* If the carry is not ``None``, process it as the next event (cascade).
* No handler for the event → quiesce.
* Per-trigger cascade budget caps internal loops.

Functions in this module are registered as meander HTTP handlers. Do **not**
add ``from __future__ import annotations`` — see ``logic/streams.py`` for
why.
"""

import logging
import re

CASCADE_LIMIT = 100

_REF_RE = re.compile(r"\$([a-zA-Z_][a-zA-Z_0-9-]*(?:\.[a-zA-Z_][a-zA-Z_0-9-]*)*)")

_log = logging.getLogger("eventstream.jobs")


class EngineError(Exception):
    """Base for engine-layer errors."""


class MissingReference(EngineError):
    """A ``$``-reference could not be resolved against the current scope."""


class CascadeBudgetExceeded(EngineError):
    """Too many internal events triggered by one external event."""


class Recorder:
    """Collects side effects during one engine run.

    Logs flow to ``eventstream.jobs`` immediately (with job_id correlation);
    emits, timers, and transitions accumulate for the persistence layer to
    flush atomically with the new job state.

    ``journal`` is the ordered, mixed record of transitions and log lines as
    they happened during the run; the persistence layer writes it to the
    job's durable history so ``LOG`` output survives the step. ``logs`` and
    ``transitions`` remain as filtered views for callers that want one kind.
    """

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        self.emits: list[dict] = []
        self.timers: list[dict] = []
        self.logs: list[str] = []
        self.transitions: list[dict] = []
        self.journal: list[dict] = []

    def emit(self, stream: str, event_type: str, payload: dict) -> dict:
        """Record an EMIT side effect and return its carry shape."""
        self.emits.append(
            {"stream": stream, "event_type": event_type, "payload": payload}
        )
        return {"name": event_type, "data": payload}

    def timer(self, event: str, delay_seconds: int) -> None:
        self.timers.append({"event": event, "delay_seconds": delay_seconds})

    def log(self, message: str, *, state: str | None = None) -> None:
        self.logs.append(message)
        self.journal.append({"kind": "log", "message": message, "state": state})
        _log.info("[job=%s] %s", self.job_id, message)

    def transition(self, from_state: str, event_name: str, to_state: str) -> None:
        entry = {"from": from_state, "event": event_name, "to": to_state}
        self.transitions.append(entry)
        self.journal.append({"kind": "transition", **entry})


def step(
    ast: dict,
    state: str,
    context: dict,
    trigger_event: dict,
    *,
    recorder: Recorder,
    job_meta: dict | None = None,
) -> str:
    """Process one trigger event, cascading until quiesce or terminal.

    ``context`` is mutated in place by ``SET`` actions. Returns the final
    state name. Raises :class:`CascadeBudgetExceeded` if internal events
    fail to converge.

    ``job_meta`` carries the static ``$job`` fields (``workflow``,
    ``version``, ``now``); ``id`` falls back to the recorder's job id and
    ``state`` is supplied per action from the state it runs in. Omit it for
    direct engine use where only ``$job.id`` is referenced.
    """
    meta = job_meta or {}
    event = trigger_event
    cascades = 0

    while True:
        cascades += 1
        if cascades > CASCADE_LIMIT:
            raise CascadeBudgetExceeded(
                f"cascade budget {CASCADE_LIMIT} exceeded; "
                f"last event was {event['name']!r}"
            )

        handler = _find_handler(ast, state, event["name"])
        if handler is None:
            return state

        carry = _run_actions(
            handler["do"],
            ast,
            context,
            event,
            recorder,
            initial=None,
            job=_job_scope(recorder, meta, state),
        )
        target = handler.get("goto")

        if target:
            recorder.transition(state, event["name"], target)
            carry = _run_actions(
                ast["states"][state].get("exit", []),
                ast,
                context,
                event,
                recorder,
                initial=carry,
                job=_job_scope(recorder, meta, state),
            )
            state = target
            carry = _run_actions(
                ast["states"][state].get("enter", []),
                ast,
                context,
                event,
                recorder,
                initial=carry,
                job=_job_scope(recorder, meta, state),
            )
            if ast["states"][state].get("terminal"):
                return state

        if carry is None:
            return state

        event = carry


def _job_scope(recorder: Recorder, meta: dict, state: str) -> dict:
    """Build the ``$job`` reference scope for actions running in ``state``.

    ``id`` defaults to the recorder's job id so direct engine use still
    resolves ``$job.id``; ``meta`` (workflow, version, now) overlays the
    rest; ``state`` reflects the state the action executes in (source state
    for ``do``/``exit``, target state for ``enter``).
    """
    return {"id": recorder.job_id, **meta, "state": state}


def _find_handler(ast: dict, state: str, event_name: str) -> dict | None:
    """Return the handler for ``event_name`` in ``state``, falling back to DEFAULT."""
    state_def = ast["states"][state]
    if state_def.get("terminal"):
        return None
    events = state_def.get("events", {})
    if event_name in events:
        return events[event_name]
    return ast["defaults"].get(event_name)


def _run_actions(
    refs: list,
    ast: dict,
    context: dict,
    event: dict,
    recorder: Recorder,
    *,
    initial,
    job: dict,
):
    """Execute a list of action refs in order; return the carrying result.

    The carry rule: only the last action's return matters — except ``LOG``,
    which is transparent. A ``LOG`` runs for its side effect but leaves the
    carry untouched, so it can be dropped anywhere in a sequence (including
    last) without changing which event cascades next. ``EMIT`` sets the
    carry; ``SET``/``TIMER`` clear it (return ``None``) when they run last.
    """
    carry = initial
    for ref in refs:
        action = ast["actions"][ref]
        result = _execute_action(action, ast, context, event, recorder, job)
        if action["type"] != "log":
            carry = result
    return carry


def _execute_action(
    action: dict, ast: dict, context: dict, event: dict, recorder: Recorder, job: dict
):
    """Execute one action against the ``job``/``context``/``event`` scopes.

    Returns its carry (EMIT) or ``None`` (SET/LOG/TIMER). ``job`` is the
    ``$job`` reference scope built by :func:`_job_scope`.
    """
    op = action["type"]

    if op == "emit":
        payload = {
            k: _resolve(v, job, context, event) for k, v in action["payload"].items()
        }
        return recorder.emit(action["stream"], action["event"], payload)

    if op == "set":
        for k, v in action["fields"].items():
            context[k] = _resolve(v, job, context, event)
        return None

    if op == "log":
        recorder.log(
            _interpolate(action["message"], job, context, event),
            state=job.get("state"),
        )
        return None

    if op == "timer":
        recorder.timer(action["event"], action["delay_seconds"])
        return None

    raise EngineError(f"unknown action type {op!r}")


def _resolve(value, job: dict, context: dict, event: dict):
    """Resolve a literal or ``{ref: path}`` tagged value against the scope."""
    if isinstance(value, dict) and "ref" in value:
        return _resolve_path(value["ref"], job, context, event)
    return value


def _resolve_path(path: str, job: dict, context: dict, event: dict):
    """Walk a dotted path like ``context.order.total`` to a concrete value."""
    parts = path.split(".")
    root = parts[0]
    if root == "job":
        obj = job
    elif root == "context":
        obj = context
    elif root == "event":
        obj = event
    else:
        raise MissingReference(f"unknown reference root: ${path}")

    for p in parts[1:]:
        if not isinstance(obj, dict) or p not in obj:
            raise MissingReference(f"missing path: ${path}")
        obj = obj[p]
    return obj


def _interpolate(message: str, job: dict, context: dict, event: dict) -> str:
    """Replace ``$word(.word)*`` refs in a message string with resolved values."""

    def sub(m):
        try:
            return str(_resolve_path(m.group(1), job, context, event))
        except MissingReference:
            return m.group(0)

    return _REF_RE.sub(sub, message)

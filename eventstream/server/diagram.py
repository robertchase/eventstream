"""Generate nomnoml diagram source from a workflow AST.

The output is text in the `nomnoml <https://nomnoml.com>`_ DSL; the browser
renders it to SVG with the vendored ``nomnoml.js`` (see ``static/diagram.js``).
Presentation-only — nothing here feeds back into the engine.
"""


def to_nomnoml(ast: dict, current_state: str | None = None) -> str:
    """Return nomnoml source describing the workflow's state graph.

    States become nodes (terminals get the ``terminal`` style), events with
    a ``goto`` become labeled directed edges, events without a ``goto``
    become labeled self-loops, and ``DEFAULT`` handlers with a target are
    drawn as dashed edges from a single pseudo-node.

    ``current_state`` highlights one state — the job page passes a running
    job's state so the diagram shows where the job sits. A terminal
    ``current_state`` keeps its terminal style (first declaration wins).
    """
    lines = [
        "#direction: down",
        "#stroke: #444444",
        "#fill: #ffffff",
        "#fontSize: 11",
        "#spacing: 56",
        "#padding: 8",
        "#.terminal: fill=#e8e8e8 visual=roundrect",
        "#.current: fill=#ffe680 bold",
    ]
    # Styled declarations come first: nomnoml styles a node from its
    # FIRST appearance, so a plain edge reference would lock in the
    # default look before the <terminal>/<current> declaration is seen.
    for name, state in ast["states"].items():
        if state.get("terminal"):
            lines.append(f"[<terminal> {name}]")
    if current_state and not ast["states"].get(current_state, {}).get("terminal"):
        lines.append(f"[<current> {current_state}]")
    lines.append(f"[<start> start] -> [{ast['initial']}]")
    for name, state in ast["states"].items():
        if state.get("terminal"):
            continue
        for event, handler in state.get("events", {}).items():
            target = handler.get("goto") or name
            # Label at the TARGET end of the edge: edges fanning out of one
            # state then carry their labels near distinct targets instead of
            # overlapping at the shared source.
            lines.append(f"[{name}] -> {event} [{target}]")
    for event, handler in ast.get("defaults", {}).items():
        if handler.get("goto"):
            lines.append(f"[<note> DEFAULT] --> {event} [{handler['goto']}]")
    return "\n".join(lines)

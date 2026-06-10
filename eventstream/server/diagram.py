"""Generate nomnoml diagram source from a workflow AST.

The output is text in the `nomnoml <https://nomnoml.com>`_ DSL; the browser
renders it to SVG with the vendored ``nomnoml.js`` (see ``static/diagram.js``).
Presentation-only — nothing here feeds back into the engine.

nomnoml has no mid-edge labels (only start/end labels, drawn flush against
the node boxes, which clip and collide on fan-in and back-edges). So each
event is reified as a small *label node* on the edge path::

    [off] - [start]          # plain line into the label pill
    [start] -> [starting]    # arrow from the pill to the target

nomnoml merges nodes by name, which would conflate repeated event names
(and an event named like a state). Every generated pseudo-node id is
therefore disambiguated with invisible zero-width spaces (U+200B): label
nodes get ``event + ZWSP*(n+1)`` and fixed pseudo-nodes get a ZWSP prefix.
The suffixes render as nothing, so duplicate events all *display* the same
name while remaining distinct nodes.
"""

_ZWSP = "​"
_START = f"{_ZWSP}start"
_DEFAULT = f"{_ZWSP}DEFAULT"


def to_nomnoml(ast: dict, current_state: str | None = None) -> str:
    """Return nomnoml source describing the workflow's state graph.

    States become nodes (terminals get the ``terminal`` style), and each
    event becomes a label node spliced into its transition edge. Events
    without a ``goto`` loop back to their own state. ``DEFAULT`` handlers
    with a target are drawn dashed from a single note pseudo-node.

    ``current_state`` highlights one state — the job page passes a running
    job's state so the diagram shows where the job sits. A terminal
    ``current_state`` keeps its terminal style (first declaration wins).
    """
    lines = [
        "#direction: down",
        "#stroke: #444444",
        "#fill: #ffffff",
        "#fontSize: 11",
        "#spacing: 40",
        "#padding: 8",
        "#.terminal: fill=#e8e8e8 visual=roundrect",
        "#.current: fill=#ffe680 bold",
        "#.ev: fill=#fcfcfc stroke=#999999 visual=roundrect italic",
    ]

    seen: dict[str, int] = {}

    def event_node(event: str) -> str:
        """Unique-but-identical-looking node id for one event occurrence."""
        n = seen.get(event, 0)
        seen[event] = n + 1
        return f"{event}{_ZWSP * (n + 1)}"

    # Styled declarations come first: nomnoml styles a node from its
    # FIRST appearance, so a plain edge reference would lock in the
    # default look before the <terminal>/<current> declaration is seen.
    for name, state in ast["states"].items():
        if state.get("terminal"):
            lines.append(f"[<terminal> {name}]")
    if current_state and not ast["states"].get(current_state, {}).get("terminal"):
        lines.append(f"[<current> {current_state}]")

    lines.append(f"[<start> {_START}] -> [{ast['initial']}]")
    for name, state in ast["states"].items():
        if state.get("terminal"):
            continue
        for event, handler in state.get("events", {}).items():
            target = handler.get("goto") or name
            ev = event_node(event)
            lines.append(f"[{name}] - [<ev> {ev}]")
            lines.append(f"[<ev> {ev}] -> [{target}]")
    for event, handler in ast.get("defaults", {}).items():
        if handler.get("goto"):
            ev = event_node(event)
            lines.append(f"[<note> {_DEFAULT}] -- [<ev> {ev}]")
            lines.append(f"[<ev> {ev}] --> [{handler['goto']}]")
    return "\n".join(lines)

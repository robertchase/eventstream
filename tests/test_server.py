"""Server-layer sanity checks. No HTTP — just route-table introspection."""

import inspect

from eventstream.server import build


def test_registered_handlers_have_real_class_annotations() -> None:
    """Guard against ``from __future__ import annotations`` creeping into a
    handler module.

    meander's query-parameter type coercion compares ``param.annotation`` to
    real classes (``int``, ``bool``). The future import turns those into
    strings, which silently disables coercion — ``?count=10`` arrives as
    ``"10"``. There is no meander-side warning when this happens, so this
    test is the only thing that catches it.
    """
    server = build(port=0)
    failures: list[str] = []
    for route in server.router.routes:
        handler = inspect.unwrap(route.handler)
        for name, param in inspect.signature(handler).parameters.items():
            if param.annotation is param.empty:
                continue
            if isinstance(param.annotation, str):
                failures.append(
                    f"{handler.__module__}.{handler.__qualname__} "
                    f"param {name!r}: annotation is the string "
                    f"{param.annotation!r}"
                )
    assert not failures, (
        "Some registered handlers have string annotations. A handler "
        "module almost certainly has `from __future__ import annotations` "
        "set, which silently disables meander's type coercion. Affected:\n"
        + "\n".join(f"  {f}" for f in failures)
    )

"""Server-layer sanity checks. No HTTP — just route-table introspection."""

import inspect

import pytest

import eventstream.server as server_pkg
from eventstream import config as CONFIG
from eventstream.server import build, routes


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


def test_catalog_is_built_and_paths_are_prettified() -> None:
    """The /endpoints catalog is populated from real registration, with
    regex captures rendered as readable {param} names."""
    build(port=0)
    paths = {(r["method"], r["path"]) for r in routes.CATALOG}
    # Captures named from handler params, not left as regex.
    assert ("GET", "/v1/streams/{name}") in paths
    assert ("POST", "/v1/streams/{stream}/events") in paths
    assert ("POST", "/v1/subscriptions/{sub}/ack/{event_id}") in paths
    assert ("GET", "/endpoints") in paths
    # No raw regex leaked into any path.
    assert not any("(" in r["path"] for r in routes.CATALOG)
    # Scopes recorded for the API write routes.
    by_path = {(r["method"], r["path"]): r for r in routes.CATALOG}
    assert by_path[("POST", "/v1/streams/{stream}/events")]["scope"] == "write"
    assert by_path[("POST", "/v1/subscriptions")]["scope"] == "admin"


@pytest.fixture
def _stub_meander(monkeypatch: pytest.MonkeyPatch) -> list:
    """Stub out the blocking/build parts of ``run`` and record sweeper tasks."""
    tasks: list = []
    monkeypatch.setattr(server_pkg, "build", lambda **kw: None)
    monkeypatch.setattr(server_pkg.meander, "add_task", lambda fn: tasks.append(fn))
    monkeypatch.setattr(server_pkg.meander, "run", lambda: None)
    return tasks


def test_run_registers_sweeper_by_default(
    monkeypatch: pytest.MonkeyPatch, _stub_meander: list
) -> None:
    monkeypatch.setattr(CONFIG, "sweep_interval", 1.0)
    server_pkg.run()
    assert len(_stub_meander) == 1


def test_run_no_sweep_skips_sweeper(
    monkeypatch: pytest.MonkeyPatch, _stub_meander: list
) -> None:
    monkeypatch.setattr(CONFIG, "sweep_interval", 1.0)
    server_pkg.run(sweep=False)
    assert _stub_meander == []


def test_run_zero_interval_skips_sweeper(
    monkeypatch: pytest.MonkeyPatch, _stub_meander: list
) -> None:
    monkeypatch.setattr(CONFIG, "sweep_interval", 0.0)
    server_pkg.run(sweep=True)
    assert _stub_meander == []

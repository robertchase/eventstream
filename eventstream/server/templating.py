"""Jinja2 environment + a small helper for the fragment-or-page pattern."""

from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup

_TEMPLATES = Path(__file__).parent / "templates"

_env = Environment(
    loader=FileSystemLoader(_TEMPLATES),
    autoescape=select_autoescape(["html"]),
    trim_blocks=True,
    lstrip_blocks=True,
)


def render(name: str, **context: object) -> str:
    """Render the named template with the given context."""
    return _env.get_template(name).render(**context)


def render_page(content_name: str, *, fragment: bool, **context: object) -> str:
    """Render ``content_name``; wrap it in ``layout.html`` unless fragmenting.

    HTMX requests get just the inner fragment so the browser only swaps the
    refreshed panel. Direct visits get the full layout-wrapped page.
    """
    body = render(content_name, **context)
    if fragment:
        return body
    return render("layout.html", content=Markup(body), **context)

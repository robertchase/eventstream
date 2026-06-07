"""Static file serving for the bundled CSS and htmx.min.js.

Registered as a meander handler. Do **not** add ``from __future__ import
annotations`` — see ``logic/streams.py`` for why.
"""

import mimetypes
from pathlib import Path

import meander

_STATIC_DIR = (Path(__file__).parent / "static").resolve()


def serve_static(path: str) -> meander.Response:
    """Return the file at ``static/<path>`` with a guessed content type."""
    target = (_STATIC_DIR / path).resolve()
    if not _is_within(target, _STATIC_DIR) or not target.is_file():
        raise meander.HTTPException(404, "Not Found")
    content_type, _ = mimetypes.guess_type(str(target))
    # charset=None tells meander not to call .encode() on the body, so we can
    # serve binary files as-is. The Content-Type header just omits the charset
    # suffix, which is fine for CSS/JS in every modern browser.
    return meander.Response(
        content=target.read_bytes(),
        content_type=content_type or "application/octet-stream",
        charset=None,
    )


def _is_within(candidate: Path, parent: Path) -> bool:
    """True if ``candidate`` resolves inside ``parent`` — guards against ../."""
    try:
        candidate.relative_to(parent)
    except ValueError:
        return False
    return True

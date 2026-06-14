"""Bearer-token auth for the HTTP API.

:func:`require` builds a meander ``before`` hook that verifies the request's
``Authorization: Bearer`` token carries a given scope (see ``design/auth.md``).
The hook delegates to :func:`eventstream.logic.apikeys.verify`, which raises
``InvalidToken`` (→ 401) or ``InsufficientScope`` (→ 403); the server's
exception handler maps those to HTTP responses.

Do **not** add ``from __future__ import annotations`` — this module's hook is
async and registered with meander; keep it consistent with the handlers.
"""

from collections.abc import Callable

import meander

from eventstream.logic import apikeys


def require(scope: str) -> Callable:
    """Return a before-hook that requires a token with ``scope``."""

    async def hook(request: meander.Request) -> None:
        token = _bearer(request.http_headers)
        # Stash the principal as an attribute, NOT in request.content: meander
        # binds handler kwargs from content and rejects any key that isn't a
        # parameter, so an injected "principal" would 400 every handler that
        # takes arguments. Attributes are invisible to that binding.
        request.principal = await apikeys.verify(token, required=scope)

    return hook


def _bearer(headers: dict) -> str:
    """Extract the token from a case-insensitive ``Authorization: Bearer`` header."""
    value = next((v for k, v in headers.items() if k.lower() == "authorization"), None)
    if not value or not value.lower().startswith("bearer "):
        raise apikeys.InvalidToken("missing bearer token")
    return value[len("bearer ") :].strip()

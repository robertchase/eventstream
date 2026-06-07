"""meander ``before=`` hook.

meander already binds URL regex captures to handler parameters by position,
so no hook is needed for path arguments. The only request shaping we need is
exposing the ``HX-Request`` header to HTML handlers.
"""

from __future__ import annotations

import meander


def hx_check(request: meander.Request) -> None:
    """Set ``content['is_hx']`` from the case-insensitive ``HX-Request`` header."""
    is_hx = any(k.lower() == "hx-request" for k in request.http_headers)
    request.content = (request.content or {}) | {"is_hx": is_hx}

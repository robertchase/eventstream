"""API key (bearer token) management and verification.

See ``design/auth.md``. Tokens are ``es_<keyid>_<secret>``; only a SHA-256 of
the secret is stored, never the raw token. Issuance/revocation are
control-plane operations (CLI over direct Redis); :func:`verify` is called
from the server's auth before-hook on the HTTP data plane.

Storage::

    eventstream:apikeys              SET of keyids
    eventstream:apikey:<keyid>       HASH {hash, name, scopes, created_at,
                                           last_used_at}

Functions here are async logic, mockable at ``backend.client()``. Do **not**
add ``from __future__ import annotations`` — see ``logic/streams.py`` for why.
"""

import hashlib
import hmac
import secrets
import time

from eventstream.logic import backend
from eventstream.logic.exceptions import EventStreamError

_INDEX = "eventstream:apikeys"
VALID_SCOPES = ("read", "write", "admin")


class AuthError(EventStreamError):
    """Base class for authentication / authorization failures."""


class InvalidToken(AuthError):
    """The bearer token is missing, malformed, unknown, or revoked."""


class InsufficientScope(AuthError):
    """The token is valid but lacks the scope required for the route."""


class APIKeyNotFound(EventStreamError):
    """No API key with the given id (e.g. on revoke)."""


def _key(keyid: str) -> str:
    return f"eventstream:apikey:{keyid}"


def _hash(secret: str) -> str:
    """Fast hash for a high-entropy secret — see design/auth.md on why not bcrypt."""
    return hashlib.sha256(secret.encode()).hexdigest()


def _parse(token: str) -> tuple[str, str]:
    """Split ``es_<keyid>_<secret>`` into ``(keyid, secret)``.

    The secret may itself contain ``_`` (urlsafe base64), so split on the
    first underscore after the ``es_`` prefix — ``keyid`` is hex and has none.
    """
    if not token or not token.startswith("es_"):
        raise InvalidToken("malformed token")
    keyid, sep, secret = token[3:].partition("_")
    if not sep or not keyid or not secret:
        raise InvalidToken("malformed token")
    return keyid, secret


async def create(name: str, scopes: list[str]) -> dict:
    """Mint a new API key. Returns the record plus the one-time ``token``.

    The raw token is returned exactly once here; only its hash is stored.
    """
    if not scopes:
        raise ValueError("at least one scope is required")
    for scope in scopes:
        if scope not in VALID_SCOPES:
            raise ValueError(f"unknown scope {scope!r}; choose from {VALID_SCOPES}")

    keyid = secrets.token_hex(4)
    secret = secrets.token_urlsafe(32)
    now = int(time.time())
    await backend.client().hset(
        _key(keyid),
        mapping={
            "hash": _hash(secret),
            "name": name,
            "scopes": ",".join(scopes),
            "created_at": str(now),
            "last_used_at": "",
        },
    )
    await backend.client().sadd(_INDEX, keyid)
    return {
        "keyid": keyid,
        "name": name,
        "scopes": list(scopes),
        "created_at": now,
        "token": f"es_{keyid}_{secret}",
    }


async def list_() -> list[dict]:
    """List keys as metadata dicts — never the secret."""
    client = backend.client()
    keyids = sorted(await client.smembers(_INDEX))
    result = []
    for keyid in keyids:
        raw = await client.hgetall(_key(keyid))
        if not raw:
            continue
        result.append(
            {
                "keyid": keyid,
                "name": raw["name"],
                "scopes": raw["scopes"].split(",") if raw["scopes"] else [],
                "created_at": int(raw["created_at"]),
                "last_used_at": (
                    int(raw["last_used_at"]) if raw["last_used_at"] else None
                ),
            }
        )
    return result


async def revoke(keyid: str) -> None:
    """Delete a key by id. Effective immediately (verify checks Redis live)."""
    client = backend.client()
    removed = await client.delete(_key(keyid))
    await client.srem(_INDEX, keyid)
    if not removed:
        raise APIKeyNotFound(f"no API key with id {keyid!r}")


async def verify(token: str, *, required: str) -> str:
    """Verify a bearer token carries ``required`` scope; return its name.

    Raises :class:`InvalidToken` (→ 401) when the token is missing/bad and
    :class:`InsufficientScope` (→ 403) when it is valid but under-scoped.
    Touches ``last_used_at`` best-effort on success.
    """
    keyid, secret = _parse(token)
    client = backend.client()
    raw = await client.hgetall(_key(keyid))
    if not raw:
        raise InvalidToken("unknown or revoked token")
    if not hmac.compare_digest(raw["hash"], _hash(secret)):
        raise InvalidToken("bad token")
    scopes = raw["scopes"].split(",") if raw["scopes"] else []
    if required not in scopes:
        raise InsufficientScope(f"token lacks {required!r} scope")
    await client.hset(_key(keyid), "last_used_at", str(int(time.time())))
    return raw["name"]

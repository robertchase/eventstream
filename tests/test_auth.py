"""Micro-tests for API-key auth: key logic + the server before-hook."""

from __future__ import annotations

import pytest

from eventstream.logic import apikeys
from eventstream.server import auth


class _Req:
    """Minimal stand-in for a meander Request (headers + content)."""

    def __init__(self, headers: dict) -> None:
        self.http_headers = headers
        self.content: dict = {}


# ---- token format / parsing -------------------------------------------------


def test_parse_splits_keyid_and_secret() -> None:
    assert apikeys._parse("es_ab12cd34_thesecret") == ("ab12cd34", "thesecret")


def test_parse_keeps_underscores_in_secret() -> None:
    # token_urlsafe can contain '_'; only the first underscore is the delimiter.
    assert apikeys._parse("es_ab12cd34_x_y_z") == ("ab12cd34", "x_y_z")


def test_parse_rejects_malformed() -> None:
    for bad in ["", "nope", "es_only", "es_", "bearerxyz"]:
        with pytest.raises(apikeys.InvalidToken):
            apikeys._parse(bad)


# ---- create / storage -------------------------------------------------------


async def test_create_returns_token_and_stores_hash_not_secret(fake_redis) -> None:
    result = await apikeys.create("billing", ["read", "write"])
    token = result["token"]
    assert token.startswith(f"es_{result['keyid']}_")
    assert result["scopes"] == ["read", "write"]
    # Stored record holds a hash, never the raw secret.
    raw = await fake_redis.hgetall(f"eventstream:apikey:{result['keyid']}")
    _, secret = apikeys._parse(token)
    assert raw["hash"] == apikeys._hash(secret)
    assert secret not in raw.values()


async def test_create_rejects_unknown_scope() -> None:
    with pytest.raises(ValueError):
        await apikeys.create("svc", ["read", "superuser"])


async def test_create_requires_a_scope() -> None:
    with pytest.raises(ValueError):
        await apikeys.create("svc", [])


# ---- verify -----------------------------------------------------------------


async def test_verify_accepts_valid_token_and_returns_name() -> None:
    result = await apikeys.create("billing", ["read"])
    assert await apikeys.verify(result["token"], required="read") == "billing"


async def test_verify_rejects_unknown_key() -> None:
    with pytest.raises(apikeys.InvalidToken):
        await apikeys.verify("es_deadbeef_nope", required="read")


async def test_verify_rejects_bad_secret() -> None:
    result = await apikeys.create("billing", ["read"])
    forged = f"es_{result['keyid']}_wrongsecret"
    with pytest.raises(apikeys.InvalidToken):
        await apikeys.verify(forged, required="read")


async def test_verify_enforces_scope() -> None:
    result = await apikeys.create("reader", ["read"])
    with pytest.raises(apikeys.InsufficientScope):
        await apikeys.verify(result["token"], required="write")


async def test_verify_updates_last_used(fake_redis) -> None:
    result = await apikeys.create("svc", ["read"])
    assert (
        await fake_redis.hget(f"eventstream:apikey:{result['keyid']}", "last_used_at")
        == ""
    )
    await apikeys.verify(result["token"], required="read")
    touched = await fake_redis.hget(
        f"eventstream:apikey:{result['keyid']}", "last_used_at"
    )
    assert touched and int(touched) > 0


# ---- list / revoke ----------------------------------------------------------


async def test_list_shows_metadata_without_secret() -> None:
    a = await apikeys.create("a", ["read"])
    b = await apikeys.create("b", ["read", "admin"])
    listing = await apikeys.list_()
    by_id = {k["keyid"]: k for k in listing}
    assert by_id[a["keyid"]]["name"] == "a"
    assert by_id[a["keyid"]]["scopes"] == ["read"]
    assert by_id[b["keyid"]]["scopes"] == ["read", "admin"]
    assert by_id[a["keyid"]]["last_used_at"] is None
    # No field anywhere is the raw token.
    for entry in listing:
        assert "token" not in entry and "hash" not in entry


async def test_revoke_removes_key_and_blocks_verify() -> None:
    result = await apikeys.create("svc", ["read"])
    await apikeys.revoke(result["keyid"])
    assert await apikeys.list_() == []
    with pytest.raises(apikeys.InvalidToken):
        await apikeys.verify(result["token"], required="read")


async def test_revoke_unknown_raises() -> None:
    with pytest.raises(apikeys.APIKeyNotFound):
        await apikeys.revoke("nope")


# ---- the server before-hook -------------------------------------------------


async def test_require_hook_accepts_and_stamps_principal() -> None:
    result = await apikeys.create("billing", ["read"])
    hook = auth.require("read")
    req = _Req({"Authorization": f"Bearer {result['token']}"})
    await hook(req)
    assert req.content["principal"] == "billing"


async def test_require_hook_case_insensitive_header() -> None:
    result = await apikeys.create("svc", ["read"])
    hook = auth.require("read")
    req = _Req({"authorization": f"bearer {result['token']}"})
    await hook(req)
    assert req.content["principal"] == "svc"


async def test_require_hook_missing_header_rejected() -> None:
    hook = auth.require("read")
    with pytest.raises(apikeys.InvalidToken):
        await hook(_Req({}))


async def test_require_hook_insufficient_scope_rejected() -> None:
    result = await apikeys.create("reader", ["read"])
    hook = auth.require("admin")
    req = _Req({"Authorization": f"Bearer {result['token']}"})
    with pytest.raises(apikeys.InsufficientScope):
        await hook(req)


# ---- `server` startup warning (sync: the command calls asyncio.run) ---------


def _run_server_cmd(monkeypatch, *, auth_on: bool, keys: list[dict]):
    """Invoke the `server` command with run() stubbed and list_() faked."""
    from click.testing import CliRunner

    from eventstream import config as CONFIG
    from eventstream.cli import server as server_mod

    async def _list() -> list[dict]:
        return keys

    monkeypatch.setattr(CONFIG, "auth", auth_on)
    monkeypatch.setattr(apikeys, "list_", _list)
    monkeypatch.setattr(server_mod, "run", lambda **kw: None)
    return CliRunner().invoke(server_mod.server, [])


def test_server_warns_when_auth_on_and_no_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _run_server_cmd(monkeypatch, auth_on=True, keys=[])
    assert result.exit_code == 0
    assert "auth: ON" in result.output
    assert "no API keys" in result.output


def test_server_no_warning_when_keys_exist(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _run_server_cmd(monkeypatch, auth_on=True, keys=[{"keyid": "x"}])
    assert result.exit_code == 0
    assert "auth: ON" in result.output
    assert "no API keys" not in result.output


def test_server_silent_about_auth_when_off(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _run_server_cmd(monkeypatch, auth_on=False, keys=[])
    assert result.exit_code == 0
    assert "auth: ON" not in result.output

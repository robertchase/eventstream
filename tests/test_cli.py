"""End-to-end wiring tests for the CLI layer."""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from eventstream.cli import cli
from eventstream.cli import server as server_cmd


def test_publish_then_pull_roundtrip() -> None:
    runner = CliRunner()
    created = runner.invoke(cli, ["sub", "create", "w", "--stream", "orders"])
    assert created.exit_code == 0

    published = runner.invoke(
        cli, ["publish", "orders", "placed", "--payload", '{"n": 1}']
    )
    assert published.exit_code == 0
    event_id = published.output.strip()

    pulled = runner.invoke(cli, ["pull", "w", "--wait", "0", "--json"])
    assert pulled.exit_code == 0
    event = json.loads(pulled.output)
    assert event["id"] == event_id
    assert event["payload"] == {"n": 1}

    assert runner.invoke(cli, ["ack", "w", event_id]).exit_code == 0


def test_pull_unknown_subscription_reports_error() -> None:
    result = CliRunner().invoke(cli, ["pull", "ghost", "--wait", "0"])
    assert result.exit_code != 0


@pytest.fixture
def _captured_run(monkeypatch: pytest.MonkeyPatch) -> dict:
    """Replace the blocking ``run`` with a stub that records its kwargs."""
    captured: dict = {}
    monkeypatch.setattr(server_cmd, "run", lambda **kw: captured.update(kw))
    return captured


def test_server_sweeps_by_default(_captured_run: dict) -> None:
    result = CliRunner().invoke(cli, ["server"])
    assert result.exit_code == 0
    assert _captured_run["sweep"] is True
    assert "timer sweep: every" in result.output


def test_server_no_sweep_flag_disables_sweeper(_captured_run: dict) -> None:
    result = CliRunner().invoke(cli, ["server", "--no-sweep"])
    assert result.exit_code == 0
    assert _captured_run["sweep"] is False
    assert "timer sweep: OFF" in result.output

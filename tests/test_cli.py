"""End-to-end wiring tests for the CLI layer."""

from __future__ import annotations

import json

from click.testing import CliRunner

from eventstream.cli import cli


def test_publish_then_pull_roundtrip() -> None:
    runner = CliRunner()
    created = runner.invoke(cli, ["sub", "create", "w", "--stream", "orders"])
    assert created.exit_code == 0

    published = runner.invoke(cli, ["publish", "orders", "--payload", '{"n": 1}'])
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

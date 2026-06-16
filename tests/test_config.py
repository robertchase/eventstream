"""Micro-tests for config-driven operational logging setup."""

from __future__ import annotations

import logging

import pytest

from eventstream import config as CONFIG


@pytest.fixture(autouse=True)
def _isolate_eventstream_logger():
    """Save/restore the ``eventstream`` logger so tests don't leak handlers."""
    logger = logging.getLogger("eventstream")
    saved_handlers, saved_level = logger.handlers[:], logger.level
    logger.handlers.clear()
    yield
    logger.handlers[:], logger.level = saved_handlers, saved_level


def test_configure_logging_is_noop_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(CONFIG, "log_level", None)
    CONFIG.configure_logging()
    assert logging.getLogger("eventstream").handlers == []


def test_configure_logging_attaches_handler_at_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(CONFIG, "log_level", "INFO")
    CONFIG.configure_logging()
    logger = logging.getLogger("eventstream")
    assert len(logger.handlers) == 1
    assert logger.level == logging.INFO


def test_configure_logging_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(CONFIG, "log_level", "DEBUG")
    CONFIG.configure_logging()
    CONFIG.configure_logging()
    assert len(logging.getLogger("eventstream").handlers) == 1

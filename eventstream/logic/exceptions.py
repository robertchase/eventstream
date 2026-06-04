"""Domain exceptions for eventstream logic.

Transport-agnostic: the CLI maps these to messages and exit codes; a future
server maps them to HTTP status codes.
"""

from __future__ import annotations


class EventStreamError(Exception):
    """Base class for all eventstream domain errors."""


class StreamNotFound(EventStreamError):
    """A referenced stream does not exist."""


class SubscriptionNotFound(EventStreamError):
    """A referenced subscription does not exist."""


class SubscriptionExists(EventStreamError):
    """A subscription with the same name already exists on another stream."""

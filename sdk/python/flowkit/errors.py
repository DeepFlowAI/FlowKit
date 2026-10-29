"""Error types raised by the Flowkit SDK."""
from __future__ import annotations

from typing import Any


class FlowkitError(Exception):
    """Base class for all SDK errors."""


class NotConnectedError(FlowkitError):
    """Raised when an operation requires an active connection but none exists."""


class RPCError(FlowkitError):
    """A JSON-RPC error response was received from the server.

    Attributes:
        code: numeric JSON-RPC error code
        message: human-readable error message
        data: optional payload attached by the server
    """

    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(f"rpc {code}: {message}")
        self.code = code
        self.message = message
        self.data = data

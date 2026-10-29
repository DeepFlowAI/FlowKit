"""Flowkit Python SDK.

Top-level surface re-exports the user-facing types. Internal modules live
under ``flowkit.transport`` and ``flowkit.events``.
"""
from __future__ import annotations

from ._version import __version__
from .events import Event, EventType
from .errors import FlowkitError, RPCError, NotConnectedError
from .kernel import Kernel
from .call import Call
from .agent import ASRSession

__all__ = [
    "Kernel",
    "Call",
    "ASRSession",
    "Event",
    "EventType",
    "FlowkitError",
    "RPCError",
    "NotConnectedError",
    "__version__",
]

"""Event types delivered to subscribers."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class EventType(str, Enum):
    """Wire-level event names emitted by the Flowkit kernel.

    Values match the kernel's JSON-RPC notification method strings.
    Inheriting from ``str`` makes them directly comparable with raw method
    strings — useful when consuming notifications generically.
    """
    INCOMING = "call.incoming"
    RINGING = "call.ringing"
    EARLY_MEDIA = "call.early_media"
    ANSWERED = "call.answered"
    SPEECH_START = "call.speech_start"
    SPEECH_PARTIAL = "call.speech_partial"
    SPEECH_END = "call.speech_end"
    PLAY_PROGRESS = "call.play_progress"
    PLAY_END = "call.play_end"
    DTMF = "call.dtmf"
    HANGUP = "call.hangup"
    ERROR = "call.error"
    HEALTH = "system.health"


@dataclass
class Event:
    """A single event delivered to a subscriber.

    The ``method`` field is the raw JSON-RPC method name; ``data`` is the
    decoded params object. For type-safe access to common fields, use the
    helpers (``call_id``, ``text`` etc.) which read from data with sensible
    fallbacks.
    """
    method: str
    data: dict[str, Any] = field(default_factory=dict)

    @property
    def call_id(self) -> str | None:
        return self.data.get("call_id")

    @property
    def text(self) -> str | None:
        return self.data.get("text")

    @property
    def reason(self) -> str | None:
        return self.data.get("reason")

    @property
    def digit(self) -> str | None:
        return self.data.get("digit")

    def __repr__(self) -> str:  # pragma: no cover — display only
        return f"Event(method={self.method!r}, data={self.data!r})"

"""High-level Call object — mirrors the kernel's call.* JSON-RPC methods."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .kernel import Kernel


class Call:
    """A single conversation handle.

    M1 implements the API surface as RPC pass-throughs. Real audio routing
    (WebRTC, SIP) lands in M2/M3 but the SDK shape doesn't change.
    """

    def __init__(self, kernel: "Kernel", call_id: str) -> None:
        self._kernel = kernel
        self._id = call_id

    @property
    def id(self) -> str:
        return self._id

    # --- core actions ---

    async def answer(self) -> None:
        await self._kernel._transport.call("call.answer", {"call_id": self._id})

    async def hangup(self, reason: str | None = None) -> None:
        params: dict[str, Any] = {"call_id": self._id}
        if reason is not None:
            params["reason"] = reason
        await self._kernel._transport.call("call.hangup", params)

    async def say(self, text: str, *, voice: str | None = None,
                  barge_in: bool | None = None) -> str:
        params: dict[str, Any] = {"call_id": self._id, "text": text}
        if voice is not None:
            params["voice"] = voice
        if barge_in is not None:
            params["barge_in"] = barge_in
        result = await self._kernel._transport.call("call.say", params)
        return result.get("play_id", "")

    async def listen(
        self,
        *,
        timeout_ms: int | None = None,
        adaptive_barge_in: bool | None = None,
        performance: dict[str, Any] | None = None,
    ) -> None:
        params: dict[str, Any] = {"call_id": self._id}
        if timeout_ms is not None:
            params["timeout_ms"] = timeout_ms
        if adaptive_barge_in is not None:
            params["adaptive_barge_in"] = adaptive_barge_in
        if performance is not None:
            params["performance"] = performance
        await self._kernel._transport.call("call.listen", params)

    async def stop(self, *, target: str = "all") -> None:
        await self._kernel._transport.call("call.stop", {
            "call_id": self._id,
            "target": target,
        })

    async def hold(self) -> None:
        await self._kernel._transport.call("call.hold", {"call_id": self._id})

    async def unhold(self) -> None:
        await self._kernel._transport.call("call.unhold", {"call_id": self._id})

    async def mute(self) -> None:
        """Silence what this party sends, leaving the call otherwise running.

        Unlike :meth:`hold` they stay in the conversation and keep hearing the
        other side. Use this when the party cannot mute themselves — a browser
        should switch off its own audio track instead, which needs no round
        trip and survives a dropped connection.
        """
        await self._kernel._transport.call("call.mute", {"call_id": self._id})

    async def unmute(self) -> None:
        await self._kernel._transport.call("call.unmute", {"call_id": self._id})

    async def send_dtmf(self, digits: str, *, duration_ms: int | None = None) -> None:
        params: dict[str, Any] = {"call_id": self._id, "digits": digits}
        if duration_ms is not None:
            params["duration_ms"] = duration_ms
        await self._kernel._transport.call("call.dtmf", params)

    async def transfer(self, target_uri: str) -> None:
        await self._kernel._transport.call("call.transfer", {
            "call_id": self._id,
            "target_uri": target_uri,
        })

    async def bridge(self, other_call_id: str) -> None:
        await self._kernel._transport.call("call.bridge", {
            "call_id": self._id,
            "other_call_id": other_call_id,
        })

    def __repr__(self) -> str:  # pragma: no cover
        return f"Call(id={self._id!r})"

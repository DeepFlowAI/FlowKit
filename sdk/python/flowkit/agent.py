"""Standalone (call-less) streaming ASR/TTS helpers.

These wrap flowkit's ``asr.*`` / ``tts.*`` RPC methods so a consumer can use the
kernel's optimized FunASR/CosyVoice providers directly — stream PCM in and get
transcripts out, or send text and get PCM out — without the SIP/WebRTC call
model. Audio is 16 kHz mono int16 little-endian, base64-encoded on the wire.

    async with Kernel(url) as k:
        # ASR
        sess = await k.asr_session()
        await sess.send_pcm(pcm_bytes)
        async for ev in sess.events():
            if ev["type"] == "final":
                print(ev["text"])
        await sess.aclose()

        # TTS
        async for pcm in k.tts_stream("你好"):
            play(pcm)
"""
from __future__ import annotations

import asyncio
import base64
from typing import TYPE_CHECKING, Any, AsyncIterator

from .errors import FlowkitError
from .events import Event

if TYPE_CHECKING:
    from .kernel import Kernel

_SENTINEL: dict[str, Any] = {"__sentinel__": True}


class ASRSession:
    """One streaming recognition session. Feed PCM with :meth:`send_pcm`, read
    ``{"type", "text", "seq"}`` dicts from :meth:`events`, and release it with
    :meth:`aclose` (closing the underlying server session)."""

    def __init__(self, kernel: "Kernel", session_id: str) -> None:
        self._kernel = kernel
        self.session_id = session_id
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._closed = False

    async def _on_event(self, event: Event) -> None:
        if event.data.get("session_id") != self.session_id:
            return
        await self._queue.put(event.data)

    async def send_pcm(self, pcm: bytes) -> None:
        """Forward one frame of 16 kHz mono int16 PCM to the recognizer."""
        if self._closed:
            return
        await self._kernel._transport.notify("asr.audio", {
            "session_id": self.session_id,
            "audio": base64.b64encode(pcm).decode("ascii"),
        })

    async def events(self) -> AsyncIterator[dict[str, Any]]:
        """Yield recognition events until the session is closed."""
        while True:
            data = await self._queue.get()
            if data is _SENTINEL:
                return
            yield data

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._kernel.off("asr.event", self._on_event)
        try:
            await self._kernel._transport.call("asr.stop", {"session_id": self.session_id})
        except Exception:  # noqa: BLE001 — teardown is best-effort
            pass
        await self._queue.put(_SENTINEL)


async def open_asr_session(kernel: "Kernel") -> ASRSession:
    result = await kernel._transport.call("asr.start", {})
    session_id = result.get("session_id")
    if not session_id:
        raise FlowkitError("asr.start returned no session_id")
    sess = ASRSession(kernel, session_id)
    kernel.on("asr.event")(sess._on_event)
    return sess


async def tts_stream(
    kernel: "Kernel",
    text: str,
    *,
    voice: str | None = None,
    sample_rate: int | None = None,
) -> AsyncIterator[bytes]:
    """Synthesize ``text`` and yield mono int16 PCM chunks until final.

    ``sample_rate`` requests the output rate (default: the server's bus rate,
    16 kHz). Closing the generator early (e.g. on barge-in) cancels server-side
    synthesis via ``tts.cancel``.
    """
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    holder: dict[str, str | None] = {"id": None}

    async def _on_chunk(event: Event) -> None:
        if event.data.get("synthesis_id") != holder["id"]:
            return
        await queue.put(event.data)

    kernel.on("tts.chunk")(_on_chunk)
    try:
        params: dict[str, Any] = {"text": text}
        if voice:
            params["voice"] = voice
        if sample_rate:
            params["sample_rate"] = sample_rate
        # The server enqueues this response before any chunk (the first chunk
        # waits on the CosyVoice HTTP round-trip), so holder["id"] is set in
        # time to match incoming notifications.
        result = await kernel._transport.call("tts.synthesize", params)
        synthesis_id = result.get("synthesis_id")
        if not synthesis_id:
            raise FlowkitError("tts.synthesize returned no synthesis_id")
        holder["id"] = synthesis_id
        while True:
            data = await queue.get()
            audio_b64 = data.get("audio") or ""
            if audio_b64:
                yield base64.b64decode(audio_b64)
            if data.get("final"):
                return
    finally:
        kernel.off("tts.chunk", _on_chunk)
        if holder["id"] is not None:
            try:
                await kernel._transport.call("tts.cancel", {"synthesis_id": holder["id"]})
            except Exception:  # noqa: BLE001 — teardown is best-effort
                pass

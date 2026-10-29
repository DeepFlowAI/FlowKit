"""Top-level Kernel — connects, handshakes, dispatches events."""
from __future__ import annotations

import asyncio
import logging
import platform
from collections import defaultdict
from typing import TYPE_CHECKING, Any, AsyncIterator, Awaitable, Callable

if TYPE_CHECKING:
    from .agent import ASRSession

from ._version import __version__
from .call import Call
from .errors import FlowkitError
from .events import Event
from .transport.ws import WebSocketTransport

logger = logging.getLogger(__name__)

# A subscriber is an async function taking one Event.
Subscriber = Callable[[Event], Awaitable[None]]


class Kernel:
    """Entry point for an SDK session against a Flowkit server.

    Lifecycle:

        async with Kernel("ws://localhost:9600/ws") as kernel:
            @kernel.on("call.incoming")
            async def handler(ev):
                ...
            await asyncio.sleep(60)

    Inside ``__aenter__`` the kernel connects, runs system.hello, and
    starts notification fan-out. ``__aexit__`` cleanly disconnects.
    """

    def __init__(
        self,
        url: str = "ws://localhost:9600/ws",
        *,
        sdk_name: str = "flowkit-sdk-py",
        sdk_version: str = __version__,
        capabilities: list[str] | None = None,
        api_key: str | None = None,
    ) -> None:
        # api_key is sent as "Authorization: Bearer <key>" during the
        # WebSocket handshake; required when the server sets FLOWKIT_API_KEY.
        self._transport = WebSocketTransport(url, api_key=api_key)
        self._transport.on_notification(self._on_notification)
        self._sdk_name = sdk_name
        self._sdk_version = sdk_version
        self._capabilities = capabilities or []
        self._subscribers: dict[str, list[Subscriber]] = defaultdict(list)
        self._server_info: dict[str, Any] = {}

    # --- properties ---

    @property
    def server_info(self) -> dict[str, Any]:
        """The result of system.hello (populated after connect)."""
        return dict(self._server_info)

    # --- lifecycle ---

    async def connect(self) -> None:
        await self._transport.connect()
        self._server_info = await self._transport.call("system.hello", {
            "sdk": self._sdk_name,
            "version": self._sdk_version,
            "capabilities": self._capabilities,
        })
        logger.info("flowkit handshake ok: %s", self._server_info)

    async def disconnect(self) -> None:
        await self._transport.disconnect()

    async def __aenter__(self) -> "Kernel":
        await self.connect()
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        await self.disconnect()

    # --- event subscription ---

    def on(self, method: str) -> Callable[[Subscriber], Subscriber]:
        """Decorator: register ``handler`` for events with the given method name.

            @kernel.on("call.incoming")
            async def handle(event): ...
        """
        def decorator(handler: Subscriber) -> Subscriber:
            self._subscribers[method].append(handler)
            return handler
        return decorator

    def off(self, method: str, handler: Subscriber | None = None) -> None:
        """Remove a previously registered handler (or all for a method)."""
        if handler is None:
            self._subscribers.pop(method, None)
            return
        if handler in self._subscribers.get(method, []):
            self._subscribers[method].remove(handler)

    # --- direct API ---

    async def shutdown_server(self) -> None:
        """Ask the kernel to shut itself down (server-side SIGTERM equivalent)."""
        await self._transport.call("system.shutdown")

    async def originate(self, uri: str, *, timeout_ms: int | None = None) -> Call:
        """Begin an outbound call. M1 returns a Call object, but the underlying
        transport implementation is wired in M2/M3 — until then this raises."""
        params: dict[str, Any] = {"uri": uri}
        if timeout_ms is not None:
            params["timeout_ms"] = timeout_ms
        result = await self._transport.call("call.originate", params)
        call_id = result.get("call_id")
        if not call_id:
            raise FlowkitError("originate returned no call_id")
        return Call(self, call_id)

    # --- standalone ASR/TTS (call-less) ---

    async def asr_session(self) -> "ASRSession":
        """Open a standalone streaming recognition session (16 kHz mono PCM)."""
        from .agent import open_asr_session

        return await open_asr_session(self)

    def tts_stream(
        self, text: str, *, voice: str | None = None, sample_rate: int | None = None
    ) -> "AsyncIterator[bytes]":
        """Synthesize ``text`` and stream mono int16 PCM chunks at ``sample_rate``."""
        from .agent import tts_stream

        return tts_stream(self, text, voice=voice, sample_rate=sample_rate)

    # --- internal ---

    async def _on_notification(self, method: str, params: dict[str, Any]) -> None:
        event = Event(method=method, data=params)
        handlers = list(self._subscribers.get(method, []))
        if not handlers:
            return
        # Fire handlers concurrently; gather to surface exceptions in logs.
        await asyncio.gather(
            *(self._safe_call(h, event) for h in handlers),
            return_exceptions=False,
        )

    async def _safe_call(self, handler: Subscriber, event: Event) -> None:
        try:
            await handler(event)
        except Exception as exc:  # noqa: BLE001 — log and swallow per-handler
            logger.warning("subscriber for %s raised: %s", event.method, exc)


# Module-level utility: expose runtime info for debugging.
def runtime_info() -> dict[str, str]:  # pragma: no cover — meta only
    return {
        "sdk_version": __version__,
        "python": platform.python_version(),
        "platform": platform.platform(),
    }

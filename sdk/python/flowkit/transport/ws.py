"""JSON-RPC 2.0 over WebSocket client.

Owns the connection, the per-request id → future map, and the inbound
notification fan-out. Higher-level code (Kernel, Call) builds on top.
"""
from __future__ import annotations

import asyncio
import itertools
import json
import logging
from typing import Any, Awaitable, Callable

import websockets
from websockets.exceptions import ConnectionClosed

from ..errors import NotConnectedError, RPCError

logger = logging.getLogger(__name__)

# Type alias for a notification handler: async function(method, params).
NotificationHandler = Callable[[str, dict[str, Any]], Awaitable[None]]


class WebSocketTransport:
    """A single-connection JSON-RPC 2.0 client over WebSocket.

    Usage:

        t = WebSocketTransport("ws://localhost:9600/ws")
        await t.connect()
        result = await t.call("system.hello", {"sdk": "py", "version": "0.1"})
        await t.disconnect()

    Concurrency: many ``call`` invocations from many coroutines are safe; each
    is matched back to its response by a server-echoed request id.
    """

    def __init__(self, url: str, *, default_timeout: float = 10.0,
                 api_key: str | None = None) -> None:
        self._url = url
        self._default_timeout = default_timeout
        self._api_key = api_key
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._reader_task: asyncio.Task | None = None
        self._next_id = itertools.count(1)
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._on_notification: NotificationHandler | None = None
        self._closing = False

    # --- lifecycle ---

    async def connect(self) -> None:
        if self._ws is not None:
            return
        headers = {"Authorization": f"Bearer {self._api_key}"} if self._api_key else None
        self._ws = await websockets.connect(self._url, additional_headers=headers)
        self._reader_task = asyncio.create_task(self._reader(), name="flowkit-ws-reader")

    async def disconnect(self) -> None:
        self._closing = True
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
        if self._reader_task is not None:
            try:
                await asyncio.wait_for(self._reader_task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            self._reader_task = None
        # Reject any pending calls.
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(NotConnectedError("connection closed"))
        self._pending.clear()

    async def __aenter__(self) -> "WebSocketTransport":
        await self.connect()
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        await self.disconnect()

    # --- API ---

    def on_notification(self, handler: NotificationHandler) -> None:
        """Register the single notification dispatcher. The Kernel typically
        uses this to fan out to per-event subscribers."""
        self._on_notification = handler

    async def call(self, method: str, params: dict[str, Any] | None = None,
                   *, timeout: float | None = None) -> dict[str, Any]:
        """Send a JSON-RPC request and await the matching response.

        Raises:
            NotConnectedError: if connect() hasn't completed.
            RPCError: on JSON-RPC error response.
            asyncio.TimeoutError: if no response arrives within ``timeout``.
        """
        if self._ws is None:
            raise NotConnectedError("call before connect()")
        rid = f"py-{next(self._next_id)}"
        frame: dict[str, Any] = {"jsonrpc": "2.0", "id": rid, "method": method}
        if params is not None:
            frame["params"] = params

        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[rid] = future
        try:
            await self._ws.send(json.dumps(frame))
            return await asyncio.wait_for(future, timeout or self._default_timeout)
        finally:
            self._pending.pop(rid, None)

    async def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        """Send a JSON-RPC notification (no response expected)."""
        if self._ws is None:
            raise NotConnectedError("notify before connect()")
        frame: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            frame["params"] = params
        await self._ws.send(json.dumps(frame))

    # --- internal ---

    async def _reader(self) -> None:
        assert self._ws is not None
        try:
            async for raw in self._ws:
                self._dispatch(raw)
        except ConnectionClosed:
            pass
        except Exception as exc:  # pragma: no cover — defensive
            logger.exception("ws reader fatal: %s", exc)
        finally:
            # When the reader exits, propagate to any pending callers.
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(NotConnectedError("connection closed"))

    def _dispatch(self, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("dropping non-JSON frame: %r", raw[:80])
            return

        # Response (has 'id', 'result' or 'error')
        if "id" in msg and ("result" in msg or "error" in msg):
            rid = msg.get("id")
            if not isinstance(rid, (str, int)):
                logger.warning("response with non-string/int id: %r", rid)
                return
            fut = self._pending.get(str(rid))
            if fut is None or fut.done():
                logger.debug("response for unknown id: %s", rid)
                return
            if err := msg.get("error"):
                fut.set_exception(RPCError(err.get("code", 0), err.get("message", ""), err.get("data")))
            else:
                fut.set_result(msg.get("result") or {})
            return

        # Notification (has 'method', no 'id')
        if "method" in msg:
            if self._on_notification is None:
                logger.debug("notification with no handler: %s", msg.get("method"))
                return
            method = msg["method"]
            params = msg.get("params") or {}
            asyncio.create_task(self._on_notification(method, params))
            return

        logger.warning("ignoring unknown frame: %r", msg)

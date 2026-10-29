"""Shared pytest fixtures for the SDK test suite."""
from __future__ import annotations

import asyncio
import json
import socket
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
import pytest_asyncio
import websockets


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# Handler signature: (method, params, send_response, send_notification, send_error)
MockHandler = Callable[
    [str, dict[str, Any], Callable[[Any], Awaitable[None]], Callable[[str, dict], Awaitable[None]], Callable[[int, str], Awaitable[None]]],
    Awaitable[None],
]


class MockServer:
    """A minimal JSON-RPC 2.0 server backed by ``websockets`` for SDK tests."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.url = f"ws://{host}:{port}/ws"
        self.handlers: dict[str, MockHandler] = {}
        # Track inbound requests + notifications for assertions.
        self.received: list[dict[str, Any]] = []
        self._server: websockets.Server | None = None
        self._conns: set[Any] = set()
        # Handshake headers (lower-cased names) of every accepted connection.
        self.request_headers: list[dict[str, str]] = []

    def on(self, method: str):
        """Register a handler for ``method``."""
        def decorator(fn: MockHandler) -> MockHandler:
            self.handlers[method] = fn
            return fn
        return decorator

    async def start(self) -> None:
        self._server = await websockets.serve(self._handle_conn, self.host, self.port)

    async def stop(self) -> None:
        for ws in list(self._conns):
            await ws.close()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def broadcast(self, method: str, params: dict[str, Any]) -> None:
        frame = json.dumps({"jsonrpc": "2.0", "method": method, "params": params})
        for ws in list(self._conns):
            try:
                await ws.send(frame)
            except websockets.ConnectionClosed:
                pass

    async def _handle_conn(self, ws) -> None:
        self._conns.add(ws)
        self.request_headers.append({k.lower(): v for k, v in ws.request.headers.raw_items()})
        try:
            async for raw in ws:
                self._dispatch_task = asyncio.create_task(self._dispatch(ws, raw))
        except websockets.ConnectionClosed:
            pass
        finally:
            self._conns.discard(ws)

    async def _dispatch(self, ws, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        self.received.append(msg)

        method = msg.get("method")
        if not method:
            return  # ignore responses

        params = msg.get("params") or {}
        rid = msg.get("id")
        handler = self.handlers.get(method)

        async def send_response(result: Any) -> None:
            if rid is None:
                return
            await ws.send(json.dumps({"jsonrpc": "2.0", "id": rid, "result": result}))

        async def send_notification(meth: str, p: dict) -> None:
            await ws.send(json.dumps({"jsonrpc": "2.0", "method": meth, "params": p}))

        async def send_error(code: int, message: str) -> None:
            if rid is None:
                return
            await ws.send(json.dumps({
                "jsonrpc": "2.0", "id": rid,
                "error": {"code": code, "message": message},
            }))

        if handler is None:
            await send_error(-32601, f"method not found: {method}")
            return
        await handler(method, params, send_response, send_notification, send_error)


@pytest_asyncio.fixture
async def mock_server() -> AsyncIterator[MockServer]:
    server = MockServer("127.0.0.1", _free_port())

    # Default: respond to system.hello so connect() doesn't hang.
    @server.on("system.hello")
    async def hello(_m, _p, send_response, _sn, _se):
        await send_response({
            "server": "mock-flowkit",
            "version": "test",
            "node_id": "mock-node",
            "time": "2026-05-15T00:00:00Z",
        })

    await server.start()
    try:
        yield server
    finally:
        await server.stop()

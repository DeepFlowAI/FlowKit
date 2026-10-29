"""Unit tests for flowkit.transport.ws.WebSocketTransport."""
from __future__ import annotations

import asyncio
import pytest

from flowkit.errors import NotConnectedError, RPCError
from flowkit.transport.ws import WebSocketTransport


async def test_connect_disconnect(mock_server):
    t = WebSocketTransport(mock_server.url)
    await t.connect()
    await t.disconnect()


async def test_call_returns_result(mock_server):
    @mock_server.on("test.echo")
    async def echo(_m, params, send_response, _sn, _se):
        await send_response({"echoed": params})

    async with WebSocketTransport(mock_server.url) as t:
        result = await t.call("test.echo", {"x": "y"})
        assert result == {"echoed": {"x": "y"}}


async def test_call_propagates_rpc_error(mock_server):
    @mock_server.on("test.fail")
    async def fail(_m, _p, _r, _sn, send_error):
        await send_error(-32602, "bad params")

    async with WebSocketTransport(mock_server.url) as t:
        with pytest.raises(RPCError) as excinfo:
            await t.call("test.fail")
        assert excinfo.value.code == -32602
        assert "bad params" in excinfo.value.message


async def test_call_without_connect_raises(tmp_path):
    t = WebSocketTransport("ws://127.0.0.1:1")  # invalid; we don't actually connect
    with pytest.raises(NotConnectedError):
        await t.call("anything")


async def test_concurrent_calls_match_by_id(mock_server):
    """Many in-flight calls must each get their own response."""
    @mock_server.on("test.delay")
    async def delay(_m, params, send_response, _sn, _se):
        await asyncio.sleep(params.get("ms", 0) / 1000)
        await send_response({"echo": params})

    async with WebSocketTransport(mock_server.url) as t:
        # Send 10 concurrent calls with different delays.
        results = await asyncio.gather(*[
            t.call("test.delay", {"ms": (i % 5) * 5, "n": i})
            for i in range(10)
        ])
        # Each response must match its own params
        for i, r in enumerate(results):
            assert r["echo"]["n"] == i


async def test_notify_no_response(mock_server):
    fired = asyncio.Event()

    @mock_server.on("test.fire")
    async def handler(_m, _p, _r, _sn, _se):
        fired.set()

    async with WebSocketTransport(mock_server.url) as t:
        await t.notify("test.fire", {})
        await asyncio.wait_for(fired.wait(), timeout=2)


async def test_inbound_notification_invokes_handler(mock_server):
    seen: list[tuple[str, dict]] = []

    async def cb(method: str, params: dict) -> None:
        seen.append((method, params))

    t = WebSocketTransport(mock_server.url)
    t.on_notification(cb)
    await t.connect()
    try:
        await mock_server.broadcast("event.tick", {"n": 42})
        # Allow a brief moment for the reader task to dispatch.
        await asyncio.sleep(0.1)
    finally:
        await t.disconnect()

    assert ("event.tick", {"n": 42}) in seen


async def test_timeout(mock_server):
    @mock_server.on("test.slow")
    async def slow(_m, _p, _r, _sn, _se):
        await asyncio.sleep(2)
        # never respond

    async with WebSocketTransport(mock_server.url, default_timeout=0.2) as t:
        with pytest.raises(asyncio.TimeoutError):
            await t.call("test.slow")


async def test_server_close_fails_pending_rpc(mock_server):
    """Peer disconnect must fail in-flight calls immediately (not wait timeout)."""

    @mock_server.on("test.hang")
    async def hang(_m, _p, _r, _sn, _se):
        await asyncio.sleep(60)  # never respond

    t = WebSocketTransport(mock_server.url, default_timeout=30.0)
    await t.connect()
    try:
        call_task = asyncio.create_task(t.call("test.hang"))
        await asyncio.sleep(0.05)
        await mock_server.stop()
        with pytest.raises(NotConnectedError):
            await asyncio.wait_for(call_task, timeout=2.0)
    finally:
        await t.disconnect()

"""Unit tests for flowkit.Kernel."""
from __future__ import annotations

import asyncio
import pytest

from flowkit import Kernel
from flowkit.errors import RPCError


async def test_handshake_populates_server_info(mock_server):
    async with Kernel(mock_server.url) as kernel:
        info = kernel.server_info
        assert info["server"] == "mock-flowkit"
        assert info["node_id"] == "mock-node"


async def test_api_key_sent_as_bearer_header(mock_server):
    async with Kernel(mock_server.url, api_key="s3cret"):
        pass
    assert mock_server.request_headers[-1].get("authorization") == "Bearer s3cret"


async def test_no_api_key_sends_no_authorization_header(mock_server):
    async with Kernel(mock_server.url):
        pass
    assert "authorization" not in mock_server.request_headers[-1]


async def test_on_decorator_registers_subscriber(mock_server):
    received = []

    async with Kernel(mock_server.url) as kernel:

        @kernel.on("call.incoming")
        async def handle(event):
            received.append(event)

        await mock_server.broadcast("call.incoming", {
            "call_id": "abc", "from": "sip:alice", "to": "sip:bob",
        })
        await asyncio.sleep(0.1)

    assert len(received) == 1
    ev = received[0]
    assert ev.method == "call.incoming"
    assert ev.call_id == "abc"


async def test_off_removes_subscriber(mock_server):
    received: list = []

    async with Kernel(mock_server.url) as kernel:

        async def handler(event):
            received.append(event)

        kernel.on("call.hangup")(handler)
        kernel.off("call.hangup", handler)

        await mock_server.broadcast("call.hangup", {"call_id": "x"})
        await asyncio.sleep(0.1)

    assert received == []


async def test_off_all_for_method(mock_server):
    fired = 0

    async def inc(_event):
        nonlocal fired
        fired += 1

    async with Kernel(mock_server.url) as kernel:
        kernel.on("call.dtmf")(inc)
        kernel.on("call.dtmf")(inc)
        kernel.off("call.dtmf")

        await mock_server.broadcast("call.dtmf", {"call_id": "x", "digit": "#"})
        await asyncio.sleep(0.1)

    assert fired == 0


async def test_subscriber_exception_does_not_kill_kernel(mock_server, caplog):
    survived = False

    async with Kernel(mock_server.url) as kernel:

        @kernel.on("call.incoming")
        async def bad(_event):
            raise RuntimeError("boom")

        @kernel.on("call.incoming")
        async def good(_event):
            nonlocal survived
            survived = True

        await mock_server.broadcast("call.incoming", {"call_id": "abc"})
        await asyncio.sleep(0.1)

    # Both handlers run because the kernel logs the exception and continues.
    # NB: gather(return_exceptions=False) means a failing handler raises; this
    # is intentional — the SDK fans out via _safe_call which catches.
    assert survived


async def test_shutdown_server_calls_method(mock_server):
    called = asyncio.Event()

    @mock_server.on("system.shutdown")
    async def handler(_m, _p, send_response, _sn, _se):
        called.set()
        await send_response({"ok": True})

    async with Kernel(mock_server.url) as kernel:
        await kernel.shutdown_server()
        assert called.is_set()


async def test_originate_returns_call(mock_server):
    @mock_server.on("call.originate")
    async def originate(_m, _p, send_response, _sn, _se):
        await send_response({"call_id": "new-call-1", "status": "ringing"})

    async with Kernel(mock_server.url) as kernel:
        call = await kernel.originate("sip:bob@example.com")
        assert call.id == "new-call-1"


async def test_originate_no_call_id_raises(mock_server):
    @mock_server.on("call.originate")
    async def originate(_m, _p, send_response, _sn, _se):
        await send_response({})  # missing call_id

    async with Kernel(mock_server.url) as kernel:
        with pytest.raises(Exception):
            await kernel.originate("sip:x@y")

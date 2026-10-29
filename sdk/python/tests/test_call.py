"""Unit tests for flowkit.Call."""
from __future__ import annotations

import asyncio
import pytest

from flowkit import Kernel


async def test_say_returns_play_id(mock_server):
    @mock_server.on("call.say")
    async def say(_m, params, send_response, _sn, _se):
        assert params["call_id"] == "c1"
        assert params["text"] == "hello"
        await send_response({"play_id": "p1"})

    async with Kernel(mock_server.url) as kernel:
        call = type("X", (), {})()  # placeholder; we'll use real Call
        from flowkit.call import Call
        c = Call(kernel, "c1")
        play_id = await c.say("hello")
        assert play_id == "p1"


async def test_answer_and_hangup(mock_server):
    methods_seen: list[str] = []

    @mock_server.on("call.answer")
    async def answer(_m, _p, send_response, _sn, _se):
        methods_seen.append("call.answer")
        await send_response({"ok": True})

    @mock_server.on("call.hangup")
    async def hangup(_m, params, send_response, _sn, _se):
        methods_seen.append("call.hangup")
        assert params["call_id"] == "c1"
        assert params.get("reason") == "user_hangup"
        await send_response({"ok": True})

    from flowkit.call import Call
    async with Kernel(mock_server.url) as kernel:
        c = Call(kernel, "c1")
        await c.answer()
        await c.hangup("user_hangup")
    assert methods_seen == ["call.answer", "call.hangup"]


async def test_dtmf_and_hold(mock_server):
    sent = {}

    @mock_server.on("call.dtmf")
    async def dtmf(_m, params, send_response, _sn, _se):
        sent.update(params)
        await send_response({"ok": True})

    @mock_server.on("call.hold")
    async def hold(_m, _p, send_response, _sn, _se):
        await send_response({"ok": True})

    @mock_server.on("call.unhold")
    async def unhold(_m, _p, send_response, _sn, _se):
        await send_response({"ok": True})

    from flowkit.call import Call
    async with Kernel(mock_server.url) as kernel:
        c = Call(kernel, "abc")
        await c.send_dtmf("123#", duration_ms=200)
        await c.hold()
        await c.unhold()

    assert sent["digits"] == "123#"
    assert sent["duration_ms"] == 200


async def test_mute_and_unmute(mock_server):
    seen: list[dict] = []

    @mock_server.on("call.mute")
    async def mute(_m, params, send_response, _sn, _se):
        seen.append(params)
        await send_response({"ok": True})

    @mock_server.on("call.unmute")
    async def unmute(_m, params, send_response, _sn, _se):
        seen.append(params)
        await send_response({"ok": True})

    from flowkit.call import Call
    async with Kernel(mock_server.url) as kernel:
        c = Call(kernel, "abc")
        await c.mute()
        await c.unmute()

    assert [p["call_id"] for p in seen] == ["abc", "abc"]


async def test_listen_with_timeout_and_adaptive_barge(mock_server):
    @mock_server.on("call.listen")
    async def listen(_m, params, send_response, _sn, _se):
        assert params["timeout_ms"] == 5000
        assert params["adaptive_barge_in"] is True
        assert params["performance"] == {
            "turn_detection_enabled": False,
            "endpoint_silence_ms": 1000,
        }
        await send_response({"ok": True})

    from flowkit.call import Call
    async with Kernel(mock_server.url) as kernel:
        c = Call(kernel, "x")
        await c.listen(
            timeout_ms=5000,
            adaptive_barge_in=True,
            performance={
                "turn_detection_enabled": False,
                "endpoint_silence_ms": 1000,
            },
        )


async def test_stop_with_target(mock_server):
    captured: dict = {}

    @mock_server.on("call.stop")
    async def stop(_m, params, send_response, _sn, _se):
        captured.update(params)
        await send_response({"ok": True})

    from flowkit.call import Call
    async with Kernel(mock_server.url) as kernel:
        c = Call(kernel, "x")
        await c.stop(target="play")
    assert captured["target"] == "play"


async def test_transfer_and_bridge(mock_server):
    seen = {}

    @mock_server.on("call.transfer")
    async def transfer(_m, params, send_response, _sn, _se):
        seen["transfer"] = params
        await send_response({"ok": True})

    @mock_server.on("call.bridge")
    async def bridge(_m, params, send_response, _sn, _se):
        seen["bridge"] = params
        await send_response({"ok": True})

    from flowkit.call import Call
    async with Kernel(mock_server.url) as kernel:
        c = Call(kernel, "leg-a")
        await c.transfer("sip:agent@host")
        await c.bridge("leg-b")

    assert seen["transfer"]["target_uri"] == "sip:agent@host"
    assert seen["bridge"]["other_call_id"] == "leg-b"


async def test_call_id_property():
    """Call.id should expose the call_id without needing a server."""
    from flowkit.call import Call
    c = Call(None, "abc-123")  # type: ignore[arg-type]
    assert c.id == "abc-123"

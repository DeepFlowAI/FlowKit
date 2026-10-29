"""Playwright headless E2E for examples/api_explorer.

Verifies Connect → Start call → WebRTC offer/answer over Trickle ICE.
"""
from __future__ import annotations

import json
import time
import urllib.request

import pytest

FAKE_MIC_INIT_SCRIPT = """
(() => {
  const realGUM = navigator.mediaDevices.getUserMedia.bind(navigator.mediaDevices);
  navigator.mediaDevices.getUserMedia = async (constraints) => {
    if (!constraints || !constraints.audio) {
      return realGUM(constraints);
    }
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const osc = ctx.createOscillator();
    osc.frequency.value = 440;
    const dst = ctx.createMediaStreamDestination();
    osc.connect(dst);
    osc.start();
    return dst.stream;
  };
})();
"""


@pytest.fixture(scope="session")
def browser_type_launch_args(browser_type_launch_args):
    args = list(browser_type_launch_args.get("args", []))
    args += ["--use-fake-ui-for-media-stream", "--no-sandbox"]
    return {**browser_type_launch_args, "args": args, "headless": True}


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args):
    return {**browser_context_args, "permissions": ["microphone"]}


@pytest.fixture
def fake_mic_page(page):
    page.add_init_script(FAKE_MIC_INIT_SCRIPT)
    return page


def _read_health(port: int) -> dict:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2) as resp:
        return json.loads(resp.read())


def _wait_log_contains(page, needle: str, timeout_s: float = 20) -> str:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        log_text = page.locator("#log").inner_text()
        if needle in log_text:
            return log_text
        time.sleep(0.15)
    return page.locator("#log").inner_text()


def test_api_explorer_start_call(fake_mic_page, flowkit_server, static_server):
    page = fake_mic_page
    port, fk_log = flowkit_server
    assert _read_health(port)["status"] == "ok"

    console_msgs = []
    page_errors = []
    page.on("console", lambda m: console_msgs.append(f"[{m.type}] {m.text}"))
    page.on("pageerror", lambda e: page_errors.append(str(e)))

    page.goto(f"http://127.0.0.1:{static_server}/index.html")
    page.fill("#url", f"ws://127.0.0.1:{port}/ws")
    page.click("#connect")

    deadline = time.time() + 15
    while time.time() < deadline:
        if page.locator("#wsState").inner_text() == "ready":
            break
        time.sleep(0.1)
    else:
        pytest.fail(f"WS not ready; state={page.locator('#wsState').inner_text()}")

    page.click("#startCall")
    log_text = _wait_log_contains(page, "offer.rpc", timeout_s=20)
    if "offer.rpc" not in log_text:
        fk_tail = fk_log.read_text()[-2000:] if fk_log.exists() else "(no log)"
        diag = "\n".join([
            "=== UI log ===", log_text,
            "=== console ===", "\n".join(console_msgs[-30:]),
            "=== page errors ===", "\n".join(page_errors),
            "=== flowkit tail ===", fk_tail,
        ])
        pytest.fail(f"start call did not reach offer.rpc\n{diag}")


def test_api_explorer_hangup(fake_mic_page, flowkit_server, static_server):
    page = fake_mic_page
    port, _ = flowkit_server
    page.goto(f"http://127.0.0.1:{static_server}/index.html")
    page.fill("#url", f"ws://127.0.0.1:{port}/ws")
    page.click("#connect")
    deadline = time.time() + 15
    while time.time() < deadline:
        if page.locator("#wsState").inner_text() == "ready":
            break
        time.sleep(0.1)
    page.click("#startCall")
    _wait_log_contains(page, "offer.rpc", timeout_s=20)
    page.click("#hangupBtn")
    deadline = time.time() + 5
    while time.time() < deadline:
        if "ended" in page.locator("#callState").inner_text():
            break
        time.sleep(0.1)
    assert "ended" in page.locator("#callState").inner_text()


def test_health_endpoint(flowkit_server):
    port, _ = flowkit_server
    assert _read_health(port) == {"status": "ok"}

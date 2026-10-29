"""Shared fixtures for end-to-end tests.

Each fixture is best-effort: if a binary or credential is missing the test
that depends on it is skipped, never erroneously failed.
"""
from __future__ import annotations

import contextlib
import http.server
import math
import os
import shutil
import socket
import socketserver
import struct
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BIN = PROJECT_ROOT / "bin" / "flowkit"
EXAMPLES = PROJECT_ROOT / "examples"
FIXTURES = PROJECT_ROOT / "tests" / "fixtures" / "audio"
FIXTURES.mkdir(parents=True, exist_ok=True)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(port: int, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"port {port} did not open within {timeout}s")


@pytest.fixture(scope="session")
def flowkit_binary() -> Path:
    if not BIN.exists():
        pytest.skip(f"flowkit binary missing — run `make build` first")
    return BIN


@pytest.fixture
def flowkit_server(flowkit_binary: Path) -> Iterator[tuple[int, Path]]:
    """Starts a fresh flowkit on a random port with mock providers.

    Yields (port, log_path).
    """
    port = _free_port()
    log_path = FIXTURES.parent / f"flowkit-{port}.log"
    config = f"""rpc:
  listen: "127.0.0.1:{port}"
  hello_timeout: 5s
asr:
  provider: mock
tts:
  provider: mock
store:
  type: local
node:
  id: e2e-{port}
observability:
  log_level: warn
"""
    cfg_path = FIXTURES.parent / f"flowkit-{port}.yaml"
    cfg_path.write_text(config)

    proc = subprocess.Popen(
        [str(flowkit_binary), "-c", str(cfg_path)],
        stdout=open(log_path, "w"),
        stderr=subprocess.STDOUT,
    )
    try:
        _wait_for_port(port)
        yield port, log_path
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        cfg_path.unlink(missing_ok=True)


@pytest.fixture(scope="session")
def static_server() -> Iterator[int]:
    """Serves examples/api_explorer over plain HTTP."""
    port = _free_port()

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(EXAMPLES / "api_explorer"), **kwargs)

        def log_message(self, *_):
            return

    srv = socketserver.TCPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        _wait_for_port(port)
        yield port
    finally:
        srv.shutdown()
        srv.server_close()


@pytest.fixture(scope="session")
def hello_wav() -> Path:
    """Synthesises a 1 second 16 kHz mono WAV (credential-free fixture)."""
    path = FIXTURES / "hello.wav"
    if path.exists():
        return path

    sample_rate = 16000
    duration_s = 1
    n_samples = sample_rate * duration_s
    samples = [
        int(math.sin(2 * math.pi * 440 * i / sample_rate) * 0.3 * 32767)
        for i in range(n_samples)
    ]

    with open(path, "wb") as f:
        # RIFF header
        f.write(b"RIFF")
        f.write(struct.pack("<I", 36 + n_samples * 2))
        f.write(b"WAVE")
        # fmt subchunk
        f.write(b"fmt ")
        f.write(struct.pack("<I", 16))               # subchunk size
        f.write(struct.pack("<H", 1))                # audio format = PCM
        f.write(struct.pack("<H", 1))                # channels
        f.write(struct.pack("<I", sample_rate))
        f.write(struct.pack("<I", sample_rate * 2))  # byte rate
        f.write(struct.pack("<H", 2))                # block align
        f.write(struct.pack("<H", 16))               # bits per sample
        # data subchunk
        f.write(b"data")
        f.write(struct.pack("<I", n_samples * 2))
        for s in samples:
            f.write(struct.pack("<h", s))

    return path


def pytest_collection_modifyitems(config, items):
    """Skip e2e tests in environments missing prerequisites."""
    has_chromium = shutil.which("playwright") is not None
    try:
        from playwright.sync_api import sync_playwright  # noqa: F401
        has_chromium = True
    except ImportError:
        has_chromium = False
    if has_chromium:
        return
    skip_browser = pytest.mark.skip(reason="playwright not installed")
    for item in items:
        if "playwright" in item.keywords or "browser_e2e" in item.name:
            item.add_marker(skip_browser)

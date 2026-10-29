# flowkit-sdk

Python SDK for the [Flowkit](https://github.com/DeepFlowAI/FlowKit) AI voice kernel.

> Status: alpha (v0.3.0). WebRTC and SIP calls, ASR/TTS and LLM RPCs are
> usable; the API may still change between minor versions.

## Install (editable, from this repo)

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
```

## Minimal example

```python
import asyncio
from flowkit import Kernel

async def main():
    async with Kernel("ws://localhost:9600/ws") as kernel:
        # System.hello already completed inside __aenter__.

        @kernel.on("call.incoming")
        async def handle_incoming(event):
            print("incoming:", event)

        await asyncio.sleep(60)  # serve until cancelled

asyncio.run(main())
```

A kernel that listens beyond localhost requires its `FLOWKIT_API_KEY`; pass it
as `Kernel(url, api_key=...)` and the SDK sends it as a Bearer token.

For the full API, see `flowkit/kernel.py` and `flowkit/call.py`.

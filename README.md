# Flowkit

> **AI 原生的实时语音内核** — Go + Pion + sipgo + Python SDK
> 把电话和 AI 之间的音频管道接好，业务层只关心 `call.say()` / `call.listen()`。

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/go-1.26+-00ADD8.svg)](https://go.dev)
[![Python](https://img.shields.io/badge/python-3.10+-3776AB.svg)](https://www.python.org)

```python
async with Kernel("ws://localhost:9600/ws") as kernel:
    @kernel.on("call.incoming")
    async def handle(event):
        call = Call(kernel, event.call_id)
        await call.answer()
        await call.listen()

    @kernel.on("call.speech_end")
    async def reply(event):
        call = Call(kernel, event.call_id)
        await call.say(f"You said: {event.text}")

    await asyncio.Future()  # serve forever
```

That's it — pick up phone calls, speak the recognised text back, no
URLs, no `curl`, no protocol details.

---

## What's in the box

| Component | Where | What it does |
|---|---|---|
| Flowkit kernel | `cmd/flowkit/` | Go binary. JSON-RPC server + WebRTC + SIP + ASR/TTS adapters |
| Python SDK | `sdk/python/` | `pip install flowkit-sdk` — `Kernel`, `Call`, async API |
| API Explorer | `examples/api_explorer/` | Single-page UI: WebRTC, TTS, ASR, kernel LLM voice chat |
| Docker deploy | `deploy/docker/` | Single + 3-instance docker-compose with nginx sticky routing |

## Transports

| Transport | Codec | Status |
|---|---|---|
| WebRTC (browser) | PCMU 8 kHz via Pion | ✅ shipped |
| SIP (telephony) | PCMU 8 kHz via emiago/sipgo | ✅ shipped (signalling + media); real trunk integration is one config away |
| Unix Socket | JSON-RPC text framing | ✅ shipped |

All three feed the same `transport.MediaIO` interface; the SDK doesn't
need to know which one is in use.

## Quickstart (single instance)

```bash
# 1. Build + run with mock ASR/TTS (no external creds needed)
make build
./bin/flowkit -c flowkit.yaml &

# 2. Open API Explorer (browser UI — no separate Python agent needed)
python3 -m http.server 8080 -d examples/api_explorer
# → http://localhost:8080  Connect → Start call → Speak / Listen
```

For a real (audible) experience, set `ASR_PROVIDER=aliyun` /
`TTS_PROVIDER=aliyun` and fill `ALIYUN_*` credentials in `.env.local`.

By default the kernel listens on `127.0.0.1:9600` and needs no key, which is
what the steps above use. To reach it from another machine, see
[Security](#security).

## Production deploy

```bash
# Every non-local listener requires a shared API key.
export FLOWKIT_API_KEY=$(openssl rand -hex 32)

# Single instance with local Jaeger for OTel tracing
make compose-up

# Three instances behind nginx with consistent-hash sticky routing
make compose-multi

# Point at your existing OTel backend (Jaeger, Grafana Tempo, etc.)
docker run -e RPC_LISTEN=0.0.0.0:9600 -e FLOWKIT_API_KEY \
           -e OTEL_EXPORTER_OTLP_ENDPOINT=https://logs.example.com \
           -e OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer ..." \
           -p 9600:9600 flowkit:v0.1.0
```

The Compose files and Dockerfile live in [`deploy/docker/`](./deploy/docker/);
`deploy/docker/onnxruntime/fetch.sh` downloads the ONNX Runtime library the
image needs. All settings are environment variables, documented in
[`.env.example`](./.env.example) and [`flowkit.yaml`](./flowkit.yaml).

## Security

- **API key.** `/ws` (JSON-RPC) and `/media/pcm` require
  `Authorization: Bearer <FLOWKIT_API_KEY>`. Browsers, which cannot set that
  header on a WebSocket, may pass `?api_key=` instead. The kernel refuses to
  start when `RPC_LISTEN` is not a loopback address and no key is set;
  `FLOWKIT_ALLOW_INSECURE_NO_AUTH=true` overrides that on a network you fully
  trust. `/health` and `/metrics` stay open for probes.
- **Outbound URLs.** RPCs that make the kernel fetch or connect to a URL
  (`audio_url`, `turn_url`, external agent `ws_url`) can be restricted with
  `FLOWKIT_OUTBOUND_URL_ALLOWED_HOSTS` (comma-separated hosts; a leading `.`
  matches subdomains).
- **SIP.** Inbound INVITEs from peers that match no configured trunk are
  rejected (`TELECOM_REJECT_UNKNOWN_PEER`, default `true`). Keep `SIP_ALLOW_ORIGINATE`
  off unless your application places outbound calls, and firewall the RPC port
  to your application servers.

## Architecture in one diagram

```
                  Browser / SIP Trunk
                         │
                    SDP / RTP
                         │
   ┌──────────────────────────────────────────┐
   │  Flowkit kernel (Go)                     │
   │  ┌─────────┐  ┌─────────┐  ┌──────────┐  │
   │  │ WebRTC  │  │  SIP    │  │  Unix    │  │
   │  └────┬────┘  └────┬────┘  └────┬─────┘  │
   │       └─── MediaIO ──┘            │      │
   │              │                    │      │
   │         AudioBus              JSON-RPC   │
   │              │                    │      │
   │      ┌───────┴────────┐           │      │
   │      ▼                ▼           │      │
   │   ASR Provider   TTS Provider     │      │
   │   (Aliyun / mock)                 │      │
   └───────────────────────────────────┼──────┘
                                       │
                                       ▼
                          Python SDK / your business code
                          (LLM, CRM, dashboards — your problem)
```

## Why this exists

Off-the-shelf voice platforms (Twilio Voice, Aliyun 智能联络中心) bundle
their own LLM, their own dashboards, their own opinions — and you pay
for all of it whether or not you use it. Flowkit is the part you actually
need: the **audio pipe** and a clean SDK to drive it. The Python LLM /
CRM / dashboard layer is yours.

This means:

- **LLM via kernel `llm.chat` RPC** (optional). Configure `LLM_*` in `.env`; use API Explorer or your Python app.
- **No business state in the kernel.** Flowkit doesn't know what a
  "customer" is. It hangs up calls and forgets them.
- **Standards out, opinions in.** WebRTC, SIP, JSON-RPC, OTLP, G.711 —
  not invented here.

## Observability

Set `OTEL_EXPORTER_OTLP_ENDPOINT` in your env and you'll see a complete
trace tree for every call:

```
sip.invite_inbound (or webrtc.offer)
   ├─ call.answer
   ├─ call.listen
   │   └─ <your asr session attributes>
   └─ call.say
       └─ <your tts session attributes>
```

Plus a Prometheus `/metrics` endpoint exposing
`flowkit_{uptime_seconds, conns_active, calls_active, calls_total,
draining, build_info}`. No `prometheus/client_golang` dependency.

## Status

This is v0.1.0 — production-ready for WebRTC-side use; SIP works
locally and against any softphone, with the real-trunk integration
gated on the user procuring a SIP trunk from a vendor.

## Documentation

| Audience | Read |
|---|---|
| **Operator** (deploying Flowkit) | [`deploy/docker/`](./deploy/docker/) |
| **SDK user** (writing Python apps) | [`sdk/python/README.md`](./sdk/python/README.md) |
| **Examples** | [`examples/`](./examples/) — API Explorer browser demo; SDK usage in [`sdk/python/tests/`](./sdk/python/tests/) |

## License

[AGPL-3.0](./LICENSE). See [`NOTICE`](./NOTICE) for third-party
attributions.

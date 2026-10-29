# API Explorer

Single-page browser harness that exercises every JSON-RPC method on the
Flowkit kernel from one UI. Each card is labelled with the exact method
it invokes; the page bottom streams every request / response /
notification with timestamps.

Use this for manual end-to-end verification when you change the kernel.
Playwright E2E: `make e2e` (`tests/e2e/test_api_explorer.py`).

## What's covered

| Card | Methods | Purpose |
|---|---|---|
| 1 Connect | `system.hello` | WebSocket open + handshake; shows `server_info` |
| 2 Start call | `webrtc.offer`, `call.answer`, `webrtc.ice` (notif) | Mic capture, SDP exchange, auto-answer |
| 2b SIP inbound | `call.incoming`, `call.bridge`, `llm.chat` | Dropdown: **notify** / **bridge** / **llm** (Explorer simulates Agent policy) |
| 3 Speak text | `call.say`, `call.play_end` (notif) | TTS through outbound track |
| 4 Listen | `call.listen`, `call.stop`, `call.speech_*` (notif) | Live partial + final transcripts |
| 5 LLM voice chat | `llm.chat`, `call.listen`, `call.say`, `call.stop`, `llm.token` / `llm.done` | Voice loop: ASR → LLM → sentence-stream TTS; barge-in on `speech_partial` |
| 6 Hangup | `call.hangup` | Tears down the call (WS stays open) |
| 7 Live log | — | Full JSON-RPC trace, with `system.health` filter |

## Prerequisites

- Go 1.22+ for building the kernel
- Python 3 for serving the static page (no build step)
- A modern browser (Chrome / Firefox / Safari)
- For Aliyun NLS providers: an Aliyun AccessKey with NLS enabled, an NLS
  AppKey — drop them into `.env.local` at repo root (see `.env.example`)

## Run

Three steps, three terminals (or use a process manager — these are all
foreground).

### 1. Build the kernel

```bash
make build
```

### 2. Boot Flowkit (with real Aliyun NLS)

```bash
set -a; source .env.local; set +a
export ASR_PROVIDER=aliyun TTS_PROVIDER=aliyun \
       NODE_ID=flowkit-local NODE_ADDR=localhost:9600 \
       LOG_LEVEL=debug
./bin/flowkit -c flowkit.yaml
```

Skip the `ASR_PROVIDER=aliyun TTS_PROVIDER=aliyun` line if you want to
test against the in-process mocks (no Aliyun cost, but the audio won't
be real Mandarin).

`NODE_ID` and `NODE_ADDR` are required by config validation; an empty
`NODE_ID=` in `.env.local` overrides the YAML default to "", so set
them explicitly.

### 3. Serve this directory

Browsers refuse `getUserMedia` from `file://`, so the page must be
served over HTTP.

```bash
python3 -m http.server 8080 -d examples/api_explorer
```

Then open <http://localhost:8080>.

## Walkthrough

1. **Connect** — defaults to `ws://localhost:9600/ws`. Hit Connect. WS
   state pill goes `connecting → open → ready`. For a kernel started with
   `FLOWKIT_API_KEY` (required whenever it listens beyond localhost), paste the
   key into **API Key**; the page sends it as `?api_key=` because browsers
   cannot set headers on a WebSocket. Query strings can end up in proxy access
   logs, so use a throwaway key for remote testing.
2. **Start call** — grants mic access on first run. Pip status walks
   through `starting → answering → answered`. Remote `<audio>`
   appears below — that's where TTS will play.
3. **Speak text** — type a Chinese sentence, pick a voice, click Say.
   Audio plays out the `<audio>` element within ~500 ms.
4. **Listen** — Start listening, then speak Mandarin. Partial
   transcripts appear in grey, finals in bold black.
5. **LLM voice chat** — with **语音对话** checked (default): auto Listen, speak
   Mandarin → LLM → streaming TTS; barge-in while AI speaks.

### WebRTC fast connect (Start call)

The explorer uses **Trickle ICE** (industry standard in CN/LiveKit-style stacks):

1. **Connect** pre-warms an ICE candidate pool (`iceCandidatePoolSize: 10`).
2. **Start call** sends the SDP offer immediately — no 5s STUN wait.
3. Local / remote candidates trickle via `webrtc.ice` notifications.
4. `system.hello` returns `ice_servers` from kernel config (domestic STUN first).

Timing lines in the live log show `mic` → `offer.local` → `offer.rpc` → `answer.rpc` → `pc.connected`.

Server-side: when `WEBRTC_PUBLIC_IP` / `NAT1To1IP` is set, the kernel embeds host candidates within ~300ms and still trickles the rest.

6. **Hangup** — closes the call cleanly; you can reconnect by clicking
   Start call again.

### SIP inbound test (Card 2b)

Kernel SIP is always **notify** (broadcast `call.incoming` only). The Explorer **dropdown** simulates three application-layer policies:

| Mode | What Explorer does |
|------|---------------------|
| **notify** | Log event; manual Bridge / LLM / Hangup buttons |
| **bridge** | Agent ready (WebRTC) → auto `call.bridge` on phone call |
| **llm** | Auto `call.answer` + `listen` + `llm.chat` + TTS on SIP leg |

Server `.env` hint (optional default for dropdown after Connect):

```bash
SIP_INBOUND_MODE=notify   # exposed in system.hello.sip.inbound_mode
```

URL override: `?sip_mode=bridge` or `?sip_mode=llm`

### SIP → WebRTC bridge (bridge mode detail)

On the **kernel host** `.env` (see `.env.example`):

```bash
SIP_LISTEN_ADDR=0.0.0.0:5060
SIP_EXT_IP=auto                    # or your public IP
SIP_TRUNK_VENDOR=zadarma           # example
SIP_TRUNK_SERVER=sip.zadarma.com
SIP_TRUNK_USERNAME=...
SIP_TRUNK_PASSWORD=...
SIP_TRUNK_DID=+86138...            # your PSTN DID
```

Open **UDP 5060** (SIP) and **UDP 16384–32767** (RTP) on the cloud security group.

1. **Connect** — `system.hello` shows `sip.enabled` and DID.
2. **Agent 就绪** — same as Start call but disables LLM voice loop; waits for PSTN.
3. **Dial the DID** from your phone — kernel emits `call.incoming` (transport=sip), explorer calls `call.bridge` to your WebRTC leg.
4. Talk — phone audio plays in the browser `<audio>` element; mic goes to the phone.

Either order works: phone first queues the SIP leg until Agent 就绪, or Agent first then phone calls in.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| WS state stuck at `connecting` | macOS / system proxy intercepts `localhost` | Add `localhost, 127.0.0.1` to the proxy bypass list |
| WS closes right after Connect, kernel logs `rpc auth rejected` | Kernel has `FLOWKIT_API_KEY` set and the page sent none or a wrong one | Paste the kernel's key into **API Key** |
| `mic_denied` after Start call | Browser blocked mic, or page served from `file://` | Use HTTP origin; grant mic permission |
| `call.answer` succeeds but no audio | PCMU codec mismatch (regression) | Check `internal/transport/webrtc/peer.go` registers ONLY PCMU on its custom MediaEngine |
| TTS plays only one syllable then cuts off | `runOutgoingPump` not chunking into 20 ms frames | See `internal/transport/webrtc/mediaio.go` |
| `speech_start` fires but no `speech_partial` ever | Incoming track is Opus, not PCMU (codec mismatch) | Same MediaEngine fix as above |
| Debug logs missing from kernel stderr | `LOG_LEVEL` not honored | `internal/observability/otel.go` reads `LOG_LEVEL` for the stderr handler |
| `config invalid: node.id required` on boot | `NODE_ID=` in `.env.local` overrides YAML default | Export `NODE_ID=flowkit-local` after sourcing `.env.local` |

## Wire-protocol reference

Full method shapes / event payloads: see `sdk/python/README.md` and the
inline comments in `main.py`.

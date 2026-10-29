# Flowkit examples

| Example | Purpose |
|---------|---------|
| [`api_explorer/`](./api_explorer/) | **Single-page acceptance UI** — Connect, WebRTC (Trickle ICE), TTS, ASR, kernel LLM voice chat, hangup. Use this for local and remote end-to-end validation. |

```bash
make build
./bin/flowkit -c flowkit.yaml
python3 -m http.server 8080 -d examples/api_explorer
# open http://127.0.0.1:8080/?host=<your-kernel-ip>
```

Python SDK integration patterns live in `sdk/python/tests/`.

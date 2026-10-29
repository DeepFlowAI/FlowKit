# SIPp driver (S1)

Industry-standard SIP load generator for **S1 · SIP 呼入保持**.

## Install

```bash
# macOS
brew install sipp

# Debian/Ubuntu
sudo apt install sipp
```

## Scenario

`s1_uac.xml`: INVITE (PCMU) → 200 → ACK → hold (`-d`) → BYE.

Call-ID uses `flowkit-cap-///[call_id]` so Flowkit does not treat legs as CN trunk probes
(numeric `X-Y-Z` Call-IDs get 486).

## Manual run

```bash
python3 tools/capacity/drivers/sipp/run_s1.py \
  --target 127.0.0.1:5060 -n 10 --cps 10 --hold 60
```

## Orchestrator

S1 scenario YAML sets `engine: sipp` under `drivers[].type: sip_inbound`.
Use `engine: go` to fall back to the Go sip driver.

```bash
bash tools/capacity/run.sh --profile profiles/local.yaml --mode boundary --scenarios s1
```

# Flowkit Capacity Test Tools

Orchestrated boundary load testing for Flowkit.

## Quick start

```bash
chmod +x run.sh capacity.py
bash run.sh --profile profiles/local.yaml --mode fixed --scenarios s1 --fixed-n 10
```

## Layout

| Path | Purpose |
|------|---------|
| `profiles/` | Local vs remote target config |
| `scenarios/` | S1–S4 YAML specs |
| `results/` | Run output (gitignored) |
| `run.sh` | CLI entry |

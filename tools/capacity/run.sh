#!/usr/bin/env bash
# Flowkit capacity test entrypoint.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

PROFILE="profiles/local.yaml"
MODE="boundary"
SCENARIOS="all"
STAMP="$(date -u +%Y%m%d-%H%M%S 2>/dev/null || date +%Y%m%d-%H%M%S)"
OUT="results/$STAMP"

usage() {
  cat <<'EOF'
Usage: run.sh [options]

Options:
  --profile PATH       Target profile (default: profiles/local.yaml)
  --mode MODE          boundary | fixed | steady (default: boundary)
  --scenarios LIST     s1,s2,s3,s4 or all (default: all)
  --output DIR         Results directory (default: results/<timestamp>)
  --i-know-its-remote  Required when profile has confirm_remote: true
  -h, --help           Show this help

Examples:
  bash run.sh --profile profiles/local.yaml --scenarios all
  bash run.sh --profile profiles/remote-prod.yaml --scenarios s1,s2 --i-know-its-remote

Note: S1–S4 drivers + boundary ramp + report are available.
EOF
}

CONFIRM_REMOTE=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile) PROFILE="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --scenarios) SCENARIOS="$2"; shift 2 ;;
    --output) OUT="$2"; shift 2 ;;
    --i-know-its-remote) CONFIRM_REMOTE=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

mkdir -p "$OUT"

if [[ ! -f "$PROFILE" ]]; then
  echo "Profile not found: $PROFILE" >&2
  exit 1
fi

if grep -q 'confirm_remote: true' "$PROFILE" && [[ "$CONFIRM_REMOTE" != true ]]; then
  echo "Remote profile requires --i-know-its-remote" >&2
  exit 1
fi

if [[ -f requirements.txt ]]; then
  python3 -c "import yaml" 2>/dev/null || pip3 install -q -r requirements.txt
fi

if [[ -f capacity.py ]]; then
  exec python3 capacity.py \
    --profile "$PROFILE" \
    --mode "$MODE" \
    --scenarios "$SCENARIOS" \
    --output "$OUT" \
    ${CONFIRM_REMOTE:+--i-know-its-remote} \
    "$@"
fi

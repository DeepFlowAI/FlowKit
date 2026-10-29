#!/usr/bin/env python3
"""Run SIPp for Flowkit S1 and emit __CAPACITY_RESULT__ JSON."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

SCENARIO = Path(__file__).resolve().parent / "s1_uac.xml"


def find_sipp() -> str:
    path = shutil.which("sipp")
    if not path:
        raise RuntimeError(
            "sipp not found in PATH (install: brew install sipp / apt install sipp)"
        )
    return path


def parse_target(target: str) -> tuple[str, int, str]:
    if "@" in target:
        user, rest = target.split("@", 1)
    else:
        user, rest = "flowkit", target
    if ":" in rest:
        host, port_s = rest.rsplit(":", 1)
        return user, host, int(port_s)
    return user, rest, 5060


def parse_sipp_stats(text: str, target_n: int) -> dict:
    successful = 0
    failed = 0
    m_ok = re.search(r"Successful call\s*\|\s*\d+\s*\|\s*(\d+)", text)
    m_fail = re.search(r"Failed call\s*\|\s*\d+\s*\|\s*(\d+)", text)
    if m_ok:
        successful = int(m_ok.group(1))
    if m_fail:
        failed = int(m_fail.group(1))

    # Response Time 1 = first INVITE transaction (rtd="true" on 200)
    p99_ms = 0.0
    avg_ms = 0.0
    m_rt = re.search(
        r"Response Time 1\s*\|\s*\d{2}:\d{2}:\d{2}:\d{3}\s*\d*\s*\|\s*(\d{2}):(\d{2}):(\d{2}):(\d{3})\s*(\d{3})?",
        text,
    )
    if m_rt:
        h, mi, s, ms, sub = m_rt.groups()
        total_ms = (
            int(h) * 3600000
            + int(mi) * 60000
            + int(s) * 1000
            + int(ms)
            + (int(sub or 0))
        )
        avg_ms = float(total_ms)
        p99_ms = float(total_ms)

    errors: list[str] = []
    if failed > 0:
        errors.append(f"sipp reported {failed} failed calls")
    for line in text.splitlines():
        if "Unexpected-Msg" in line:
            continue
        if "Unexpected" in line or "failed" in line.lower():
            s = line.strip()
            if s and len(errors) < 5:
                errors.append(s[:200])

    completed = successful
    rate = (completed / target_n) if target_n > 0 else 0.0
    status = {"200": completed}
    if failed:
        status["error"] = failed

    return {
        "driver": "sipp",
        "target_n": target_n,
        "completed": completed,
        "failed": max(0, target_n - completed),
        "success_rate": rate,
        "setup_p50_ms": avg_ms,
        "setup_p99_ms": p99_ms,
        "avg_setup_ms": avg_ms,
        "status_counts": status,
        "errors": errors[:20],
    }


def main() -> int:
    p = argparse.ArgumentParser(description="Flowkit S1 SIPp driver wrapper")
    p.add_argument("--target", default="127.0.0.1:5060", help="host:port")
    p.add_argument("--user", default="flowkit", help="SIP request-URI user")
    p.add_argument("-n", type=int, default=10, help="total calls this step")
    p.add_argument("--cps", type=int, default=10, help="call rate (-r)")
    p.add_argument("--hold", type=int, default=60, help="hold seconds after answer")
    p.add_argument("--bind-ip", default="127.0.0.1", help="local SIP bind (-i)")
    p.add_argument("--bind-port", type=int, default=0, help="local SIP port (-p), 0=auto")
    args = p.parse_args()

    if args.n <= 0:
        print("error: -n must be > 0", file=sys.stderr)
        return 2

    sipp = find_sipp()
    if not SCENARIO.is_file():
        print(f"error: missing scenario {SCENARIO}", file=sys.stderr)
        return 2

    user, host, port = parse_target(args.target)
    if args.user:
        user = args.user
    hold_ms = max(0, args.hold) * 1000
    cps = args.cps if args.cps > 0 else args.n

    cmd = [
        sipp,
        "-sf",
        str(SCENARIO),
        "-s",
        user,
        "-i",
        args.bind_ip,
        "-r",
        str(cps),
        "-l",
        str(args.n),
        "-m",
        str(args.n),
        "-d",
        str(hold_ms),
        "-rtp_echo",
        f"{host}:{port}",
    ]
    if args.bind_port > 0:
        cmd.extend(["-p", str(args.bind_port)])

    t0 = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    wall = time.time() - t0
    combined = (proc.stdout or "") + "\n" + (proc.stderr or "")

    payload = parse_sipp_stats(combined, args.n)
    payload["wall_seconds"] = round(wall, 3)

    print(f"__CAPACITY_RESULT__={json.dumps(payload)}")
    if payload["completed"] < args.n:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Sample Flowkit /metrics and optional process RSS/CPU."""

from __future__ import annotations

import json
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


_METRIC_RE = re.compile(r"^(flowkit_[a-z_]+)\s+(\S+)")


def parse_prometheus(text: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = _METRIC_RE.match(line)
        if not m:
            continue
        name, val = m.group(1), m.group(2)
        try:
            out[name] = float(val)
        except ValueError:
            continue
    return out


def _ps_sample(process_name: str) -> tuple[float, float]:
    try:
        if sys.platform == "darwin":
            proc = subprocess.run(
                ["pgrep", "-x", process_name],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if proc.returncode != 0 or not proc.stdout.strip():
                return 0.0, 0.0
            pids = proc.stdout.strip().split("\n")
            cpu = 0.0
            rss_kb = 0.0
            for pid in pids:
                stat = subprocess.run(
                    ["ps", "-p", pid, "-o", "pcpu=", "-o", "rss="],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if stat.returncode != 0 or not stat.stdout.strip():
                    continue
                parts = stat.stdout.strip().split()
                if len(parts) >= 2:
                    cpu += float(parts[0])
                    rss_kb += float(parts[1])
            return cpu, rss_kb / 1024.0

        proc = subprocess.run(
            ["ps", "-C", process_name, "-o", "pcpu,rss", "--no-headers"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if proc.returncode != 0 or not proc.stdout.strip():
            return 0.0, 0.0
        cpu = 0.0
        rss_kb = 0.0
        for line in proc.stdout.strip().split("\n"):
            parts = line.split()
            if len(parts) >= 2:
                cpu += float(parts[0])
                rss_kb += float(parts[1])
        return cpu, rss_kb / 1024.0
    except Exception:
        return 0.0, 0.0


@dataclass
class Monitor:
    http_base: str
    process_name: str = "flowkit"
    interval: float = 1.0
    samples: list[dict[str, Any]] = field(default_factory=list)
    _stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _thread: threading.Thread | None = field(default=None, repr=False)

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop.is_set():
            sample = self._sample_once()
            if sample:
                self.samples.append(sample)
            self._stop.wait(self.interval)

    def _sample_once(self) -> dict[str, Any] | None:
        ts = datetime.now(timezone.utc).isoformat()
        sample: dict[str, Any] = {"ts": ts}
        url = self.http_base.rstrip("/") + "/metrics"
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=3) as resp:
                body = resp.read().decode()
            metrics = parse_prometheus(body)
            sample.update(metrics)
        except (urllib.error.URLError, TimeoutError, OSError):
            return None

        cpu, rss = _ps_sample(self.process_name)
        sample["cpu_pct"] = cpu
        sample["rss_mb"] = rss
        return sample

    def write_jsonl(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            for row in self.samples:
                f.write(json.dumps(row, default=str) + "\n")

    def summary(self) -> dict[str, Any]:
        if not self.samples:
            return {}
        calls = [s.get("flowkit_calls_active", 0) for s in self.samples]
        rss = [s.get("rss_mb", 0) for s in self.samples]
        cpu = [s.get("cpu_pct", 0) for s in self.samples]
        goroutines = [s.get("flowkit_goroutines", 0) for s in self.samples]
        alloc = [s.get("flowkit_mem_alloc_bytes", 0) for s in self.samples]
        peak_calls = max(calls) if calls else 0
        peak_rss = max(rss) if rss else 0
        baseline_rss = rss[0] if rss else 0
        mem_per_call = 0.0
        if peak_calls > 0 and peak_rss > baseline_rss:
            mem_per_call = (peak_rss - baseline_rss) / peak_calls
        conns = [s.get("flowkit_conns_active", 0) for s in self.samples]
        mem_sys = [s.get("flowkit_mem_sys_bytes", 0) for s in self.samples]
        return {
            "samples": len(self.samples),
            "calls_peak": int(peak_calls),
            "calls_avg": sum(calls) / len(calls) if calls else 0,
            "rss_peak_mb": peak_rss,
            "rss_baseline_mb": baseline_rss,
            "cpu_peak_pct": max(cpu) if cpu else 0,
            "cpu_avg_pct": sum(cpu) / len(cpu) if cpu else 0,
            "goroutines_peak": int(max(goroutines)) if goroutines else 0,
            "mem_alloc_peak_bytes": int(max(alloc)) if alloc else 0,
            "mem_sys_peak_bytes": int(max(mem_sys)) if mem_sys else 0,
            "conns_peak": int(max(conns)) if conns else 0,
            "mem_per_call_mb": round(mem_per_call, 3),
        }

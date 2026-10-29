#!/usr/bin/env python3
"""Flowkit capacity test orchestrator."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent.parent

sys.path.insert(0, str(ROOT))
from monitor.collector import Monitor, parse_prometheus  # noqa: E402
from report.analyze import recommended, render_report, write_summary_json  # noqa: E402
from report.boundary import classify_scenario_boundary  # noqa: E402
from report.html_report import write_html_report  # noqa: E402

SCENARIO_FILES = {
    "s1": "s1_sip_inbound.yaml",
    "s2": "s2_webrtc_offer.yaml",
    "s3": "s3_sip_bridge_webrtc.yaml",
    "s4": "s4_full_cc_mock.yaml",
}

IMPLEMENTED = {"s1", "s2", "s3", "s4"}


def load_yaml(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Flowkit capacity orchestrator")
    p.add_argument("--profile", required=True, help="Profile YAML path")
    p.add_argument("--scenarios", default="all", help="s1,s2,... or all")
    p.add_argument("--mode", default="boundary", choices=["boundary", "fixed", "steady"])
    p.add_argument("--fixed-n", type=int, default=0, help="Concurrent N for fixed mode")
    p.add_argument("--output", required=True, help="Output directory")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--skip-preflight", action="store_true")
    p.add_argument("--i-know-its-remote", action="store_true")
    return p.parse_args()


def resolve_scenarios(spec: str) -> list[str]:
    if spec.strip().lower() == "all":
        return list(SCENARIO_FILES.keys())
    return [s.strip().lower() for s in spec.split(",") if s.strip()]


def preflight(profile: dict[str, Any], confirm_remote: bool) -> None:
    safety = profile.get("safety") or {}
    if safety.get("confirm_remote") and not confirm_remote:
        raise SystemExit("Remote profile requires --i-know-its-remote")
    if not (profile.get("preflight") or {}).get("check_health", True):
        return
    base = profile["target"]["http_base"].rstrip("/")
    try:
        with urllib.request.urlopen(base + "/health", timeout=5) as resp:
            if resp.status != 200:
                raise SystemExit(f"Preflight failed: /health status {resp.status}")
    except urllib.error.URLError as e:
        raise SystemExit(f"Preflight failed: cannot reach {base}/health: {e}") from e
    try:
        with urllib.request.urlopen(base + "/metrics", timeout=5) as resp:
            body = resp.read().decode()
        metrics = parse_prometheus(body)
        for key in ("flowkit_calls_active", "flowkit_goroutines", "flowkit_mem_alloc_bytes"):
            if key not in metrics:
                print(f"warning: /metrics missing {key}", file=sys.stderr)
    except urllib.error.URLError as e:
        raise SystemExit(f"Preflight failed: /metrics: {e}") from e


def sip_target(profile: dict[str, Any]) -> str:
    t = profile["target"]
    return f"{t['sip_host']}:{t['sip_port']}"


def fetch_metrics_snapshot(http_base: str) -> dict[str, Any]:
    base = http_base.rstrip("/")
    out: dict[str, Any] = {}
    try:
        with urllib.request.urlopen(base + "/metrics", timeout=5) as resp:
            metrics = parse_prometheus(resp.read().decode())
        out["metrics"] = metrics
        out["calls_active"] = int(metrics.get("flowkit_calls_active", 0))
        out["goroutines"] = int(metrics.get("flowkit_goroutines", 0))
        out["mem_alloc_mb"] = round(metrics.get("flowkit_mem_alloc_bytes", 0) / 1024 / 1024, 2)
        out["mem_sys_mb"] = round(metrics.get("flowkit_mem_sys_bytes", 0) / 1024 / 1024, 2)
        out["conns_active"] = int(metrics.get("flowkit_conns_active", 0))
        out["uptime_seconds"] = round(metrics.get("flowkit_uptime_seconds", 0), 1)
    except (urllib.error.URLError, TimeoutError, OSError):
        out["metrics_error"] = "unreachable"
    process_name = "flowkit"
    cpu, rss = 0.0, 0.0
    try:
        from monitor.collector import _ps_sample

        cpu, rss = _ps_sample(process_name)
    except Exception:
        pass
    out["process_cpu_pct"] = round(cpu, 1)
    out["process_rss_mb"] = round(rss, 2)
    return out


def host_ram_total_gb() -> float | None:
    try:
        if sys.platform == "darwin":
            proc = subprocess.run(
                ["sysctl", "-n", "hw.memsize"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if proc.returncode == 0:
                return round(int(proc.stdout.strip()) / 1024**3, 1)
        if sys.platform == "linux":
            with open("/proc/meminfo", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        kb = int(line.split()[1])
                        return round(kb / 1024 / 1024, 1)
    except (OSError, ValueError, subprocess.SubprocessError):
        return None
    return None


def resources_from_monitor(mon_sum: dict[str, Any]) -> dict[str, Any]:
    alloc_b = mon_sum.get("mem_alloc_peak_bytes", 0) or 0
    sys_b = mon_sum.get("mem_sys_peak_bytes", 0) or 0
    return {
        "calls_peak": int(mon_sum.get("calls_peak", 0)),
        "calls_avg": round(float(mon_sum.get("calls_avg", 0)), 1),
        "rss_peak_mb": round(float(mon_sum.get("rss_peak_mb", 0)), 2),
        "rss_baseline_mb": round(float(mon_sum.get("rss_baseline_mb", 0)), 2),
        "cpu_peak_pct": round(float(mon_sum.get("cpu_peak_pct", 0)), 1),
        "cpu_avg_pct": round(float(mon_sum.get("cpu_avg_pct", 0)), 1),
        "goroutines_peak": int(mon_sum.get("goroutines_peak", 0)),
        "mem_alloc_peak_mb": round(alloc_b / 1024 / 1024, 2),
        "mem_sys_peak_mb": round(sys_b / 1024 / 1024, 2),
        "conns_peak": int(mon_sum.get("conns_peak", 0)),
        "mem_per_call_mb": mon_sum.get("mem_per_call_mb", 0),
    }


def pick_limit_step(scenario: dict[str, Any]) -> dict[str, Any] | None:
    steps = scenario.get("ramp_steps") or []
    passed = [s for s in steps if s.get("passed")]
    if passed:
        return passed[-1]
    return steps[-1] if steps else None


def pick_breaking_step(scenario: dict[str, Any]) -> dict[str, Any] | None:
    for step in scenario.get("ramp_steps") or []:
        if not step.get("passed"):
            return step
    return None


def driver_executable(name: str) -> list[str]:
    """Prefer prebuilt linux binary on server (bin/cap-{name}-linux)."""
    for candidate in (
        REPO_ROOT / "bin" / f"cap-{name}-linux",
        REPO_ROOT / "bin" / f"cap-{name}",
    ):
        if candidate.is_file():
            return [str(candidate)]
    return ["go", "run", f"./tools/capacity/drivers/{name}"]


def sip_inbound_driver_cfg(scenario: dict[str, Any]) -> dict[str, Any]:
    for d in scenario.get("drivers") or []:
        if d.get("type") == "sip_inbound":
            return d
    return {}


def run_sipp_s1_driver(
    profile: dict[str, Any],
    scenario: dict[str, Any],
    n: int,
    cps: int,
    hold_seconds: int,
) -> dict[str, Any]:
    target = sip_target(profile)
    tcfg = profile.get("target") or {}
    drv = sip_inbound_driver_cfg(scenario)
    bind_ip = drv.get("bind_ip") or tcfg.get("sip_bind_ip") or tcfg.get("sip_host") or "127.0.0.1"
    sip_user = drv.get("sip_user") or "flowkit"
    wrapper = ROOT / "drivers" / "sipp" / "run_s1.py"
    cmd = [
        sys.executable,
        str(wrapper),
        "--target",
        target,
        "--user",
        sip_user,
        "-n",
        str(n),
        "--cps",
        str(cps),
        "--hold",
        str(hold_seconds),
        "--bind-ip",
        bind_ip,
    ]
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=max(600, hold_seconds * n // max(cps, 1) + 180),
    )
    combined = (proc.stdout or "") + (proc.stderr or "")
    m = re.search(r"__CAPACITY_RESULT__=(\{.*\})", combined)
    if not m:
        raise RuntimeError(
            f"sipp driver produced no result (exit {proc.returncode}):\n{combined[-3000:]}"
        )
    result = json.loads(m.group(1))
    result["exit_code"] = proc.returncode
    return result


def run_sip_driver(
    profile: dict[str, Any],
    n: int,
    cps: int,
    hold_seconds: int,
) -> dict[str, Any]:
    target = sip_target(profile)
    cmd = driver_executable("sip") + [
        "-target",
        target,
        "-n",
        str(n),
        "-cps",
        str(cps),
        "-hold",
        f"{hold_seconds}s",
    ]
    env = os.environ.copy()
    env.setdefault("GOFLAGS", "-mod=mod")
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=max(600, hold_seconds * n // max(cps, 1) + 120),
        env=env,
    )
    combined = (proc.stdout or "") + (proc.stderr or "")
    m = re.search(r"__CAPACITY_RESULT__=(\{.*\})", combined)
    if not m:
        raise RuntimeError(
            f"sip driver produced no result (exit {proc.returncode}):\n{combined[-2000:]}"
        )
    result = json.loads(m.group(1))
    result["exit_code"] = proc.returncode
    return result


def run_webrtc_driver(
    profile: dict[str, Any],
    n: int,
    cps: int,
    hold_seconds: int,
) -> dict[str, Any]:
    target = profile["target"]
    ws_url = target.get("ws_url") or (
        target["http_base"].replace("https://", "wss://").replace("http://", "ws://").rstrip("/") + "/ws"
    )
    ice = (target.get("ice_servers") or ["stun:stun.l.google.com:19302"])[0]
    cmd = driver_executable("webrtc") + [
        "-ws",
        ws_url,
        "-n",
        str(n),
        "-cps",
        str(cps),
        "-hold",
        f"{hold_seconds}s",
        "-stun",
        ice,
    ]
    env = os.environ.copy()
    env.setdefault("GOFLAGS", "-mod=mod")
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=max(600, hold_seconds * n // max(cps, 1) + 180),
        env=env,
    )
    combined = (proc.stdout or "") + (proc.stderr or "")
    m = re.search(r"__CAPACITY_RESULT__=(\{.*\})", combined)
    if not m:
        raise RuntimeError(
            f"webrtc driver produced no result (exit {proc.returncode}):\n{combined[-2000:]}"
        )
    result = json.loads(m.group(1))
    result["exit_code"] = proc.returncode
    return result


def run_bridge_driver(
    profile: dict[str, Any],
    n: int,
    cps: int,
    hold_seconds: int,
    timeout_seconds: int = 45,
) -> dict[str, Any]:
    target = profile["target"]
    ws_url = target.get("ws_url") or (
        target["http_base"].replace("https://", "wss://").replace("http://", "ws://").rstrip("/") + "/ws"
    )
    ice = (target.get("ice_servers") or ["stun:stun.l.google.com:19302"])[0]
    cmd = driver_executable("bridge") + [
        "-ws",
        ws_url,
        "-sip-target",
        sip_target(profile),
        "-n",
        str(n),
        "-cps",
        str(cps),
        "-hold",
        f"{hold_seconds}s",
        "-timeout",
        f"{timeout_seconds}s",
        "-stun",
        ice,
    ]
    env = os.environ.copy()
    env.setdefault("GOFLAGS", "-mod=mod")
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=max(900, hold_seconds * n // max(cps, 1) + 300),
        env=env,
    )
    combined = (proc.stdout or "") + (proc.stderr or "")
    m = re.search(r"__CAPACITY_RESULT__=(\{.*\})", combined)
    if not m:
        raise RuntimeError(
            f"bridge driver produced no result (exit {proc.returncode}):\n{combined[-2000:]}"
        )
    result = json.loads(m.group(1))
    result["exit_code"] = proc.returncode
    return result


def run_mockcc_driver(
    profile: dict[str, Any],
    scenario: dict[str, Any],
    n: int,
    cps: int,
    timeout_seconds: int = 90,
) -> dict[str, Any]:
    target = profile["target"]
    ws_url = target.get("ws_url") or (
        target["http_base"].replace("https://", "wss://").replace("http://", "ws://").rstrip("/") + "/ws"
    )
    drivers = scenario.get("drivers") or []
    mock_cfg = next((d for d in drivers if d.get("type") == "mock_cc"), {})
    say_text = mock_cfg.get("say_text", "capacity test")
    listen_sec = mock_cfg.get("listen_timeout_seconds", 5)
    cmd = driver_executable("mockcc") + [
        "-ws",
        ws_url,
        "-sip-target",
        sip_target(profile),
        "-n",
        str(n),
        "-cps",
        str(cps),
        "-say",
        say_text,
        "-listen-seconds",
        str(listen_sec),
        "-timeout",
        f"{timeout_seconds}s",
    ]
    env = os.environ.copy()
    env.setdefault("GOFLAGS", "-mod=mod")
    per_call = listen_sec + 20
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=max(900, per_call * n // max(cps, 1) + 120),
        env=env,
    )
    combined = (proc.stdout or "") + (proc.stderr or "")
    m = re.search(r"__CAPACITY_RESULT__=(\{.*\})", combined)
    if not m:
        raise RuntimeError(
            f"mockcc driver produced no result (exit {proc.returncode}):\n{combined[-2000:]}"
        )
    result = json.loads(m.group(1))
    result["exit_code"] = proc.returncode
    return result


def run_driver(
    sid: str,
    profile: dict[str, Any],
    scenario: dict[str, Any],
    n: int,
    cps: int,
    hold: int,
) -> dict[str, Any]:
    if sid == "s1":
        drv = sip_inbound_driver_cfg(scenario)
        if drv.get("engine", "sipp") == "sipp":
            return run_sipp_s1_driver(profile, scenario, n, cps, hold)
        return run_sip_driver(profile, n, cps, hold)
    if sid == "s2":
        return run_webrtc_driver(profile, n, cps, hold)
    if sid == "s3":
        return run_bridge_driver(profile, n, cps, hold)
    if sid == "s4":
        return run_mockcc_driver(profile, scenario, n, cps)
    raise ValueError(f"no driver for {sid}")


def ramp_policy_from_profile(profile: dict[str, Any]) -> dict[str, Any]:
    policy = dict(profile.get("ramp_policy") or {})
    safety = profile.get("safety") or {}
    if "kind" not in policy:
        policy["kind"] = "vertex" if safety.get("max_concurrent_cap") == 0 else "dev"
    if policy["kind"] == "dev" and not policy.get("label"):
        policy.setdefault("label", "开发验证")
        policy.setdefault(
            "description",
            "尊重 scenario load.max 与 safety.max_concurrent_cap，适合本地快速/摸底。",
        )
    if policy["kind"] == "vertex" and not policy.get("label"):
        policy.setdefault("label", "顶点探测")
        policy.setdefault(
            "description",
            "忽略 scenario load.max，按 step 递增直至失败或安全硬顶。",
        )
    policy.setdefault("vertex_hard_ceiling", 10000)
    policy.setdefault("respect_scenario_max", policy["kind"] != "vertex")
    return policy


def resolve_ramp_limits(
    profile: dict[str, Any],
    load_cfg: dict[str, Any],
) -> tuple[int, int, int, dict[str, Any]]:
    """Return (start, step, max_n, ramp_policy)."""
    policy = ramp_policy_from_profile(profile)
    safety = profile.get("safety") or {}
    start = int(load_cfg["start"])
    step = int(load_cfg["step"])
    hard = int(policy.get("vertex_hard_ceiling") or 10000)
    cap = safety.get("max_concurrent_cap")
    scenario_max = int(load_cfg.get("max") or hard)

    if policy["kind"] == "vertex":
        if cap is not None and int(cap) > 0:
            max_n = min(hard, int(cap))
        else:
            max_n = hard
    else:
        limits = [scenario_max]
        if cap is not None and int(cap) > 0:
            limits.append(int(cap))
        max_n = min(limits)
    return start, step, max_n, policy


def evaluate_step(
    driver: dict[str, Any],
    monitor_summary: dict[str, Any],
    success_cfg: dict[str, Any],
    target_n: int,
    scenario_id: str = "",
    ramp_policy: dict[str, Any] | None = None,
    ram_total_gb: float | None = None,
) -> tuple[bool, str]:
    rate = driver.get("success_rate", 0)
    min_rate = success_cfg.get("min_rate", 0.95)
    if rate < min_rate:
        return False, f"R1 success_rate {rate:.3f} < {min_rate}"
    p99 = driver.get("setup_p99_ms", 0)
    max_p99 = success_cfg.get("max_setup_p99_ms", 3000)
    if p99 > max_p99:
        return False, f"R2 setup_p99 {p99:.0f}ms > {max_p99}ms"
    calls_peak = monitor_summary.get("calls_peak", 0)
    min_factor = success_cfg.get("min_calls_factor")
    if min_factor is None and scenario_id == "s3":
        min_factor = 1.8
    if min_factor is None:
        min_factor = 0.9
    if calls_peak < target_n * min_factor:
        return False, f"R3 calls_peak {calls_peak} < {target_n * min_factor:.0f}"
    policy = ramp_policy or {}
    cpu_peak = float(monitor_summary.get("cpu_peak_pct") or 0)
    stop_cpu = policy.get("stop_on_cpu_pct")
    if stop_cpu is not None and cpu_peak >= float(stop_cpu):
        return False, f"R8 cpu_peak {cpu_peak:.1f}% >= {stop_cpu}%"
    rss_peak = float(monitor_summary.get("rss_peak_mb") or 0)
    stop_rss_pct = policy.get("stop_on_rss_pct_of_ram")
    if stop_rss_pct is not None and ram_total_gb and ram_total_gb > 0:
        rss_limit_mb = ram_total_gb * 1024 * float(stop_rss_pct) / 100
        if rss_peak >= rss_limit_mb:
            return False, f"R9 rss_peak {rss_peak:.0f}MB >= {stop_rss_pct}% RAM"
    return True, "ok"


def ramp_scenario(
    sid: str,
    profile: dict[str, Any],
    scenario: dict[str, Any],
    output_dir: Path,
    *,
    ram_total_gb: float | None = None,
) -> dict[str, Any]:
    load_cfg = scenario["load"]
    success_cfg = scenario["success"]
    start, step, max_n, ramp_policy = resolve_ramp_limits(profile, load_cfg)
    cps = load_cfg["cps"]
    hold = load_cfg["hold_seconds"]

    http_base = profile["target"]["http_base"]
    process_name = profile["target"].get("process_name") or "flowkit"
    label = sid.upper()
    mode_label = ramp_policy.get("kind", "dev")

    print(
        f"\n--- {label} ramp policy: {mode_label} "
        f"start={start} step={step} max_n={max_n} ---"
    )

    ramp_steps: list[dict[str, Any]] = []
    max_stable = 0
    stop_reason = "max reached"
    last_driver: dict[str, Any] = {}
    last_monitor: dict[str, Any] = {}

    for n in range(start, max_n + 1, step):
        print(f"\n=== {label} ramp N={n} cps={cps} hold={hold}s ===")
        monitor = Monitor(http_base=http_base, process_name=process_name, interval=1.0)
        monitor.start()
        try:
            driver = run_driver(sid, profile, scenario, n, cps, hold)
        finally:
            time.sleep(2)
            monitor.stop()
        mon_sum = monitor.summary()
        step_dir = output_dir / "ramp_steps"
        step_dir.mkdir(parents=True, exist_ok=True)
        monitor.write_jsonl(str(step_dir / f"n{n}_samples.jsonl"))

        ok, reason = evaluate_step(
            driver,
            mon_sum,
            success_cfg,
            n,
            sid,
            ramp_policy=ramp_policy,
            ram_total_gb=ram_total_gb,
        )
        resources = resources_from_monitor(mon_sum)
        step_rec = {
            "n": n,
            "completed": driver.get("completed", 0),
            "success_rate": driver.get("success_rate", 0),
            "setup_p99_ms": driver.get("setup_p99_ms", 0),
            "calls_peak": resources["calls_peak"],
            "rss_peak_mb": resources["rss_peak_mb"],
            "cpu_peak_pct": resources["cpu_peak_pct"],
            "goroutines_peak": resources["goroutines_peak"],
            "mem_alloc_peak_mb": resources["mem_alloc_peak_mb"],
            "resources": resources,
            "passed": ok,
            "reason": reason,
        }
        ramp_steps.append(step_rec)
        last_driver = driver
        last_monitor = mon_sum

        print(
            f"  success={driver.get('success_rate', 0)*100:.1f}% "
            f"p99={driver.get('setup_p99_ms', 0):.0f}ms "
            f"calls_peak={mon_sum.get('calls_peak', 0)} "
            f"rss={mon_sum.get('rss_peak_mb', 0):.1f}MB "
            f"-> {'PASS' if ok else 'STOP'} ({reason})"
        )
        if ok:
            max_stable = n
        else:
            stop_reason = reason
            break

    if max_stable >= max_n and ramp_steps and ramp_steps[-1].get("passed"):
        if ramp_policy.get("kind") == "vertex":
            stop_reason = f"R7 vertex_hard_ceiling reached N={max_n} (all PASS)"
        else:
            stop_reason = "max reached (dev cap)"

    breaking = max_stable + step if max_stable < max_n else None
    limit_step = pick_limit_step({"ramp_steps": ramp_steps}) if ramp_steps else None
    breaking_step = pick_breaking_step({"ramp_steps": ramp_steps})
    boundary = classify_scenario_boundary(
        {
            "id": sid,
            "max_stable": max_stable,
            "breaking_n": breaking_step.get("n") if breaking_step else breaking,
            "breaking_point": breaking,
            "stop_reason": stop_reason,
            "ramp_steps": ramp_steps,
            "driver_last": last_driver,
            "breaking_resources": (breaking_step or {}).get("resources"),
        },
        ram_total_gb=ram_total_gb,
        ramp_kind=ramp_policy.get("kind", "dev"),
    )
    default_names = {
        "s1": "S1 SIP inbound",
        "s2": "S2 WebRTC offer",
        "s3": "S3 SIP bridge WebRTC",
        "s4": "S4 Mock CC workflow",
    }
    return {
        "id": sid,
        "name": scenario.get("name", default_names.get(sid, sid)),
        "max_stable": max_stable,
        "breaking_point": breaking,
        "recommended": recommended(max_stable),
        "success_rate": last_driver.get("success_rate", 0),
        "setup_p99_ms": last_driver.get("setup_p99_ms", 0),
        "mem_per_call_mb": last_monitor.get("mem_per_call_mb", 0),
        "cpu_peak_pct": last_monitor.get("cpu_peak_pct", 0),
        "calls_peak": last_monitor.get("calls_peak", 0),
        "stop_reason": stop_reason,
        "ramp_steps": ramp_steps,
        "driver_last": last_driver,
        "limit_n": limit_step.get("n") if limit_step else max_stable,
        "limit_resources": (limit_step or {}).get("resources") or resources_from_monitor(last_monitor),
        "breaking_n": breaking_step.get("n") if breaking_step else breaking,
        "breaking_resources": (breaking_step or {}).get("resources"),
        "boundary_analysis": boundary,
        "ramp_policy": ramp_policy,
    }


def run_fixed(
    sid: str,
    profile: dict[str, Any],
    scenario: dict[str, Any],
    n: int,
    output_dir: Path,
) -> dict[str, Any]:
    load_cfg = scenario["load"]
    monitor = Monitor(
        http_base=profile["target"]["http_base"],
        process_name=profile["target"].get("process_name") or "flowkit",
    )
    monitor.start()
    try:
        driver = run_driver(sid, profile, scenario, n, load_cfg["cps"], load_cfg["hold_seconds"])
    finally:
        time.sleep(2)
        monitor.stop()
    mon_sum = monitor.summary()
    monitor.write_jsonl(str(output_dir / "samples.jsonl"))
    ok, reason = evaluate_step(driver, mon_sum, scenario["success"], n, sid)
    resources = resources_from_monitor(mon_sum)
    default_names = {
        "s1": "S1 SIP inbound",
        "s2": "S2 WebRTC offer",
        "s3": "S3 SIP bridge WebRTC",
        "s4": "S4 Mock CC workflow",
    }
    stable = n if ok else max(0, n - load_cfg["step"])
    return {
        "id": sid,
        "name": scenario.get("name", default_names.get(sid, sid)),
        "max_stable": stable,
        "breaking_point": n if not ok else None,
        "recommended": recommended(stable),
        "success_rate": driver.get("success_rate", 0),
        "setup_p99_ms": driver.get("setup_p99_ms", 0),
        "mem_per_call_mb": mon_sum.get("mem_per_call_mb", 0),
        "cpu_peak_pct": mon_sum.get("cpu_peak_pct", 0),
        "calls_peak": mon_sum.get("calls_peak", 0),
        "stop_reason": reason if not ok else "ok",
        "ramp_steps": [],
        "driver_last": driver,
        "limit_n": n if ok else stable,
        "limit_resources": resources if ok else None,
        "breaking_n": n if not ok else None,
        "breaking_resources": resources if not ok else None,
    }


def stub_scenario(scenario_id: str) -> dict[str, Any]:
    return {
        "id": scenario_id,
        "name": SCENARIO_FILES.get(scenario_id, scenario_id),
        "max_stable": 0,
        "breaking_point": None,
        "recommended": 0,
        "success_rate": 0,
        "setup_p99_ms": 0,
        "mem_per_call_mb": 0,
        "cpu_peak_pct": 0,
        "calls_peak": 0,
        "stop_reason": "not implemented (P2)",
        "ramp_steps": [],
        "skipped": True,
    }


def collect_environment(profile: dict[str, Any]) -> dict[str, Any]:
    target = profile.get("target") or {}
    safety = profile.get("safety") or {}
    http_base = target.get("http_base", "")
    process_name = target.get("process_name") or "flowkit"
    idle = fetch_metrics_snapshot(http_base) if http_base else {}
    return {
        "host": {
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "ram_total_gb": host_ram_total_gb(),
            "python": platform.python_version(),
            "go": shutil.which("go") or "not found",
        },
        "target": {
            "profile": profile.get("name", "unknown"),
            "http_base": http_base,
            "ws_url": target.get("ws_url"),
            "sip_target": sip_target(profile),
            "sip_transport": target.get("sip_transport"),
            "webrtc_public_ip": target.get("webrtc_public_ip"),
            "ice_servers": target.get("ice_servers") or [],
            "process_name": process_name,
            "max_concurrent_cap": safety.get("max_concurrent_cap"),
        },
        "flowkit_config": profile.get("env") or {},
        "flowkit_idle": idle,
    }


def main() -> int:
    args = parse_args()
    profile_path = Path(args.profile)
    if not profile_path.is_absolute():
        profile_path = ROOT / profile_path
    profile = load_yaml(profile_path)

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_dir / "config.yaml", "w", encoding="utf-8") as f:
        yaml.safe_dump({"profile": profile, "args": vars(args)}, f, allow_unicode=True)

    if args.dry_run:
        print(f"dry-run ok: profile={profile.get('name')} scenarios={args.scenarios}")
        return 0

    if not args.skip_preflight:
        preflight(profile, args.i_know_its_remote)

    env = collect_environment(profile)
    ramp_policy = ramp_policy_from_profile(profile)
    ram_gb = (env.get("host") or {}).get("ram_total_gb")

    scenario_ids = resolve_scenarios(args.scenarios)
    results: list[dict[str, Any]] = []

    for sid in scenario_ids:
        if sid not in SCENARIO_FILES:
            print(f"warning: unknown scenario {sid}", file=sys.stderr)
            continue
        scenario_path = ROOT / "scenarios" / SCENARIO_FILES[sid]
        overrides = profile.get("scenario_overrides") or {}
        if sid in overrides:
            scenario_path = ROOT / "scenarios" / overrides[sid]
        scenario = load_yaml(scenario_path)
        sc_out = output_dir / f"scenario_{sid}"
        sc_out.mkdir(parents=True, exist_ok=True)

        if sid in IMPLEMENTED:
            if args.mode == "fixed":
                n = args.fixed_n or scenario["load"]["start"]
                rec = run_fixed(sid, profile, scenario, n, sc_out)
            else:
                rec = ramp_scenario(sid, profile, scenario, sc_out, ram_total_gb=ram_gb)
            results.append(rec)
            with open(sc_out / "result.json", "w", encoding="utf-8") as f:
                json.dump(rec, f, indent=2, default=str)
            continue

        print(f"\n--- {sid}: skipped (not implemented) ---")
        rec = stub_scenario(sid)
        results.append(rec)
        with open(sc_out / "result.json", "w", encoding="utf-8") as f:
            json.dump(rec, f, indent=2)

    implemented = [r for r in results if not r.get("skipped")]
    prod_rec = None
    prod_basis = "none"
    if implemented:
        recs = [r["recommended"] for r in implemented if r.get("recommended", 0) > 0]
        if recs:
            prod_rec = min(recs)
            ids = ",".join(r["id"] for r in implemented)
            prod_basis = f"min({ids}) recommended"
    summary = {
        "run_id": output_dir.name,
        "environment": env,
        "ramp_policy": ramp_policy,
        "scenarios": results,
        "production_recommended": prod_rec,
        "production_basis": prod_basis,
        "boundary_analyses": [
            r.get("boundary_analysis")
            for r in results
            if r.get("boundary_analysis")
        ],
    }
    write_summary_json(output_dir, summary)

    report_text = render_report(
        run_id=output_dir.name,
        profile_name=profile.get("name", "unknown"),
        mode=args.mode,
        environment=env,
        scenarios=results,
        summary=summary,
        ramp_policy=ramp_policy,
    )
    (output_dir / "report.md").write_text(report_text, encoding="utf-8")
    html_path = write_html_report(
        output_dir,
        run_id=output_dir.name,
        profile_name=profile.get("name", "unknown"),
        mode=args.mode,
        environment=env,
        scenarios=results,
        summary=summary,
        ramp_policy=ramp_policy,
    )
    print(f"\nReport: {output_dir / 'report.md'}")
    print(f"HTML:   {html_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

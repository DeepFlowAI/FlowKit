"""Classify capacity test stop reasons into human-readable boundary factors."""

from __future__ import annotations

import re
from typing import Any

_R1 = re.compile(r"R1 success_rate")
_R2 = re.compile(r"R2 setup_p99")
_R3 = re.compile(r"R3 calls_peak")
_R7 = re.compile(r"R7")
_R8 = re.compile(r"R8 cpu")
_R9 = re.compile(r"R9 rss")


def _breaking_step(sc: dict[str, Any]) -> dict[str, Any] | None:
    for step in sc.get("ramp_steps") or []:
        if not step.get("passed"):
            return step
    return None


def _driver_errors(sc: dict[str, Any]) -> list[str]:
    drv = sc.get("driver_last") or {}
    return list(drv.get("errors") or [])


def classify_scenario_boundary(
    sc: dict[str, Any],
    *,
    ram_total_gb: float | None = None,
    ramp_kind: str = "dev",
) -> dict[str, Any]:
    """Return structured boundary analysis for one scenario result."""
    sid = str(sc.get("id", "")).lower()
    stop = str(sc.get("stop_reason") or "")
    break_step = _breaking_step(sc)
    break_res = (break_step or {}).get("resources") or sc.get("breaking_resources") or {}
    errors = _driver_errors(sc)
    max_stable = int(sc.get("max_stable") or 0)
    breaking_n = sc.get("breaking_n") or sc.get("breaking_point")

    factors: list[dict[str, str]] = []
    primary = "unknown"
    summary = "未能确定边界原因（可能尚未压测或数据不完整）。"

    if sc.get("skipped"):
        return {
            "primary_factor": "skipped",
            "primary_label": "未执行",
            "summary": "本场景未运行。",
            "factors": [],
            "likely_limit": "-",
            "next_steps": [],
        }

    if stop == "max reached" and ramp_kind == "dev":
        primary = "dev_cap"
        summary = (
            f"在开发/有界模式下到达配置上限（max_stable={max_stable}），"
            "尚未触发失败停止条件；真实顶点可能更高。"
        )
        factors.append({
            "id": "dev_cap",
            "label": "配置上限",
            "detail": "scenario load.max 或 profile safety.max_concurrent_cap 阻止继续加压。",
            "severity": "info",
        })
    elif _R7.search(stop) or "vertex_hard_ceiling" in stop or "hard_ceiling" in stop:
        primary = "hard_ceiling"
        summary = (
            f"在顶点模式下达到安全硬顶（N={breaking_n or max_stable}）且仍 PASS，"
            "建议提高 vertex_hard_ceiling 或确认是否还有余量。"
        )
        factors.append({
            "id": "hard_ceiling",
            "label": "安全硬顶",
            "detail": stop,
            "severity": "warn",
        })
    elif _R1.search(stop):
        primary = "success_rate"
        err_text = " ".join(errors).lower()
        if "play_end timeout" in err_text:
            primary = "orchestration_timeout"
            summary = (
                f"N={breaking_n} 时出现 call.play_end / 编排超时（成功率低于阈值）。"
                "常见于单 WS Mock CC 或 CC 编排吞吐不足，不一定是 Flowkit 媒体上限。"
            )
            factors.append({
                "id": "orchestration_timeout",
                "label": "编排超时",
                "detail": "; ".join(errors[:3]) or stop,
                "severity": "high",
            })
        elif "timeout" in err_text or "deadline" in err_text:
            primary = "timeout"
            summary = f"N={breaking_n} 时驱动/信令超时导致成功率下降。"
            factors.append({
                "id": "timeout",
                "label": "超时",
                "detail": "; ".join(errors[:3]) or stop,
                "severity": "high",
            })
        else:
            summary = f"N={breaking_n} 时成功率低于 min_rate（{stop}）。"
            factors.append({
                "id": "success_rate",
                "label": "成功率",
                "detail": "; ".join(errors[:3]) or stop,
                "severity": "high",
            })
    elif _R2.search(stop):
        primary = "latency"
        summary = f"N={breaking_n} 时 setup p99 超过 max_setup_p99_ms（信令/ICE/工作流过慢）。"
        factors.append({
            "id": "latency",
            "label": "建立时延",
            "detail": stop,
            "severity": "high",
        })
    elif _R3.search(stop):
        primary = "session_establishment"
        summary = (
            f"N={breaking_n} 时 flowkit_calls_active 未达到目标（会话未真正建立）。"
            "可能是 SIP/WebRTC 接入失败、端口或内核连接上限。"
        )
        factors.append({
            "id": "session_establishment",
            "label": "会话建立",
            "detail": stop,
            "severity": "high",
        })
    elif _R8.search(stop):
        primary = "cpu"
        summary = f"N={breaking_n} 时进程 CPU 超过安全阈值（{stop}）。"
        factors.append({
            "id": "cpu",
            "label": "CPU",
            "detail": stop,
            "severity": "high",
        })
    elif _R9.search(stop):
        primary = "memory"
        summary = f"N={breaking_n} 时 RSS 接近主机内存安全阈值（{stop}）。"
        factors.append({
            "id": "memory",
            "label": "内存",
            "detail": stop,
            "severity": "high",
        })
    elif max_stable > 0 and not breaking_n:
        primary = "none_observed"
        summary = f"在已测范围内全部 PASS，最大稳定 N={max_stable}。"
        if ramp_kind == "dev":
            summary += " 开发模式下可能未压到真实顶点。"

    cpu_peak = float(break_res.get("cpu_peak_pct") or sc.get("cpu_peak_pct") or 0)
    rss_peak = float(break_res.get("rss_peak_mb") or 0)
    if cpu_peak >= 80 and primary not in ("cpu",):
        factors.append({
            "id": "cpu_pressure",
            "label": "CPU 压力",
            "detail": f"失败档 CPU 峰值 {cpu_peak:.1f}%",
            "severity": "medium",
        })
    if ram_total_gb and rss_peak > 0:
        rss_pct = rss_peak / (ram_total_gb * 1024) * 100
        if rss_pct >= 70 and primary not in ("memory",):
            factors.append({
                "id": "memory_pressure",
                "label": "内存压力",
                "detail": f"失败档 RSS {rss_peak:.0f} MB（约主机内存 {rss_pct:.0f}%）",
                "severity": "medium",
            })

    sid_hints = {
        "s1": "检查 SIP dialog、RTP 端口范围、ulimit、SIP_MAX_INBOUND_CALLS。",
        "s2": "检查 WebRTC ICE、UDP 16384–32767、STUN 可达性。",
        "s3": "检查 bridge 时序、双 leg 资源、WS RPC 延迟。",
        "s4": "检查 CC 编排吞吐；生产环境用真实 CC 集群而非单 Mock WS。",
    }
    next_steps = [sid_hints.get(sid, "对比失败档 errors 与 Flowkit 日志。")]
    if primary == "dev_cap":
        next_steps.insert(0, "在服务器上使用 profiles/server-vertex.yaml 做顶点探测。")
    if primary == "orchestration_timeout":
        next_steps.insert(0, "单独跑 S4；或改进 mockcc 驱动（多 WS、调大 play_end 等待）。")

    labels = {
        "dev_cap": "配置上限（非真实顶点）",
        "hard_ceiling": "安全硬顶",
        "success_rate": "成功率",
        "orchestration_timeout": "CC 编排超时",
        "timeout": "驱动/信令超时",
        "latency": "建立时延",
        "session_establishment": "会话未建立",
        "cpu": "CPU 饱和",
        "memory": "内存饱和",
        "none_observed": "未观测到失败",
        "unknown": "未知",
    }

    return {
        "primary_factor": primary,
        "primary_label": labels.get(primary, primary),
        "summary": summary,
        "factors": factors,
        "likely_limit": labels.get(primary, primary),
        "next_steps": next_steps,
        "breaking_n": breaking_n,
        "max_stable": max_stable,
    }


def classify_all_scenarios(
    scenarios: list[dict[str, Any]],
    *,
    environment: dict[str, Any] | None = None,
    ramp_policy: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    env = environment or {}
    ram = (env.get("host") or {}).get("ram_total_gb")
    kind = (ramp_policy or {}).get("kind", "dev")
    out = []
    for sc in scenarios:
        analysis = classify_scenario_boundary(sc, ram_total_gb=ram, ramp_kind=kind)
        out.append({"scenario_id": sc.get("id"), **analysis})
    return out

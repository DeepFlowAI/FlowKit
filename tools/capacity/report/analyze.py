"""Generate Markdown capacity reports from run artifacts."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_CAP_ROOT = Path(__file__).resolve().parents[1]
if str(_CAP_ROOT) not in sys.path:
    sys.path.insert(0, str(_CAP_ROOT))

from report.boundary import classify_all_scenarios

SCENARIO_DESCRIPTIONS: dict[str, dict[str, str]] = {
    "s1": {
        "title": "S1 · SIP 呼入保持",
        "business": "模拟用户电话进线：Flowkit 只接 SIP、保持 RTP 媒体，不连接 CC Orchestrator，也不建立 WebRTC。",
        "flow": "N 个 SIP 客户端并发 INVITE → Flowkit 200 OK → 静音 RTP 保持 → BYE",
        "n_unit": "并发 SIP 通话路数",
        "bottleneck": "SIP Dialog 数、RTP/UDP 端口、SIP_MAX_INBOUND_CALLS",
    },
    "s2": {
        "title": "S2 · WebRTC 坐席腿",
        "business": "模拟坐席侧单独建 WebRTC 连接（浏览器/软电话），不含 SIP 用户腿。",
        "flow": "N 个客户端经 WS 并发 webrtc.offer → ICE 连通 → 保持 → call.hangup",
        "n_unit": "并发 WebRTC 会话数",
        "bottleneck": "Pion PeerConnection、ICE、UDP 16384–32767",
    },
    "s3": {
        "title": "S3 · SIP bridge WebRTC",
        "business": "模拟呼叫中心转人工：用户走 SIP，坐席走 WebRTC，两路经 call.bridge 绑在一起。",
        "flow": "每对并发：SIP 呼入 + webrtc.offer/answer → call.bridge → 保持 → 双腿挂断",
        "n_unit": "并发 bridge 对数（metrics 中 flowkit_calls_active 约为 2×N）",
        "bottleneck": "双 leg 内存、bridge 时序、RPC 延迟",
    },
    "s4": {
        "title": "S4 · Mock CC 完整工作流",
        "business": "模拟 CC 编排服务接管来电后的完整 IVR（mock ASR/TTS，需 SIP_KERNEL_AUTO_LLM=false）。",
        "flow": "1 个 WS Mock CC + N 路 SIP 呼入：call.answer → call.say → call.play_end → call.listen → call.hangup",
        "n_unit": "并发 SIP 来电数（单 Orchestrator 处理全部 call.incoming）",
        "bottleneck": "WS 事件吞吐、mock TTS/ASR 管道、Session 状态机",
    },
}


def scenario_title(sid: str) -> str:
    return SCENARIO_DESCRIPTIONS.get(sid, {}).get("title", sid.upper())


def scenario_label(sc: dict[str, Any]) -> str:
    sid = str(sc.get("id", "")).lower()
    name = sc.get("name") or scenario_title(sid)
    return name


def render_scenario_descriptions(scenarios: list[dict[str, Any]]) -> list[str]:
    lines = [
        "## 场景说明",
        "",
        "本节说明**本次报告实际运行的场景**。Ramp 档位 N 表示逐步加压的并发目标。",
        "",
    ]
    seen: set[str] = set()
    for sc in scenarios:
        sid = str(sc.get("id", "")).lower()
        if not sid or sid in seen:
            continue
        seen.add(sid)
        desc = SCENARIO_DESCRIPTIONS.get(sid)
        if not desc:
            lines.extend([f"### {sc.get('name', sid.upper())}", "", f"- 未注册场景 ID：`{sid}`", ""])
            continue
        if sc.get("skipped"):
            lines.extend([
                f"### {desc['title']}（未执行）",
                "",
                f"- **模拟业务**：{desc['business']}",
                f"- **压测路径**：{desc['flow']}",
                f"- **N 的含义**：{desc['n_unit']}",
                "- **状态**：本次跳过（驱动未实现或未选中）",
                "",
            ])
            continue
        lines.extend([
            f"### {desc['title']}",
            "",
            f"- **模拟业务**：{desc['business']}",
            f"- **压测路径**：{desc['flow']}",
            f"- **N 的含义**：{desc['n_unit']}",
            f"- **预期瓶颈**：{desc['bottleneck']}",
            "",
        ])
    return lines


def render_scenario_detail_intro(sid: str) -> list[str]:
    desc = SCENARIO_DESCRIPTIONS.get(sid)
    if not desc:
        return []
    return [
        f"- **模拟业务**：{desc['business']}",
        f"- **压测路径**：{desc['flow']}",
        f"- **N 的含义**：{desc['n_unit']}",
        "",
    ]


def _fmt_env_val(v: Any) -> str:
    if v is None:
        return "-"
    if isinstance(v, list):
        return ", ".join(str(x) for x in v) if v else "-"
    if isinstance(v, dict):
        if not v:
            return "-"
        return "; ".join(f"{k}={val}" for k, val in v.items())
    return str(v)


def render_environment_section(environment: dict[str, Any]) -> list[str]:
    lines = [
        "## 被压测环境",
        "",
        "压测开始前采集的主机、Flowkit 目标与空闲基线（`flowkit_idle`）。",
        "",
        "### 主机",
        "",
    ]
    host = environment.get("host") or {}
    if isinstance(environment.get("platform"), str):
        host = {
            "platform": environment.get("platform"),
            "python": environment.get("python"),
            "go": environment.get("go"),
            **host,
        }
    for key, label in (
        ("platform", "操作系统"),
        ("cpu_count", "CPU 核数"),
        ("ram_total_gb", "内存总量 (GB)"),
        ("python", "Python"),
        ("go", "Go"),
    ):
        if key in host and host[key] is not None:
            lines.append(f"- **{label}**: {host[key]}")
    lines.extend(["", "### Flowkit 目标", ""])
    target = environment.get("target") or {}
    if isinstance(environment.get("http_base"), str):
        target = {
            "profile": environment.get("profile"),
            "http_base": environment.get("http_base"),
            "ws_url": environment.get("ws_url"),
            "sip_target": environment.get("sip_target"),
            **target,
        }
    for key, label in (
        ("profile", "Profile"),
        ("http_base", "HTTP / metrics"),
        ("ws_url", "WebSocket"),
        ("sip_target", "SIP 目标"),
        ("sip_transport", "SIP 传输"),
        ("webrtc_public_ip", "WebRTC 公网 IP"),
        ("ice_servers", "ICE STUN"),
        ("process_name", "进程名"),
        ("max_concurrent_cap", "Ramp 上限 cap"),
    ):
        if key in target and target.get(key) not in (None, ""):
            lines.append(f"- **{label}**: {_fmt_env_val(target[key])}")
    cfg = environment.get("flowkit_config") or {}
    if cfg:
        lines.extend(["", "### Flowkit 配置 (profile.env)", ""])
        for k, v in sorted(cfg.items()):
            lines.append(f"- `{k}` = `{v}`")
    idle = environment.get("flowkit_idle") or {}
    if idle and idle.get("metrics_error") != "unreachable":
        lines.extend(["", "### 压测前空闲基线", ""])
        lines.append(f"- **进程 RSS**: {idle.get('process_rss_mb', '-')} MB")
        lines.append(f"- **进程 CPU**: {idle.get('process_cpu_pct', '-')} %")
        lines.append(f"- **flowkit_calls_active**: {idle.get('calls_active', 0)}")
        lines.append(f"- **flowkit_goroutines**: {idle.get('goroutines', 0)}")
        lines.append(f"- **heap alloc**: {idle.get('mem_alloc_mb', '-')} MB")
        lines.append(f"- **heap sys**: {idle.get('mem_sys_mb', '-')} MB")
        if idle.get("uptime_seconds") is not None:
            lines.append(f"- **uptime**: {idle.get('uptime_seconds')} s")
    lines.append("")
    return lines


def step_resources(step: dict[str, Any]) -> dict[str, Any]:
    if step.get("resources"):
        return step["resources"]
    return {
        "calls_peak": step.get("calls_peak", 0),
        "rss_peak_mb": step.get("rss_peak_mb", 0),
        "cpu_peak_pct": step.get("cpu_peak_pct", 0),
        "goroutines_peak": step.get("goroutines_peak", 0),
        "mem_alloc_peak_mb": step.get("mem_alloc_peak_mb", 0),
        "mem_per_call_mb": step.get("mem_per_call_mb", 0),
    }


def scenario_limit_resources(sc: dict[str, Any]) -> tuple[int | None, dict[str, Any] | None]:
    if sc.get("limit_resources"):
        return sc.get("limit_n") or sc.get("max_stable"), sc["limit_resources"]
    step = None
    for s in sc.get("ramp_steps") or []:
        if s.get("passed") and s.get("n") == sc.get("max_stable"):
            step = s
            break
    if step is None:
        steps = [s for s in sc.get("ramp_steps") or [] if s.get("passed")]
        step = steps[-1] if steps else None
    if step:
        return step.get("n"), step_resources(step)
    if sc.get("max_stable"):
        return sc.get("max_stable"), {
            "calls_peak": sc.get("calls_peak", 0),
            "rss_peak_mb": 0,
            "cpu_peak_pct": sc.get("cpu_peak_pct", 0),
            "mem_per_call_mb": sc.get("mem_per_call_mb", 0),
        }
    return None, None


def render_resources_block(title: str, n: int | None, res: dict[str, Any] | None) -> list[str]:
    if not res:
        return [f"### {title}", "", "- 无数据", ""]
    lines = [
        f"### {title}" + (f"（N={n}）" if n is not None else ""),
        "",
        "| 指标 | 峰值 |",
        "|------|------|",
        f"| flowkit_calls_active | {res.get('calls_peak', '-')} |",
        f"| 进程 RSS (MB) | {res.get('rss_peak_mb', '-')} |",
        f"| 进程 CPU (%) | {res.get('cpu_peak_pct', '-')} |",
        f"| goroutines | {res.get('goroutines_peak', '-')} |",
        f"| heap alloc (MB) | {res.get('mem_alloc_peak_mb', '-')} |",
        f"| heap sys (MB) | {res.get('mem_sys_peak_mb', '-')} |",
        f"| WS conns | {res.get('conns_peak', '-')} |",
        f"| 基线 RSS (MB) | {res.get('rss_baseline_mb', '-')} |",
        f"| 增量 RSS / call (MB) | {res.get('mem_per_call_mb', '-')} |",
        "",
    ]
    return lines


def render_limit_resources_summary(scenarios: list[dict[str, Any]]) -> list[str]:
    lines = [
        "## 上限资源消耗",
        "",
        "各场景在**最大稳定档位**（boundary 模式下最后一档 PASS）采样期间的 Flowkit 进程与 `/metrics` 峰值。",
        "若存在失败档位，另列**首次失败档位**资源供对比。",
        "",
        "| 场景 | 稳定 N | RSS 峰值 (MB) | CPU 峰值 (%) | calls 峰值 | goroutines | alloc (MB) |",
        "|------|--------|---------------|--------------|------------|------------|------------|",
    ]
    for sc in scenarios:
        if sc.get("skipped"):
            continue
        sid = str(sc.get("id", "")).upper()
        ln, res = scenario_limit_resources(sc)
        if not res:
            lines.append(f"| {sid} | - | - | - | - | - | - |")
            continue
        lines.append(
            f"| {sid} | {ln or sc.get('max_stable', 0)} | "
            f"{res.get('rss_peak_mb', '-')} | {res.get('cpu_peak_pct', '-')} | "
            f"{res.get('calls_peak', '-')} | {res.get('goroutines_peak', '-')} | "
            f"{res.get('mem_alloc_peak_mb', '-')} |"
        )
        break_res = sc.get("breaking_resources")
        break_n = sc.get("breaking_n")
        if not break_res:
            for step in sc.get("ramp_steps") or []:
                if not step.get("passed"):
                    break_n = step.get("n")
                    break_res = step_resources(step)
                    break
        if break_res and break_n:
            lines.append(
                f"| {sid} (失败 N={break_n}) | {break_n} | "
                f"{break_res.get('rss_peak_mb', '-')} | {break_res.get('cpu_peak_pct', '-')} | "
                f"{break_res.get('calls_peak', '-')} | {break_res.get('goroutines_peak', '-')} | "
                f"{break_res.get('mem_alloc_peak_mb', '-')} |"
            )
    lines.append("")
    return lines


def render_ramp_policy_section(ramp_policy: dict[str, Any] | None) -> list[str]:
    if not ramp_policy:
        return []
    kind = ramp_policy.get("kind", "dev")
    kind_zh = "顶点探测" if kind == "vertex" else "开发验证"
    lines = [
        "## 压测模式",
        "",
        f"- **模式**: {kind_zh} (`{kind}`)",
        f"- **说明**: {ramp_policy.get('description', '-')}",
        f"- **硬顶 ceiling**: {ramp_policy.get('vertex_hard_ceiling', '-')}",
    ]
    if ramp_policy.get("stop_on_cpu_pct") is not None:
        lines.append(f"- **CPU 安全停止**: ≥ {ramp_policy['stop_on_cpu_pct']}%")
    if ramp_policy.get("stop_on_rss_pct_of_ram") is not None:
        lines.append(f"- **内存安全停止**: ≥ {ramp_policy['stop_on_rss_pct_of_ram']}% 主机 RAM")
    lines.append("")
    return lines


def render_boundary_section(
    scenarios: list[dict[str, Any]],
    *,
    environment: dict[str, Any] | None = None,
    ramp_policy: dict[str, Any] | None = None,
) -> list[str]:
    analyses = classify_all_scenarios(
        scenarios, environment=environment, ramp_policy=ramp_policy
    )
    if not analyses:
        return []
    lines = [
        "## 临界点与边界因素",
        "",
        "根据停止条件、驱动错误与资源峰值推断**为何无法继续加压**。",
        "",
        "| 场景 | 最大稳定 N | 临界点 N | 边界因素 | 摘要 |",
        "|------|-----------|----------|----------|------|",
    ]
    for ba in analyses:
        sid = str(ba.get("scenario_id", "")).upper()
        lines.append(
            f"| {sid} | {ba.get('max_stable', 0)} | {ba.get('breaking_n') or '-'} | "
            f"{ba.get('primary_label', '-')} | {ba.get('summary', '-')} |"
        )
    lines.extend(["", "### 分场景建议", ""])
    for ba in analyses:
        sid = str(ba.get("scenario_id", "")).upper()
        steps = ba.get("next_steps") or []
        if not steps:
            continue
        lines.append(f"**{sid}**: " + "；".join(steps))
        lines.append("")
    return lines


def recommended(max_stable: int, ratio: float = 0.8) -> int:
    if max_stable <= 0:
        return 0
    return int(max_stable * ratio)


def render_report(
    *,
    run_id: str,
    profile_name: str,
    mode: str,
    environment: dict[str, Any],
    scenarios: list[dict[str, Any]],
    summary: dict[str, Any],
    ramp_policy: dict[str, Any] | None = None,
) -> str:
    lines = [
        "# Flowkit 容量压测报告",
        "",
        f"- **Run ID**: {run_id}",
        f"- **Profile**: {profile_name}",
        f"- **Mode**: {mode}",
        f"- **Generated**: {datetime.now(timezone.utc).isoformat()}",
        "",
    ]
    lines.extend(render_environment_section(environment))
    policy = ramp_policy or summary.get("ramp_policy")
    lines.extend(render_ramp_policy_section(policy))
    lines.extend(render_scenario_descriptions(scenarios))
    lines.extend(render_limit_resources_summary(scenarios))
    lines.extend(render_boundary_section(scenarios, environment=environment, ramp_policy=policy))
    lines.extend([
        "## Executive Summary",
        "",
        "| 场景 | 最大稳定并发 | 推荐生产值 (×0.8) | 成功率 | 首要停止原因 |",
        "|------|-------------|-------------------|--------|--------------|",
    ])
    for sc in scenarios:
        sid = str(sc.get("id", "?")).upper()
        name = sc.get("name") or sid
        lines.append(
            f"| {sid} ({name}) | {sc.get('max_stable', 0)} | "
            f"{sc.get('recommended', 0)} | "
            f"{sc.get('success_rate', 0)*100:.1f}% | {sc.get('stop_reason', '-')} |"
        )

    lines.extend(["", "## Per-Scenario Detail", ""])
    for sc in scenarios:
        sid = str(sc.get("id", "")).lower()
        lines.extend([
            f"### {scenario_label(sc)}",
            "",
        ])
        lines.extend(render_scenario_detail_intro(sid))
        limit_n, limit_res = scenario_limit_resources(sc)
        if limit_res:
            lines.extend(render_resources_block("最大稳定档位资源", limit_n, limit_res))
        break_n = sc.get("breaking_n")
        break_res = sc.get("breaking_resources")
        if not break_res:
            for step in sc.get("ramp_steps") or []:
                if not step.get("passed"):
                    break_n = break_n or step.get("n")
                    break_res = step_resources(step)
                    break
        if break_res and break_n:
            lines.extend(render_resources_block("首次失败档位资源", break_n, break_res))
        lines.extend([
            f"- max_stable: **{sc.get('max_stable', 0)}**",
            f"- breaking_point: {sc.get('breaking_point', 'n/a')}",
            f"- setup_p99_ms: {sc.get('setup_p99_ms', 0):.1f}",
            "",
        ])
        ramp = sc.get("ramp_steps") or []
        if ramp:
            lines.append(
                "| N | 成功 | 成功率 | p99 (ms) | calls | RSS (MB) | CPU (%) | goroutines | 停止 |"
            )
            lines.append("|---|------|--------|----------|-------|----------|---------|------------|------|")
            for step in ramp:
                res = step_resources(step)
                lines.append(
                    f"| {step['n']} | {step.get('completed', 0)} | "
                    f"{step.get('success_rate', 0)*100:.1f}% | "
                    f"{step.get('setup_p99_ms', 0):.1f} | "
                    f"{res.get('calls_peak', 0)} | "
                    f"{res.get('rss_peak_mb', 0)} | "
                    f"{res.get('cpu_peak_pct', 0)} | "
                    f"{res.get('goroutines_peak', '-')} | "
                    f"{step.get('passed', '')} |"
                )
            lines.append("")

    prod = summary.get("production_recommended")
    if prod is not None:
        lines.extend([
            "## Capacity Recommendations",
            "",
            f"- **建议生产并发上限 (CC 路径)**: {prod} 路",
            f"- 依据: {summary.get('production_basis', 'S1')}",
            "",
        ])

    lines.extend([
        "## Raw Data",
        "",
        "- `report.html`（交互图表）",
        "- `report.md`",
        "- `summary.json`",
        "- `scenario_results/`",
        "",
    ])
    return "\n".join(lines)


def write_report(output_dir: Path, **kwargs: Any) -> Path:
    text = render_report(**kwargs)
    path = output_dir / "report.md"
    path.write_text(text, encoding="utf-8")
    return path


def write_summary_json(output_dir: Path, data: dict[str, Any]) -> Path:
    path = output_dir / "summary.json"
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    return path


def regenerate_report_from_dir(result_dir: Path) -> Path:
    """Rebuild report.md from summary.json + config.yaml (in-place)."""
    import yaml

    summary_path = result_dir / "summary.json"
    if not summary_path.is_file():
        raise FileNotFoundError(f"missing {summary_path}")

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    config: dict[str, Any] = {}
    config_path = result_dir / "config.yaml"
    if config_path.is_file():
        config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}

    profile = config.get("profile") or {}
    args = config.get("args") or {}
    environment = summary.get("environment")
    if not environment and profile:
        import sys

        cap_root = Path(__file__).resolve().parents[1]
        if str(cap_root) not in sys.path:
            sys.path.insert(0, str(cap_root))
        try:
            from capacity import collect_environment

            environment = collect_environment(profile)
        except Exception:
            environment = None
    if not environment:
        target = profile.get("target") or {}
        environment = {
            "host": {"platform": "unknown (regenerated, no live snapshot)"},
            "target": {
                "profile": profile.get("name", "unknown"),
                "http_base": target.get("http_base"),
                "ws_url": target.get("ws_url"),
                "sip_target": f"{target.get('sip_host', '?')}:{target.get('sip_port', '?')}",
                "ice_servers": target.get("ice_servers") or [],
            },
            "flowkit_config": profile.get("env") or {},
        }
    return write_report(
        result_dir,
        run_id=summary.get("run_id", result_dir.name),
        profile_name=profile.get("name", "unknown"),
        mode=args.get("mode", "boundary"),
        environment=environment,
        scenarios=summary.get("scenarios") or [],
        summary=summary,
        ramp_policy=summary.get("ramp_policy") or profile.get("ramp_policy"),
    )


if __name__ == "__main__":
    import sys

    cap_root = Path(__file__).resolve().parents[1]
    if str(cap_root) not in sys.path:
        sys.path.insert(0, str(cap_root))

    from report.html_report import regenerate_html_from_dir

    root = Path(__file__).resolve().parents[1] / "results"
    dirs = [Path(p) for p in sys.argv[1:]] if len(sys.argv) > 1 else sorted(root.glob("*/"))
    for d in dirs:
        if not (d / "summary.json").is_file():
            continue
        path = regenerate_report_from_dir(d)
        print(f"regenerated {path}")
        html_path = regenerate_html_from_dir(d)
        if html_path:
            print(f"regenerated {html_path}")

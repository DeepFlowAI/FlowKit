"""Generate interactive HTML capacity reports with Chart.js charts."""

from __future__ import annotations

import html
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_CAP_ROOT = Path(__file__).resolve().parents[1]
if str(_CAP_ROOT) not in sys.path:
    sys.path.insert(0, str(_CAP_ROOT))

from report.analyze import SCENARIO_DESCRIPTIONS, render_environment_section, scenario_title
from report.boundary import classify_all_scenarios


def _esc(s: Any) -> str:
    return html.escape(str(s) if s is not None else "")


def _chart_data(scenarios: list[dict[str, Any]]) -> dict[str, Any]:
    charts: dict[str, Any] = {}
    for sc in scenarios:
        if sc.get("skipped"):
            continue
        sid = str(sc.get("id", "")).lower()
        steps = sc.get("ramp_steps") or []
        if not steps:
            continue
        ns = [s["n"] for s in steps]
        charts[sid] = {
            "label": sc.get("name") or scenario_title(sid),
            "n": ns,
            "success_rate": [round(s.get("success_rate", 0) * 100, 1) for s in steps],
            "setup_p99_ms": [round(s.get("setup_p99_ms", 0), 1) for s in steps],
            "calls_peak": [int((s.get("resources") or {}).get("calls_peak", s.get("calls_peak", 0))) for s in steps],
            "rss_peak_mb": [round((s.get("resources") or {}).get("rss_peak_mb", s.get("rss_peak_mb", 0)), 2) for s in steps],
            "cpu_peak_pct": [round((s.get("resources") or {}).get("cpu_peak_pct", s.get("cpu_peak_pct", 0)), 1) for s in steps],
            "goroutines_peak": [int((s.get("resources") or {}).get("goroutines_peak", s.get("goroutines_peak", 0))) for s in steps],
            "passed": [bool(s.get("passed")) for s in steps],
            "max_stable": sc.get("max_stable", 0),
            "breaking_n": sc.get("breaking_n") or sc.get("breaking_point"),
        }
    return charts


def render_html_report(
    *,
    run_id: str,
    profile_name: str,
    mode: str,
    environment: dict[str, Any],
    scenarios: list[dict[str, Any]],
    summary: dict[str, Any],
    ramp_policy: dict[str, Any] | None = None,
) -> str:
    policy = ramp_policy or {}
    kind = policy.get("kind", "dev")
    kind_label = policy.get("label") or ("顶点探测" if kind == "vertex" else "开发验证")
    kind_badge = "vertex" if kind == "vertex" else "dev"
    boundary_analyses = classify_all_scenarios(
        scenarios, environment=environment, ramp_policy=policy
    )
    charts = _chart_data(scenarios)
    payload = {
        "run_id": run_id,
        "profile": profile_name,
        "mode": mode,
        "ramp_policy": policy,
        "charts": charts,
        "scenarios": [
            {
                "id": sc.get("id"),
                "name": sc.get("name"),
                "max_stable": sc.get("max_stable"),
                "breaking_n": sc.get("breaking_n") or sc.get("breaking_point"),
                "stop_reason": sc.get("stop_reason"),
                "recommended": sc.get("recommended"),
            }
            for sc in scenarios
            if not sc.get("skipped")
        ],
        "production_recommended": summary.get("production_recommended"),
    }
    data_json = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")

    env_md_lines = render_environment_section(environment)
    env_html = "<br>".join(_esc(line) for line in env_md_lines if line.strip())

    summary_rows = ""
    for sc in scenarios:
        if sc.get("skipped"):
            continue
        sid = str(sc.get("id", "?")).upper()
        ba = next((b for b in boundary_analyses if b.get("scenario_id") == sc.get("id")), {})
        summary_rows += f"""
        <tr>
          <td>{_esc(sid)}</td>
          <td>{_esc(sc.get('name', sid))}</td>
          <td class="num">{sc.get('max_stable', 0)}</td>
          <td class="num">{sc.get('breaking_n') or sc.get('breaking_point') or '-'}</td>
          <td class="num">{sc.get('recommended', 0)}</td>
          <td>{_esc(ba.get('primary_label', '-'))}</td>
          <td class="reason">{_esc(sc.get('stop_reason', '-'))}</td>
        </tr>"""

    scenario_sections = ""
    for sc in scenarios:
        if sc.get("skipped"):
            continue
        sid = str(sc.get("id", "")).lower()
        desc = SCENARIO_DESCRIPTIONS.get(sid, {})
        ba = next((b for b in boundary_analyses if b.get("scenario_id") == sc.get("id")), {})
        factors_html = "".join(
            f'<li><strong>{_esc(f.get("label"))}</strong>: {_esc(f.get("detail"))}</li>'
            for f in ba.get("factors") or []
        )
        if not factors_html:
            factors_html = "<li>无附加因子</li>"
        next_html = "".join(f"<li>{_esc(s)}</li>" for s in ba.get("next_steps") or [])
        scenario_sections += f"""
        <section class="scenario" id="sc-{sid}">
          <h2>{_esc(desc.get('title') or sc.get('name', sid.upper()))}</h2>
          <p class="muted">{_esc(desc.get('business', ''))}</p>
          <div class="stat-grid">
            <div class="stat"><span>最大稳定 N</span><strong>{sc.get('max_stable', 0)}</strong></div>
            <div class="stat"><span>临界点 N</span><strong>{sc.get('breaking_n') or sc.get('breaking_point') or '—'}</strong></div>
            <div class="stat"><span>推荐生产 (×0.8)</span><strong>{sc.get('recommended', 0)}</strong></div>
            <div class="stat"><span>边界因素</span><strong>{_esc(ba.get('primary_label', '-'))}</strong></div>
          </div>
          <div class="boundary-box">
            <h3>边界分析</h3>
            <p>{_esc(ba.get('summary', ''))}</p>
            <ul>{factors_html}</ul>
            <h4>建议下一步</h4>
            <ul>{next_html}</ul>
          </div>
          <div class="charts">
            <div class="chart-row">
              <div class="chart-wrap"><canvas id="chart-{sid}-success"></canvas></div>
              <div class="chart-wrap"><canvas id="chart-{sid}-latency"></canvas></div>
            </div>
            <div class="chart-row">
              <div class="chart-wrap"><canvas id="chart-{sid}-rss"></canvas></div>
              <div class="chart-wrap"><canvas id="chart-{sid}-cpu"></canvas></div>
            </div>
            <div class="chart-row">
              <div class="chart-wrap"><canvas id="chart-{sid}-calls"></canvas></div>
              <div class="chart-wrap"><canvas id="chart-{sid}-goroutines"></canvas></div>
            </div>
          </div>
        </section>"""

    prod = summary.get("production_recommended")
    prod_block = ""
    if prod is not None:
        prod_block = f"""
        <section class="recommend">
          <h2>生产容量建议</h2>
          <p class="big-num">{prod} 路</p>
          <p class="muted">依据: {_esc(summary.get('production_basis', ''))}</p>
        </section>"""

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Flowkit 容量压测 — {_esc(run_id)}</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    :root {{
      --bg: #0f1419;
      --card: #1a2332;
      --text: #e7ecf3;
      --muted: #8b9cb3;
      --accent: #3b82f6;
      --dev: #f59e0b;
      --vertex: #10b981;
      --border: #2d3a4f;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.5;
    }}
    header {{
      padding: 2rem 2rem 1rem;
      border-bottom: 1px solid var(--border);
      background: linear-gradient(135deg, #1a2332 0%, #0f1419 100%);
    }}
    header h1 {{ margin: 0 0 0.5rem; font-size: 1.75rem; }}
    .meta {{ color: var(--muted); font-size: 0.9rem; }}
    .badge {{
      display: inline-block;
      padding: 0.2rem 0.6rem;
      border-radius: 999px;
      font-size: 0.75rem;
      font-weight: 600;
      margin-left: 0.5rem;
    }}
    .badge.dev {{ background: rgba(245,158,11,0.2); color: var(--dev); }}
    .badge.vertex {{ background: rgba(16,185,129,0.2); color: var(--vertex); }}
    main {{ max-width: 1200px; margin: 0 auto; padding: 1.5rem 2rem 3rem; }}
    section {{ margin-bottom: 2.5rem; }}
    h2 {{ font-size: 1.25rem; margin: 0 0 1rem; border-left: 3px solid var(--accent); padding-left: 0.75rem; }}
    .env-box {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 1rem 1.25rem;
      font-size: 0.85rem;
      color: var(--muted);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.9rem;
      background: var(--card);
      border-radius: 8px;
      overflow: hidden;
    }}
    th, td {{ padding: 0.65rem 0.75rem; text-align: left; border-bottom: 1px solid var(--border); }}
    th {{ background: #243044; color: var(--muted); font-weight: 600; }}
    td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    td.reason {{ font-size: 0.8rem; color: var(--muted); max-width: 220px; }}
    .stat-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 0.75rem;
      margin-bottom: 1rem;
    }}
    .stat {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 0.75rem 1rem;
    }}
    .stat span {{ display: block; font-size: 0.75rem; color: var(--muted); }}
    .stat strong {{ font-size: 1.35rem; }}
    .boundary-box {{
      background: #1e2a3d;
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 1rem 1.25rem;
      margin-bottom: 1.25rem;
    }}
    .boundary-box h3 {{ margin: 0 0 0.5rem; font-size: 1rem; }}
    .boundary-box h4 {{ margin: 1rem 0 0.35rem; font-size: 0.85rem; color: var(--muted); }}
    .charts {{ margin-top: 1rem; }}
    .chart-row {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 1rem;
      margin-bottom: 1rem;
    }}
    @media (max-width: 800px) {{ .chart-row {{ grid-template-columns: 1fr; }} }}
    .chart-wrap {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 0.75rem;
      min-height: 220px;
    }}
    .recommend .big-num {{ font-size: 2.5rem; font-weight: 700; color: var(--accent); margin: 0; }}
    .muted {{ color: var(--muted); }}
    footer {{ text-align: center; padding: 2rem; color: var(--muted); font-size: 0.8rem; }}
  </style>
</head>
<body>
  <header>
    <h1>Flowkit 容量压测报告
      <span class="badge {kind_badge}">{_esc(kind_label)}</span>
    </h1>
    <p class="meta">
      Run ID: {_esc(run_id)} · Profile: {_esc(profile_name)} · Mode: {_esc(mode)} · {_esc(generated)}
    </p>
    <p class="meta">{_esc(policy.get('description', ''))}</p>
  </header>
  <main>
    <section>
      <h2>被压测环境</h2>
      <div class="env-box">{env_html}</div>
    </section>
    <section>
      <h2>临界点总览</h2>
      <table>
        <thead>
          <tr>
            <th>场景</th><th>名称</th><th>最大稳定 N</th><th>临界点 N</th>
            <th>推荐生产</th><th>边界因素</th><th>停止原因</th>
          </tr>
        </thead>
        <tbody>{summary_rows}</tbody>
      </table>
    </section>
    {prod_block}
    {scenario_sections}
  </main>
  <footer>
    Raw data: summary.json · report.md · scenario_*/ramp_steps/*.jsonl
  </footer>
  <script>
    const REPORT = {data_json};
    const chartDefaults = {{
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{ legend: {{ labels: {{ color: '#8b9cb3' }} }} }},
      scales: {{
        x: {{ ticks: {{ color: '#8b9cb3' }}, grid: {{ color: '#2d3a4f' }} }},
        y: {{ ticks: {{ color: '#8b9cb3' }}, grid: {{ color: '#2d3a4f' }} }}
      }}
    }};
    function lineChart(id, title, series, yLabel, color, yMax) {{
      const el = document.getElementById(id);
      if (!el || !series || !series.length) return;
      const d = REPORT.charts[sidFromChart(id)];
      if (!d) return;
      const opts = JSON.parse(JSON.stringify(chartDefaults));
      if (yMax) opts.scales.y.max = yMax;
      new Chart(el, {{
        type: 'line',
        data: {{
          labels: d.n.map(n => 'N=' + n),
          datasets: [{{
            label: title,
            data: series,
            borderColor: color,
            backgroundColor: color + '33',
            tension: 0.2,
            fill: true
          }}]
        }},
        options: {{
          ...opts,
          plugins: {{
            ...opts.plugins,
            title: {{ display: true, text: title, color: '#e7ecf3' }}
          }},
          scales: {{
            ...opts.scales,
            y: {{ ...opts.scales.y, title: {{ display: true, text: yLabel, color: '#8b9cb3' }} }}
          }}
        }}
      }});
    }}
    function sidFromChart(id) {{
      const m = id.match(/^chart-(s\\d)-/);
      return m ? m[1] : '';
    }}
    Object.entries(REPORT.charts || {{}}).forEach(([sid, d]) => {{
      lineChart('chart-' + sid + '-success', '成功率 (%)', d.success_rate, '%', '#10b981', 100);
      lineChart('chart-' + sid + '-latency', 'Setup p99 (ms)', d.setup_p99_ms, 'ms', '#f59e0b');
      lineChart('chart-' + sid + '-rss', 'RSS 峰值 (MB)', d.rss_peak_mb, 'MB', '#3b82f6');
      lineChart('chart-' + sid + '-cpu', 'CPU 峰值 (%)', d.cpu_peak_pct, '%', '#ef4444');
      lineChart('chart-' + sid + '-calls', 'calls_active 峰值', d.calls_peak, 'calls', '#8b5cf6');
      lineChart('chart-' + sid + '-goroutines', 'Goroutines 峰值', d.goroutines_peak, 'count', '#06b6d4');
    }});
  </script>
</body>
</html>
"""


def write_html_report(output_dir: Path, **kwargs: Any) -> Path:
    text = render_html_report(**kwargs)
    path = output_dir / "report.html"
    path.write_text(text, encoding="utf-8")
    return path


def regenerate_html_from_dir(result_dir: Path) -> Path | None:
    import yaml

    summary_path = result_dir / "summary.json"
    if not summary_path.is_file():
        return None
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    config: dict[str, Any] = {}
    config_path = result_dir / "config.yaml"
    if config_path.is_file():
        config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    profile = config.get("profile") or {}
    args = config.get("args") or {}
    environment = summary.get("environment") or {}
    ramp_policy = summary.get("ramp_policy") or profile.get("ramp_policy") or {}
    return write_html_report(
        result_dir,
        run_id=summary.get("run_id", result_dir.name),
        profile_name=profile.get("name", "unknown"),
        mode=args.get("mode", "boundary"),
        environment=environment,
        scenarios=summary.get("scenarios") or [],
        summary=summary,
        ramp_policy=ramp_policy,
    )

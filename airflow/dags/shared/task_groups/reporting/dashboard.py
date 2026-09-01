# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Portable default HTML view for any orchestration report."""

from __future__ import annotations

import json
import re
from functools import lru_cache
from html import escape
from pathlib import Path
from typing import Any, Mapping

ASSET_DIR = Path(__file__).resolve().parent / "assets"


@lru_cache(maxsize=None)
def _read_asset(filename: str) -> str:
    return (ASSET_DIR / filename).read_text(encoding="utf-8")


def _embed_json(value: Any) -> str:
    return (
        json.dumps(value, separators=(",", ":"), default=str)
        .replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
    )


def render_polished_dashboard(
    report: dict[str, Any],
    display: Mapping[str, Any],
    task_categories: Mapping[str, str] | None = None,
) -> str:
    """Render the shared, self-contained dashboard with workflow display metadata."""
    values = {
        "__DASHBOARD_TITLE__": escape(str(display["title"])),
        "__DASHBOARD_STYLE__": _read_asset("dashboard.css"),
        "__DASHBOARD_SCRIPT__": _read_asset("dashboard.js"),
        "__REPORT_DATA__": _embed_json(report),
        "__TASK_CATEGORIES__": _embed_json(task_categories or {}),
        "__DASHBOARD_CONFIG__": _embed_json(display),
    }
    return re.compile("|".join(re.escape(name) for name in values)).sub(
        lambda match: values[match.group(0)], _read_asset("dashboard.html")
    )


def render_default_dashboard(
    report: dict[str, Any], title: str = "Orchestration statistics"
) -> str:
    """Render a dependency-free dashboard without workflow-specific assumptions."""
    report_json = (
        json.dumps(report, separators=(",", ":"), default=str)
        .replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
    )
    title_json = json.dumps(title)
    return f"""<!doctype html>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title><style>body{{font:14px system-ui;margin:2rem;color:#172019}}table{{border-collapse:collapse;width:100%}}th,td{{border-bottom:1px solid #dfe7e1;padding:.5rem;text-align:left}}code{{overflow-wrap:anywhere}}</style>
<h1 id="title"></h1><p id="run"></p><h2>Workload</h2><pre id="workload"></pre><h2>Throughput</h2><pre id="throughput"></pre><h2>Task instances</h2><table><thead><tr><th>Task</th><th>State</th><th>Queued</th><th>Runtime</th></tr></thead><tbody id="tasks"></tbody></table>
<script type="application/json" id="report-data">{report_json}</script><script>
const report=JSON.parse(document.querySelector('#report-data').textContent), title={title_json};
const text=(id,value)=>document.querySelector(id).textContent=value;
text('#title',title);text('#run',`${{report.dag_run.dag_id}} · ${{report.dag_run.run_id}}`);
text('#workload',JSON.stringify(report.workload,null,2));text('#throughput',JSON.stringify(report.throughput,null,2));
document.querySelector('#tasks').innerHTML=(report.task_instances||[]).map(t=>`<tr><td><code>${{String(t.task_id||'').replaceAll('&','&amp;').replaceAll('<','&lt;')}}</code></td><td>${{t.state??''}}</td><td>${{t.queued_duration_seconds??'—'}}</td><td>${{t.task_duration_seconds??'—'}}</td></tr>`).join('');
</script>"""

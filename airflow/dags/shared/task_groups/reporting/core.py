# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Common collection, measurement, and artifact writing for orchestration reports."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import yaml
from airflow.exceptions import AirflowFailException

from dags.shared.task_groups.reporting.airflow_api import AirflowApiClient
from dags.shared.task_groups.reporting.dashboard import render_default_dashboard
from dags.shared.task_groups.reporting.task_timing import (
    latest_end_time,
    normalize_task_instance,
    seconds_between,
    summarize_tasks,
    tasks_ending_by,
)
from dags.shared.utils.msc_utils import write_file_to_directory
from dags.shared.utils.video_input_utils import join_storage_base_path_filename

PayloadParser = Callable[[str | dict[str, Any]], Any]
WorkloadBuilder = Callable[[Any, dict[str, Any]], dict[str, Any]]
ThroughputBuilder = Callable[[dict[str, Any], list[dict[str, Any]], float | None], dict[str, Any]]
HtmlRenderer = Callable[[dict[str, Any]], str]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrchestrationReportDefinition:
    """Workflow-owned semantics required by the DAG-agnostic report engine."""

    report_type: str
    schema_version: int
    yaml_filename: str
    html_filename: str
    parse_payload: PayloadParser
    output_directory: Callable[[Any], str]
    workload_builder: WorkloadBuilder
    throughput_builder: ThroughputBuilder
    render_html: HtmlRenderer | None = None
    dashboard_title: str = "Orchestration statistics"


def xcom_length(ti: Any, task_id: str | None) -> int | None:
    """Read a conventional count/list XCom; callers may supply another extractor instead."""
    if not task_id:
        return None
    try:
        value = ti.xcom_pull(task_ids=task_id, key="return_value")
    except Exception:
        return None
    if isinstance(value, dict):
        if isinstance(value.get("length"), int):
            return value["length"]
        if isinstance(value.get("videos"), list):
            return len(value["videos"])
    return len(value) if isinstance(value, list) else None


def collect_run_metadata(dag_id: str, run_id: str) -> dict[str, Any]:
    client = AirflowApiClient.from_connection()
    return {
        "base_url": client.base_url,
        "dag_run": client.get_dag_run(dag_id, run_id),
        "task_instances": client.list_task_instances(dag_id, run_id),
    }


def _identity(run_id: str, context: dict[str, Any]) -> tuple[str, str, Any]:
    dag_run, ti = context.get("dag_run"), context["ti"]
    dag_id = str(
        context.get("dag_id") or getattr(ti, "dag_id", None) or getattr(dag_run, "dag_id", "")
    )
    effective_run_id = run_id or str(context.get("run_id") or getattr(dag_run, "run_id", ""))
    if not dag_id or not effective_run_id:
        raise AirflowFailException("DAG ID and run ID are required for performance reporting")
    return dag_id, effective_run_id, dag_run


def _iso(value: Any) -> Any:
    if isinstance(value, datetime):
        return (value if value.tzinfo else value.replace(tzinfo=timezone.utc)).isoformat()
    return value


def generate_orchestration_report(
    definition: OrchestrationReportDefinition,
    payload: str | dict[str, Any] = "",
    run_id: str = "",
    *,
    completion_task_id: str = "",
    excluded_task_ids: list[str] | None = None,
    metadata_collector: Callable[[str, str], dict[str, Any]] | None = None,
    writer: Callable[[str, bytes], None] | None = None,
    **context: Any,
) -> dict[str, Any]:
    """Collect a run's common orchestration facts and write required YAML/HTML artifacts."""
    try:
        config = definition.parse_payload(payload)
        dag_id, effective_run_id, dag_run_context = _identity(run_id, context)
        generated_at, warnings, metadata = datetime.now(timezone.utc), [], {}
        try:
            metadata = (metadata_collector or collect_run_metadata)(dag_id, effective_run_id)
        except Exception:
            logger.exception("Airflow REST API timing collection unavailable")
            warnings.append("Airflow REST API timing collection unavailable")
        api_run = metadata.get("dag_run") or {}
        run_start = _iso(api_run.get("start_date") or getattr(dag_run_context, "start_date", None))
        collected_tasks = [
            normalize_task_instance(task) for task in metadata.get("task_instances") or []
        ]
        completed_at = latest_end_time(collected_tasks, completion_task_id)
        if completed_at is None:
            warnings.append(
                "Workflow completion task timing was unavailable, so the measurement window could not be established; elapsed time and throughput are omitted"
            )
        ended_at = completed_at.isoformat() if completed_at else None
        elapsed = seconds_between(ended_at, run_start)
        tasks = tasks_ending_by(
            collected_tasks, ended_at or generated_at.isoformat(), excluded_task_ids or ()
        )
        tasks.sort(
            key=lambda task: (
                str(task.get("task_id")),
                -1 if task.get("map_index") is None else int(task["map_index"]),
            )
        )
        report_directory = join_storage_base_path_filename(
            join_storage_base_path_filename(definition.output_directory(config), effective_run_id),
            "reports",
        )
        yaml_path = join_storage_base_path_filename(report_directory, definition.yaml_filename)
        html_path = join_storage_base_path_filename(report_directory, definition.html_filename)
        state = api_run.get("state") or getattr(dag_run_context, "state", None)
        report = {
            "schema_version": definition.schema_version,
            "report_type": definition.report_type,
            "generated_at": generated_at.isoformat(),
            "artifact_path": yaml_path,
            "artifacts": {"yaml": yaml_path, "html": html_path},
            "dag_run": {
                "dag_id": dag_id,
                "run_id": effective_run_id,
                "state_at_collection": str(getattr(state, "value", state))
                if state is not None
                else None,
                "started_at": run_start,
                "measurement_ended_at": ended_at,
                "measurement_end_task_id": completion_task_id if completed_at else None,
                "elapsed_seconds": elapsed,
            },
            "workload": definition.workload_builder(config, context),
            "throughput": {},
            "airflow_api": {
                "status": "collected" if metadata else "unavailable",
                "base_url": metadata.get("base_url"),
                "task_instance_count": len(tasks),
                "collected_task_instance_count": len(collected_tasks),
                "excluded_task_instance_count": len(collected_tasks) - len(tasks),
                "warnings": warnings,
            },
            "task_summary": summarize_tasks(tasks),
            "task_instances": tasks,
        }
        report["throughput"] = definition.throughput_builder(report["workload"], tasks, elapsed)
        output_writer = writer or write_file_to_directory
        output_writer(
            yaml_path, yaml.safe_dump(report, sort_keys=False, allow_unicode=True).encode("utf-8")
        )
        html = (
            definition.render_html(report)
            if definition.render_html
            else render_default_dashboard(report, definition.dashboard_title)
        )
        output_writer(html_path, html.encode("utf-8"))
        return report
    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(f"Performance report generation failed: {e}") from e

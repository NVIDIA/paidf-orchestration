# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""EVG workload semantics for the shared orchestration reporting framework."""

from __future__ import annotations

import ast
from typing import Any

from dags.shared.task_groups.reporting import (
    OrchestrationReportDefinition,
    generate_orchestration_report,
    render_polished_dashboard,
    xcom_length,
)
from dags.shared.task_groups.reporting import (
    collect_run_metadata as _collect_run_metadata,
)
from dags.shared.task_groups.reporting.task_timing import rate, stage_wall_seconds
from dags.shared.utils.msc_utils import write_file_to_directory
from dags.workflows.event_video_generation_dag.models import EventVideoGenerationDagPayloadConfig

REPORT_FILENAME = "paidf_orchestration_stats.yaml"
HTML_REPORT_FILENAME = "paidf_orchestration_stats.html"
REPORT_TYPE = "paidf_event_video_generation_orchestration_stats"
SCHEMA_VERSION = 1

_TASK_CATEGORY_RULES = (
    ("cosmos_augmentation.augmentation", "augmentation"),
    ("detection_and_tracking", "processing"),
    ("captioning", "inference"),
    ("visual_qa", "inference"),
    ("person_attribute_search", "inference"),
)


def _task_categories(report: dict[str, Any]) -> dict[str, str]:
    """Classify EVG tasks for the shared dashboard's timeline legend."""
    categories = {}
    for task in report.get("task_instances") or []:
        task_id = task.get("task_id")
        if not task_id:
            continue
        category = next(
            (value for fragment, value in _TASK_CATEGORY_RULES if fragment in task_id),
            "validation"
            if "validate" in task_id
            else "wait"
            if task_id.startswith("wait_")
            else "orchestration",
        )
        categories[task_id] = category
    return categories


DASHBOARD_DISPLAY = {
    "title": "Event Video Generation",
    "unit_singular": "scene",
    "unit_plural": "scenes",
    "input_label": "input images",
    "input_count_key": "input_images",
    "per_input_count_key": "augmentations_per_input",
    "per_input_label": "augmentations/input",
    "final_label": "Final dataset scenes",
    "final_count_key": "final_dataset_scenes",
    "successful_count_key": "successful_augmented_videos",
    "successful_label": "successful augmented videos",
    "end_to_end_rate_key": "end_to_end_final_dataset_scenes",
    "augmentation_rate_key": "augmentation_stage",
    "stage_label": "Augmentation",
}


def collect_run_metadata(dag_id: str, run_id: str) -> dict[str, Any]:
    """Compatibility seam that delegates REST collection to shared reporting."""
    return _collect_run_metadata(dag_id, run_id)


def _parse_payload(payload: str | dict[str, Any]) -> EventVideoGenerationDagPayloadConfig:
    raw = payload if isinstance(payload, dict) else ast.literal_eval(payload or "{}")
    return EventVideoGenerationDagPayloadConfig.model_validate(raw)


def generate_event_video_generation_performance_report(
    payload: str | dict[str, Any] = "",
    run_id: str = "",
    *,
    completion_task_id: str = "",
    excluded_task_ids: list[str] | None = None,
    workload_task_ids: dict[str, str] | None = None,
    augmentation_stage_task_ids: list[str] | None = None,
    **context: Any,
) -> dict[str, Any]:
    """Write EVG orchestration timing, throughput, YAML, and HTML artifacts."""
    workload_task_ids = workload_task_ids or {}
    augmentation_stage_task_ids = augmentation_stage_task_ids or []

    def workload(
        config: EventVideoGenerationDagPayloadConfig, report_context: dict[str, Any]
    ) -> dict[str, Any]:
        input_images = xcom_length(report_context["ti"], workload_task_ids.get("input"))
        successful_videos = xcom_length(
            report_context["ti"], workload_task_ids.get("successful_augmentations")
        )
        final_scenes = xcom_length(report_context["ti"], workload_task_ids.get("final_dataset"))
        augmentations_per_input = int(config.cosmos.num_augmentation or 1)
        return {
            "input_images": input_images,
            "augmentations_per_input": augmentations_per_input,
            "expected_augmentations": (
                None if input_images is None else input_images * augmentations_per_input
            ),
            "successful_augmented_videos": successful_videos,
            "final_dataset_scenes": final_scenes,
        }

    def throughput(
        workload_data: dict[str, Any], tasks: list[dict[str, Any]], elapsed: float | None
    ) -> dict[str, Any]:
        augmentation_wall_seconds = stage_wall_seconds(tasks, augmentation_stage_task_ids)
        return {
            "end_to_end_final_dataset_scenes": rate(workload_data["final_dataset_scenes"], elapsed),
            "end_to_end_successful_augmented_videos": rate(
                workload_data["successful_augmented_videos"], elapsed
            ),
            "augmentation_stage_wall_seconds": augmentation_wall_seconds,
            "augmentation_stage": rate(
                workload_data["successful_augmented_videos"], augmentation_wall_seconds
            ),
        }

    definition = OrchestrationReportDefinition(
        report_type=REPORT_TYPE,
        schema_version=SCHEMA_VERSION,
        yaml_filename=REPORT_FILENAME,
        html_filename=HTML_REPORT_FILENAME,
        parse_payload=_parse_payload,
        output_directory=lambda config: config.output_directory,
        workload_builder=workload,
        throughput_builder=throughput,
        render_html=lambda report: render_polished_dashboard(
            report, DASHBOARD_DISPLAY, _task_categories(report)
        ),
    )
    return generate_orchestration_report(
        definition,
        payload,
        run_id,
        completion_task_id=completion_task_id,
        excluded_task_ids=excluded_task_ids,
        metadata_collector=collect_run_metadata,
        writer=write_file_to_directory,
        **context,
    )

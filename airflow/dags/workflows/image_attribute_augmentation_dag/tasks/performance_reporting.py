# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""IAA-specific semantics and presentation for shared orchestration reporting."""

from __future__ import annotations

import ast
from collections.abc import Mapping
from typing import Any

from dags.shared.task_groups.reporting import (
    OrchestrationReportDefinition,
    generate_orchestration_report,
    render_polished_dashboard,
    xcom_length,
)
from dags.shared.task_groups.reporting.task_timing import rate, stage_wall_seconds
from dags.workflows.image_attribute_augmentation_dag.models import (
    ImageAttributeAugmentationDagPayloadConfig,
)

REPORT_FILENAME = "paidf_orchestration_stats.yaml"
HTML_REPORT_FILENAME = "paidf_orchestration_stats.html"
REPORT_TYPE = "paidf_image_attribute_augmentation_orchestration_stats"
SCHEMA_VERSION = 1
_CATEGORY_SUBSTRING_RULES = (
    ("augmentation_external", "augmentation"),
    ("augmentation_internal", "augmentation"),
    ("cosmos_post_processing", "processing"),
    ("event_and_person", "inference"),
)

DASHBOARD_DISPLAY = {
    "title": "Image Attribute Augmentation",
    "unit_singular": "image",
    "unit_plural": "images",
    "input_label": "inputs",
    "input_count_key": "input_items",
    "per_input_count_key": "augmentations_per_input",
    "per_input_label": "augmentations/input",
    "final_label": "Final images",
    "final_count_key": "final_dataset_items",
    "successful_count_key": "successful_augmentations",
    "successful_label": "successful augmentations",
    "end_to_end_rate_key": "end_to_end_final_dataset",
    "augmentation_rate_key": "augmentation_stage",
    "stage_label": "Augmentation",
}


def resolve_task_category(task_id: str, overrides: Mapping[str, str] | None = None) -> str:
    if overrides and task_id in overrides:
        return overrides[task_id]
    for substring, category in _CATEGORY_SUBSTRING_RULES:
        if substring in task_id:
            return category
    if task_id.startswith("wait_"):
        return "wait"
    return "validation" if "validate" in task_id else "orchestration"


def build_task_categories(
    report: Mapping[str, Any], overrides: Mapping[str, str] | None = None
) -> dict[str, str]:
    return {
        task_id: resolve_task_category(task_id, overrides)
        for task_id in sorted(
            {
                str(task["task_id"])
                for task in report.get("task_instances") or []
                if task.get("task_id")
            }
        )
    }


def render_performance_report_html(
    report: dict[str, Any], task_categories: Mapping[str, str] | None = None
) -> str:
    return render_polished_dashboard(
        report,
        DASHBOARD_DISPLAY,
        build_task_categories(report, task_categories),
    )


def _parse_payload(payload: str | dict[str, Any]) -> ImageAttributeAugmentationDagPayloadConfig:
    return ImageAttributeAugmentationDagPayloadConfig.model_validate(
        payload if isinstance(payload, dict) else ast.literal_eval(payload or "{}")
    )


def generate_image_attribute_augmentation_performance_report(
    payload: str | dict[str, Any] = "",
    run_id: str = "",
    *,
    completion_task_id: str = "",
    excluded_task_ids: list[str] | None = None,
    workload_task_ids: dict[str, str] | None = None,
    augmentation_stage_task_ids: list[str] | None = None,
    **context: Any,
) -> dict[str, Any]:
    """Supply IAA workload semantics to the DAG-agnostic reporting engine."""
    workload_task_ids = workload_task_ids or {}
    augmentation_stage_task_ids = augmentation_stage_task_ids or []

    def workload(
        config: ImageAttributeAugmentationDagPayloadConfig, report_context: dict[str, Any]
    ) -> dict[str, Any]:
        inputs = xcom_length(report_context["ti"], workload_task_ids.get("input"))
        successful = xcom_length(
            report_context["ti"], workload_task_ids.get("successful_augmentations")
        )
        final = xcom_length(report_context["ti"], workload_task_ids.get("final_dataset"))
        count = int(config.cosmos.num_augmentation or 1)
        return {
            "input_items": inputs,
            "augmentations_per_input": count,
            "expected_augmentations": None if inputs is None else inputs * count,
            "successful_augmentations": successful,
            "final_dataset_items": final,
        }

    def throughput(
        workload_data: dict[str, Any], tasks: list[dict[str, Any]], elapsed: float | None
    ) -> dict[str, Any]:
        stage = stage_wall_seconds(tasks, augmentation_stage_task_ids)
        return {
            "end_to_end_final_dataset": rate(workload_data["final_dataset_items"], elapsed),
            "end_to_end_successful_augmentations": rate(
                workload_data["successful_augmentations"], elapsed
            ),
            "augmentation_stage_wall_seconds": stage,
            "augmentation_stage": rate(workload_data["successful_augmentations"], stage),
        }

    categories = {task_id: "augmentation" for task_id in augmentation_stage_task_ids}
    if completion_task_id:
        categories[completion_task_id] = "validation"
    definition = OrchestrationReportDefinition(
        REPORT_TYPE,
        SCHEMA_VERSION,
        REPORT_FILENAME,
        HTML_REPORT_FILENAME,
        _parse_payload,
        lambda config: config.output_directory,
        workload,
        throughput,
        lambda report: render_performance_report_html(report, categories),
    )
    return generate_orchestration_report(
        definition,
        payload,
        run_id,
        completion_task_id=completion_task_id,
        excluded_task_ids=excluded_task_ids,
        **context,
    )

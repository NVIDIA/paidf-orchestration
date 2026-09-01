# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Reusable, DAG-agnostic orchestration performance reporting task group."""

from dags.shared.task_groups.reporting.airflow_api import AirflowApiClient, AirflowApiError
from dags.shared.task_groups.reporting.core import (
    OrchestrationReportDefinition,
    collect_run_metadata,
    generate_orchestration_report,
    xcom_length,
)
from dags.shared.task_groups.reporting.dashboard import (
    render_default_dashboard,
    render_polished_dashboard,
)
from dags.shared.task_groups.reporting.task_group import PerformanceReportingTaskGroup

__all__ = [
    "AirflowApiClient",
    "AirflowApiError",
    "OrchestrationReportDefinition",
    "PerformanceReportingTaskGroup",
    "collect_run_metadata",
    "generate_orchestration_report",
    "render_default_dashboard",
    "render_polished_dashboard",
    "xcom_length",
]

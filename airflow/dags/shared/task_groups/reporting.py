# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Generic reporting task group."""

import logging
from collections.abc import Callable
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup


class ReportingTaskGroup:
    """Task group wrapper for workflow-specific report generation callables."""

    def __init__(
        self,
        *,
        report_generation_callable: Callable[..., Any],
        group_id: str = "reporting_and_output",
    ):
        """Initialize the reporting task group."""
        self.report_generation_callable = report_generation_callable
        self.group_id = group_id
        self.logger = logging.getLogger(__name__)

    def get_generate_report_task(self) -> PythonOperator:
        """Get the workflow-specific report generation task."""
        return PythonOperator(
            task_id="generate_report_and_output",
            python_callable=self.report_generation_callable,
            op_kwargs={"payload": "{{ params.payload }}"},
        )

    def get_reporting_task_group(self) -> TaskGroup:
        """Get the reporting task group."""
        with TaskGroup(group_id=self.group_id) as task_group:
            self.get_generate_report_task()
        return task_group

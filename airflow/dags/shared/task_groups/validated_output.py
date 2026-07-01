# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Task group-style output validation helpers for workflow DAGs."""

from collections.abc import Callable
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup


class ValidatedOutputTaskGroup:
    """Reusable task-group wrapper for post-processing output validation."""

    def __init__(
        self,
        validation_callable: Callable[..., Any],
        op_kwargs: dict[str, Any] | None = None,
        group_id: str = "validated_output",
    ):
        self.validation_callable = validation_callable
        self.op_kwargs = op_kwargs or {
            "payload": "{{ params.payload }}",
            "run_id": "{{ run_id }}",
        }
        self.group_id = group_id

    def get_validate_output_task(self, task_id: str = "validate_outputs") -> PythonOperator:
        """Create a generic output validation task for the configured callable."""
        return PythonOperator(
            task_id=task_id,
            python_callable=self.validation_callable,
            op_kwargs=self.op_kwargs,
            retries=0,
        )

    def get_validated_output_task_group(self) -> TaskGroup:
        """Get the validated output task group."""
        with TaskGroup(group_id=self.group_id) as task_group:
            self.get_validate_output_task()
        return task_group

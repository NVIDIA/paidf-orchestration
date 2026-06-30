# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Generic task group for mapped auto-labeling container execution."""

import datetime
import logging
from collections.abc import Callable
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup

from dags.shared.utils.component_builder import ComponentBuilder


class AutoLabelingTaskGroup:
    """
    Task Group for Auto Labeling.

    Auto labeling processes videos using RFDETR for object detection
    and BoxMOT for multi-object tracking, producing structured metadata.
    """

    def __init__(
        self,
        builder: ComponentBuilder,
        input_xcom_task_id: str,
        input_xcom_key: str,
        task_config: str = "{{ params.payload.auto_labeling }}",
        group_id: str = "auto_labeling",
        component_name: str = "auto_labeling",
        prepare_args_callable: Callable[..., list[str]] | None = None,
        prepare_args_op_kwargs: dict[str, Any] | None = None,
    ):
        """
        Initialize the Auto Labeling Task Group.

        Args:
            builder: ComponentBuilder instance for building operators from manifest.
            input_xcom_task_id: Upstream task id that provides auto-labeling input list.
            input_xcom_key: XCom key to pull from ``input_xcom_task_id``.
            task_config: Auto-labeling task config payload (templated from DAG params).
            group_id: Unique identifier for this task group instance.
                      Use different IDs when the same task group is used
                      multiple times in a DAG (e.g., "auto_labeling_input",
                      "auto_labeling_output").
            prepare_args_callable: Workflow-specific callable that returns one
                container argument string per mapped auto-labeling task.
        """
        if prepare_args_callable is None:
            raise ValueError("prepare_args_callable is required")
        self.logger = logging.getLogger(__name__)
        self.builder = builder
        self.input_xcom_task_id = input_xcom_task_id
        self.input_xcom_key = input_xcom_key
        self.task_config = task_config
        self.group_id = group_id
        self.component_name = component_name
        self.prepare_args_callable = prepare_args_callable
        self.prepare_args_op_kwargs = prepare_args_op_kwargs or {}

    def prepare_auto_labeling_args(
        self,
        auto_labeling_config: str | None = None,
        run_id: str = "",
        **context,
    ) -> list[str]:
        """Run the workflow-specific auto-labeling argument preparation callable."""
        try:
            return self.prepare_args_callable(
                auto_labeling_config=auto_labeling_config,
                run_id=run_id,
                input_xcom_task_id=self.input_xcom_task_id,
                input_xcom_key=self.input_xcom_key,
                group_id=self.group_id,
                **self.prepare_args_op_kwargs,
                **context,
            )

        except Exception as e:
            raise AirflowFailException(f"Auto-labeling argument preparation failed: {e}") from e

    def get_auto_labeling_task_group(self) -> TaskGroup:
        """
        Get the Task Group needed for the Auto Labeling step.

        Uses Airflow's dynamic task mapping to run one container per video
        in parallel for maximum throughput.

        Returns:
            TaskGroup: Task group with config generation and parallel auto_labeling tasks.
        """
        with TaskGroup(group_id=self.group_id) as task_group:
            op_kwargs = {
                "auto_labeling_config": self.task_config,
                "run_id": "{{ run_id }}",
            }
            prepare_auto_labeling_configs = PythonOperator(
                task_id="prepare_auto_labeling_configs",
                python_callable=self.prepare_auto_labeling_args,
                op_kwargs=op_kwargs,
                retries=2,
                retry_delay=datetime.timedelta(seconds=30),
            )

            # Run auto labeling containers in parallel using dynamic task mapping
            # Each video gets its own container for parallel processing
            self.logger.info(
                "Using ComponentBuilder with dynamic task mapping for auto_labeling (group_id=%s)",
                self.group_id,
            )

            # Use partial_task + expand for parallel execution
            # This creates N parallel tasks where N = number of videos
            auto_labeling_task = self.builder.partial_task(
                component_name=self.component_name,
                task_id="auto_labeling",
                name="auto_labeling-{{ run_id | replace(':', '_') | replace('+', '_') | replace('.', '_') }}",
            ).expand(container_args=XComArg(prepare_auto_labeling_configs))

            prepare_auto_labeling_configs >> auto_labeling_task

        return task_group

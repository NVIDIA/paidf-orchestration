# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.

"""Optional, terminal reporting branch usable by any DAG."""

from __future__ import annotations

import ast
from collections.abc import Callable, Mapping
from typing import Any

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import TaskGroup
from airflow.task.trigger_rule import TriggerRule


class PerformanceReportingTaskGroup:
    """Create an opt-in reporting branch without assuming DAG topology."""

    def __init__(
        self,
        *,
        report_generation_callable: Callable[..., Any],
        report_op_kwargs: dict[str, Any] | None = None,
        validate_payload_task_id: str = "validate_payload",
        enabled_field: str = "enable_performance_reporting",
        group_id: str = "performance_reporting",
    ):
        self.report_generation_callable = report_generation_callable
        self.report_op_kwargs = report_op_kwargs or {}
        self.validate_payload_task_id = validate_payload_task_id
        self.enabled_field = enabled_field
        self.group_id = group_id

    @property
    def select_task_id(self) -> str:
        return f"{self.group_id}.select_report"

    @property
    def report_task_id(self) -> str:
        return f"{self.group_id}.generate_report"

    @property
    def skip_task_id(self) -> str:
        return f"{self.group_id}.skip_report"

    @property
    def done_task_id(self) -> str:
        return f"{self.group_id}.report_done"

    @property
    def payload_template(self) -> str:
        return f"{{{{ ti.xcom_pull(task_ids='{self.validate_payload_task_id}', key='return_value') }}}}"

    def select_report_branch(self, **context: Any) -> str:
        payload = (
            context["ti"].xcom_pull(task_ids=self.validate_payload_task_id, key="return_value")
            or {}
        )
        if isinstance(payload, str):
            payload = ast.literal_eval(payload)
        if not isinstance(payload, Mapping):
            payload = {}
        return self.report_task_id if payload.get(self.enabled_field) else self.skip_task_id

    def get_select_report_task(self) -> BranchPythonOperator:
        return BranchPythonOperator(
            task_id="select_report",
            python_callable=self.select_report_branch,
            trigger_rule=TriggerRule.ALL_DONE,
        )

    def get_generate_report_task(self) -> PythonOperator:
        return PythonOperator(
            task_id="generate_report",
            python_callable=self.report_generation_callable,
            retries=0,
            op_kwargs={
                "payload": self.payload_template,
                "run_id": "{{ run_id }}",
                "excluded_task_ids": [self.report_task_id],
                **self.report_op_kwargs,
            },
        )

    def get_performance_reporting_task_group(self) -> TaskGroup:
        with TaskGroup(group_id=self.group_id) as task_group:
            select = self.get_select_report_task()
            report = self.get_generate_report_task()
            skip_report = EmptyOperator(task_id="skip_report")
            report_done = EmptyOperator(task_id="report_done", trigger_rule=TriggerRule.ALL_DONE)
            select >> [report, skip_report] >> report_done
        return task_group

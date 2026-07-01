# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Input Preparation Task Group for video processing DAGs.

DAG-specific callables resolve inputs and return ``InputPreparationResult``
(``length`` + ``videos`` with ``video_key`` and ``video_path``).
"""

from collections.abc import Callable
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup

from dags.shared.models import InputPreparationResult


def validate_input_preparation_result(
    raw: Any,
    *,
    source: str = "prepare_input",
) -> InputPreparationResult:
    """
    Parse and validate a value as ``InputPreparationResult``.

    Raises:
        AirflowFailException: If validation fails or ``length`` does not match ``videos``.
    """
    try:
        return InputPreparationResult.model_validate(raw)
    except Exception as e:
        raise AirflowFailException(f"{source} is not a valid InputPreparationResult: {e}") from e


def require_prepared_input_from_xcom(ti: Any) -> InputPreparationResult:
    """
    Load and validate ``prepare_input`` return_value from a task instance.

    Used by Cosmos, auto_labeling, and validation so multi-video work always
    follows the same manifest as ``input_preparation.prepare_input``.
    """
    prepared_raw = ti.xcom_pull(task_ids="input_preparation.prepare_input", key="return_value")
    if prepared_raw is None:
        raise AirflowFailException(
            "prepare_input XCom is missing. Ensure input_preparation.prepare_input completed."
        )
    return validate_input_preparation_result(
        prepared_raw,
        source="prepare_input XCom",
    )


class InputPreparationTaskGroup:
    """
    Task Group for preparing inputs before downstream processing.

    Invokes a DAG-specific ``prepare_input_callable`` (same pattern as
    ``CosmosTaskGroup`` config/validation callables). The callable must return
    a dict compatible with ``InputPreparationResult``.
    """

    def __init__(
        self,
        prepare_input_callable: Callable[..., dict[str, Any]],
        prepare_input_op_kwargs: dict[str, Any] | None = None,
        group_id: str = "input_preparation",
    ):
        """
        Initialize the Input Preparation Task Group.

        Args:
            prepare_input_callable: DAG-specific callable that prepares inputs
                and returns an ``InputPreparationResult`` dict.
            prepare_input_op_kwargs: Extra keyword arguments passed to the callable.
            group_id: Unique identifier for this task group instance.
        """
        if prepare_input_callable is None:
            raise ValueError("prepare_input_callable is required")
        self.prepare_input_callable = prepare_input_callable
        self.prepare_input_op_kwargs = prepare_input_op_kwargs or {}
        self.group_id = group_id

    def prepare_video_input(
        self,
        payload: str = "",
        run_id: str = "",
        **context,
    ) -> dict[str, Any]:
        """Run the DAG-specific input preparer and return its validated XCom payload."""
        try:
            raw_result = self.prepare_input_callable(
                payload=payload,
                run_id=run_id,
                **self.prepare_input_op_kwargs,
                **context,
            )
        except AirflowFailException:
            raise
        except Exception as e:
            raise AirflowFailException(f"Input preparation failed: {e}") from e

        prepared = validate_input_preparation_result(
            raw_result,
            source="prepare_input callable return value",
        )
        return prepared.model_dump()

    def get_prepare_input_task(self) -> PythonOperator:
        """
        Get the input preparation task.

        Returns:
            PythonOperator that prepares inputs via the DAG-specific callable.
        """
        return PythonOperator(
            task_id="prepare_input",
            python_callable=self.prepare_video_input,
            op_kwargs={
                "payload": "{{ params.payload }}",
                "run_id": "{{ run_id }}",
            },
            retries=0,
        )

    def get_input_preparation_task_group(self) -> TaskGroup:
        """
        Get the Task Group for input preparation.

        Returns:
            TaskGroup with input preparation task.
        """
        with TaskGroup(group_id=self.group_id) as task_group:
            prepare_task = self.get_prepare_input_task()  # noqa: F841 - task registered with group

        return task_group

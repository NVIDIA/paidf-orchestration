# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared utilities for pulling and validating XCom payloads."""

from typing import Any, TypeVar

from airflow.exceptions import AirflowFailException
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


def pull_and_validate_xcom(ti: Any, task_id: str, key: str, model_class: type[T]) -> T:
    """
    Pull XCom data and validate it against the provided model class.

    Args:
        ti: Airflow task instance from execution context.
        task_id: Upstream task id to pull from.
        key: XCom key to pull.
        model_class: Pydantic model used for schema validation.

    Returns:
        Validated model instance.

    Raises:
        AirflowFailException: If the XCom payload is missing or validation fails.
    """
    raw_value = ti.xcom_pull(task_ids=task_id, key=key)
    if raw_value is None:
        raise AirflowFailException(f"Missing XCom value for task_id='{task_id}', key='{key}'.")

    try:
        return model_class.model_validate(raw_value)
    except Exception as e:
        raise AirflowFailException(
            f"XCom from task_id='{task_id}', key='{key}' is not a valid {model_class.__name__}: {e}"
        ) from e

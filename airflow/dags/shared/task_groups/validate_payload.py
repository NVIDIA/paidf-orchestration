# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Task group-style payload validation helpers for workflow DAGs."""

import ast
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator


class ValidatePayloadTaskGroup:
    """Reusable task-group style payload validator."""

    def __init__(self, model_class: Any):
        self.model_class = model_class

    def validate_payload(self, payload: str) -> dict[str, Any]:
        """Validate payload string against configured pydantic model class."""
        try:
            validated = self.model_class.model_validate(ast.literal_eval(payload))
            if hasattr(validated, "model_dump"):
                return validated.model_dump()
            if isinstance(validated, dict):
                return validated
            raise AirflowFailException(
                f"Validated payload for {self.model_class.__name__} "
                "did not return a dict-like value."
            )
        except Exception as e:
            raise AirflowFailException(
                f"Payload validation failed for {self.model_class.__name__}: {e}"
            ) from e

    def get_validate_payload_task(
        self,
        task_id: str = "validate_payload",
        payload: str = "{{ params.payload }}",
    ) -> PythonOperator:
        """Create a generic payload validation task for configured model class."""
        return PythonOperator(
            task_id=task_id,
            python_callable=self.validate_payload,
            op_kwargs={"payload": payload},
            retries=0,
        )

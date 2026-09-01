# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Compatibility task group for Event Video Generation's attribute-search stage."""

import datetime
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup

from dags.shared.task_groups.image_attribute_augmentation import (
    ImageAttributeAugmentationTaskGroup,
)


class EventAndPersonAttributeSearchTaskGroup(ImageAttributeAugmentationTaskGroup):
    """Preserve the Event Video Generation attribute-search callable interface."""

    @staticmethod
    def prepare_event_and_person_attribute_search_configs(
        event_and_person_attribute_search_config: str | None = None,
        **kwargs: Any,
    ) -> list[str]:
        return ImageAttributeAugmentationTaskGroup.prepare_image_attribute_augmentation_configs(
            image_attribute_augmentation_config=event_and_person_attribute_search_config,
            **kwargs,
        )

    def prepare_event_and_person_attribute_search_args(
        self,
        event_and_person_attribute_search_config: str | None = None,
        run_id: str = "",
        **context: Any,
    ) -> list[str]:
        try:
            kwargs = {
                "event_and_person_attribute_search_config": event_and_person_attribute_search_config,
                "run_id": run_id,
                "input_xcom_task_id": self.input_xcom_task_id,
                "input_xcom_key": self.input_xcom_key,
                "group_id": self.group_id,
                **self.prepare_args_op_kwargs,
                **context,
            }
            if self.output_group_id is not None:
                kwargs["output_group_id"] = self.output_group_id
            return self.prepare_args_callable(**kwargs)
        except Exception as e:
            raise AirflowFailException(
                f"Event and person attribute search argument preparation failed: {e}"
            ) from e

    def get_event_and_person_attribute_search_task_group(self) -> TaskGroup:
        """Build the dynamically mapped task group using its original interface."""
        with TaskGroup(group_id=self.group_id) as task_group:
            prepare_event_and_person_attribute_search_args = PythonOperator(
                task_id="prepare_event_and_person_attribute_search_args",
                python_callable=self.prepare_event_and_person_attribute_search_args,
                op_kwargs={
                    "event_and_person_attribute_search_config": self.task_config,
                    "run_id": "{{ run_id }}",
                },
                retries=2,
                retry_delay=datetime.timedelta(seconds=30),
            )
            task = self.builder.partial_task(
                component_name=self.component_name,
                task_id="event_and_person_attribute_search",
                name=(
                    f"{self.group_id}-"
                    "{{ run_id | replace(':', '_') | replace('+', '_') | replace('.', '_') }}"
                ),
            ).expand(container_args=XComArg(prepare_event_and_person_attribute_search_args))
            prepare_event_and_person_attribute_search_args >> task
        return task_group

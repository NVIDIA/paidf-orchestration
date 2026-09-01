# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Task group for Image Attribute Augmentation Cosmos postprocessing."""

from __future__ import annotations

import ast
import shlex

from airflow.exceptions import AirflowFailException
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup
from airflow.task.trigger_rule import TriggerRule

from dags.shared.models import CosmosOutputResult, CosmosTaskConfig, InputPreparationResult
from dags.shared.task_groups.service_lifecycle import require_service_endpoint_from_xcom
from dags.shared.utils.component_builder import ComponentBuilder
from dags.shared.utils.xcom import pull_and_validate_xcom

DEFAULT_OUTPUT_JSON = "augmented_data.json"
POST_PROCESSING_SCRIPT = "modules/data_processing/create_attribute_augmented_dataset.py"


def _parent_storage_path(path: str) -> str:
    normalized = path.rstrip("/")
    if "/" not in normalized:
        raise AirflowFailException(f"Cannot derive parent storage path from {path}")
    return normalized.rsplit("/", 1)[0]


def _metadata_path(prepared_image_path: str) -> str:
    parent, separator, filename = prepared_image_path.rpartition("/")
    if not separator or "." not in filename:
        raise AirflowFailException(
            f"Cannot derive preprocessing metadata path from {prepared_image_path}"
        )
    stem = filename.rsplit(".", 1)[0]
    return f"{parent}/{stem}.json"


def _quote(value: str) -> str:
    return shlex.quote(str(value))


class CosmosPostProcessingTaskGroup:
    """Build the mapped Image Attribute Augmentation Cosmos postprocessing tasks."""

    def __init__(
        self,
        builder: ComponentBuilder,
        task_config: str = "{{ params.payload.cosmos }}",
        prepared_input_task_id: str = "input_preparation.prepare_input",
        cosmos_output_task_id: str = "cosmos_augmentation.validate_outputs",
        input_xcom_key: str = "return_value",
        output_json: str = DEFAULT_OUTPUT_JSON,
        group_id: str = "cosmos_post_processing",
        component_name: str = "cosmos_post_processing",
    ):
        self.builder = builder
        self.task_config = task_config
        self.prepared_input_task_id = prepared_input_task_id
        self.cosmos_output_task_id = cosmos_output_task_id
        self.input_xcom_key = input_xcom_key
        self.output_json = output_json
        self.group_id = group_id
        self.component_name = component_name

    def prepare_args(
        self,
        cosmos_config: str | dict | None = None,
        **context,
    ) -> list[str]:
        """Build one postprocessing command per Cosmos augmentation output."""
        try:
            raw_config = (
                ast.literal_eval(cosmos_config) if isinstance(cosmos_config, str) else cosmos_config
            )
            task_config = CosmosTaskConfig.model_validate(raw_config or {})
        except Exception as e:
            raise AirflowFailException(
                f"Cosmos postprocessing config validation failed: {e}"
            ) from e

        vlm_endpoint = task_config.vlm_service_url
        if not task_config.external_services:
            vlm_endpoint = require_service_endpoint_from_xcom(context["ti"], "vlm_service")

        prepared_inputs = pull_and_validate_xcom(
            ti=context["ti"],
            task_id=self.prepared_input_task_id,
            key=self.input_xcom_key,
            model_class=InputPreparationResult,
        )
        cosmos_outputs = pull_and_validate_xcom(
            ti=context["ti"],
            task_id=self.cosmos_output_task_id,
            key=self.input_xcom_key,
            model_class=CosmosOutputResult,
        )

        prepared_by_key = {video.video_key: video.video_path for video in prepared_inputs.videos}
        commands = []
        for output in cosmos_outputs.videos:
            prepared_path = prepared_by_key.get(output.video_key)
            if prepared_path is None:
                raise AirflowFailException(
                    f"Cosmos output has no matching prepared input: {output.video_key}"
                )

            augmentation_dir = _parent_storage_path(output.video_path)
            post_processing_dir = f"{augmentation_dir}/postprocessing"
            args = [
                "run",
                "--no-sync",
                "python",
                POST_PROCESSING_SCRIPT,
                "--base-dir",
                _metadata_path(prepared_path),
                "--augmented-folders",
                augmentation_dir,
                "--output-dir",
                post_processing_dir,
                "--output-json",
                self.output_json,
                "--vlm-endpoint",
                vlm_endpoint or "",
                "--vlm-model",
                task_config.vlm_model or "",
                "--vlm-api-key-env",
                "VLM_API_KEY",
            ]
            commands.append(" ".join(_quote(arg) for arg in args))

        if not commands:
            raise AirflowFailException("Cosmos postprocessing received no augmentation outputs")
        return commands

    def get_cosmos_post_processing_task_group(self) -> TaskGroup:
        """Build the argument-preparation and mapped container tasks."""
        with TaskGroup(group_id=self.group_id) as task_group:
            prepare_cosmos_post_processing_args = PythonOperator(
                task_id="prepare_cosmos_post_processing_args",
                python_callable=self.prepare_args,
                op_kwargs={"cosmos_config": self.task_config},
            )
            post_processing = self.builder.partial_task(
                component_name=self.component_name,
                task_id="cosmos_post_processing",
                name=(
                    "cosmos-post-processing-"
                    "{{ run_id | replace(':', '_') | replace('+', '_') | replace('.', '_') }}"
                ),
            ).expand(container_args=XComArg(prepare_cosmos_post_processing_args))

            join_after_post_processing = EmptyOperator(
                task_id="join_after_post_processing",
                trigger_rule=TriggerRule.ALL_DONE_MIN_ONE_SUCCESS,
            )

            prepare_cosmos_post_processing_args >> post_processing >> join_after_post_processing

        return task_group

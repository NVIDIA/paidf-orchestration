# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Mapped Image Attribute Augmentation container execution."""

import ast
import copy
import datetime
import json
import logging
import shlex
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml
from airflow.exceptions import AirflowFailException
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup
from airflow.task.trigger_rule import TriggerRule
from yarl import URL

from dags.shared.models import ImageAttributeAugmentationTaskConfig, StoragePathListXcom
from dags.shared.task_groups.service_lifecycle import require_service_endpoint_from_xcom
from dags.shared.utils.component_builder import ComponentBuilder
from dags.shared.utils.image_attribute_augmentation_paths import get_cosmos_augmented_data_path
from dags.shared.utils.msc_utils import convert_msc_to_storage_url, write_file_to_directory
from dags.shared.utils.xcom import pull_and_validate_xcom


class ImageAttributeAugmentationTaskGroup:
    """Task group for Image Attribute Augmentation."""

    @staticmethod
    def _quote_arg(value: str) -> str:
        return shlex.quote(str(value))

    @staticmethod
    def _load_file(path: str, description: str) -> bytes:
        try:
            body = Path(path).read_bytes()
        except FileNotFoundError as e:
            raise AirflowFailException(f"{description} not found: {path}") from e
        except Exception as e:
            raise AirflowFailException(f"Failed to read {description} {path}: {e}") from e
        if not body:
            raise AirflowFailException(f"{description} is empty: {path}")
        return body

    @staticmethod
    def _load_config_template(path: str) -> dict[str, Any]:
        body = ImageAttributeAugmentationTaskGroup._load_file(
            path, "Image Attribute Augmentation config template"
        )
        try:
            config = yaml.safe_load(body)
        except Exception as e:
            raise AirflowFailException(
                f"Failed to parse Image Attribute Augmentation config template {path}: {e}"
            ) from e
        if not isinstance(config, dict):
            raise AirflowFailException(
                f"Image Attribute Augmentation config template must be a mapping: {path}"
            )
        return config

    @staticmethod
    def _build_remote_config(
        *,
        template: dict[str, Any],
        attribute_json: str,
        query_prompt_file: str,
        task_config: ImageAttributeAugmentationTaskConfig,
        llm_base_url: str,
    ) -> dict[str, Any]:
        config = copy.deepcopy(template)
        config.update(
            {
                "attribute_json": attribute_json,
                "query_prompt_file": query_prompt_file,
                "llm_endpoint_url": llm_base_url,
                "llm_model": task_config.llm_model,
            }
        )
        return config

    @staticmethod
    def _build_args(
        *,
        media_path: str,
        data_path: str,
        attribute_json: str,
        task_config: ImageAttributeAugmentationTaskConfig,
        llm_base_url: str,
    ) -> str:
        input_payload = json.dumps(
            [{"media_path": media_path, "data_path": data_path}],
            separators=(",", ":"),
        )
        args = [
            "--input",
            ImageAttributeAugmentationTaskGroup._quote_arg(input_payload),
            "--config-file",
            ImageAttributeAugmentationTaskGroup._quote_arg(task_config.config_file),
            "--attribute-json",
            ImageAttributeAugmentationTaskGroup._quote_arg(attribute_json),
            "--llm-endpoint-url",
            ImageAttributeAugmentationTaskGroup._quote_arg(llm_base_url),
            "--llm-model",
            ImageAttributeAugmentationTaskGroup._quote_arg(task_config.llm_model or ""),
        ]
        return " ".join(args)

    @staticmethod
    def prepare_image_attribute_augmentation_configs(
        image_attribute_augmentation_config: str | None = None,
        run_id: str = "",
        input_xcom_task_id: str = "input_preparation.prepare_input",
        input_xcom_key: str = "return_value",
        group_id: str = "event_and_person_attribute_search",
        output_group_id: str | None = None,
        config_template_path: str | None = None,
        query_prompt_file_path: str | None = None,
        **context,
    ) -> list[str]:
        """Generate one service command per prepared media object."""
        try:
            task_config = ImageAttributeAugmentationTaskConfig.model_validate(
                ast.literal_eval(image_attribute_augmentation_config)
            )
            llm_base_url = task_config.llm_service_url
            if not task_config.external_services:
                llm_base_url = require_service_endpoint_from_xcom(context["ti"], "llm_service")

            inputs = pull_and_validate_xcom(
                ti=context["ti"],
                task_id=input_xcom_task_id,
                key=input_xcom_key,
                model_class=StoragePathListXcom,
            )
            output_base = str(
                URL(task_config.output_directory) / run_id / (output_group_id or group_id)
            )
            config_template = (
                ImageAttributeAugmentationTaskGroup._load_config_template(config_template_path)
                if config_template_path is not None
                else None
            )
            if config_template is not None and query_prompt_file_path is None:
                raise AirflowFailException(
                    "query_prompt_file_path is required when config_template_path is provided"
                )

            query_prompt_body = None
            if config_template is not None:
                query_prompt_body = ImageAttributeAugmentationTaskGroup._load_file(
                    query_prompt_file_path, "Image Attribute Augmentation query prompt"
                )

            commands = []
            for media in inputs.videos:
                media_path = convert_msc_to_storage_url(media.video_path)
                attribute_json = get_cosmos_augmented_data_path(media_path)
                aug_token = media_path.rstrip("/").rsplit("/", 2)[-2]
                augmentation_index = aug_token.removeprefix("aug_")
                if not augmentation_index.isdigit():
                    raise AirflowFailException(
                        f"Cannot derive augmentation index from media path: {media_path}"
                    )
                data_path = f"{output_base}/{media.video_key}/{augmentation_index}"
                config_for_args = task_config
                if config_template is not None:
                    assets_base = f"{data_path}/sidecars/person_attribute_search/assets"
                    query_prompt_file = f"{assets_base}/{Path(query_prompt_file_path).name}"
                    remote_config_path = f"{assets_base}/{Path(config_template_path).name}"
                    remote_config = ImageAttributeAugmentationTaskGroup._build_remote_config(
                        template=config_template,
                        attribute_json=attribute_json,
                        query_prompt_file=query_prompt_file,
                        task_config=task_config,
                        llm_base_url=llm_base_url or "",
                    )
                    write_file_to_directory(query_prompt_file, query_prompt_body)
                    write_file_to_directory(
                        remote_config_path,
                        yaml.safe_dump(remote_config, sort_keys=False).encode("utf-8"),
                    )
                    config_for_args = task_config.model_copy(
                        update={"config_file": remote_config_path}
                    )
                if not config_for_args.config_file:
                    raise AirflowFailException(
                        "config_file is required when no config_template_path is provided"
                    )
                commands.append(
                    ImageAttributeAugmentationTaskGroup._build_args(
                        media_path=media_path,
                        data_path=data_path,
                        attribute_json=attribute_json,
                        task_config=config_for_args,
                        llm_base_url=llm_base_url or "",
                    )
                )
            return commands
        except AirflowFailException:
            raise
        except Exception as e:
            raise AirflowFailException(
                f"Image Attribute Augmentation config generation failed: {e}"
            ) from e

    def __init__(
        self,
        builder: ComponentBuilder,
        input_xcom_task_id: str,
        input_xcom_key: str,
        task_config: str = "{{ params.payload.event_and_person_attribute_search }}",
        group_id: str = "event_and_person_attribute_search",
        component_name: str = "event_and_person_attribute_search",
        output_group_id: str | None = None,
        prepare_args_callable: Callable[..., list[str]] | None = None,
        prepare_args_op_kwargs: dict[str, Any] | None = None,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
    ):
        self.logger = logging.getLogger(__name__)
        self.builder = builder
        self.input_xcom_task_id = input_xcom_task_id
        self.input_xcom_key = input_xcom_key
        self.task_config = task_config
        self.group_id = group_id
        self.component_name = component_name
        self.output_group_id = output_group_id
        self.prepare_args_callable = (
            prepare_args_callable
            or ImageAttributeAugmentationTaskGroup.prepare_image_attribute_augmentation_configs
        )
        self.prepare_args_op_kwargs = prepare_args_op_kwargs or {}
        # Callers whose immediate upstream is a mapped, multi-instance task should
        # pass a rule that tolerates partially failed upstream instances.
        self.trigger_rule = trigger_rule

    def prepare_image_attribute_augmentation_args(
        self,
        image_attribute_augmentation_config: str | None = None,
        run_id: str = "",
        **context,
    ) -> list[str]:
        """Run the configured argument preparation callable."""
        try:
            kwargs = {
                "image_attribute_augmentation_config": image_attribute_augmentation_config,
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
                f"Image Attribute Augmentation argument preparation failed: {e}"
            ) from e

    def get_image_attribute_augmentation_task_group(self) -> TaskGroup:
        """Build the dynamically mapped task group."""
        with TaskGroup(group_id=self.group_id) as task_group:
            prepare_event_and_person_attribute_search_args = PythonOperator(
                task_id="prepare_event_and_person_attribute_search_args",
                python_callable=self.prepare_image_attribute_augmentation_args,
                op_kwargs={
                    "image_attribute_augmentation_config": self.task_config,
                    "run_id": "{{ run_id }}",
                },
                retries=2,
                retry_delay=datetime.timedelta(seconds=30),
                trigger_rule=self.trigger_rule,
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

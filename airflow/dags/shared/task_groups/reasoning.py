# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Generic task group for mapped reasoning container execution."""

import ast
import datetime
import json
import logging
import shlex
from collections import defaultdict
from collections.abc import Callable
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup
from yarl import URL

from dags.shared.models import ReasoningTaskConfig, StoragePathListXcom
from dags.shared.models.payload import DEFAULT_OPENAI_COMPATIBLE_PROVIDER
from dags.shared.task_groups.service_lifecycle import require_service_endpoint_from_xcom
from dags.shared.utils.component_builder import ComponentBuilder
from dags.shared.utils.msc_utils import convert_msc_to_storage_url
from dags.shared.utils.xcom import pull_and_validate_xcom


class ReasoningTaskGroup:
    """Task group for reasoning labels with a manifest-defined container task."""

    @staticmethod
    def _parse_reasoning_config(value: Any) -> ReasoningTaskConfig:
        if isinstance(value, ReasoningTaskConfig):
            return value
        if isinstance(value, dict):
            return ReasoningTaskConfig.model_validate(value)
        if value is None:
            return ReasoningTaskConfig()
        if not isinstance(value, str):
            raise AirflowFailException(
                f"reasoning_config must be a dict or str, got {type(value).__name__}"
            )

        raw = value.strip()
        if not raw:
            return ReasoningTaskConfig()

        try:
            return ReasoningTaskConfig.model_validate(json.loads(raw))
        except json.JSONDecodeError:
            pass

        try:
            return ReasoningTaskConfig.model_validate(ast.literal_eval(raw))
        except Exception as e:
            raise AirflowFailException(
                "reasoning_config is not valid JSON or a Python literal dict"
            ) from e

    @staticmethod
    def _quote_arg(value: str | int | float | bool) -> str:
        return shlex.quote(str(value))

    @staticmethod
    def _build_reasoning_args(
        *,
        media_path: str,
        data_path: str,
        reasoning_config: ReasoningTaskConfig,
        llm_base_url: str | None,
    ) -> str:
        input_payload = json.dumps(
            [{"media_path": media_path, "data_path": data_path}],
            separators=(",", ":"),
        )
        args = [
            "--input",
            ReasoningTaskGroup._quote_arg(input_payload),
            "--llm-provider",
            ReasoningTaskGroup._quote_arg(DEFAULT_OPENAI_COMPATIBLE_PROVIDER),
            "--llm-endpoint-url",
            ReasoningTaskGroup._quote_arg(llm_base_url or ""),
            "--llm-model",
            ReasoningTaskGroup._quote_arg(reasoning_config.llm_model or ""),
            "--reasoning-mode",
            ReasoningTaskGroup._quote_arg(reasoning_config.reasoning_mode),
        ]

        return " ".join(args)

    @staticmethod
    def prepare_reasoning_configs(
        reasoning_config: str | dict[str, Any] | None = None,
        run_id: str = "",
        input_xcom_task_id: str = "input_preparation.prepare_input",
        input_xcom_key: str = "return_value",
        group_id: str = "reasoning",
        output_group_id: str | None = None,
        **context,
    ) -> list[str]:
        """Generate mapped reasoning container arguments."""
        try:
            task_config = ReasoningTaskGroup._parse_reasoning_config(reasoning_config)
            llm_base_url = task_config.llm_service_url

            if not task_config.external_services:
                llm_base_url = require_service_endpoint_from_xcom(context["ti"], "llm_service")

            storage_path_list_xcom = pull_and_validate_xcom(
                ti=context["ti"],
                task_id=input_xcom_task_id,
                key=input_xcom_key,
                model_class=StoragePathListXcom,
            )
            output_base = str(
                URL(task_config.output_directory) / run_id / (output_group_id or group_id)
            )

            reasoning_args = []
            per_key_index = defaultdict(int)
            for video in storage_path_list_xcom.videos:
                media_path = convert_msc_to_storage_url(video.video_path)
                video_key = video.video_key
                current_idx = per_key_index[video_key]
                data_path = f"{output_base}/{video_key}/{current_idx}"
                per_key_index[video_key] += 1

                reasoning_args.append(
                    ReasoningTaskGroup._build_reasoning_args(
                        media_path=media_path,
                        data_path=data_path,
                        reasoning_config=task_config,
                        llm_base_url=llm_base_url,
                    )
                )

            return reasoning_args
        except AirflowFailException:
            raise
        except Exception as e:
            raise AirflowFailException(f"Reasoning config generation failed: {e}") from e

    def __init__(
        self,
        builder: ComponentBuilder,
        input_xcom_task_id: str,
        input_xcom_key: str,
        task_config: str = "{{ params.payload.reasoning }}",
        group_id: str = "reasoning",
        component_name: str = "reasoning",
        output_group_id: str | None = None,
        prepare_args_callable: Callable[..., list[str]] | None = None,
        prepare_args_op_kwargs: dict[str, Any] | None = None,
    ):
        """
        Initialize the reasoning task group.

        Args:
            builder: ComponentBuilder instance for building operators from manifest.
            input_xcom_task_id: Upstream task id that provides reasoning input list.
            input_xcom_key: XCom key to pull from ``input_xcom_task_id``.
            task_config: Reasoning task config payload.
            group_id: Unique identifier for this task group instance.
            component_name: Manifest task component to build for reasoning.
            output_group_id: Optional output directory group name. When set,
                generated arguments write to ``output_directory/run_id/output_group_id``
                instead of ``output_directory/run_id/group_id``.
            prepare_args_callable: Optional override that returns one container
                argument string per mapped reasoning task.
            prepare_args_op_kwargs: Extra keyword arguments for the prepare callable.
        """
        if prepare_args_callable is None:
            prepare_args_callable = ReasoningTaskGroup.prepare_reasoning_configs
        self.logger = logging.getLogger(__name__)
        self.builder = builder
        self.input_xcom_task_id = input_xcom_task_id
        self.input_xcom_key = input_xcom_key
        self.task_config = task_config
        self.group_id = group_id
        self.component_name = component_name
        self.output_group_id = output_group_id
        self.prepare_args_callable = prepare_args_callable
        self.prepare_args_op_kwargs = prepare_args_op_kwargs or {}

    def prepare_reasoning_args(
        self,
        reasoning_config: str | None = None,
        run_id: str = "",
        **context,
    ) -> list[str]:
        """Run the configured reasoning argument preparation callable."""
        try:
            prepare_kwargs = {
                "reasoning_config": reasoning_config,
                "run_id": run_id,
                "input_xcom_task_id": self.input_xcom_task_id,
                "input_xcom_key": self.input_xcom_key,
                "group_id": self.group_id,
                **self.prepare_args_op_kwargs,
                **context,
            }
            if self.output_group_id is not None:
                prepare_kwargs["output_group_id"] = self.output_group_id
            return self.prepare_args_callable(**prepare_kwargs)
        except Exception as e:
            raise AirflowFailException(f"Reasoning argument preparation failed: {e}") from e

    def get_reasoning_task_group(self) -> TaskGroup:
        """Get the reasoning task group with one mapped container task per input video."""
        with TaskGroup(group_id=self.group_id) as task_group:
            prepare_reasoning_args = PythonOperator(
                task_id="prepare_reasoning_args",
                python_callable=self.prepare_reasoning_args,
                op_kwargs={
                    "reasoning_config": self.task_config,
                    "run_id": "{{ run_id }}",
                },
                retries=2,
                retry_delay=datetime.timedelta(seconds=30),
            )

            self.logger.info(
                "Using ComponentBuilder with dynamic task mapping for reasoning (group_id=%s)",
                self.group_id,
            )
            reasoning_task = self.builder.partial_task(
                component_name=self.component_name,
                task_id="reasoning",
                name=(
                    f"{self.group_id}-"
                    "{{ run_id | replace(':', '_') | replace('+', '_') | replace('.', '_') }}"
                ),
            ).expand(container_args=XComArg(prepare_reasoning_args))

            prepare_reasoning_args >> reasoning_task

        return task_group

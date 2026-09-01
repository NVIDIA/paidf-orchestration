# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Generic task group for mapped captioning container execution."""

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

from dags.shared.models import CaptioningTaskConfig, StoragePathListXcom
from dags.shared.models.payload import DEFAULT_OPENAI_COMPATIBLE_PROVIDER
from dags.shared.task_groups.service_lifecycle import require_service_endpoint_from_xcom
from dags.shared.utils.component_builder import ComponentBuilder
from dags.shared.utils.msc_utils import convert_msc_to_storage_url
from dags.shared.utils.xcom import pull_and_validate_xcom


class CaptioningTaskGroup:
    """Task group for captioning videos with a manifest-defined container task."""

    @staticmethod
    def _parse_captioning_config(value: Any) -> CaptioningTaskConfig:
        if isinstance(value, CaptioningTaskConfig):
            return value
        if isinstance(value, dict):
            return CaptioningTaskConfig.model_validate(value)
        if value is None:
            return CaptioningTaskConfig()
        if not isinstance(value, str):
            raise AirflowFailException(
                f"captioning_config must be a dict or str, got {type(value).__name__}"
            )

        raw = value.strip()
        if not raw:
            return CaptioningTaskConfig()

        try:
            return CaptioningTaskConfig.model_validate(json.loads(raw))
        except json.JSONDecodeError:
            pass

        try:
            return CaptioningTaskConfig.model_validate(ast.literal_eval(raw))
        except Exception as e:
            raise AirflowFailException(
                "captioning_config is not valid JSON or a Python literal dict"
            ) from e

    @staticmethod
    def _quote_arg(value: str | int | float | bool) -> str:
        return shlex.quote(str(value))

    @staticmethod
    def _build_captioning_args(
        *,
        media_path: str,
        data_path: str,
        captioning_config: CaptioningTaskConfig,
        vlm_base_url: str | None,
        llm_base_url: str | None,
    ) -> str:
        input_payload = json.dumps(
            [{"media_path": media_path, "data_path": data_path}],
            separators=(",", ":"),
        )
        args = [
            "--input",
            CaptioningTaskGroup._quote_arg(input_payload),
            "--vlm-provider",
            CaptioningTaskGroup._quote_arg(DEFAULT_OPENAI_COMPATIBLE_PROVIDER),
            "--vlm-endpoint-url",
            CaptioningTaskGroup._quote_arg(vlm_base_url or ""),
            "--vlm-model",
            CaptioningTaskGroup._quote_arg(captioning_config.vlm_model or ""),
        ]

        if captioning_config.enable_llm_summary:
            args.extend(
                [
                    "--enable-llm-summary",
                    "--llm-provider",
                    CaptioningTaskGroup._quote_arg(DEFAULT_OPENAI_COMPATIBLE_PROVIDER),
                    "--llm-endpoint-url",
                    CaptioningTaskGroup._quote_arg(llm_base_url or ""),
                    "--llm-model",
                    CaptioningTaskGroup._quote_arg(captioning_config.llm_model or ""),
                ]
            )

        return " ".join(args)

    @staticmethod
    def prepare_captioning_configs(
        captioning_config: str | dict[str, Any] | None = None,
        run_id: str = "",
        input_xcom_task_id: str = "input_preparation.prepare_input",
        input_xcom_key: str = "return_value",
        group_id: str = "captioning",
        output_group_id: str | None = None,
        **context,
    ) -> list[str]:
        """Generate mapped captioning container arguments."""
        try:
            task_config = CaptioningTaskGroup._parse_captioning_config(captioning_config)
            vlm_base_url = task_config.vlm_service_url
            llm_base_url = task_config.llm_service_url

            if not task_config.external_services:
                vlm_base_url = require_service_endpoint_from_xcom(context["ti"], "vlm_service")
                if task_config.enable_llm_summary:
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

            captioning_args = []
            per_key_index = defaultdict(int)
            for video in storage_path_list_xcom.videos:
                media_path = convert_msc_to_storage_url(video.video_path)
                video_key = video.video_key
                current_idx = per_key_index[video_key]
                data_path = f"{output_base}/{video_key}/{current_idx}"
                per_key_index[video_key] += 1

                captioning_args.append(
                    CaptioningTaskGroup._build_captioning_args(
                        media_path=media_path,
                        data_path=data_path,
                        captioning_config=task_config,
                        vlm_base_url=vlm_base_url,
                        llm_base_url=llm_base_url,
                    )
                )

            return captioning_args
        except AirflowFailException:
            raise
        except Exception as e:
            raise AirflowFailException(f"Captioning config generation failed: {e}") from e

    def __init__(
        self,
        builder: ComponentBuilder,
        input_xcom_task_id: str,
        input_xcom_key: str,
        task_config: str = "{{ params.payload.captioning }}",
        group_id: str = "captioning",
        component_name: str = "captioning",
        output_group_id: str | None = None,
        prepare_args_callable: Callable[..., list[str]] | None = None,
        prepare_args_op_kwargs: dict[str, Any] | None = None,
    ):
        """
        Initialize the captioning task group.

        Args:
            builder: ComponentBuilder instance for building operators from manifest.
            input_xcom_task_id: Upstream task id that provides captioning input list.
            input_xcom_key: XCom key to pull from ``input_xcom_task_id``.
            task_config: Captioning task config payload (templated from DAG params).
            group_id: Unique identifier for this task group instance.
            component_name: Manifest task component to build for captioning.
            output_group_id: Optional output directory group name. When set,
                generated arguments write to ``output_directory/run_id/output_group_id``
                instead of ``output_directory/run_id/group_id``.
            prepare_args_callable: Optional override that returns one container
                argument string per mapped captioning task. Defaults to the
                shared captioning argument generator.
            prepare_args_op_kwargs: Extra keyword arguments for the prepare callable.
        """
        if prepare_args_callable is None:
            prepare_args_callable = CaptioningTaskGroup.prepare_captioning_configs
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

    def prepare_captioning_args(
        self,
        captioning_config: str | None = None,
        run_id: str = "",
        **context,
    ) -> list[str]:
        """Run the configured captioning argument preparation callable."""
        try:
            prepare_kwargs = {
                "captioning_config": captioning_config,
                "run_id": run_id,
                "input_xcom_task_id": self.input_xcom_task_id,
                "input_xcom_key": self.input_xcom_key,
                "group_id": self.group_id,
                **self.prepare_args_op_kwargs,
                **context,
            }
            if self.output_group_id is not None:
                prepare_kwargs["output_group_id"] = self.output_group_id
            return self.prepare_args_callable(
                **prepare_kwargs,
            )
        except Exception as e:
            raise AirflowFailException(f"Captioning argument preparation failed: {e}") from e

    def get_captioning_task_group(self) -> TaskGroup:
        """Get the captioning task group with one mapped container task per input video."""
        with TaskGroup(group_id=self.group_id) as task_group:
            prepare_captioning_args = PythonOperator(
                task_id="prepare_captioning_args",
                python_callable=self.prepare_captioning_args,
                op_kwargs={
                    "captioning_config": self.task_config,
                    "run_id": "{{ run_id }}",
                },
                retries=2,
                retry_delay=datetime.timedelta(seconds=30),
            )

            self.logger.info(
                "Using ComponentBuilder with dynamic task mapping for captioning (group_id=%s)",
                self.group_id,
            )
            captioning_task = self.builder.partial_task(
                component_name=self.component_name,
                task_id="captioning",
                name=(
                    f"{self.group_id}-"
                    "{{ run_id | replace(':', '_') | replace('+', '_') | replace('.', '_') }}"
                ),
            ).expand(container_args=XComArg(prepare_captioning_args))

            prepare_captioning_args >> captioning_task

        return task_group

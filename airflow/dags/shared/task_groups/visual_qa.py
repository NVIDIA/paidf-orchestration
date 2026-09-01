# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Generic task group for mapped visual QA container execution."""

import ast
import datetime
import json
import logging
import shlex
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup
from yarl import URL

from dags.shared.models import StoragePathListXcom, VisualQATaskConfig
from dags.shared.models.payload import DEFAULT_OPENAI_COMPATIBLE_PROVIDER
from dags.shared.task_groups.service_lifecycle import require_service_endpoint_from_xcom
from dags.shared.utils.component_builder import ComponentBuilder
from dags.shared.utils.msc_utils import convert_msc_to_storage_url, write_file_to_directory
from dags.shared.utils.xcom import pull_and_validate_xcom


class VisualQATaskGroup:
    """Task group for visual question-answering with a manifest-defined container task."""

    @staticmethod
    def _parse_visual_qa_config(value: Any) -> VisualQATaskConfig:
        if isinstance(value, VisualQATaskConfig):
            return value
        if isinstance(value, dict):
            return VisualQATaskConfig.model_validate(value)
        if value is None:
            return VisualQATaskConfig()
        if not isinstance(value, str):
            raise AirflowFailException(
                f"visual_qa_config must be a dict or str, got {type(value).__name__}"
            )

        raw = value.strip()
        if not raw:
            return VisualQATaskConfig()

        try:
            return VisualQATaskConfig.model_validate(json.loads(raw))
        except json.JSONDecodeError:
            pass

        try:
            return VisualQATaskConfig.model_validate(ast.literal_eval(raw))
        except Exception as e:
            raise AirflowFailException(
                "visual_qa_config is not valid JSON or a Python literal dict"
            ) from e

    @staticmethod
    def _quote_arg(value: str | int | float | bool) -> str:
        return shlex.quote(str(value))

    @staticmethod
    def _build_visual_qa_args(
        *,
        media_path: str,
        data_path: str,
        visual_qa_config: VisualQATaskConfig,
        vlm_base_url: str | None,
        llm_base_url: str | None,
    ) -> str:
        input_payload = json.dumps(
            [{"media_path": media_path, "data_path": data_path}],
            separators=(",", ":"),
        )
        args = [
            "--input",
            VisualQATaskGroup._quote_arg(input_payload),
            "--generation-mode",
            VisualQATaskGroup._quote_arg(visual_qa_config.generation_mode),
            "--question-bank-file",
            VisualQATaskGroup._quote_arg(visual_qa_config.question_bank_file),
            "--input-source",
            VisualQATaskGroup._quote_arg(visual_qa_config.input_source),
            "--vlm-provider",
            VisualQATaskGroup._quote_arg(DEFAULT_OPENAI_COMPATIBLE_PROVIDER),
            "--vlm-endpoint-url",
            VisualQATaskGroup._quote_arg(vlm_base_url or ""),
            "--vlm-model",
            VisualQATaskGroup._quote_arg(visual_qa_config.vlm_model or ""),
            "--llm-provider",
            VisualQATaskGroup._quote_arg(DEFAULT_OPENAI_COMPATIBLE_PROVIDER),
            "--llm-endpoint-url",
            VisualQATaskGroup._quote_arg(llm_base_url or ""),
            "--llm-model",
            VisualQATaskGroup._quote_arg(visual_qa_config.llm_model or ""),
        ]

        if visual_qa_config.include_reasoning:
            args.append("--include-reasoning")

        args.extend(
            [
                "--media-mode",
                VisualQATaskGroup._quote_arg(visual_qa_config.media_mode),
            ]
        )

        return " ".join(args)

    @staticmethod
    def _load_question_bank_file(question_bank_file_path: str) -> bytes:
        try:
            body = Path(question_bank_file_path).read_bytes()
        except FileNotFoundError as e:
            raise AirflowFailException(
                f"Visual QA question bank file not found: {question_bank_file_path}"
            ) from e
        except Exception as e:
            raise AirflowFailException(
                f"Failed to read Visual QA question bank file {question_bank_file_path}: {e}"
            ) from e

        if not body:
            raise AirflowFailException(
                f"Visual QA question bank file is empty: {question_bank_file_path}"
            )
        return body

    @staticmethod
    def prepare_visual_qa_configs(
        visual_qa_config: str | dict[str, Any] | None = None,
        run_id: str = "",
        input_xcom_task_id: str = "input_preparation.prepare_input",
        input_xcom_key: str = "return_value",
        group_id: str = "visual_qa",
        output_group_id: str | None = None,
        question_bank_file_path: str | None = None,
        **context,
    ) -> list[str]:
        """Generate mapped visual QA container arguments."""
        try:
            task_config = VisualQATaskGroup._parse_visual_qa_config(visual_qa_config)
            vlm_base_url = task_config.vlm_service_url
            llm_base_url = task_config.llm_service_url

            if not task_config.external_services:
                vlm_base_url = require_service_endpoint_from_xcom(context["ti"], "vlm_service")
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
            question_bank_body = (
                VisualQATaskGroup._load_question_bank_file(question_bank_file_path)
                if question_bank_file_path is not None
                else None
            )

            visual_qa_args = []
            per_key_index = defaultdict(int)
            for video in storage_path_list_xcom.videos:
                media_path = convert_msc_to_storage_url(video.video_path)
                video_key = video.video_key
                current_idx = per_key_index[video_key]
                data_path = f"{output_base}/{video_key}/{current_idx}"
                per_key_index[video_key] += 1
                config_for_args = task_config
                if question_bank_body is not None:
                    question_bank_file = f"{data_path}/sidecars/question_bank.json"
                    write_file_to_directory(question_bank_file, question_bank_body)
                    config_for_args = task_config.model_copy(
                        update={"question_bank_file": question_bank_file}
                    )

                visual_qa_args.append(
                    VisualQATaskGroup._build_visual_qa_args(
                        media_path=media_path,
                        data_path=data_path,
                        visual_qa_config=config_for_args,
                        vlm_base_url=vlm_base_url,
                        llm_base_url=llm_base_url,
                    )
                )

            return visual_qa_args
        except AirflowFailException:
            raise
        except Exception as e:
            raise AirflowFailException(f"Visual QA config generation failed: {e}") from e

    def __init__(
        self,
        builder: ComponentBuilder,
        input_xcom_task_id: str,
        input_xcom_key: str,
        task_config: str = "{{ params.payload.visual_qa }}",
        group_id: str = "visual_qa",
        component_name: str = "visual_qa",
        output_group_id: str | None = None,
        prepare_args_callable: Callable[..., list[str]] | None = None,
        prepare_args_op_kwargs: dict[str, Any] | None = None,
    ):
        """
        Initialize the visual QA task group.

        Args:
            builder: ComponentBuilder instance for building operators from manifest.
            input_xcom_task_id: Upstream task id that provides visual QA input list.
            input_xcom_key: XCom key to pull from ``input_xcom_task_id``.
            task_config: Visual QA task config payload.
            group_id: Unique identifier for this task group instance.
            component_name: Manifest task component to build for visual QA.
            output_group_id: Optional output directory group name. When set,
                generated arguments write to ``output_directory/run_id/output_group_id``
                instead of ``output_directory/run_id/group_id``.
            prepare_args_callable: Optional override that returns one container
                argument string per mapped visual QA task.
            prepare_args_op_kwargs: Extra keyword arguments for the prepare callable.
        """
        if prepare_args_callable is None:
            prepare_args_callable = VisualQATaskGroup.prepare_visual_qa_configs
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

    def prepare_visual_qa_args(
        self,
        visual_qa_config: str | None = None,
        run_id: str = "",
        **context,
    ) -> list[str]:
        """Run the configured visual QA argument preparation callable."""
        try:
            prepare_kwargs = {
                "visual_qa_config": visual_qa_config,
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
            raise AirflowFailException(f"Visual QA argument preparation failed: {e}") from e

    def get_visual_qa_task_group(self) -> TaskGroup:
        """Get the visual QA task group with one mapped container task per input video."""
        with TaskGroup(group_id=self.group_id) as task_group:
            prepare_visual_qa_args = PythonOperator(
                task_id="prepare_visual_qa_args",
                python_callable=self.prepare_visual_qa_args,
                op_kwargs={
                    "visual_qa_config": self.task_config,
                    "run_id": "{{ run_id }}",
                },
                retries=2,
                retry_delay=datetime.timedelta(seconds=30),
            )

            self.logger.info(
                "Using ComponentBuilder with dynamic task mapping for visual_qa (group_id=%s)",
                self.group_id,
            )
            visual_qa_task = self.builder.partial_task(
                component_name=self.component_name,
                task_id="visual_qa",
                name=(
                    f"{self.group_id}-"
                    "{{ run_id | replace(':', '_') | replace('+', '_') | replace('.', '_') }}"
                ),
            ).expand(container_args=XComArg(prepare_visual_qa_args))

            prepare_visual_qa_args >> visual_qa_task

        return task_group

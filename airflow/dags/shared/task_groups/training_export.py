# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Task group for non-scaled training-export container execution."""

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

from dags.shared.models import StoragePathListXcom, TrainingExportTaskConfig
from dags.shared.utils.component_builder import ComponentBuilder
from dags.shared.utils.msc_utils import convert_msc_to_storage_url
from dags.shared.utils.xcom import pull_and_validate_xcom


class TrainingExportTaskGroup:
    """Task group that aggregates completed scenes into one training-export container."""

    @staticmethod
    def _parse_training_export_config(value: Any) -> TrainingExportTaskConfig:
        if isinstance(value, TrainingExportTaskConfig):
            return value
        if isinstance(value, dict):
            return TrainingExportTaskConfig.model_validate(value)
        if value is None:
            return TrainingExportTaskConfig()
        if not isinstance(value, str):
            raise AirflowFailException(
                f"training_export_config must be a dict or str, got {type(value).__name__}"
            )

        raw = value.strip()
        if not raw:
            return TrainingExportTaskConfig()

        try:
            return TrainingExportTaskConfig.model_validate(json.loads(raw))
        except json.JSONDecodeError:
            pass

        try:
            return TrainingExportTaskConfig.model_validate(ast.literal_eval(raw))
        except Exception as e:
            raise AirflowFailException(
                "training_export_config is not valid JSON or a Python literal dict"
            ) from e

    @staticmethod
    def _quote_arg(value: str | int | float | bool) -> str:
        return shlex.quote(str(value))

    @staticmethod
    def _build_training_export_args(
        *,
        entries: list[dict[str, str]],
        training_export_config: TrainingExportTaskConfig,
        export_dir: str,
    ) -> str:
        input_payload = json.dumps(entries, separators=(",", ":"))
        args = [
            "--input",
            TrainingExportTaskGroup._quote_arg(input_payload),
            "--training-export-dir",
            TrainingExportTaskGroup._quote_arg(export_dir),
        ]
        for export_format in training_export_config.formats:
            args.extend(
                [
                    "--training-export-format",
                    TrainingExportTaskGroup._quote_arg(export_format),
                ]
            )
        for task_type in training_export_config.tasks:
            args.extend(
                [
                    "--training-export-task",
                    TrainingExportTaskGroup._quote_arg(task_type),
                ]
            )
        if training_export_config.description:
            args.extend(
                [
                    "--training-export-description",
                    TrainingExportTaskGroup._quote_arg(training_export_config.description),
                ]
            )
        if training_export_config.license:
            args.extend(
                [
                    "--training-export-license",
                    TrainingExportTaskGroup._quote_arg(training_export_config.license),
                ]
            )
        for tag in training_export_config.tags:
            args.extend(
                [
                    "--training-export-tag",
                    TrainingExportTaskGroup._quote_arg(tag),
                ]
            )
        if not training_export_config.copy_media:
            args.append("--training-export-no-copy-media")
        if training_export_config.emit_media_root_as_null:
            args.append("--training-export-emit-media-root-as-null")

        return " ".join(args)

    @staticmethod
    def prepare_training_export_configs(
        training_export_config: str | dict[str, Any] | None = None,
        run_id: str = "",
        input_xcom_task_id: str = "input_preparation.prepare_input",
        input_xcom_key: str = "return_value",
        group_id: str = "training_export",
        output_group_id: str | None = None,
        **context,
    ) -> list[str]:
        """Generate a single training-export container argument string for all scenes."""
        try:
            task_config = TrainingExportTaskGroup._parse_training_export_config(
                training_export_config
            )
            storage_path_list_xcom = pull_and_validate_xcom(
                ti=context["ti"],
                task_id=input_xcom_task_id,
                key=input_xcom_key,
                model_class=StoragePathListXcom,
            )
            scene_group = output_group_id or group_id
            scene_base = str(URL(task_config.output_directory) / run_id / scene_group)
            export_dir = str(
                URL(task_config.output_directory) / run_id / "training_export" / scene_group
            )

            entries: list[dict[str, str]] = []
            per_key_index: dict[str, int] = defaultdict(int)
            for video in storage_path_list_xcom.videos:
                media_path = convert_msc_to_storage_url(video.video_path)
                video_key = video.video_key
                current_idx = per_key_index[video_key]
                data_path = f"{scene_base}/{video_key}/{current_idx}"
                per_key_index[video_key] += 1
                entries.append({"media_path": media_path, "data_path": data_path})

            if not entries:
                raise AirflowFailException(
                    "training_export requires at least one input video/scene entry"
                )

            return [
                TrainingExportTaskGroup._build_training_export_args(
                    entries=entries,
                    training_export_config=task_config,
                    export_dir=export_dir,
                )
            ]
        except AirflowFailException:
            raise
        except Exception as e:
            raise AirflowFailException(f"Training export config generation failed: {e}") from e

    def __init__(
        self,
        builder: ComponentBuilder,
        input_xcom_task_id: str,
        input_xcom_key: str,
        task_config: str = "{{ params.payload.training_export }}",
        group_id: str = "training_export",
        component_name: str = "training_export",
        output_group_id: str | None = None,
        prepare_args_callable: Callable[..., list[str]] | None = None,
        prepare_args_op_kwargs: dict[str, Any] | None = None,
    ):
        """
        Initialize the training-export task group.

        Unlike captioning/visual_qa/reasoning, this stage is intentionally
        non-scaled: one container receives every completed scene and aggregates
        them into training datasets.
        """
        if prepare_args_callable is None:
            prepare_args_callable = TrainingExportTaskGroup.prepare_training_export_configs
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

    def prepare_training_export_args(
        self,
        training_export_config: str | None = None,
        run_id: str = "",
        **context,
    ) -> list[str]:
        """Run the configured training-export argument preparation callable."""
        try:
            prepare_kwargs = {
                "training_export_config": training_export_config,
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
            raise AirflowFailException(f"Training export argument preparation failed: {e}") from e

    def get_training_export_task_group(self) -> TaskGroup:
        """Get the training-export task group with a single (non-scaled) container task."""
        with TaskGroup(group_id=self.group_id) as task_group:
            prepare_training_export_args = PythonOperator(
                task_id="prepare_training_export_args",
                python_callable=self.prepare_training_export_args,
                op_kwargs={
                    "training_export_config": self.task_config,
                    "run_id": "{{ run_id }}",
                },
                retries=2,
                retry_delay=datetime.timedelta(seconds=30),
            )

            self.logger.info(
                "Using ComponentBuilder for non-scaled training_export (group_id=%s)",
                self.group_id,
            )
            # Expand over a one-element list so all backends share the same
            # container_args mapping path while still launching a single pod.
            training_export_task = self.builder.partial_task(
                component_name=self.component_name,
                task_id="training_export",
                name=(
                    f"{self.group_id}-"
                    "{{ run_id | replace(':', '_') | replace('+', '_') | replace('.', '_') }}"
                ),
            ).expand(container_args=XComArg(prepare_training_export_args))

            prepare_training_export_args >> training_export_task

        return task_group

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Strict artifact validation for Event Video Generation Cosmos3 augmentation."""

import ast
from typing import Any

from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.task_groups.input_preparation import require_prepared_input_from_xcom
from dags.shared.utils.msc_utils import is_file
from dags.workflows.event_video_generation_dag.models import (
    EventVideoGenerationAugmentationOutputResult,
    EventVideoGenerationCosmosTaskConfig,
)


def _parse_config(value: str | dict[str, Any] | None) -> EventVideoGenerationCosmosTaskConfig:
    raw = ast.literal_eval(value) if isinstance(value, str) else value
    return EventVideoGenerationCosmosTaskConfig.model_validate(raw or {})


def validate_event_video_generation_cosmos_outputs(
    cosmos_config: str | dict[str, Any] | None = None,
    run_id: str = "",
    **context,
) -> dict[str, Any]:
    """Require config, video, caption, and metadata for every expected augmentation."""
    try:
        task_config = _parse_config(cosmos_config)
        prepared = require_prepared_input_from_xcom(context["ti"])
        output_root = str(URL(task_config.output_directory) / run_id / "cosmos")
        augmentations = []
        videos = []

        for prepared_image in prepared.videos:
            for augmentation_index in range(task_config.num_augmentation):
                output_base = f"{output_root}/{prepared_image.video_key}/{augmentation_index}"
                paths = {
                    "config": f"{output_base}/config.yaml",
                    "video": f"{output_base}/output.mp4",
                    "caption": f"{output_base}/caption.txt",
                    "metadata": f"{output_base}/metadata.json",
                }
                for artifact_name, artifact_path in paths.items():
                    if not is_file(artifact_path):
                        raise AirflowFailException(
                            f"Missing Event Video Generation {artifact_name} artifact for "
                            f"{prepared_image.video_key}[{augmentation_index}]: "
                            f"{artifact_path}"
                        )

                augmentations.append(
                    {
                        "input_key": prepared_image.video_key,
                        "augmentation_index": augmentation_index,
                        "video_path": paths["video"],
                        "caption_path": paths["caption"],
                        "metadata_path": paths["metadata"],
                    }
                )
                videos.append(
                    {
                        "video_key": prepared_image.video_key,
                        "video_path": paths["video"],
                    }
                )

        return EventVideoGenerationAugmentationOutputResult(
            length=len(augmentations),
            augmentations=augmentations,
            videos=videos,
        ).model_dump()
    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(
            f"Event Video Generation Cosmos output validation failed: {e}"
        ) from e

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Input preparation for Event Video Generation image-to-video augmentation."""

import ast
import logging
from typing import Any

from airflow.exceptions import AirflowFailException
from multistorageclient.types import PatternType

from dags.shared.models import InputPreparationResult
from dags.shared.utils.msc_utils import (
    convert_msc_to_storage_url,
    is_file,
    list_directory,
)
from dags.shared.utils.video_input_utils import derive_video_key, normalize_directory_path
from dags.workflows.event_video_generation_dag.models import EventVideoGenerationDagPayloadConfig

IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tiff", ".webp")
IMAGE_PATTERNS = [(PatternType.INCLUDE, f"*{extension}") for extension in IMAGE_EXTENSIONS]


def _parse_payload(payload: str | dict[str, Any]) -> EventVideoGenerationDagPayloadConfig:
    raw = ast.literal_eval(payload) if isinstance(payload, str) else payload
    return EventVideoGenerationDagPayloadConfig.model_validate(raw)


def prepare_event_video_generation_image_input(
    payload: str | dict[str, Any] = "",
    run_id: str = "",
    **context,
) -> dict[str, Any]:
    """Resolve one image or a bounded, sorted image directory into shared input XCom."""
    try:
        config = _parse_payload(payload)
        raw_path = config.input_path

        if is_file(raw_path):
            paths = [convert_msc_to_storage_url(raw_path)]
            key_base = raw_path
        else:
            directory = normalize_directory_path(raw_path)
            objects = list(
                list_directory(
                    directory_url=directory,
                    patterns=IMAGE_PATTERNS,
                )
            )
            paths = sorted(convert_msc_to_storage_url(obj.key) for obj in objects)
            if config.max_images > 0:
                paths = paths[: config.max_images]
            key_base = directory

        if not paths:
            raise AirflowFailException(
                f"No image files found at {raw_path!r} "
                f"(supported extensions={list(IMAGE_EXTENSIONS)})."
            )

        result = InputPreparationResult(
            length=len(paths),
            videos=[
                {
                    "video_key": derive_video_key(image_path, key_base),
                    "video_path": image_path,
                }
                for image_path in paths
            ],
        )
        logging.info(
            "Prepared %d Event Video Generation image(s) for run_id=%s", result.length, run_id
        )
        return result.model_dump()
    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(f"Event Video Generation input preparation failed: {e}") from e

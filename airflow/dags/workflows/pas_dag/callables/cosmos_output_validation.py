# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Output validation callable for PAS image-edit augmentation."""

import ast
from typing import Any

import multistorageclient as msc
import yaml
from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.models import CosmosOutputResult, CosmosTaskConfig
from dags.shared.task_groups.input_preparation import require_prepared_input_from_xcom
from dags.shared.utils.msc_utils import is_file


def validate_pas_image_edit_outputs(
    cosmos_config: str | None = None,
    run_id: str = "",
    **context,
) -> dict[str, Any]:
    """Validate PAS image-edit outputs using each generated config's output path."""
    try:
        cosmos_task_config = CosmosTaskConfig.model_validate(ast.literal_eval(cosmos_config))
        prepared = require_prepared_input_from_xcom(context["ti"])
        cosmos_dir = str(URL(cosmos_task_config.output_directory) / run_id / "cosmos")

        videos = []
        for prepared_video in prepared.videos:
            for aug_idx in range(cosmos_task_config.num_augmentation):
                config_path = f"{cosmos_dir}/{prepared_video.video_key}/{aug_idx}/config.yaml"
                if not is_file(config_path):
                    raise AirflowFailException(f"Generated config not found: {config_path}")
                with msc.open(config_path, "r") as f:
                    generated_config = yaml.safe_load(f)
                output_video = ((generated_config.get("data") or [{}])[0].get("output") or {}).get(
                    "video"
                )
                if not output_video:
                    raise AirflowFailException(
                        f"Generated config missing data[0].output.video: {config_path}"
                    )
                if not is_file(output_video):
                    raise AirflowFailException(
                        f"Augmentation output not found for {prepared_video.video_key}: "
                        f"{output_video}"
                    )
                videos.append(
                    {
                        "video_key": prepared_video.video_key,
                        "video_path": output_video,
                    }
                )

        return CosmosOutputResult(length=len(videos), videos=videos).model_dump()

    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(f"PAS image-edit output validation failed: {e}") from e

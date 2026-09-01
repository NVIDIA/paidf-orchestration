# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Input preparation callable for the Image Attribute Augmentation DAG."""

import ast
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowFailException

from dags.shared.models import InputPreparationResult
from dags.shared.utils.video_input_utils import join_storage_base_path_filename
from dags.workflows.image_attribute_augmentation_dag.models import (
    ImageAttributeAugmentationDagPayloadConfig,
)
from dags.workflows.image_attribute_augmentation_dag.utils import IMAGE_EXTENSIONS
from dags.workflows.image_attribute_augmentation_dag.utils import (
    combine_panes as combine_panes_helper,
)


def prepare_image_attribute_augmentation_input(
    payload: str = "",
    run_id: str = "",
    **context,
) -> dict[str, Any]:
    """Combine Image Attribute Augmentation panes and return prepared image paths for downstream Cosmos tasks."""
    try:
        config = ImageAttributeAugmentationDagPayloadConfig.model_validate(
            ast.literal_eval(payload)
        )
        run_base = Path("/tmp/image_attribute_augmentation_preprocess_runs") / f"{run_id}"
        run_base.mkdir(parents=True, exist_ok=True)

        remote_run_dir = join_storage_base_path_filename(config.output_directory, run_id)
        remote_preprocessing_dir = join_storage_base_path_filename(
            remote_run_dir,
            "preprocessing",
        )

        combine_panes_helper(
            run_base=run_base,
            input_path=config.input_path,
            output_directory=remote_preprocessing_dir,
        )

        combined_dir = run_base / "combined_imgs"
        combined_paths = sorted(
            path
            for path in combined_dir.iterdir()
            if path.is_file() and path.suffix.lower() in IMAGE_EXTENSIONS
        )
        if config.max_imgs is not None and config.max_imgs > 0:
            combined_paths = combined_paths[: config.max_imgs]
        return InputPreparationResult(
            length=len(combined_paths),
            videos=[
                {
                    "video_key": combined_path.stem,
                    "video_path": join_storage_base_path_filename(
                        join_storage_base_path_filename(
                            remote_preprocessing_dir,
                            combined_path.stem,
                        ),
                        f"{combined_path.stem}.jpg",
                    ),
                }
                for combined_path in combined_paths
            ],
        ).model_dump()

    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(f"Input preparation failed: {e}") from e

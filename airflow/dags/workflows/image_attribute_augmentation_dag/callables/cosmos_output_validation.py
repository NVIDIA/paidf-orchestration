# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Output validation callable for Image Attribute Augmentation image-edit augmentation."""

import ast
import json
import logging
from typing import Any

import multistorageclient as msc
import yaml
from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.models import CosmosOutputResult, CosmosTaskConfig
from dags.shared.task_groups.input_preparation import require_prepared_input_from_xcom
from dags.shared.utils.msc_utils import is_file, write_file_to_directory

COSMOS_SKIPPED_JSON = "cosmos_skipped.json"


def _read_output_metadata(output_video: str) -> dict[str, Any] | None:
    """Read output_metadata.json co-located with output.jpg.

    Returns ``{}`` when the file is genuinely absent, and ``None`` when it exists
    but cannot be read or decoded, so the caller can treat unreadable metadata as
    a verification failure rather than as an absent verdict.
    """
    run_dir = output_video.rstrip("/").rsplit("/", 1)[0]
    metadata_path = f"{run_dir}/output_metadata.json"
    if not is_file(metadata_path):
        return {}
    try:
        with msc.open(metadata_path, "r") as f:
            return json.load(f)
    except Exception:
        logging.warning("Unable to read output metadata at %s", metadata_path, exc_info=True)
        return None


def _attribute_verification_passed(output_metadata: dict[str, Any] | None) -> bool:
    """Return False when verification explicitly failed or its metadata is unreadable."""
    if output_metadata is None:
        return False
    attr_check = output_metadata.get("attribute_verification")
    if not isinstance(attr_check, dict):
        return True
    passed = attr_check.get("passed")
    # Only reject when the field is present and explicitly False
    return passed is not False


def validate_image_attribute_augmentation_image_edit_outputs(
    cosmos_config: str | None = None,
    run_id: str = "",
    **context,
) -> dict[str, Any]:
    """Validate Image Attribute Augmentation image-edit outputs using each generated config's output path.

    Entries are included only when:
    - output.jpg exists in S3, and
    - attribute_verification.passed is not explicitly False in output_metadata.json.

    All skipped entries (with their reason) are written to cosmos_skipped.json in the
    run output directory so users can inspect what was excluded and why.
    """
    try:
        cosmos_task_config = CosmosTaskConfig.model_validate(ast.literal_eval(cosmos_config))
        prepared = require_prepared_input_from_xcom(context["ti"])
        cosmos_dir = str(URL(cosmos_task_config.output_directory) / run_id / "cosmos")

        videos = []
        skipped: list[dict[str, Any]] = []

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
                    logging.warning(
                        "Skipping failed augmentation output for %s (augmentation %d): %s",
                        prepared_video.video_key,
                        aug_idx,
                        output_video,
                    )
                    skipped.append(
                        {
                            "person_key": prepared_video.video_key,
                            "aug_idx": aug_idx,
                            "output_path": output_video,
                            "reason": "generation_failed",
                        }
                    )
                    continue
                output_metadata = _read_output_metadata(output_video)
                if not _attribute_verification_passed(output_metadata):
                    logging.warning(
                        "Skipping augmentation output that failed VLM attribute verification "
                        "for %s (augmentation %d): %s",
                        prepared_video.video_key,
                        aug_idx,
                        output_video,
                    )
                    skipped.append(
                        {
                            "person_key": prepared_video.video_key,
                            "aug_idx": aug_idx,
                            "output_path": output_video,
                            "reason": "vlm_verification_failed",
                        }
                    )
                    continue
                videos.append(
                    {
                        "video_key": prepared_video.video_key,
                        "video_path": output_video,
                    }
                )

        if skipped:
            sidecar_path = str(
                URL(cosmos_task_config.output_directory.rstrip("/")) / run_id / COSMOS_SKIPPED_JSON
            )
            write_file_to_directory(sidecar_path, json.dumps(skipped, indent=2).encode("utf-8"))
            logging.info(
                "Wrote %d skipped augmentation entry(ies) to %s", len(skipped), sidecar_path
            )

        if not videos:
            raise AirflowFailException(
                "No successful Image Attribute Augmentation image-edit augmentation outputs found"
            )

        return CosmosOutputResult(length=len(videos), videos=videos).model_dump()

    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(
            f"Image Attribute Augmentation image-edit output validation failed: {e}"
        ) from e

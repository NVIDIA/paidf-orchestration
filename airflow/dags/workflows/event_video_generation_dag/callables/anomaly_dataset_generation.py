# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Assemble Event Video Generation outputs into an anomaly dataset."""

from __future__ import annotations

import ast
import json
import logging
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.utils.state import TaskInstanceState
from yarl import URL

from dags.shared.models import StoragePathListXcom
from dags.shared.utils.msc_utils import (
    copy_file,
    list_directory,
    write_file_to_directory,
)
from dags.shared.utils.video_input_utils import get_relative_storage_path
from dags.workflows.event_video_generation_dag.models import (
    EventVideoGenerationAugmentationOutputResult,
    EventVideoGenerationDagPayloadConfig,
)

logger = logging.getLogger(__name__)

DATASET_DIRECTORY = "anomaly_dataset"
DATASET_MANIFEST = "dataset.json"
COSMOS_AUGMENTATION_TASK_IDS = {
    "cosmos_augmentation.augmentation_external",
    "cosmos_augmentation.augmentation_internal",
}


def parse_payload(value: str | dict[str, Any] | None) -> EventVideoGenerationDagPayloadConfig:
    if isinstance(value, dict):
        raw = value
    elif value is None or not value.strip():
        raw = {}
    else:
        try:
            raw = json.loads(value)
        except json.JSONDecodeError:
            raw = ast.literal_eval(value)
    return EventVideoGenerationDagPayloadConfig.model_validate(raw)


def anomaly_dataset_dir(output_directory: str, run_id: str) -> str:
    return str(URL(output_directory.rstrip("/")) / run_id / DATASET_DIRECTORY)


def anomaly_scene_dir(dataset_dir: str, input_key: str, augmentation_index: int) -> str:
    return f"{dataset_dir.rstrip('/')}/{input_key}_aug{augmentation_index}"


def auto_labeling_scene_dir(
    output_directory: str, run_id: str, input_key: str, augmentation_index: int
) -> str:
    return str(
        URL(output_directory.rstrip("/"))
        / run_id
        / "auto_labeling"
        / input_key
        / str(augmentation_index)
    )


def _copy_directory(source_dir: str, destination_dir: str) -> None:
    source_prefix = source_dir.rstrip("/") + "/"
    copied = 0
    for obj in list_directory(source_prefix):
        relative_path = (
            obj.key[len(source_prefix) :]
            if obj.key.startswith(source_prefix)
            else get_relative_storage_path(obj.key, source_prefix)
        )
        if not relative_path or relative_path.endswith("/"):
            continue
        copy_file(obj.key, f"{destination_dir.rstrip('/')}/{relative_path}")
        copied += 1
    if copied == 0:
        raise AirflowFailException(f"Annotation scene is empty: {source_dir}")


def _failed_cosmos_augmentation_map_indexes(ti: Any, map_count: int) -> set[int]:
    """Return mapped Cosmos augmentation indexes that exhausted their Airflow retries."""
    task_states_by_run = ti.get_task_states(
        dag_id=ti.dag_id,
        task_ids=sorted(COSMOS_AUGMENTATION_TASK_IDS),
        run_ids=[ti.run_id],
    )
    task_states = task_states_by_run.get(ti.run_id, {})
    return {
        map_index
        for map_index in range(map_count)
        if any(
            task_states.get(f"{task_id}_{map_index}") == TaskInstanceState.FAILED
            for task_id in COSMOS_AUGMENTATION_TASK_IDS
        )
    }


def generate_anomaly_dataset(
    payload: str | dict[str, Any] | None = None,
    run_id: str = "",
    input_xcom_task_id: str = "cosmos_augmentation.validate_outputs",
    input_xcom_key: str = "return_value",
    **context: Any,
) -> dict[str, Any]:
    """Copy Cosmos videos and their annotations into the final dataset."""
    try:
        config = parse_payload(payload)
        ti = context["ti"]
        discovered = EventVideoGenerationAugmentationOutputResult.model_validate(
            ti.xcom_pull(task_ids=input_xcom_task_id, key=input_xcom_key)
        )
        dataset_dir = anomaly_dataset_dir(config.output_directory, run_id)
        map_count = (
            max(
                (augmentation.augmentation_index for augmentation in discovered.augmentations),
                default=-1,
            )
            + 1
        )
        failed_map_indexes = _failed_cosmos_augmentation_map_indexes(ti, map_count)

        entries: list[dict[str, Any]] = []
        for augmentation in discovered.augmentations:
            if augmentation.augmentation_index in failed_map_indexes:
                logger.info(
                    "Excluding Cosmos augmentation %s[%d] from the anomaly dataset; "
                    "its mapped task exhausted all retries",
                    augmentation.input_key,
                    augmentation.augmentation_index,
                )
                continue
            key = (augmentation.input_key, augmentation.augmentation_index)
            cosmos_dir = augmentation.video_path.rsplit("/", 1)[0]
            config_path = f"{cosmos_dir}/config.yaml"
            annotation_source = auto_labeling_scene_dir(config.output_directory, run_id, *key)
            scene_dir = anomaly_scene_dir(dataset_dir, *key)
            _copy_directory(annotation_source, scene_dir)
            cosmos_sidecars = f"{scene_dir}/sidecars/cosmos"
            copy_file(augmentation.video_path, f"{scene_dir}/raw/video.mp4")
            copy_file(config_path, f"{cosmos_sidecars}/config.yaml")
            copy_file(augmentation.caption_path, f"{cosmos_sidecars}/caption.txt")
            copy_file(augmentation.metadata_path, f"{cosmos_sidecars}/metadata.json")
            entries.append(
                {
                    "scene_id": f"{augmentation.input_key}_aug{augmentation.augmentation_index}",
                    "input_key": augmentation.input_key,
                    "augmentation_index": augmentation.augmentation_index,
                    "scene_path": scene_dir,
                    "auto_labeling_source_path": annotation_source,
                    "paths": {
                        "video": f"{scene_dir}/raw/video.mp4",
                        "config": f"{cosmos_sidecars}/config.yaml",
                        "caption": f"{cosmos_sidecars}/caption.txt",
                        "metadata": f"{cosmos_sidecars}/metadata.json",
                        "contextual": f"{scene_dir}/contextual",
                        "sidecars": f"{scene_dir}/sidecars",
                    },
                }
            )

        input_keys = {augmentation.input_key for augmentation in discovered.augmentations}
        manifest = {
            "metadata": {
                "description": "Event Video Generation anomaly dataset",
                "total_scenes": len(entries),
                "original_inputs": len(input_keys),
                "scene_structure": f"{dataset_dir}/{{input_key}}_aug{{augmentation_index}}/",
            },
            "entries": entries,
        }
        write_file_to_directory(
            f"{dataset_dir}/{DATASET_MANIFEST}",
            json.dumps(manifest, indent=2).encode("utf-8"),
        )
        logger.info("EVG anomaly dataset complete: scenes=%d output=%s", len(entries), dataset_dir)
        return StoragePathListXcom(
            length=len(entries),
            videos=[
                {"video_key": item["scene_id"], "video_path": item["scene_path"]}
                for item in entries
            ],
        ).model_dump()
    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(f"Event Video Generation dataset generation failed: {e}") from e

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Assemble Image Attribute Augmentation pipeline outputs into the final augmented dataset."""

from __future__ import annotations

import ast
import json
import logging
from typing import Any

from airflow.exceptions import AirflowFailException

from dags.shared.models import CosmosOutputResult, StoragePathListXcom
from dags.shared.utils.image_attribute_augmentation_paths import (
    get_cosmos_augmented_data_path,
    get_cosmos_augmented_images_dir,
)
from dags.shared.utils.msc_utils import (
    copy_file,
    is_file,
    list_directory,
    read_file_as_dict,
    write_file_to_directory,
)
from dags.shared.utils.video_input_utils import (
    get_relative_storage_path,
    join_storage_base_path_filename,
)
from dags.workflows.image_attribute_augmentation_dag.models import (
    ImageAttributeAugmentationDagPayloadConfig,
)

COSMOS_SKIPPED_JSON = "cosmos_skipped.json"

logger = logging.getLogger(__name__)

SKIP_VALUES = {"", "none", "not visible", "unknown", "other"}
DEFAULT_OUTPUT_JSON = "augmented_data.json"
PERSON_ATTRIBUTE_SEARCH_CONFIG_RELATIVE_PATH = (
    "sidecars/person_attribute_search/assets/event_and_person_attribute_search_config.yaml"
)


def join_storage_paths(base_path: str, *parts: str) -> str:
    """Join storage URL path fragments with the repo's storage-path helper."""
    joined = base_path
    for part in parts:
        joined = join_storage_base_path_filename(joined, part)
    return joined


def normalize_attribute_key(key: str) -> str:
    return key.replace("_", " ")


def _usable(value: Any) -> bool:
    return str(value).strip().lower() not in SKIP_VALUES


def _garment_phrase(attrs: dict[str, Any], color_key: str, type_key: str) -> str:
    color = str(attrs.get(color_key, "")).strip()
    garment_type = str(attrs.get(type_key, "")).strip()
    return " ".join(part for part in (color, garment_type) if _usable(part))


def build_queries(attrs: dict[str, Any]) -> dict[str, list[str]]:
    top = _garment_phrase(attrs, "top outer color", "top outer type")
    bottom = _garment_phrase(attrs, "bottom color", "bottom type")
    shoes = _garment_phrase(attrs, "shoe color", "shoe type")
    parts = [part for part in (top, bottom, shoes) if part]
    clothing = ", ".join(parts)

    return {
        "easy": parts,
        "medium": [f"person wearing {clothing}"] if clothing else ["person"],
        "hard": [f"Person wearing {clothing}."] if clothing else ["Person."],
    }


def _parent_storage_path(path: str) -> str:
    return path.rstrip("/").rsplit("/", 1)[0]


def get_output_metadata_path(output_image_path: str) -> str:
    return join_storage_base_path_filename(
        _parent_storage_path(output_image_path), "output_metadata.json"
    )


def get_scene_augmented_data_path(dataset_scene_dir: str) -> str:
    """Return the final sidecar path for one augmentation's metadata."""
    return join_storage_paths(dataset_scene_dir, "sidecars", DEFAULT_OUTPUT_JSON)


def get_augmented_dataset_dir(output_directory: str, run_id: str) -> str:
    return join_storage_paths(output_directory, run_id, "augmented_dataset")


def get_auto_labeling_scene_dir(
    output_directory: str,
    run_id: str,
    person_key: str,
    augmentation_index: str,
) -> str:
    return join_storage_paths(
        output_directory,
        run_id,
        "auto_labeling",
        person_key,
        augmentation_index,
    )


def get_dataset_scene_dir(
    dataset_dir: str,
    person_key: str,
    augmentation_index: str,
) -> str:
    return join_storage_base_path_filename(dataset_dir, f"{person_key}_aug{augmentation_index}")


def _augmentation_id_from_output_path(output_image_path: str) -> str:
    parts = output_image_path.rstrip("/").split("/")
    if len(parts) < 2:
        raise AirflowFailException(f"Cannot infer augmentation id from {output_image_path}")
    aug_dir = parts[-2]
    return aug_dir if aug_dir.startswith("aug_") else f"aug_{aug_dir}"


def _augmentation_suffix(augmentation_id: str) -> str:
    return augmentation_id.replace("aug_", "", 1)


def _directory_prefix(directory_path: str) -> str:
    return directory_path.rstrip("/") + "/"


def _prefix_has_objects(directory_path: str) -> bool:
    return (
        next(iter(list_directory(_directory_prefix(directory_path), max_files=1)), None) is not None
    )


def _path_without_scheme(path: str) -> str:
    return path.rstrip("/").split("://", 1)[-1]


def _relative_listed_object_path(object_path: str, source_prefix: str) -> str | None:
    source_prefix = _directory_prefix(source_prefix)
    if object_path.startswith(source_prefix):
        return object_path[len(source_prefix) :].lstrip("/")

    object_prefix = _path_without_scheme(object_path)
    source_prefix_without_scheme = _path_without_scheme(source_prefix)
    source_prefix_without_scheme = _directory_prefix(source_prefix_without_scheme)
    if object_prefix.startswith(source_prefix_without_scheme):
        return object_prefix[len(source_prefix_without_scheme) :].lstrip("/")

    return get_relative_storage_path(object_path, source_prefix)


def _read_optional_json(path: str, description: str) -> dict[str, Any]:
    if not is_file(path):
        logger.warning("Missing optional %s: %s", description, path)
        return {}

    data = read_file_as_dict(path)
    if not isinstance(data, dict):
        raise AirflowFailException(f"Invalid {description}: {path}")
    return data


def _copy_directory_contents(
    source_dir: str,
    destination_dir: str,
    description: str,
) -> int:
    """Copy every file below one storage prefix while preserving relative paths."""
    source_prefix = _directory_prefix(source_dir)
    destination_prefix = _directory_prefix(destination_dir)
    source_objects = list(list_directory(source_prefix))
    if not source_objects:
        raise AirflowFailException(f"{description} is empty: {source_dir}")

    copied_files = 0
    for source_object in source_objects:
        rel_path = _relative_listed_object_path(source_object.key, source_prefix)
        if rel_path is None:
            raise AirflowFailException(
                f"Could not derive relative path while copying {description}: "
                f"object={source_object.key}, source_prefix={source_prefix}"
            )
        if not rel_path or rel_path.endswith("/"):
            continue
        destination_path = join_storage_base_path_filename(destination_prefix, rel_path)
        copy_file(source_object.key, destination_path)
        copied_files += 1

    if copied_files == 0:
        raise AirflowFailException(f"{description} has no files to copy: {source_dir}")

    logger.info(
        "Copied %d %s file(s) from %s to %s",
        copied_files,
        description,
        source_dir,
        destination_dir,
    )
    return copied_files


def copy_auto_labeling_scene(source_scene_dir: str, destination_scene_dir: str) -> None:
    """Copy one auto-labeling DAFT scene into the final dataset."""
    copied_files = _copy_directory_contents(
        source_scene_dir,
        destination_scene_dir,
        "auto-labeling output",
    )

    if not _prefix_has_objects(destination_scene_dir):
        raise AirflowFailException(
            f"Copied dataset scene is empty after copying {copied_files} file(s): "
            f"{destination_scene_dir}"
        )


def copy_cosmos_augmented_images(
    output_image_path: str,
    seed_image_id: str,
    destination_scene_dir: str,
) -> None:
    """Copy Cosmos postprocessed images into the final scene's raw directory."""
    source_dir = get_cosmos_augmented_images_dir(output_image_path, seed_image_id)
    raw_dir = join_storage_base_path_filename(destination_scene_dir, "raw")
    _copy_directory_contents(source_dir, raw_dir, "Cosmos augmented image output")


def build_entry(
    *,
    person_key: str,
    output_image_path: str,
    output_metadata: dict[str, Any],
    dataset_dir: str,
    auto_labeling_source_dir: str,
) -> dict[str, Any]:
    selections = output_metadata.get("selections", {})
    if not isinstance(selections, dict):
        raise AirflowFailException(f"Invalid selections for {output_image_path}")

    augmentation_id = _augmentation_id_from_output_path(output_image_path)
    augmentation_index = _augmentation_suffix(augmentation_id)
    augmented_key = f"{person_key}_aug{augmentation_index}"
    scene_path = get_dataset_scene_dir(dataset_dir, person_key, augmentation_index)
    selected_attributes = {normalize_attribute_key(key): value for key, value in selections.items()}

    return {
        "person_key": augmented_key,
        "person_id": person_key,
        "source_person_key": person_key,
        "augmentation_id": augmentation_id,
        "augmentation_index": augmentation_index,
        "scene_path": scene_path,
        "auto_labeling_source_path": auto_labeling_source_dir,
        "paths": {
            "config": join_storage_base_path_filename(
                scene_path, PERSON_ATTRIBUTE_SEARCH_CONFIG_RELATIVE_PATH
            ),
            "raw": join_storage_base_path_filename(scene_path, "raw"),
            "contextual": join_storage_base_path_filename(scene_path, "contextual"),
            "task": join_storage_base_path_filename(scene_path, "task"),
            "sidecars": join_storage_base_path_filename(scene_path, "sidecars"),
        },
        "selected_attributes": selected_attributes,
        "queries": build_queries(selected_attributes),
        "attribute_verification": output_metadata.get("attribute_verification", {}),
    }


def compute_metadata(
    entries: list[dict[str, Any]],
    dataset_dir: str,
    skipped_augmentations: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    source_ids = {entry["source_person_key"] for entry in entries}

    return {
        "description": "Image Attribute Augmentation auto-labeling dataset from augmented inputs",
        "total_ids": len(entries),
        "original_ids": len(source_ids),
        "total_scenes": len(entries),
        "skipped_augmentations": skipped_augmentations or [],
        "scene_structure": f"{dataset_dir}/{{person_key}}_aug{{augmentation_index}}/",
        "path_format": {
            "config": ("{scene_path}/" + PERSON_ATTRIBUTE_SEARCH_CONFIG_RELATIVE_PATH),
            "raw": "{scene_path}/raw/",
            "contextual": "{scene_path}/contextual/",
            "task": "{scene_path}/task/",
            "sidecars": "{scene_path}/sidecars/",
        },
        "query_format": {
            "levels": ["easy", "medium", "hard"],
            "description": "Queries are built from selected augmentation attributes",
        },
    }


def _read_cosmos_skipped(output_directory: str, run_id: str) -> list[dict[str, Any]]:
    """Return skipped augmentation entries written by cosmos_output_validation, or [].

    The sidecar holds a JSON array, so it cannot go through the dict-only
    ``_read_optional_json`` helper. It is purely informational, so a missing or
    unreadable sidecar never fails dataset generation.
    """
    path = join_storage_paths(output_directory.rstrip("/"), run_id, COSMOS_SKIPPED_JSON)
    if not is_file(path):
        return []
    try:
        data = read_file_as_dict(path)
    except AirflowFailException:
        logger.warning("Ignoring unreadable cosmos skipped sidecar: %s", path, exc_info=True)
        return []
    if not isinstance(data, list):
        logger.warning("Ignoring cosmos skipped sidecar that is not a JSON array: %s", path)
        return []
    return data


def _validate_payload(payload: str | dict[str, Any]) -> ImageAttributeAugmentationDagPayloadConfig:
    if isinstance(payload, dict):
        return ImageAttributeAugmentationDagPayloadConfig.model_validate(payload)
    return ImageAttributeAugmentationDagPayloadConfig.model_validate(ast.literal_eval(payload))


def generate_augmented_dataset(
    payload: str | dict[str, Any] = "",
    run_id: str = "",
    input_xcom_task_id: str = "cosmos_augmentation.validate_outputs",
    input_xcom_key: str = "return_value",
    output_json: str = DEFAULT_OUTPUT_JSON,
    **context,
) -> dict[str, Any]:
    """Create the final Image Attribute Augmentation dataset by copying auto-labeling DAFT scene outputs."""
    try:
        if not payload:
            raise AirflowFailException("Payload is required")
        config = _validate_payload(payload)
        ti = context["ti"]
        raw_outputs = ti.xcom_pull(task_ids=input_xcom_task_id, key=input_xcom_key)
        cosmos_outputs = CosmosOutputResult.model_validate(raw_outputs)
        dataset_dir = get_augmented_dataset_dir(config.output_directory, run_id)

        entries: list[dict[str, Any]] = []
        for video in cosmos_outputs.videos:
            augmentation_id = _augmentation_id_from_output_path(video.video_path)
            augmentation_index = _augmentation_suffix(augmentation_id)
            auto_labeling_source_dir = get_auto_labeling_scene_dir(
                config.output_directory,
                run_id,
                video.video_key,
                augmentation_index,
            )
            dataset_scene_dir = get_dataset_scene_dir(
                dataset_dir,
                video.video_key,
                augmentation_index,
            )

            copy_auto_labeling_scene(auto_labeling_source_dir, dataset_scene_dir)
            copy_cosmos_augmented_images(
                video.video_path,
                video.video_key,
                dataset_scene_dir,
            )

            cosmos_augmented_data_path = get_cosmos_augmented_data_path(video.video_path)
            if not is_file(cosmos_augmented_data_path):
                raise AirflowFailException(
                    f"Missing Cosmos postprocessing augmented data: {cosmos_augmented_data_path}"
                )
            copy_file(
                cosmos_augmented_data_path,
                get_scene_augmented_data_path(dataset_scene_dir),
            )

            output_metadata_path = get_output_metadata_path(video.video_path)
            output_metadata = _read_optional_json(output_metadata_path, "output metadata")
            entries.append(
                build_entry(
                    person_key=video.video_key,
                    output_image_path=video.video_path,
                    output_metadata=output_metadata,
                    dataset_dir=dataset_dir,
                    auto_labeling_source_dir=auto_labeling_source_dir,
                )
            )

        skipped_augmentations = _read_cosmos_skipped(config.output_directory, run_id)
        output_data = {
            "metadata": compute_metadata(entries, dataset_dir, skipped_augmentations),
            "entries": entries,
        }
        output_json_path = join_storage_base_path_filename(dataset_dir, output_json)
        write_file_to_directory(
            output_json_path,
            json.dumps(output_data, indent=2).encode("utf-8"),
        )

        logger.info(
            "Image Attribute Augmentation post-processing complete: scenes=%d output=%s",
            len(entries),
            output_json_path,
        )
        videos = [
            {
                "video_key": entry["person_key"],
                "video_path": entry["scene_path"],
            }
            for entry in entries
        ]
        return StoragePathListXcom(length=len(videos), videos=videos).model_dump()

    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(
            f"Image Attribute Augmentation post-processing failed: {e}"
        ) from e

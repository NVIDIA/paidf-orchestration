# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""PAS end-of-pipeline output validation callable."""

from __future__ import annotations

import ast
from typing import Any

from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.utils.msc_utils import is_file, list_directory, read_file_as_dict
from dags.shared.utils.video_input_utils import (
    get_relative_storage_path,
    normalize_directory_path,
)
from dags.workflows.pas_dag.models import PasDagPayloadConfig
from dags.workflows.pas_dag.tasks.post_processing import (
    DEFAULT_OUTPUT_JSON,
    get_augmented_dataset_dir,
    get_dataset_scene_dir,
)

REQUIRED_COSMOS_FILES = ("output.jpg", "output_metadata.json")
DATASET_SCENE_OUTPUT_DIRS = ("raw", "contextual", "task", "sidecars")


class PipelineOutputValidationError(Exception):
    """Raised when required PAS pipeline outputs are missing or invalid."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("; ".join(errors))


def _run_base_url(output_directory: str, run_id: str) -> str:
    return str(URL(output_directory.rstrip("/")) / run_id)


def _prefix_has_objects(directory_url: str) -> bool:
    directory_url = normalize_directory_path(directory_url)
    try:
        return next(iter(list_directory(directory_url, max_files=1)), None) is not None
    except Exception:
        return False


def group_cosmos_run_entries(
    run_entries: list[tuple[str, int]],
) -> dict[str, set[int]]:
    """Group discovered cosmos runs by person key."""
    grouped: dict[str, set[int]] = {}
    for person_key, aug_idx in run_entries:
        grouped.setdefault(person_key, set()).add(aug_idx)
    return grouped


def verify_preprocessing_artifacts(
    output_directory: str,
    run_id: str,
    person_keys: list[str],
) -> list[str]:
    """Ensure pane metadata exists for each processed person key."""
    missing: list[str] = []
    base = _run_base_url(output_directory, run_id)
    for person_key in person_keys:
        metadata_path = f"{base}/preprocessing/{person_key}/{person_key}.json"
        if not is_file(metadata_path):
            missing.append(metadata_path)
    return missing


def discover_preprocessing_person_keys(output_directory: str, run_id: str) -> list[str]:
    """Return sorted person keys under ``.../preprocessing/``."""
    preprocessing_dir = normalize_directory_path(
        str(URL(_run_base_url(output_directory, run_id)) / "preprocessing")
    )
    person_keys: set[str] = set()
    try:
        for obj in list_directory(preprocessing_dir):
            rel = get_relative_storage_path(obj.key, preprocessing_dir)
            if not rel:
                continue
            parts = rel.split("/")
            if parts[0]:
                person_keys.add(parts[0])
    except Exception:
        return []
    return sorted(person_keys)


def discover_cosmos_run_entries(output_directory: str, run_id: str) -> list[tuple[str, int]]:
    """Return sorted ``(person_key, augmentation_idx)`` entries under ``.../cosmos/``."""
    cosmos_dir = normalize_directory_path(
        str(URL(_run_base_url(output_directory, run_id)) / "cosmos")
    )
    run_dirs: set[tuple[str, int]] = set()
    try:
        for obj in list_directory(cosmos_dir):
            rel = get_relative_storage_path(obj.key, cosmos_dir)
            if not rel:
                continue
            parts = rel.split("/")
            if len(parts) >= 2 and parts[1].isdigit():
                run_dirs.add((parts[0], int(parts[1])))
    except Exception:
        return []
    return sorted(run_dirs)


def discover_auto_labeling_run_entries(
    output_directory: str,
    run_id: str,
) -> list[tuple[str, int]]:
    """Return sorted ``(person_key, augmentation_idx)`` entries under ``.../auto_labeling/``."""
    auto_labeling_dir = normalize_directory_path(
        str(URL(_run_base_url(output_directory, run_id)) / "auto_labeling")
    )
    run_dirs: set[tuple[str, int]] = set()
    try:
        for obj in list_directory(auto_labeling_dir):
            rel = get_relative_storage_path(obj.key, auto_labeling_dir)
            if not rel:
                continue
            parts = rel.split("/")
            if len(parts) >= 2 and parts[1].isdigit():
                run_dirs.add((parts[0], int(parts[1])))
    except Exception:
        return []
    return sorted(run_dirs)


def verify_cosmos_run_files(
    output_directory: str,
    run_id: str,
    person_key: str,
    aug_idx: int,
) -> list[str]:
    """Return missing required PAS image-edit artifact paths for one run directory."""
    run_dir = str(URL(output_directory.rstrip("/")) / run_id / "cosmos" / person_key / str(aug_idx))
    missing = []
    for fname in REQUIRED_COSMOS_FILES:
        file_path = f"{run_dir.rstrip('/')}/{fname}"
        if not is_file(file_path):
            missing.append(file_path)
    return missing


def verify_augmented_data_contents(
    data: dict[str, Any],
    expected_run_entries: list[tuple[str, int]],
    dataset_dir: str,
) -> list[str]:
    """Validate ``augmented_data.json`` structure and minimum counts."""
    errors: list[str] = []
    if not isinstance(data, dict):
        return ["augmented_data.json is not a JSON object"]

    expected_entries = len(expected_run_entries)
    expected_person_keys = {
        f"{person_key}_aug{aug_idx}" for person_key, aug_idx in expected_run_entries
    }
    expected_scene_paths = {
        get_dataset_scene_dir(dataset_dir, person_key, str(aug_idx)).rstrip("/")
        for person_key, aug_idx in expected_run_entries
    }

    metadata = data.get("metadata")
    if not isinstance(metadata, dict):
        errors.append("augmented_data.json: 'metadata' section is missing or invalid")
    else:
        if metadata.get("total_ids") != expected_entries:
            errors.append(
                f"augmented_data.json: metadata.total_ids is {metadata.get('total_ids')} "
                f"(expected {expected_entries})"
            )
        if metadata.get("total_scenes") != expected_entries:
            errors.append(
                f"augmented_data.json: metadata.total_scenes is "
                f"{metadata.get('total_scenes')} (expected {expected_entries})"
            )

    entries = data.get("entries")
    if not isinstance(entries, list):
        errors.append("augmented_data.json: 'entries' section is missing or invalid")
    elif len(entries) != expected_entries:
        errors.append(
            f"augmented_data.json: expected {expected_entries} entries, found {len(entries)}"
        )
    else:
        seen_person_keys: set[str] = set()
        seen_scene_paths: set[str] = set()
        for index, entry in enumerate(entries):
            if not isinstance(entry, dict):
                errors.append(f"augmented_data.json: entries[{index}] is not an object")
                continue
            person_key = entry.get("person_key")
            if person_key not in expected_person_keys:
                errors.append(
                    f"augmented_data.json: entries[{index}].person_key is "
                    f"{person_key!r}, expected one of {sorted(expected_person_keys)}"
                )
            elif person_key in seen_person_keys:
                errors.append(
                    f"augmented_data.json: entries[{index}].person_key duplicates {person_key!r}"
                )
            else:
                seen_person_keys.add(person_key)

            scene_path = entry.get("scene_path")
            if not isinstance(scene_path, str) or not scene_path.strip():
                errors.append(f"augmented_data.json: entries[{index}] has no scene_path")
                continue
            normalized_scene_path = scene_path.rstrip("/")
            if normalized_scene_path not in expected_scene_paths:
                errors.append(
                    f"augmented_data.json: entries[{index}].scene_path is "
                    f"{scene_path!r}, expected one of {sorted(expected_scene_paths)}"
                )
            elif normalized_scene_path in seen_scene_paths:
                errors.append(
                    f"augmented_data.json: entries[{index}].scene_path duplicates {scene_path!r}"
                )
            else:
                seen_scene_paths.add(normalized_scene_path)

        missing_person_keys = expected_person_keys - seen_person_keys
        if missing_person_keys:
            errors.append(
                f"augmented_data.json: missing person_key entries {sorted(missing_person_keys)}"
            )
        missing_scene_paths = expected_scene_paths - seen_scene_paths
        if missing_scene_paths:
            errors.append(
                f"augmented_data.json: missing scene_path entries {sorted(missing_scene_paths)}"
            )

    return errors


def verify_dataset_scene(entry: dict[str, Any], index: int) -> list[str]:
    """Validate one copied DAFT scene referenced by ``augmented_data.json``."""
    errors: list[str] = []
    scene_path = entry.get("scene_path")
    if not isinstance(scene_path, str) or not scene_path.strip():
        return [f"augmented_data.json: entries[{index}] has no scene_path"]

    if not _prefix_has_objects(scene_path):
        errors.append(f"{scene_path.rstrip('/')}/ (no objects)")

    paths = entry.get("paths")
    paths = paths if isinstance(paths, dict) else {}
    config_path = paths.get("config") or f"{scene_path.rstrip('/')}/config.yaml"
    if not is_file(config_path):
        errors.append(config_path)

    has_daft_outputs = False
    for dirname in DATASET_SCENE_OUTPUT_DIRS:
        scene_subdir = paths.get(dirname) or f"{scene_path.rstrip('/')}/{dirname}"
        if _prefix_has_objects(scene_subdir):
            has_daft_outputs = True
            break
    if not has_daft_outputs:
        errors.append(
            f"{scene_path.rstrip('/')}/ (no DAFT output objects under "
            f"{', '.join(DATASET_SCENE_OUTPUT_DIRS)})"
        )

    return errors


def verify_augmented_dataset(
    output_directory: str,
    run_id: str,
    expected_run_entries: list[tuple[str, int]],
) -> list[str]:
    """Check ``augmented_dataset/`` artifacts and ``augmented_data.json`` contents."""
    missing: list[str] = []
    dataset_dir = get_augmented_dataset_dir(output_directory, run_id)

    augmented_data_path = f"{dataset_dir.rstrip('/')}/{DEFAULT_OUTPUT_JSON}"
    if not is_file(augmented_data_path):
        missing.append(augmented_data_path)
        return missing

    augmented_data = read_file_as_dict(augmented_data_path)
    if not isinstance(augmented_data, dict):
        missing.append(f"Failed to read augmented dataset at {augmented_data_path}")
        return missing

    missing.extend(
        verify_augmented_data_contents(
            augmented_data,
            expected_run_entries,
            dataset_dir,
        )
    )

    entries = augmented_data.get("entries")
    if isinstance(entries, list):
        for index, entry in enumerate(entries):
            if isinstance(entry, dict):
                missing.extend(verify_dataset_scene(entry, index))
    return missing


def validate_pipeline_outputs(payload: dict[str, Any], run_id: str) -> None:
    """Validate end-of-pipeline storage artifacts for a PAS run."""
    output_directory = (payload.get("output_directory") or "").strip()
    if not output_directory:
        raise PipelineOutputValidationError(["Payload has no output_directory"])

    cosmos_cfg = payload.get("cosmos") or {}
    num_augmentation = max(1, int(cosmos_cfg.get("num_augmentation", 1)))
    max_imgs = payload.get("max_imgs")

    run_entries = discover_cosmos_run_entries(output_directory, run_id)
    if not run_entries:
        raise PipelineOutputValidationError(["No cosmos output directories found for any person"])

    grouped_runs = group_cosmos_run_entries(run_entries)
    processed_person_keys = sorted(grouped_runs)

    if max_imgs is not None and max_imgs > 0 and len(processed_person_keys) > max_imgs:
        raise PipelineOutputValidationError(
            [
                "Expected at most "
                f"{max_imgs} processed person(s) (max_imgs), "
                f"found {len(processed_person_keys)} in cosmos outputs"
            ]
        )

    errors: list[str] = []
    expected_aug_indices = set(range(num_augmentation))
    for person_key, aug_indices in grouped_runs.items():
        if aug_indices != expected_aug_indices:
            errors.append(
                f"cosmos/{person_key}: expected augmentation indices "
                f"{sorted(expected_aug_indices)}, found {sorted(aug_indices)}"
            )

    errors.extend(verify_preprocessing_artifacts(output_directory, run_id, processed_person_keys))
    for person_key, aug_idx in run_entries:
        errors.extend(verify_cosmos_run_files(output_directory, run_id, person_key, aug_idx))
    errors.extend(verify_augmented_dataset(output_directory, run_id, run_entries))

    auto_labeling_entries = discover_auto_labeling_run_entries(output_directory, run_id)
    if set(auto_labeling_entries) != set(run_entries):
        errors.append(
            f"Expected auto_labeling run directories {run_entries}, found {auto_labeling_entries}"
        )
    elif not _prefix_has_objects(f"{_run_base_url(output_directory, run_id)}/auto_labeling"):
        errors.append(f"{_run_base_url(output_directory, run_id)}/auto_labeling/ (no objects)")

    if errors:
        raise PipelineOutputValidationError(errors)


def validate_pas_pipeline_outputs(
    payload: str | dict[str, Any] | None = None,
    run_id: str = "",
    **context,
) -> None:
    """Validate PAS pipeline storage outputs for the current run."""
    del context
    try:
        if isinstance(payload, dict):
            config = PasDagPayloadConfig.model_validate(payload)
        else:
            config = PasDagPayloadConfig.model_validate(ast.literal_eval(payload or "{}"))
        validate_pipeline_outputs(config.model_dump(), run_id)
    except PipelineOutputValidationError as e:
        raise AirflowFailException(
            "Pipeline output validation failed: " + "; ".join(e.errors)
        ) from e
    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(f"Pipeline output validation failed: {e}") from e

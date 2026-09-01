# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Validate the terminal artifacts of the Event Video Generation video EPAS workflow."""

from __future__ import annotations

import ast
import json
import logging
from typing import Any

from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.task_groups.input_preparation import require_prepared_input_from_xcom
from dags.shared.utils.msc_utils import is_file
from dags.workflows.event_video_generation_dag.models import EventVideoGenerationDagPayloadConfig

logger = logging.getLogger(__name__)

ALWAYS_REQUIRED_ANNOTATION_FILES = (
    "contextual/objects.json",
    "contextual/instances.json",
    "sidecars/captioning/video_captions.json",
    "sidecars/visual_qa_anomaly/items.json",
    "sidecars/visual_qa_per_track/items.json",
    "sidecars/visual_qa_per_track/windows.normalized.json",
)
TRACKS_SIDECAR = "sidecars/detection_and_tracking/tracks.json"
TRACK_REQUIRED_ANNOTATION_FILES = (
    "sidecars/person_attribute_search/pas.json",
    "sidecars/person_attribute_search/chunk_queries.json",
    "sidecars/person_attribute_search/pas_anomaly.json",
    "contextual/person_attributes.json",
    "contextual/pas_queries.json",
)


def _parse_payload(value: str | dict[str, Any] | None) -> EventVideoGenerationDagPayloadConfig:
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


def validate_event_video_generation_pipeline_outputs(
    payload: str | dict[str, Any] | None = None,
    run_id: str = "",
    **context: Any,
) -> dict[str, Any]:
    """Require the cookbook handoffs and terminal deliverables for every augmentation."""
    try:
        config = _parse_payload(payload)
        prepared = require_prepared_input_from_xcom(context["ti"])
        auto_labeling_root = str(URL(config.output_directory) / run_id / "auto_labeling")
        missing = []
        scenes = []
        trackless_scenes = []
        for image in prepared.videos:
            for augmentation_index in range(config.cosmos.num_augmentation):
                scene = f"{auto_labeling_root}/{image.video_key}/{augmentation_index}"
                scenes.append(scene)
                for artifact in ALWAYS_REQUIRED_ANNOTATION_FILES:
                    path = f"{scene}/{artifact}"
                    if not is_file(path):
                        missing.append(path)
                tracks_path = f"{scene}/{TRACKS_SIDECAR}"
                if is_file(tracks_path):
                    for artifact in TRACK_REQUIRED_ANNOTATION_FILES:
                        path = f"{scene}/{artifact}"
                        if not is_file(path):
                            missing.append(path)
                else:
                    trackless_scenes.append(scene)
        if missing:
            preview = ", ".join(missing[:10])
            suffix = "" if len(missing) <= 10 else f" (+{len(missing) - 10} more)"
            raise AirflowFailException(
                f"Event Video Generation annotation validation found {len(missing)} missing artifacts: "
                f"{preview}{suffix}"
            )
        if trackless_scenes:
            logger.warning(
                "Event Video Generation annotation validation found %d scene(s) with no crop-eligible "
                "person tracks; person attribute search outputs were correctly skipped: %s",
                len(trackless_scenes),
                ", ".join(trackless_scenes),
            )
        return {
            "length": len(scenes),
            "scenes": scenes,
            "trackless_scenes": trackless_scenes,
        }
    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(
            f"Event Video Generation pipeline output validation failed: {e}"
        ) from e

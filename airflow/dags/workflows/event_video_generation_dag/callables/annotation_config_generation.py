# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Generate container arguments for the Event Video Generation video EPAS annotation stages."""

from __future__ import annotations

import ast
import copy
import json
import shlex
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml
from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.models import StoragePathListXcom
from dags.shared.models.payload import DEFAULT_OPENAI_COMPATIBLE_PROVIDER
from dags.shared.task_groups.service_lifecycle import require_service_endpoint_from_xcom
from dags.shared.utils.msc_utils import convert_msc_to_storage_url, write_file_to_directory
from dags.shared.utils.xcom import pull_and_validate_xcom
from dags.workflows.event_video_generation_dag.models import EventVideoGenerationDagPayloadConfig

EVENT_VIDEO_GENERATION_CONFIG_DIR = Path(__file__).resolve().parents[1] / "configs"
ANOMALY_QUESTION_BANK_PATH = EVENT_VIDEO_GENERATION_CONFIG_DIR / "question_bank.anomaly_tags.json"
PERSON_ATTRIBUTE_QUESTION_BANK_PATH = (
    EVENT_VIDEO_GENERATION_CONFIG_DIR / "question_bank.person_attributes.json"
)
PERSON_ATTRIBUTE_SEARCH_CONFIG_PATH = (
    EVENT_VIDEO_GENERATION_CONFIG_DIR / "person_attribute_search_config.yaml"
)

DETECTION_AND_TRACKING_ARGS = (
    "--tracker",
    "rfdetr-boosttrack",
    "--classes",
    "person",
    "--threshold",
    "0.5",
    "--extract-crops",
    "--crop-classes",
    "person",
    "--crops-per-track",
    "16",
    "--crop-padding",
    "0.1",
    "--min-crop-size",
    "48",
    "--allow-model-download",
)
CAPTIONING_ARGS = (
    "--input-source",
    "original",
    "--window-seconds",
    "4.0",
    "--window-frames",
    "0",
    "--remainder-threshold",
    "0",
    "--sampling-fps",
    "2.0",
    "--max-frames",
    "24",
    "--resolution",
    "768",
)
VISUAL_QA_ANOMALY_PROFILE_ARGS = (
    "--input-source",
    "original",
    "--single-window",
    "--max-frames",
    "16",
    "--sampling-fps",
    "3.0",
    "--resolution",
    "768",
    "--raw-windows-sidecar",
    "visual_qa_anomaly/windows.json",
    "--output-items-sidecar",
    "visual_qa_anomaly/items.json",
    "--output-windows-sidecar",
    "visual_qa_anomaly/windows.normalized.json",
    "--state-artifacts-key",
    "visual_qa_anomaly",
)
VISUAL_QA_PERSON_ATTRIBUTE_PROFILE_ARGS = (
    "--track-crops-sidecar",
    "detection_and_tracking/tracks.json",
    "--max-crops-per-track",
    "12",
    "--resolution",
    "896",
    "--raw-windows-sidecar",
    "visual_qa_per_track/windows.json",
    "--output-items-sidecar",
    "visual_qa_per_track/items.json",
    "--output-windows-sidecar",
    "visual_qa_per_track/windows.normalized.json",
    "--state-artifacts-key",
    "visual_qa_per_track",
)
VISUAL_QA_PRE_QUESTION_BANK_ARGS = (
    "--generation-mode",
    "window-direct-vlm",
)
VISUAL_QA_POST_QUESTION_BANK_ARGS = (
    "--temperature",
    "0",
    "--max-tokens",
    "4096",
    "--no-flat-qa-tasks",
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


def _quote(value: str | int | float) -> str:
    return shlex.quote(str(value))


def _input_arg(media_path: str, data_path: str) -> list[str]:
    payload = json.dumps(
        [{"media_path": media_path, "data_path": data_path}],
        separators=(",", ":"),
    )
    return ["--input", _quote(payload)]


def _inputs_with_data_paths(
    *,
    payload: EventVideoGenerationDagPayloadConfig,
    run_id: str,
    input_xcom_task_id: str,
    input_xcom_key: str,
    output_group_id: str | None,
    group_id: str,
    ti: Any,
) -> list[tuple[str, str]]:
    inputs = pull_and_validate_xcom(
        ti=ti,
        task_id=input_xcom_task_id,
        key=input_xcom_key,
        model_class=StoragePathListXcom,
    )
    output_base = str(URL(payload.output_directory) / run_id / (output_group_id or group_id))
    per_key_index: defaultdict[str, int] = defaultdict(int)
    resolved = []
    for video in inputs.videos:
        index = per_key_index[video.video_key]
        per_key_index[video.video_key] += 1
        resolved.append(
            (
                convert_msc_to_storage_url(video.video_path),
                f"{output_base}/{video.video_key}/{index}",
            )
        )
    return resolved


def _vlm_endpoint(payload: EventVideoGenerationDagPayloadConfig, ti: Any) -> str:
    if payload.external_services:
        return payload.cosmos.vlm_service_url or ""
    return require_service_endpoint_from_xcom(ti, "vlm_service")


def _llm_endpoint(payload: EventVideoGenerationDagPayloadConfig, ti: Any) -> str:
    if payload.external_services:
        return payload.cosmos.llm_service_url or ""
    return require_service_endpoint_from_xcom(ti, "llm_service")


def _load_asset(path: Path, description: str) -> bytes:
    try:
        body = path.read_bytes()
    except Exception as e:
        raise AirflowFailException(f"Failed to read {description} at {path}: {e}") from e
    if not body:
        raise AirflowFailException(f"{description} is empty: {path}")
    return body


def prepare_event_video_generation_detection_and_tracking_configs(
    detection_and_tracking_config: str | dict[str, Any] | None = None,
    run_id: str = "",
    input_xcom_task_id: str = "cosmos_augmentation.validate_outputs",
    input_xcom_key: str = "return_value",
    group_id: str = "detection_and_tracking",
    output_group_id: str | None = None,
    **context: Any,
) -> list[str]:
    """Build RF-DETR/BoostTrack commands with the EPAS crop handoff."""
    try:
        payload = _parse_payload(detection_and_tracking_config)
        commands = []
        for media_path, data_path in _inputs_with_data_paths(
            payload=payload,
            run_id=run_id,
            input_xcom_task_id=input_xcom_task_id,
            input_xcom_key=input_xcom_key,
            output_group_id=output_group_id,
            group_id=group_id,
            ti=context["ti"],
        ):
            args = _input_arg(media_path, data_path)
            args.extend(DETECTION_AND_TRACKING_ARGS)
            commands.append(" ".join(args))
        return commands
    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(
            f"Event Video Generation detection-and-tracking config generation failed: {e}"
        ) from e


def prepare_event_video_generation_captioning_configs(
    captioning_config: str | dict[str, Any] | None = None,
    run_id: str = "",
    input_xcom_task_id: str = "cosmos_augmentation.validate_outputs",
    input_xcom_key: str = "return_value",
    group_id: str = "captioning",
    output_group_id: str | None = None,
    **context: Any,
) -> list[str]:
    """Build captioning commands matching the video EPAS cookbook."""
    try:
        payload = _parse_payload(captioning_config)
        vlm_endpoint = _vlm_endpoint(payload, context["ti"])
        commands = []
        for media_path, data_path in _inputs_with_data_paths(
            payload=payload,
            run_id=run_id,
            input_xcom_task_id=input_xcom_task_id,
            input_xcom_key=input_xcom_key,
            output_group_id=output_group_id,
            group_id=group_id,
            ti=context["ti"],
        ):
            args = _input_arg(media_path, data_path)
            args.extend(
                [
                    *CAPTIONING_ARGS,
                    "--vlm-provider",
                    DEFAULT_OPENAI_COMPATIBLE_PROVIDER,
                    "--vlm-endpoint-url",
                    _quote(vlm_endpoint),
                    "--vlm-model",
                    _quote(payload.cosmos.vlm_model),
                ]
            )
            commands.append(" ".join(args))
        return commands
    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(
            f"Event Video Generation captioning config generation failed: {e}"
        ) from e


def prepare_event_video_generation_visual_qa_configs(
    visual_qa_config: str | dict[str, Any] | None = None,
    run_id: str = "",
    input_xcom_task_id: str = "cosmos_augmentation.validate_outputs",
    input_xcom_key: str = "return_value",
    group_id: str = "visual_qa",
    output_group_id: str | None = None,
    visual_qa_profile: str = "anomaly",
    **context: Any,
) -> list[str]:
    """Build either the anomaly or per-track Visual QA cookbook pass."""
    try:
        payload = _parse_payload(visual_qa_config)
        vlm_endpoint = _vlm_endpoint(payload, context["ti"])
        if visual_qa_profile == "anomaly":
            question_bank_path = ANOMALY_QUESTION_BANK_PATH
            profile_args = VISUAL_QA_ANOMALY_PROFILE_ARGS
        elif visual_qa_profile == "person_attribute":
            question_bank_path = PERSON_ATTRIBUTE_QUESTION_BANK_PATH
            profile_args = VISUAL_QA_PERSON_ATTRIBUTE_PROFILE_ARGS
        else:
            raise AirflowFailException(
                f"Unknown Event Video Generation Visual QA profile: {visual_qa_profile}"
            )

        question_bank_body = _load_asset(
            question_bank_path, "Event Video Generation Visual QA question bank"
        )
        commands = []
        for media_path, data_path in _inputs_with_data_paths(
            payload=payload,
            run_id=run_id,
            input_xcom_task_id=input_xcom_task_id,
            input_xcom_key=input_xcom_key,
            output_group_id=output_group_id,
            group_id=group_id,
            ti=context["ti"],
        ):
            remote_question_bank = f"{data_path}/sidecars/assets/{question_bank_path.name}"
            write_file_to_directory(remote_question_bank, question_bank_body)
            args = _input_arg(media_path, data_path)
            args.extend(
                [
                    *VISUAL_QA_PRE_QUESTION_BANK_ARGS,
                    "--question-bank-file",
                    _quote(remote_question_bank),
                    *profile_args,
                    *VISUAL_QA_POST_QUESTION_BANK_ARGS,
                    "--vlm-provider",
                    DEFAULT_OPENAI_COMPATIBLE_PROVIDER,
                    "--vlm-endpoint-url",
                    _quote(vlm_endpoint),
                    "--vlm-model",
                    _quote(payload.cosmos.vlm_model),
                ]
            )
            commands.append(" ".join(args))
        return commands
    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(
            f"Event Video Generation Visual QA config generation failed: {e}"
        ) from e


def prepare_event_video_generation_person_attribute_search_configs(
    event_and_person_attribute_search_config: str | dict[str, Any] | None = None,
    run_id: str = "",
    input_xcom_task_id: str = "cosmos_augmentation.validate_outputs",
    input_xcom_key: str = "return_value",
    group_id: str = "person_attribute_search",
    output_group_id: str | None = None,
    **context: Any,
) -> list[str]:
    """Build the terminal PAS command without the image-only attribute JSON override."""
    try:
        payload = _parse_payload(event_and_person_attribute_search_config)
        llm_endpoint = _llm_endpoint(payload, context["ti"])
        config_body = _load_asset(
            PERSON_ATTRIBUTE_SEARCH_CONFIG_PATH,
            "Event Video Generation person attribute search config",
        )
        template = yaml.safe_load(config_body)
        if not isinstance(template, dict):
            raise AirflowFailException(
                "Event Video Generation person attribute search config must be a mapping"
            )
        commands = []
        for media_path, data_path in _inputs_with_data_paths(
            payload=payload,
            run_id=run_id,
            input_xcom_task_id=input_xcom_task_id,
            input_xcom_key=input_xcom_key,
            output_group_id=output_group_id,
            group_id=group_id,
            ti=context["ti"],
        ):
            remote_config = (
                f"{data_path}/sidecars/assets/{PERSON_ATTRIBUTE_SEARCH_CONFIG_PATH.name}"
            )
            configured = copy.deepcopy(template)
            configured.update(
                {
                    "llm_endpoint_url": llm_endpoint,
                    "llm_model": payload.cosmos.llm_model,
                }
            )
            write_file_to_directory(
                remote_config,
                yaml.safe_dump(configured, sort_keys=False).encode("utf-8"),
            )
            args = _input_arg(media_path, data_path)
            args.extend(
                [
                    "--config-file",
                    _quote(remote_config),
                    "--llm-endpoint-url",
                    _quote(llm_endpoint),
                    "--llm-model",
                    _quote(payload.cosmos.llm_model),
                ]
            )
            commands.append(" ".join(args))
        return commands
    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(
            f"Event Video Generation person attribute search config generation failed: {e}"
        ) from e

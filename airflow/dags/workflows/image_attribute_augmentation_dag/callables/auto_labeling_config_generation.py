# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Image Attribute Augmentation auto-labeling config generation callable."""

import ast
import copy
from pathlib import Path
from typing import Any

import yaml
from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.models import AutoLabelingTaskConfig, StoragePathListXcom
from dags.shared.task_groups.service_lifecycle import require_service_endpoint_from_xcom
from dags.shared.utils.msc_utils import convert_msc_to_storage_url, write_file_to_directory
from dags.shared.utils.xcom import pull_and_validate_xcom

IMAGE_ATTRIBUTE_AUGMENTATION_AUTO_LABELING_CONFIG_TEMPLATE_PATH = (
    Path(__file__).resolve().parents[1] / "configs" / "al_config.yaml"
)


def _load_config_template() -> dict[str, Any]:
    try:
        with open(IMAGE_ATTRIBUTE_AUGMENTATION_AUTO_LABELING_CONFIG_TEMPLATE_PATH, "r") as config_file:
            template = yaml.safe_load(config_file)
    except FileNotFoundError as e:
        raise AirflowFailException(
            f"Image Attribute Augmentation auto-labeling config template not found: {IMAGE_ATTRIBUTE_AUGMENTATION_AUTO_LABELING_CONFIG_TEMPLATE_PATH}"
        ) from e
    except Exception as e:
        raise AirflowFailException(
            f"Failed to load Image Attribute Augmentation auto-labeling config template "
            f"{IMAGE_ATTRIBUTE_AUGMENTATION_AUTO_LABELING_CONFIG_TEMPLATE_PATH}: {e}"
        ) from e

    if not isinstance(template, dict):
        raise AirflowFailException(
            f"Invalid Image Attribute Augmentation auto-labeling config template: {IMAGE_ATTRIBUTE_AUGMENTATION_AUTO_LABELING_CONFIG_TEMPLATE_PATH}"
        )
    return template


def _augmentation_index_from_output_path(output_path: str) -> str:
    parts = output_path.rstrip("/").split("/")
    if len(parts) < 2:
        raise AirflowFailException(f"Cannot infer augmentation index from {output_path}")
    aug_dir = parts[-2]
    return aug_dir.replace("aug_", "", 1)


def _build_config(
    *,
    template: dict[str, Any],
    video_path: str,
    output_dir: str,
    vlm_base_url: str | None,
    vlm_model: str | None,
    llm_base_url: str | None,
    llm_model: str | None,
) -> dict[str, Any]:
    generated_config = copy.deepcopy(template)
    generated_config["data"][0]["inputs"]["video_path"] = video_path
    generated_config["data"][0]["output"]["out_dir"] = output_dir
    generated_config["data"][0]["output"]["config_path"] = f"{output_dir}/config.yaml"
    generated_config["endpoints"]["vlm"]["url"] = vlm_base_url
    generated_config["endpoints"]["vlm"]["model"] = vlm_model
    generated_config["endpoints"]["llm"]["url"] = llm_base_url
    generated_config["endpoints"]["llm"]["model"] = llm_model

    return generated_config


def generate_image_attribute_augmentation_auto_labeling_configs(
    auto_labeling_config: str | None = None,
    run_id: str = "",
    input_xcom_task_id: str = "",
    input_xcom_key: str = "return_value",
    **context,
) -> list[str]:
    """Generate Image Attribute Augmentation auto-labeling configs and return mapped ``--config`` arguments."""
    try:
        al_config = AutoLabelingTaskConfig.model_validate(ast.literal_eval(auto_labeling_config))
        vlm_base_url = al_config.vlm_service_url
        vlm_model = al_config.vlm_model
        llm_base_url = al_config.llm_service_url
        llm_model = al_config.llm_model

        if not al_config.external_services:
            vlm_base_url = require_service_endpoint_from_xcom(context["ti"], "vlm_service")
            llm_base_url = require_service_endpoint_from_xcom(context["ti"], "llm_service")

        storage_path_list_xcom = pull_and_validate_xcom(
            ti=context["ti"],
            task_id=input_xcom_task_id,
            key=input_xcom_key,
            model_class=StoragePathListXcom,
        )
        output_base = str(URL(al_config.output_directory) / run_id / "auto_labeling")
        template = _load_config_template()

        config_args = []
        for video in storage_path_list_xcom.videos:
            video_path = convert_msc_to_storage_url(video.video_path)
            augmentation_index = _augmentation_index_from_output_path(video.video_path)
            output_dir = f"{output_base}/{video.video_key}/{augmentation_index}"
            config_path = f"{output_dir}/config.yaml"
            generated_config = _build_config(
                template=template,
                video_path=video_path,
                output_dir=output_dir,
                vlm_base_url=vlm_base_url,
                vlm_model=vlm_model,
                llm_base_url=llm_base_url,
                llm_model=llm_model,
            )
            write_file_to_directory(
                config_path,
                yaml.safe_dump(generated_config, sort_keys=False).encode("utf-8"),
            )
            config_args.append(f"--config {config_path}")

        return config_args

    except Exception as e:
        raise AirflowFailException(f"Image Attribute Augmentation auto-labeling config generation failed: {e}") from e

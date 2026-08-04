# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Payload models for the Image Attribute Augmentation DAG."""

import logging
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from dags.shared.models import (
    AutoLabelingTaskConfig,
    CosmosTaskConfig,
    ServiceLifecycleTaskConfig,
)

logger = logging.getLogger(__name__)


class ImageAttributeAugmentationDagPayloadConfig(BaseModel):
    """Runtime payload schema for Image Attribute Augmentation pre-processing DAG."""

    model_config = ConfigDict(extra="ignore")

    input_path: str = Field(
        description=(
            "Storage directory containing PAS image subdirectories; each subdirectory "
            "is one person ID."
        ),
    )
    max_imgs: Optional[int] = Field(
        default=1,
        description=(
            "Maximum number of person-ID folders (combined images) to process. "
            "When set to 0 or negative, all IDs under input_path are processed."
        ),
    )
    output_directory: str = Field(
        description="Storage output directory for Image Attribute Augmentation preprocessing artifacts.",
    )
    external_services: bool = Field(
        default=True,
        description=(
            "Top-level service mode switch. When true, provided external service URLs "
            "are used. When false, internal VLM, LLM, and image-edit services are deployed."
        ),
    )
    service_lifecycle: ServiceLifecycleTaskConfig = Field(
        default=ServiceLifecycleTaskConfig(),
        description=(
            "VLM/LLM/image-edit internal deployment flags. Each must be false when "
            "external_services is true, and true when external_services is false."
        ),
    )
    cosmos: CosmosTaskConfig = Field(
        description="Cosmos/augmentation task configuration for Image Attribute Augmentation image-edit execution.",
    )
    auto_labeling: AutoLabelingTaskConfig = Field(
        description="Auto-labeling task configuration for Image Attribute Augmentation image-edit execution.",
    )

    @model_validator(mode="before")
    @classmethod
    def populate_nested_payload(cls, data):
        if not isinstance(data, dict):
            raise ValueError("payload must be a dictionary")

        input_path = data.get("input_path")
        if input_path is None or (
            isinstance(input_path, str) and not input_path.strip()
        ):
            raise ValueError("input_path is required")

        output_directory = data.get("output_directory")
        if output_directory is None or (
            isinstance(output_directory, str) and not output_directory.strip()
        ):
            raise ValueError("output_directory is required")
        external_services = bool(data.get("external_services", True))
        deploy_internal = not external_services
        for field_name in ("cosmos", "auto_labeling"):
            nested_cfg = data.get(field_name)
            if nested_cfg is None:
                data[field_name] = {
                    "output_directory": output_directory,
                    "external_services": external_services,
                }
                continue
            if not isinstance(nested_cfg, dict):
                continue
            if (
                "output_directory" in nested_cfg
                and nested_cfg["output_directory"] != output_directory
            ):
                raise ValueError(
                    f"{field_name}.output_directory must match top-level output_directory"
                )
            if (
                nested_cfg.get("external_services") is not None
                and bool(nested_cfg["external_services"]) != external_services
            ):
                raise ValueError(
                    f"{field_name}.external_services must match top-level external_services"
                )
            if nested_cfg.get("output_directory") is None:
                nested_cfg["output_directory"] = output_directory
            if nested_cfg.get("external_services") is None:
                nested_cfg["external_services"] = external_services

        sl_cfg = data.get("service_lifecycle")
        if sl_cfg is None:
            data["service_lifecycle"] = {
                "vlm_service": {"enabled": deploy_internal},
                "llm_service": {"enabled": deploy_internal},
                "image_edit_service": {"enabled": deploy_internal},
            }
        elif isinstance(sl_cfg, dict):
            for service_key in ("vlm_service", "llm_service", "image_edit_service"):
                entry = sl_cfg.get(service_key)
                if entry is None:
                    sl_cfg[service_key] = {"enabled": deploy_internal}
                elif isinstance(entry, dict) and entry.get("enabled") is None:
                    entry["enabled"] = deploy_internal
        return data

    @model_validator(mode="after")
    def check_service_lifecycle_matches_external_services(self):
        expected = not self.external_services
        if self.service_lifecycle.vlm_service.enabled != expected:
            logger.warning(
                "service_lifecycle.vlm_service.enabled must be false when external_services "
                "is true, and true when external_services is false"
            )
            self.service_lifecycle.vlm_service.enabled = expected
        if self.service_lifecycle.llm_service.enabled != expected:
            logger.warning(
                "service_lifecycle.llm_service.enabled must be false when external_services "
                "is true, and true when external_services is false"
            )
            self.service_lifecycle.llm_service.enabled = expected
        if self.service_lifecycle.image_edit_service.enabled != expected:
            logger.warning(
                "service_lifecycle.image_edit_service.enabled must be false when external_services "
                "is true, and true when external_services is false"
            )
            self.service_lifecycle.image_edit_service.enabled = expected

        return self

    @model_validator(mode="after")
    def check_service_url_requirements(self):
        if self.cosmos.external_services:
            if not self.cosmos.vlm_service_url:
                raise ValueError(
                    "cosmos.vlm_service_url is required when cosmos.external_services is true"
                )
            if not self.cosmos.llm_service_url:
                raise ValueError(
                    "cosmos.llm_service_url is required when cosmos.external_services is true"
                )
            if not self.cosmos.image_edit_service_url:
                raise ValueError(
                    "cosmos.image_edit_service_url is required when "
                    "cosmos.external_services is true"
                )
        if self.auto_labeling.external_services:
            if not self.auto_labeling.vlm_service_url:
                raise ValueError(
                    "auto_labeling.vlm_service_url is required when "
                    "auto_labeling.external_services is true"
                )
            if not self.auto_labeling.llm_service_url:
                raise ValueError(
                    "auto_labeling.llm_service_url is required when "
                    "auto_labeling.external_services is true"
                )
        return self

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Payload models for the Event Video Generation DAG."""

import logging
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from dags.shared.models import ServiceLifecycleServiceConfig, ServiceLifecycleTaskConfig
from dags.shared.models.payload import (
    DEFAULT_LLM_MODEL,
    DEFAULT_LLM_SERVICE_URL,
    DEFAULT_VLM_MODEL,
    DEFAULT_VLM_SERVICE_URL,
    WeightedDistribution,
)

logger = logging.getLogger(__name__)

# Placeholders, not real data locations or endpoints: every run must override them from
# the payload. They keep this model constructible without arguments so the DAG can render
# its Param default at parse time.
DEFAULT_INPUT_PATH = "s3://<your-input-bucket>/<your-workflow>/input"
DEFAULT_OUTPUT_DIRECTORY = "s3://<your-output-bucket>/<your-workflow>/output"
DEFAULT_IMAGE2VIDEO_SERVICE_URL = "https://<your-image2video-endpoint>/v1"
DEFAULT_IMAGE2VIDEO_MODEL = "nvidia/Cosmos3-Super-Image2Video"


def default_event_video_generation_variable_distribution() -> dict:
    """Return the deterministic default anomaly/environment distribution."""
    return {
        "variables": {
            "anomaly_type": {"person_falling": 1.0},
            "env_type": {"warehouse": 1.0},
        }
    }


class EventVideoGenerationVariableDistribution(BaseModel):
    """Weighted Event Video Generation variables sampled once per generated augmentation config."""

    model_config = ConfigDict(extra="forbid")

    variables: dict[str, WeightedDistribution] = Field(
        default_factory=lambda: default_event_video_generation_variable_distribution()["variables"],
        min_length=2,
        validate_default=True,
    )

    @model_validator(mode="after")
    def require_event_video_generation_variables(self):
        expected = {"anomaly_type", "env_type"}
        actual = set(self.variables)
        if actual != expected:
            raise ValueError(
                "variable_distribution.variables must contain exactly 'anomaly_type' and 'env_type'"
            )
        return self


class EventVideoGenerationCosmosTaskConfig(BaseModel):
    """Cosmos3 augmentation settings exposed by the Event Video Generation payload."""

    model_config = ConfigDict(extra="ignore")

    vlm_service_url: Optional[str] = Field(default=DEFAULT_VLM_SERVICE_URL)
    llm_service_url: Optional[str] = Field(default=DEFAULT_LLM_SERVICE_URL)
    image2video_service_url: Optional[str] = Field(default=DEFAULT_IMAGE2VIDEO_SERVICE_URL)
    vlm_model: str = Field(default=DEFAULT_VLM_MODEL, min_length=1)
    llm_model: str = Field(default=DEFAULT_LLM_MODEL, min_length=1)
    image2video_model: str = Field(default=DEFAULT_IMAGE2VIDEO_MODEL, min_length=1)
    num_augmentation: int = Field(default=1, ge=1)
    external_services: bool = Field(default=True)
    output_directory: str = Field(default=DEFAULT_OUTPUT_DIRECTORY)
    base_config_path: Optional[str] = Field(
        default=None,
        description=(
            "Optional MSC-readable URI to the Cosmos YAML base configuration template. "
            "When omitted, this DAG's bundled cosmos_config.yaml is used."
        ),
    )
    variable_distribution: EventVideoGenerationVariableDistribution = Field(
        default_factory=EventVideoGenerationVariableDistribution,
    )


class EventVideoGenerationServiceLifecycleConfig(ServiceLifecycleTaskConfig):
    """Lifecycle settings for services used by Event Video Generation."""

    image2video_service: ServiceLifecycleServiceConfig = Field(
        default_factory=ServiceLifecycleServiceConfig,
        description="Image2video service lifecycle settings.",
    )


class EventVideoGenerationDagPayloadConfig(BaseModel):
    """Payload configuration for the Event Video Generation DAG."""

    model_config = ConfigDict(extra="ignore")

    input_path: str = Field(
        default=DEFAULT_INPUT_PATH,
        description="Storage path to one Event Video Generation image or a directory of input images.",
    )
    max_images: int = Field(
        default=10,
        description=(
            "Maximum images processed from a directory. Values less than or equal to "
            "zero process all matching images."
        ),
    )
    output_directory: str = Field(
        default=DEFAULT_OUTPUT_DIRECTORY,
        description="Storage output directory for Event Video Generation artifacts.",
    )
    external_services: bool = Field(
        default=True,
        description=(
            "Top-level service mode switch. When true, provided external service URLs "
            "are used. When false, internal VLM, LLM, and image2video services are deployed."
        ),
    )
    service_lifecycle: EventVideoGenerationServiceLifecycleConfig = Field(
        default_factory=EventVideoGenerationServiceLifecycleConfig,
        description=(
            "VLM/LLM/image2video internal deployment flags. Each must be false when "
            "external_services is true, and true when external_services is false."
        ),
    )
    cosmos: EventVideoGenerationCosmosTaskConfig = Field(
        default_factory=EventVideoGenerationCosmosTaskConfig,
        description="Cosmos3 image-to-video augmentation configuration.",
    )
    enable_performance_reporting: bool = Field(
        default=False,
        description=(
            "Write YAML orchestration statistics and a self-contained graphical HTML "
            "dashboard after the workflow finishes."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def populate_service_lifecycle(cls, data):
        if not isinstance(data, dict):
            raise ValueError("payload must be a dictionary")

        external_services = bool(data.get("external_services", True))
        deploy_internal = not external_services
        output_directory = data.get("output_directory", DEFAULT_OUTPUT_DIRECTORY)

        cosmos_cfg = data.get("cosmos")
        if cosmos_cfg is None:
            data["cosmos"] = {
                "external_services": external_services,
                "output_directory": output_directory,
            }
        elif isinstance(cosmos_cfg, dict):
            if (
                cosmos_cfg.get("external_services") is not None
                and bool(cosmos_cfg["external_services"]) != external_services
            ):
                raise ValueError("cosmos.external_services must match top-level external_services")
            if (
                cosmos_cfg.get("output_directory") is not None
                and cosmos_cfg["output_directory"] != output_directory
            ):
                raise ValueError("cosmos.output_directory must match top-level output_directory")
            cosmos_cfg.setdefault("external_services", external_services)
            cosmos_cfg.setdefault("output_directory", output_directory)

        sl_cfg = data.get("service_lifecycle")
        if sl_cfg is None:
            data["service_lifecycle"] = {
                "vlm_service": {"enabled": deploy_internal},
                "llm_service": {"enabled": deploy_internal},
                "image2video_service": {"enabled": deploy_internal},
            }
        elif isinstance(sl_cfg, dict):
            for service_key in ("vlm_service", "llm_service", "image2video_service"):
                entry = sl_cfg.get(service_key)
                if entry is None:
                    sl_cfg[service_key] = {"enabled": deploy_internal}
                elif isinstance(entry, dict) and entry.get("enabled") is None:
                    entry["enabled"] = deploy_internal
        return data

    @model_validator(mode="after")
    def check_service_lifecycle_matches_external_services(self):
        expected = not self.external_services
        for service_key in ("vlm_service", "llm_service", "image2video_service"):
            service = getattr(self.service_lifecycle, service_key)
            if service.enabled != expected:
                logger.warning(
                    "service_lifecycle.%s.enabled must be false when external_services "
                    "is true, and true when external_services is false",
                    service_key,
                )
                service.enabled = expected
        return self

    @model_validator(mode="after")
    def check_external_service_urls(self):
        if not self.external_services:
            return self

        for field_name in (
            "vlm_service_url",
            "llm_service_url",
            "image2video_service_url",
        ):
            if not getattr(self.cosmos, field_name):
                raise ValueError(f"cosmos.{field_name} is required when external_services is true")
        return self

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pydantic models and helpers for Airflow XCom payloads between tasks."""

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class VideoMetadata(BaseModel):
    """Per-video descriptor in ``InputPreparationResult``."""

    video_key: str = Field(..., description="Stable output key for this video.")
    video_path: str = Field(..., description="Full storage URL for this video object.")


class StoragePathListXcom(BaseModel):
    """Shared XCom shape: ``length`` + ``videos`` entries."""

    length: int = Field(..., description="Number of video entries.")
    videos: list[VideoMetadata] = Field(
        ..., description="Video descriptors with stable key and full object path."
    )

    @model_validator(mode="after")
    def length_matches_videos(self):
        if self.length != len(self.videos):
            raise ValueError(f"length ({self.length}) must equal len(videos) ({len(self.videos)})")
        return self


class InputPreparationResult(StoragePathListXcom):
    """
    XCom ``return_value`` from ``input_preparation.prepare_input``."""


class CosmosOutputResult(StoragePathListXcom):
    """XCom ``return_value`` from ``cosmos_augmentation.validate_outputs``."""


class EndpointXComValue(BaseModel):
    """
    XCom key ``endpoint`` from deploy operators (e.g. k8s).

    Matches the flat dict pushed by ``K8sServiceOperator`` (url, resource_id, resource_version).
    """

    model_config = ConfigDict(extra="ignore")

    url: str = Field(..., description="Full invocation URL (e.g. https://.../v1)")
    resource_id: Optional[str] = Field(None, description="Platform resource id (e.g. function_id)")
    resource_version: Optional[str] = Field(
        None, description="Platform resource version (e.g. function_version_id)"
    )
    status: Optional[str] = Field(
        None, description="Optional deployment status from the platform operator"
    )

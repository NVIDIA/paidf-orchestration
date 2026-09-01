# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Event Video Generation-specific XCom models."""

from pydantic import BaseModel, Field, model_validator

from dags.shared.models.xcom import VideoMetadata


class EventVideoGenerationAugmentationArtifact(BaseModel):
    """Paths for one complete Event Video Generation augmentation."""

    input_key: str
    augmentation_index: int = Field(ge=0)
    video_path: str
    caption_path: str
    metadata_path: str


class EventVideoGenerationAugmentationOutputResult(BaseModel):
    """Validated Event Video Generation augmentation outputs published through XCom."""

    length: int = Field(ge=0)
    augmentations: list[EventVideoGenerationAugmentationArtifact]
    videos: list[VideoMetadata]

    @model_validator(mode="after")
    def length_matches_augmentations(self):
        if self.length != len(self.augmentations):
            raise ValueError(
                f"length ({self.length}) must equal len(augmentations) ({len(self.augmentations)})"
            )
        if self.length != len(self.videos):
            raise ValueError(f"length ({self.length}) must equal len(videos) ({len(self.videos)})")
        return self

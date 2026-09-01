# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Task Groups for SDG Workflow DAGs."""

from dags.shared.task_groups.captioning import CaptioningTaskGroup
from dags.shared.task_groups.cosmos import CosmosTaskGroup
from dags.shared.task_groups.detection_and_tracking import DetectionAndTrackingTaskGroup
from dags.shared.task_groups.image_attribute_augmentation import (
    ImageAttributeAugmentationTaskGroup,
)
from dags.shared.task_groups.input_preparation import InputPreparationTaskGroup
from dags.shared.task_groups.reasoning import ReasoningTaskGroup
from dags.shared.task_groups.reporting import PerformanceReportingTaskGroup
from dags.shared.task_groups.service_lifecycle import ServiceLifecycleTaskGroup
from dags.shared.task_groups.super_resolution import SuperResolutionTaskGroup
from dags.shared.task_groups.training_export import TrainingExportTaskGroup
from dags.shared.task_groups.validate_payload import ValidatePayloadTaskGroup
from dags.shared.task_groups.validated_output import ValidatedOutputTaskGroup
from dags.shared.task_groups.visual_qa import VisualQATaskGroup

__all__ = [
    "CaptioningTaskGroup",
    "CosmosTaskGroup",
    "DetectionAndTrackingTaskGroup",
    "ImageAttributeAugmentationTaskGroup",
    "InputPreparationTaskGroup",
    "ReasoningTaskGroup",
    "PerformanceReportingTaskGroup",
    "ServiceLifecycleTaskGroup",
    "SuperResolutionTaskGroup",
    "TrainingExportTaskGroup",
    "ValidatePayloadTaskGroup",
    "ValidatedOutputTaskGroup",
    "VisualQATaskGroup",
]

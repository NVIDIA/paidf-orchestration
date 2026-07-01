# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Task Groups for SDG Workflow DAGs."""

from dags.shared.task_groups.auto_labeling import AutoLabelingTaskGroup
from dags.shared.task_groups.cosmos import CosmosTaskGroup
from dags.shared.task_groups.input_preparation import InputPreparationTaskGroup
from dags.shared.task_groups.reporting import ReportingTaskGroup
from dags.shared.task_groups.service_lifecycle import ServiceLifecycleTaskGroup
from dags.shared.task_groups.validate_payload import ValidatePayloadTaskGroup
from dags.shared.task_groups.validated_output import ValidatedOutputTaskGroup

__all__ = [
    "CosmosTaskGroup",
    "InputPreparationTaskGroup",
    "AutoLabelingTaskGroup",
    "ReportingTaskGroup",
    "ServiceLifecycleTaskGroup",
    "ValidatePayloadTaskGroup",
    "ValidatedOutputTaskGroup",
]

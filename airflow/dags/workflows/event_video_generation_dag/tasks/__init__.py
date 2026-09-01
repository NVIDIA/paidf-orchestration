# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Event Video Generation DAG-specific tasks."""

from dags.workflows.event_video_generation_dag.tasks.performance_reporting import (
    generate_event_video_generation_performance_report,
)

__all__ = ["generate_event_video_generation_performance_report"]

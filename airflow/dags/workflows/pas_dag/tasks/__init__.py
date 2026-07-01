# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""PAS DAG task callables."""

from dags.workflows.pas_dag.tasks.post_processing import generate_augmented_dataset

__all__ = ["generate_augmented_dataset"]

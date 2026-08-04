# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Image Attribute Augmentation DAG task callables."""

from dags.workflows.image_attribute_augmentation_dag.tasks.post_processing import (
    generate_augmented_dataset,
)

__all__ = ["generate_augmented_dataset"]

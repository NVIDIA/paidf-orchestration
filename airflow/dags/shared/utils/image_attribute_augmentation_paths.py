# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared storage paths for the Image Attribute Augmentation workflow."""

COSMOS_POSTPROCESSING_DIR = "postprocessing"
COSMOS_AUGMENTED_DATA_FILENAME = "augmented_data.json"
COSMOS_AUGMENTED_IMAGES_DIR = "augmented_imgs"


def _augmentation_dir(output_image_path: str) -> str:
    return output_image_path.rstrip("/").rsplit("/", 1)[0]


def get_cosmos_augmentation_suffix(output_image_path: str) -> str:
    """Normalize ``aug_0`` and Cosmos-style ``0`` run directory names to one suffix."""
    run_dir_name = _augmentation_dir(output_image_path).rsplit("/", 1)[-1]
    return run_dir_name[4:] if run_dir_name.startswith("aug_") else run_dir_name


def get_cosmos_augmented_data_path(output_image_path: str) -> str:
    """Return the attribute metadata produced beside one Cosmos augmentation."""
    return (
        f"{_augmentation_dir(output_image_path)}/{COSMOS_POSTPROCESSING_DIR}/"
        f"{COSMOS_AUGMENTED_DATA_FILENAME}"
    )


def get_cosmos_augmented_images_dir(output_image_path: str, seed_image_id: str) -> str:
    """
    Return the image directory produced for one Cosmos postprocessing invocation.

    The postprocessing container names this directory ``{seed}_aug{suffix}`` from the
    augmentation run directory it processed, so the suffix must track that run index.
    """
    return (
        f"{_augmentation_dir(output_image_path)}/{COSMOS_POSTPROCESSING_DIR}/"
        f"{COSMOS_AUGMENTED_IMAGES_DIR}/"
        f"{seed_image_id}_aug{get_cosmos_augmentation_suffix(output_image_path)}"
    )

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Image Attribute Augmentation image pre-processing helpers for pane combination."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowFailException
from multistorageclient.types import PatternType

from dags.shared.utils.msc_utils import download_file, list_directory, upload_file
from dags.shared.utils.video_input_utils import (
    get_relative_storage_path,
    join_storage_base_path_filename,
    normalize_directory_path,
)

# Pillow is imported inside callables, never at module scope, to avoid the DAG processor
# racing the python-deps S3 sync mid-update (SQA: Core 12.3.0 vs Pillow 12.2.0).
if TYPE_CHECKING:
    from PIL import Image

logger = logging.getLogger(__name__)

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tiff", ".webp"}
IMAGE_PATTERNS = [(PatternType.INCLUDE, f"*{ext}") for ext in IMAGE_EXTENSIONS]


def resize_to_height(image: Image.Image, target_height: int = 512) -> Image.Image:
    """Resize image to target height while preserving aspect ratio."""
    from PIL import Image

    width, height = image.size
    aspect_ratio = width / height
    new_width = int(target_height * aspect_ratio)
    return image.resize((new_width, target_height), Image.LANCZOS)


def get_image_files(directory: Path) -> list[Path]:
    """Return supported image files from a directory sorted by name."""
    return [
        file
        for file in sorted(directory.iterdir())
        if file.is_file() and file.suffix.lower() in IMAGE_EXTENSIONS
    ]


def combine_id_images(
    image_paths: list[Path], output_path: Path, metadata_path: Path
) -> None:
    """Combine one-or-more images into a single horizontal strip plus metadata."""
    from PIL import Image

    if not image_paths:
        raise ValueError("Need at least one image to combine")

    resized_images: list[Image.Image] = []
    widths: list[int] = []
    original_names: list[str] = []
    original_resolutions: list[dict[str, int]] = []

    for img_path in image_paths:
        with Image.open(img_path) as opened_img:
            original_resolutions.append(
                {"width": opened_img.width, "height": opened_img.height}
            )
            img = (
                opened_img.convert("RGB")
                if opened_img.mode != "RGB"
                else opened_img.copy()
            )
        resized = resize_to_height(img, target_height=512)
        resized_images.append(resized)
        widths.append(resized.width)
        original_names.append(img_path.name)

    total_width = sum(widths)
    combined_image = Image.new("RGB", (total_width, 512))

    x_offset = 0
    for img in resized_images:
        combined_image.paste(img, (x_offset, 0))
        x_offset += img.width

    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined_image.save(output_path, quality=95)

    metadata = {
        "image_order": original_names,
        "widths": widths,
        "original_resolutions": original_resolutions,
        "total_width": total_width,
        "height": 512,
    }
    metadata_path.write_text(json.dumps(metadata, indent=2))


def _relative_remote_path(remote_path: str, input_path: str) -> str:
    """Return path of a listed object relative to the requested input prefix."""
    relative_path = get_relative_storage_path(remote_path, input_path)
    if relative_path:
        return relative_path

    # Fall back to the final path components. Image Attribute Augmentation image files are expected to
    # live under <person_id>/<image_file>, so keep the last two components.
    parts = remote_path.rstrip("/").split("/")
    if len(parts) >= 2:
        return "/".join(parts[-2:])
    return Path(remote_path).name


def stage_input_images(input_path: str, staging_dir: str) -> str:
    """Download Image Attribute Augmentation image objects into a local directory tree."""
    input_dir = normalize_directory_path(input_path)
    staging_root = Path(staging_dir)
    staging_root.mkdir(parents=True, exist_ok=True)

    downloaded = 0
    for obj in sorted(
        list_directory(input_dir, patterns=IMAGE_PATTERNS),
        key=lambda item: item.key,
    ):
        remote_path = obj.key
        relative_path = _relative_remote_path(remote_path, input_dir)
        local_path = staging_root / relative_path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        download_file(remote_path, str(local_path))
        downloaded += 1

    if downloaded == 0:
        raise AirflowFailException(
            f"No Image Attribute Augmentation image files found under {input_dir}"
        )

    logger.info(
        "Downloaded %d Image Attribute Augmentation image(s) from %s",
        downloaded,
        input_dir,
    )
    return str(staging_root)


def upload_combined_outputs(combined_dir: Path, output_directory: str) -> int:
    """Upload combined Image Attribute Augmentation images and metadata files to storage."""
    output_dir = normalize_directory_path(output_directory)
    uploaded = 0
    for path in sorted(combined_dir.iterdir()):
        if not path.is_file() or path.suffix.lower() not in IMAGE_EXTENSIONS | {
            ".json"
        }:
            continue
        remote_path = join_storage_base_path_filename(
            join_storage_base_path_filename(output_dir, path.stem),
            path.name,
        )
        upload_file(str(path), remote_path)
        uploaded += 1

    if uploaded == 0:
        raise AirflowFailException(
            f"No combined Image Attribute Augmentation outputs found in {combined_dir}"
        )

    logger.info(
        "Uploaded %d Image Attribute Augmentation preprocessing artifact(s) to %s",
        uploaded,
        output_dir,
    )
    return uploaded


def combine_panes(
    run_base: str | Path,
    input_path: str,
    output_directory: str | None = None,
) -> None:
    """Combine Image Attribute Augmentation ID folders into multi-pane images and metadata."""
    from PIL import UnidentifiedImageError

    run_base_path = Path(run_base)
    staging_dir = run_base_path / "staged_imgs"
    output_root = run_base_path / "combined_imgs"
    input_root = Path(
        stage_input_images(input_path=input_path, staging_dir=str(staging_dir))
    )
    if not input_root.exists():
        raise AirflowFailException(f"Input image root does not exist: {input_root}")

    subdirs = sorted([d for d in input_root.iterdir() if d.is_dir()])
    if not subdirs:
        raise AirflowFailException(f"No ID directories found in {input_root}")

    success = 0
    skipped = 0
    for subdir in subdirs:
        try:
            image_files = get_image_files(subdir)
            if not image_files:
                skipped += 1
                continue
            combined_path = output_root / f"{subdir.name}.jpg"
            metadata_path = output_root / f"{subdir.name}.json"
            combine_id_images(image_files, combined_path, metadata_path)
            success += 1
        except (FileNotFoundError, UnidentifiedImageError, ValueError, OSError) as e:
            logger.warning(
                "Skipping %s due to image processing issue: %s", subdir.name, e
            )
            skipped += 1
        except Exception as e:
            logger.exception("Unexpected error processing %s: %s", subdir.name, e)
            skipped += 1

    logger.info("combine_panes done: success=%d skipped=%d", success, skipped)
    if success == 0:
        raise AirflowFailException(
            f"No Image Attribute Augmentation IDs were successfully combined under {input_root}"
        )
    if output_directory:
        upload_combined_outputs(output_root, output_directory)

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Low-level video input utilities for URL parsing and path normalization.

Provides simple helper functions for video input processing:
- URL parsing and validation
- Directory path normalization
- Last path segment (filename) from a storage key or URL path
- Format detection utilities

These are low-level utilities used by task groups for video processing.
High-level orchestration logic belongs in DAG-specific input preparation callables.
"""

import os

from dags.shared.utils.msc_utils import VIDEO_EXTENSIONS, convert_to_msc_url


def join_storage_base_path_filename(base_path: str, filename: str) -> str:
    """
    Append *filename* to a storage directory URL *base_path*.

    *base_path* is typically from ``prepare_input`` and already ends with ``/``.
    Avoids ``rstrip("/")`` on values like ``s3://``, which would break the scheme.
    """
    if base_path.endswith("/"):
        return f"{base_path}{filename}"
    return f"{base_path}/{filename}"


def normalize_directory_path(path: str) -> str:
    """
    Normalize a directory path to ensure it ends with '/'.

    Args:
        path: Directory path (any URL format)

    Returns:
        str: Path guaranteed to end with '/'

    Examples:
        >>> normalize_directory_path("s3://bucket/videos")
        "s3://bucket/videos/"
        >>> normalize_directory_path("s3://bucket/videos/")  # Already normalized
        "s3://bucket/videos/"
    """
    if not path.endswith("/"):
        path += "/"
    return path


def get_relative_storage_path(path: str, base_directory: str) -> str | None:
    """
    Return object path relative to *base_directory* when *path* is under that prefix.

    Both inputs are normalized to a storage-comparable representation first.
    Returns None when prefix relationship cannot be established.
    """
    normalized_path = convert_to_msc_url(path).rstrip("/")
    normalized_base = convert_to_msc_url(base_directory).rstrip("/")
    normalized_base_dir = normalize_directory_path(normalized_base)
    if normalized_path.startswith(normalized_base_dir):
        rel = normalized_path[len(normalized_base_dir) :]
        return rel.lstrip("/")
    return None


def derive_video_key(video_path: str, input_path: str) -> str:
    """
    Build a stable, flat video key for outputs from input location + object path.

    The base key is the input object path without the input directory prefix.
    For single-file inputs, the base key is the filename.
    The final returned key is flattened by replacing '/' with '__' so output
    artifacts are written under a single directory segment per video.
    """
    normalized_video = convert_to_msc_url(video_path).rstrip("/")
    normalized_input = convert_to_msc_url(input_path).rstrip("/")

    def _flatten(key: str) -> str:
        return key.replace("/", "__")

    # Single-file input
    if normalized_video == normalized_input:
        return _flatten(extract_filename_from_path(normalized_video))

    # Directory input (or file input where parent is the configured prefix)
    rel_from_input = get_relative_storage_path(normalized_video, normalized_input)
    if rel_from_input:
        return _flatten(rel_from_input)

    parent = normalized_input.rsplit("/", 1)[0] if "/" in normalized_input else normalized_input
    rel_from_parent = get_relative_storage_path(normalized_video, parent)
    if rel_from_parent:
        return _flatten(rel_from_parent)

    base_key = extract_filename_from_path(normalized_video)
    return _flatten(base_key)


def extract_filename_from_path(key: str) -> str:
    """Return the last path segment (filename) from a URL path."""
    return key.rsplit("/", 1)[-1]


def extract_format_from_path(input_path: str) -> str:
    """
    Extract likely input format from input path characteristics.

    Args:
        input_path: Input path to analyze

    Returns:
        str: Suggested format ('file', 'folder', or 'unknown')

    Examples:
        >>> extract_format_from_path("s3://bucket/video.mp4")
        "file"
        >>> extract_format_from_path("s3://bucket/videos/")
        "folder"
    """
    if input_path.lower().endswith(VIDEO_EXTENSIONS):
        return "file"
    elif input_path.endswith("/") or "." not in input_path.split("/")[-1]:
        return "folder"
    else:
        return "unknown"


def normalize_video_name(video_name: str) -> str:
    """
    Get the base name of a video name by stripping all extensions from the end.

    Args:
        video_name: Video name to get the base name from

    Returns:
        str: Base name of the video name
    """
    # Strip repeated extensions (".xyz") from end until none remain
    while True:
        parts = video_name.rsplit(".", 1)
        if len(parts) == 2 and parts[1]:
            video_name = parts[0]
        else:
            break
    return video_name


def get_subdirectory_name(path: str) -> str | None:
    """
    Get the subdirectory name from a path.

    Args:
        path: Path to get the subdirectory name from

    Returns:
        str: Subdirectory name
    """
    obj_key_parts = os.path.normpath(path).split("/")
    if not obj_key_parts or len(obj_key_parts) < 2:
        return None
    return obj_key_parts[-1]

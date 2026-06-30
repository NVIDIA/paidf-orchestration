# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Multistorageclient (MSC) utilities for cloud-agnostic storage operations.

Provides utilities for configuring MSC, URL conversions between different storage
formats (HTTPS, s3://, gs://, msc://), and directory listing operations across
all cloud backends supported by MSC (S3, GCS, Azure, etc.).

Supports multiple bucket profiles - use bucket name directly to get the correct profile.
"""

import ast
import json
import logging
import os
import tempfile
from functools import wraps
from typing import Iterator, Optional, Sequence, Tuple, Type, TypeVar

import multistorageclient as msc
import multistorageclient.shortcuts as msc_shortcuts
from airflow.exceptions import AirflowFailException
from airflow.sdk import Variable
from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.types import ObjectMetadata, PatternType
from pydantic import ValidationError

# Supported video file extensions
VIDEO_EXTENSIONS = (".mp4", ".avi", ".mov", ".mkv", ".webm")

# Patterns for list_directory to filter by video extensions (for msc.list)
VIDEO_PATTERNS: Sequence[Tuple[PatternType, str]] = [
    (PatternType.INCLUDE, "*" + ext) for ext in VIDEO_EXTENSIONS
]

# Pattern to list Cosmos output.mp4 files
COSMOS_OUTPUT_MP4_PATTERNS: Sequence[Tuple[PatternType, str]] = [
    (PatternType.INCLUDE, "*output.mp4"),
]

T = TypeVar("T")


def read_file_as_dict(
    path: str,
    model_class: Optional[Type[T]] = None,
) -> T | dict | None:
    """
    Load and optionally validate metadata from a JSON file in storage.

    Args:
        msc_client: Storage client to use for access.
        path: Path to the file (bucket-relative or key).
        model_class: Pydantic model class to validate the JSON (e.g. CosmosMetadata). If None, returns the raw JSON dict.

    Returns:
        Validated model instance, the raw JSON dict if model_class is None, or None if the path is empty (no file).
    """
    if msc.is_empty(path):
        logging.warning("No metadata file found at path: %s. No metadata will be processed.", path)
        return None
    try:
        with msc.open(path, "r") as f:
            data = json.load(f)
        if model_class is None:
            return data
        else:
            return model_class.model_validate(data)
    except ValidationError as e:
        raise AirflowFailException(f"Validation error for metadata file at path {path}: {e}") from e
    except json.JSONDecodeError as e:
        raise AirflowFailException(f"Invalid JSON in metadata file at path {path}: {e}") from e
    except Exception as e:
        raise AirflowFailException(f"Error loading metadata from file at path {path}: {e}") from e


def get_msc_config() -> dict:
    """Get the MSC configuration dictionary from Airflow variables."""
    msc_config_str = Variable.get("multistorageclient_configuration_secret")
    return ast.literal_eval(msc_config_str)


def _get_path_mapping() -> dict:
    """
    Return path_mapping from MSC config (keys ending with / for prefix matching).

    Reads config from Airflow Variable via get_msc_config().

    Returns:
        Dict mapping URL prefixes (e.g. https://bucket..., s3://bucket/) to msc://profile/.
    """
    config_dict = get_msc_config()
    return {k: v for k, v in config_dict.get("path_mapping", {}).items() if k.endswith("/")}


def convert_to_msc_url(url: str) -> str:
    """
    Convert any storage URL (HTTPS, s3://, gs://, etc.) to msc:// using path_mapping.

    Single entry point for "input URL → msc://" so all path_mapping logic lives here.
    Reads path_mapping from Airflow Variable (get_msc_config()). If the URL is already
    msc://, it is returned unchanged. If no mapping matches, the original URL is
    returned (callers that require msc:// should check and raise).

    Args:
        url: URL in any supported form (https://..., s3://..., msc://..., etc.)

    Returns:
        msc://profile/path when a path_mapping prefix matches, otherwise url unchanged.

    Examples:
        >>> convert_to_msc_url("https://bucket.s3.region.amazonaws.com/data/file.mp4")
        "msc://bucket/data/file.mp4"
        >>> convert_to_msc_url("s3://bucket/path/")
        "msc://bucket/path/"
        >>> convert_to_msc_url("msc://bucket/path")
        "msc://bucket/path"
    """
    if url.startswith("msc://"):
        return url

    path_mapping = _get_path_mapping()
    best_src = max(
        (path for path in path_mapping if url.startswith(path)),
        key=len,
        default=None,
    )

    if best_src:
        dest = path_mapping[best_src]
        resolved = dest + url[len(best_src) :]
        return resolved

    logging.warning(f"No path mapping found for URL: {url}")
    return url


def ensure_msc_configured(f):
    """
    Decorator that ensures multistorageclient is configured before invoking the function.

    Checks if MSC_CONFIG is set and valid; otherwise fetches config from Airflow Variables
    and initializes the MSC client. Needed for PythonOperator pods that don't have the
    secret injected like NVCF tasks do.
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        _ensure_msc_configured()
        return f(*args, **kwargs)

    return wrapper


def _ensure_msc_configured() -> None:
    """Ensure MSC is configured (config file exists and is non-empty, or init from Variable)."""
    msc_config_path = os.getenv("MSC_CONFIG")

    if msc_config_path and os.path.isfile(msc_config_path):
        if os.path.getsize(msc_config_path) > 0:
            return

    msc_config = Variable.get("multistorageclient_configuration_secret", default=None)
    if not msc_config:
        raise AirflowFailException("MSC config is not set")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp_file:
        tmp_file.write(msc_config)
        msc_config_path = tmp_file.name
    os.environ["MSC_CONFIG"] = msc_config_path
    msc_shortcuts._reinitialize_after_fork()


def convert_msc_to_storage_url(url: str) -> str:
    """
    Convert MSC URL (msc://) to its underlying storage format (s3://, gs://, etc.).

    MSC returns URLs in msc:// format (e.g., msc://bucket/path/file.mp4),
    but some services (like cosmos) expect the original storage format.

    This function uses MSC's resolve_storage_client to determine the underlying
    storage provider and reconstructs the appropriate URL format.

    Args:
        url: URL in msc:// format

    Returns:
        URL converted to underlying storage format (s3://, gs://, etc.),
        or original URL if not msc://

    Examples:
        >>> convert_msc_to_storage_url("msc://bucket/path/file.mp4")
        "s3://bucket/path/file.mp4"  # If MSC profile maps to S3
        >>> convert_msc_to_storage_url("s3://bucket/path/file.mp4")  # Already s3://, unchanged
        "s3://bucket/path/file.mp4"
    """
    if not url.startswith("msc://"):
        return url

    try:
        # Use MSC to resolve the storage client and determine the provider type
        client, _ = msc_shortcuts.resolve_storage_client(url)

        # Get the storage provider to determine the protocol
        if hasattr(client, "_storage_provider"):
            provider = client._storage_provider
            provider_class = provider.__class__.__name__

            # Extract bucket/path from msc:// URL
            path = url[6:]  # Remove "msc://"

            # Map provider types to URL protocols
            if "S3" in provider_class:
                return f"s3://{path}"
            elif "GCS" in provider_class or "Google" in provider_class:
                return f"gs://{path}"
            elif "Azure" in provider_class:
                return f"az://{path}"
            elif "AIS" in provider_class:
                return f"ais://{path}"
            # Fallback: assume s3:// for unknown providers (most common case in current deployments)
            else:
                logging.warning(f"Unknown MSC provider class {provider_class}, assuming S3")
                return f"s3://{path}"
        else:
            # Fallback: if we can't determine provider, assume s3://
            logging.warning(f"Cannot determine provider for {url}, assuming S3")
            path = url[6:]
            return f"s3://{path}"
    except Exception:
        # If resolution fails, fallback to simple conversion (assume s3://)
        # This handles cases where MSC config isn't available during testing
        logging.warning(f"MSC resolution failed for {url}, assuming S3")
        path = url[6:]
        return f"s3://{path}"


@ensure_msc_configured
def list_directory(
    directory_url: str,
    max_files: Optional[int] = None,
    patterns: Optional[Sequence[Tuple[PatternType, str]]] = None,
    include_directories: bool = False,
) -> Iterator[ObjectMetadata]:
    """
    List objects in a directory using multistorageclient (MSC).

    Supports various storage backends (S3, GCS, etc.) through MSC's unified interface.
    Pass patterns directly to msc.list() for filtering (e.g. VIDEO_PATTERNS for video files).

    Args:
        directory_url: URL to the directory (supports HTTPS, s3://, gs://, msc://, etc.).
            Normalized to end with / for listing.
        max_files: Optional limit on number of objects to yield.
        patterns: Optional list of (PatternType, pattern_str) passed to msc.list() as-is.
            e.g. [(PatternType.INCLUDE, "*.mp4")] or VIDEO_PATTERNS.
        include_directories: If True, include directory prefixes in the result.

    Yields:
        ObjectMetadata for each object (use .key for the object key/URL).

    Examples:
        >>> for obj in list_directory("s3://bucket/videos/"):
        ...     print(obj.key)
        >>> list(list_directory("s3://bucket/videos/", patterns=VIDEO_PATTERNS, max_files=10))
    """
    try:
        directory_url = directory_url.rstrip("/") + "/"
        count = 0
        for obj in msc.list(
            directory_url, patterns=patterns, include_directories=include_directories
        ):
            yield obj
            count += 1
            if max_files is not None and max_files > 0 and count >= max_files:
                break
    except Exception as e:
        raise AirflowFailException(f"Failed to list directory {directory_url}: {e}") from e


@ensure_msc_configured
def is_file(file_path: str) -> bool:
    """
    Check if the file exists at the given path using multistorageclient (MSC).

    Args:
        file_path: Path to the file (s3://, gs://, msc://, https://, etc.)

    Returns:
        True if the file exists, False otherwise.

    Raises:
        AirflowFailException: If there is an error during the MSC check.
    """
    try:
        return msc.is_file(file_path)
    except Exception as e:
        raise AirflowFailException(
            f"Failed to check if file exists at path {file_path}: {e}"
        ) from e


@ensure_msc_configured
def is_directory_empty(directory_url: str) -> bool:
    """
    Return True if the directory has no objects (files or prefixes); False otherwise.

    Uses MSC to list the directory and checks for the presence of at least one item.
    Does not fetch the full listing, so it is efficient for large buckets.

    Args:
        directory_url: URL to the directory (supports HTTPS, s3://, gs://, msc://, etc.).

    Returns:
        True if the directory is empty, False if it contains at least one object.

    Raises:
        AirflowFailException: If listing the directory fails.
    """
    return msc.is_empty(directory_url)


@ensure_msc_configured
def delete_path(path_url: str, recursive: bool = False) -> None:
    """
    Delete a file or folder at the given path using MSC.

    Args:
        path_url: URL or path to the file or folder (s3://, gs://, msc://, etc.).
        recursive: If True, delete the folder and all of its contents. Use True for
            directories; False (default) for a single object.

    Raises:
        AirflowFailException: If the delete operation fails.
    """
    try:
        msc.delete(path_url, recursive=recursive)
    except Exception as e:
        raise AirflowFailException(f"Failed to delete {path_url}: {e}") from e


@ensure_msc_configured
def get_multistorage_client(bucket_name: str) -> StorageClient:
    """
    Get a Multistorage client for the specified bucket.

    Args:
        bucket_name: The bucket name (which is the profile name in MSC config)

    Returns:
        StorageClient configured for the specified bucket

    Examples:
        >>> client = get_multistorage_client("my-input-bucket")
        >>> files = client.list(path="data/")

        >>> client = get_multistorage_client("my-output-bucket")
        >>> client.write(path="results/output.txt", body="data")

    Raises:
        AirflowFailException: If the bucket name is not found in MSC config
    """
    config_dict = get_msc_config()
    available_profiles = list(config_dict.get("profiles", {}).keys())

    if bucket_name not in available_profiles:
        raise AirflowFailException(
            f"Bucket '{bucket_name}' not found in MSC config. "
            f"Available buckets: {available_profiles}"
        )

    config = StorageClientConfig.from_dict(config_dict=config_dict, profile=bucket_name)
    return StorageClient(config=config)


def get_bucket_name_from_url(url: str) -> str:
    """
    Extract the bucket name (profile name) from a URL using path_mapping.

    Converts the URL to msc:// via convert_to_msc_url (reads config from Airflow
    Variable), then returns the first path segment (profile/bucket name).

    Args:
        url: URL (supports s3://, https://, msc://, etc.)

    Returns:
        Bucket name (profile name) that matches the URL

    Example:
        >>> get_bucket_name_from_url("s3://my-bucket/path/to/file")
        "my-bucket"
        >>> get_bucket_name_from_url("https://my-bucket.s3.us-west-2.amazonaws.com/path")
        "my-bucket"
        >>> get_bucket_name_from_url("msc://my-bucket/path/to/file")
        "my-bucket"

    Raises:
        AirflowFailException: If no path_mapping matches the URL.
    """
    msc_url = convert_to_msc_url(url)
    if not msc_url.startswith("msc://"):
        raise AirflowFailException(f"No path mapping found for URL: {url}")
    msc_path = msc_url[len("msc://") :]
    return msc_path.lstrip("/").split("/")[0]


def get_multistorage_file_path(input_path: str) -> str:
    """
    Get the object path (key) from a storage URL using path_mapping.

    Converts the URL to msc:// via convert_to_msc_url (reads config from Airflow
    Variable), then returns the path after the profile (e.g. msc://bucket/foo/bar → foo/bar).

    Args:
        input_path: URL or path (s3://..., https://..., msc://...).

    Returns:
        Object path (key) within the bucket.

    Raises:
        AirflowFailException: If no path_mapping matches the input path.
    """
    msc_url = convert_to_msc_url(input_path)
    if not msc_url.startswith("msc://"):
        raise AirflowFailException(f"No path mapping found for input filepath {input_path}")
    msc_path = msc_url[len("msc://") :]
    parts = msc_path.split("/", 1)
    object_path = parts[1] if len(parts) > 1 else ""
    logging.info(f"Resolved to msc path: {msc_url} → object path: {object_path}")
    return object_path


def _copy_via_client(source_path: str, destination_path: str) -> None:
    """
    Copy a single file using StorageClient.copy (same-profile only).

    multistorageclient does not expose a top-level copy(); use the client from
    resolve_storage_client and client.copy(src_path, dest_path).
    """
    source_client, source_resolved = msc_shortcuts.resolve_storage_client(source_path)
    target_client, target_resolved = msc_shortcuts.resolve_storage_client(destination_path)
    if source_client.profile != target_client.profile:
        raise AirflowFailException(
            f"Cross-profile copy not supported (source profile '{source_client.profile}' "
            f"!= target profile '{target_client.profile}'). Use sync or download+upload for cross-profile."
        )
    source_client.copy(source_resolved, target_resolved)


@ensure_msc_configured
def copy_directory(source_path: str, destination_path: str, extension: str | None = None):
    """
    Recursively copies a directory (prefix) from a source path to a destination path
    using the multi-storage-client.

    If 'extension' is specified, only files with that extension will be copied.
    """
    for source_object_name in msc.list(
        source_path, patterns=[(PatternType.INCLUDE, f"*{extension}")] if extension else None
    ):
        dest_object_name = source_object_name.key.replace(source_path, destination_path, 1)

        try:
            _copy_via_client(source_object_name.key, dest_object_name)
        except Exception as e:
            raise AirflowFailException(
                f"Failed to copy {source_object_name.key} to {dest_object_name}: {e}"
            ) from e


@ensure_msc_configured
def copy_file(source_path: str, destination_path: str) -> None:
    """
    Copy a single file from source_path to destination_path using the multi-storage-client.

    Args:
        source_path: The source file location (URL or MSC path).
        destination_path: The destination file location (URL or MSC path).

    Raises:
        AirflowFailException: If the copy fails.
    """
    try:
        _copy_via_client(source_path, destination_path)
    except Exception as e:
        raise AirflowFailException(
            f"Failed to copy file from {source_path} to {destination_path}: {e}"
        ) from e


@ensure_msc_configured
def write_file_to_directory(file_path: str, body: bytes) -> None:
    """
    Write a file (bytes) to a storage path using MSC.

    Args:
        file_path: Full storage URL or msc:// path (e.g. s3://bucket/key, msc://profile/key).
        body: File contents as bytes.

    Raises:
        AirflowFailException: If the write fails.
    """
    try:
        msc.write(file_path, body)
    except Exception as e:
        raise AirflowFailException(f"Failed to write file to {file_path}: {e}") from e


@ensure_msc_configured
def download_file(remote_path: str, local_path: str) -> None:
    """
    Download a file from storage using the MultiStorageClient (MSC).

    Args:
        remote_path: The storage file location (URL or MSC path).
        local_path: The local file location.

    Returns:
        None

    Raises:
        AirflowFailException: If the download fails.
    """
    try:
        msc.download_file(remote_path, local_path)
    except Exception as e:
        raise AirflowFailException(f"Failed to download file from {remote_path}: {e}") from e


@ensure_msc_configured
def upload_file(local_path: str, remote_path: str) -> None:
    """
    Upload a file from local path to storage using the MultiStorageClient (MSC).

    Args:
        local_path: The local file location.
        remote_path: The storage file location (URL or MSC path).

    Raises:
        AirflowFailException: If the upload fails.
    """
    try:
        msc.upload_file(remote_path, local_path)
    except Exception as e:
        raise AirflowFailException(
            f"Failed to upload file from {local_path} to {remote_path}: {e}"
        ) from e

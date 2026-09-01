# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared model-cache PVC mount helpers for K8s operators."""

from __future__ import annotations

from typing import Optional, Tuple

from kubernetes import client

MODEL_MOUNT_PATH = "/config/models"
MODEL_VOLUME_NAME = "model-cache"
DSHM_VOLUME_NAME = "dshm"
DSHM_SIZE_LIMIT = "2Gi"
DSHM_MOUNT_PATH = "/dev/shm"


def build_model_volume(pvc_name: str) -> client.V1Volume:
    return client.V1Volume(
        name=MODEL_VOLUME_NAME,
        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
            claim_name=pvc_name,
        ),
    )


def build_model_volume_mount() -> client.V1VolumeMount:
    return client.V1VolumeMount(
        name=MODEL_VOLUME_NAME,
        mount_path=MODEL_MOUNT_PATH,
    )


def build_dshm_volume(size_limit: str = DSHM_SIZE_LIMIT) -> client.V1Volume:
    """Memory-backed volume for ``/dev/shm`` (vLLM multiprocessing needs more than 64Mi default)."""
    return client.V1Volume(
        name=DSHM_VOLUME_NAME,
        empty_dir=client.V1EmptyDirVolumeSource(
            medium="Memory",
            size_limit=size_limit,
        ),
    )


def build_dshm_volume_mount() -> client.V1VolumeMount:
    return client.V1VolumeMount(
        name=DSHM_VOLUME_NAME,
        mount_path=DSHM_MOUNT_PATH,
    )


def build_model_mounts(
    model_cache_pvc: Optional[str],
) -> Tuple[list[client.V1Volume], list[client.V1VolumeMount]]:
    """Build PVC volume and mount at ``/config/models`` when a claim name is provided."""
    if not model_cache_pvc:
        return [], []

    return [build_model_volume(model_cache_pvc)], [build_model_volume_mount()]


def build_inference_pod_volumes(
    *,
    gpu: bool = False,
    model_cache_pvc: Optional[str] = None,
    shm_size: Optional[str] = None,
) -> Tuple[list[client.V1Volume], list[client.V1VolumeMount]]:
    """Build pod volumes/mounts for GPU inference services.

    ``shm_size`` overrides the default ``/dev/shm`` size limit. Some NIMs stage
    large intermediate artifacts there (e.g. Cosmos Transfer writes per-request
    temp dirs under ``/dev/shm`` and needs tens of GiB) and fail with
    ``[Errno 28] No space left on device`` at the default.
    """
    volumes: list[client.V1Volume] = []
    mounts: list[client.V1VolumeMount] = []

    if gpu:
        volumes.append(build_dshm_volume(shm_size or DSHM_SIZE_LIMIT))
        mounts.append(build_dshm_volume_mount())

    if model_cache_pvc:
        model_volumes, model_mounts = build_model_mounts(model_cache_pvc)
        volumes.extend(model_volumes)
        mounts.extend(model_mounts)

    return volumes, mounts

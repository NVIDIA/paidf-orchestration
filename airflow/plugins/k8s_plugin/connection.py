# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Resolve Kubernetes connection / cluster-context for k8s plugin operators."""

from __future__ import annotations

from typing import Optional, Tuple

DEFAULT_KUBERNETES_CONN_ID = "kubernetes_remote"


def resolve_kubernetes_backend(
    *,
    context: Optional[str] = None,
    kubernetes_conn_id: Optional[str] = None,
    in_cluster: Optional[bool] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Resolve ``(kubernetes_conn_id, cluster_context)`` for a job.

    ``context`` selects a named context inside the kubeconfig stored on the
    Airflow Kubernetes connection (``kubectl`` ``current-context`` when omitted).
    Returns ``(None, None)`` when running in-cluster so callers skip remote wiring.
    """
    if in_cluster is True:
        return None, None
    return kubernetes_conn_id, context

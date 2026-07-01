# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Adapter operator that runs a container as a K8s Pod on a (possibly remote) cluster.

Wraps KubernetesPodOperator, translating manifest-format arguments (container_args,
container_environment_variables, secrets) into KubernetesPodOperator parameters.
When ``model_cache_pvc`` is set, the Helm-provisioned PVC is mounted at ``/config/models``.
"""

from __future__ import annotations

import shlex
from typing import Any, List, Optional, Tuple

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes import client

from k8s_plugin.model_cache import build_model_mounts


class K8sTaskOperator(KubernetesPodOperator):
    """
    Runs a container as a Kubernetes Pod, with support for remote clusters.

    When ``model_cache_pvc`` is set, mounts the claim at ``/config/models``.
    Model downloads are handled by the container (via ``HF_TOKEN``, ``HF_HOME``, etc.).
    """

    def __init__(
        self,
        *,
        name: str,
        container_image: str,
        container_command: Optional[str] = None,
        container_args: Optional[str] = None,
        container_environment_variables: Optional[List[str]] = None,
        secrets: Optional[List[str]] = None,
        gpu: Optional[str] = None,
        namespace: str = "default",
        kubernetes_conn_id: Optional[str] = None,
        in_cluster: Optional[bool] = None,
        image_pull_secrets: Optional[List[str]] = None,
        model_cache_pvc: Optional[str] = None,
        startup_timeout_seconds: int = 900,
        # Absorbed silently -- NVCF-specific fields that may appear in manifest overrides
        instance_type: Optional[str] = None,
        backend: Optional[str] = None,
        clusters: Optional[List[str]] = None,
        nvcf_conn_id: Optional[str] = None,
        poll_interval_seconds: Optional[int] = None,
        max_runtime_duration_iso: Optional[str] = None,
        max_queued_duration_iso: Optional[str] = None,
        termination_grace_period_iso: Optional[str] = None,
        result_handling_strategy: Optional[str] = None,
        max_request_concurrency: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        cmds, arguments = _parse_container_command_and_args(
            container_args,
            container_command,
        )
        env_vars = _parse_env_list(container_environment_variables)
        env_vars.extend(_parse_env_list(secrets))

        container_resources = None
        if gpu:
            container_resources = client.V1ResourceRequirements(
                limits={"nvidia.com/gpu": "1"},
                requests={"nvidia.com/gpu": "1"},
            )

        pull_secrets = None
        if image_pull_secrets:
            pull_secrets = [client.V1LocalObjectReference(name=s) for s in image_pull_secrets]

        volumes, volume_mounts = build_model_mounts(model_cache_pvc)

        kpo_kwargs: dict[str, Any] = {}
        if kubernetes_conn_id and in_cluster is not True:
            kpo_kwargs["kubernetes_conn_id"] = kubernetes_conn_id
        if in_cluster is not None:
            kpo_kwargs["in_cluster"] = in_cluster

        super().__init__(
            image=container_image,
            name=name,
            namespace=namespace,
            cmds=cmds,
            arguments=arguments,
            env_vars=env_vars,
            container_resources=container_resources,
            volumes=volumes or None,
            volume_mounts=volume_mounts or None,
            image_pull_secrets=pull_secrets,
            random_name_suffix=True,
            on_finish_action="delete_pod",
            get_logs=True,
            log_pod_spec_on_failure=False,
            startup_timeout_seconds=startup_timeout_seconds,
            is_delete_operator_pod=True,
            labels={"managed-by": "k8s-task-operator"},
            **kpo_kwargs,
            **kwargs,
        )

        # Keep references for manifest compatibility
        self.container_command = container_command
        self.container_args = container_args
        self.container_image = container_image
        self.gpu = gpu
        self.model_cache_pvc = model_cache_pvc


def _parse_env_list(env_list: Optional[List[str]]) -> list[client.V1EnvVar]:
    """Convert ["KEY:VALUE", ...] strings to V1EnvVar objects."""
    if not env_list:
        return []
    result = []
    for item in env_list:
        if ":" in item:
            key, value = item.split(":", 1)
            result.append(client.V1EnvVar(name=key, value=value))
    return result


def _parse_container_command_and_args(
    container_args: Optional[str],
    container_command: Optional[str] = None,
) -> Tuple[Optional[List[str]], Optional[List[str]]]:
    """
    Convert manifest command/args strings to Kubernetes ``command`` + ``args``.

    - If ``container_command`` is set, use it as the command and treat all
      ``container_args`` tokens as args.
    - If the first token starts with "-", treat the full list as args and keep
      image entrypoint untouched.
    - Otherwise treat first token as executable and the rest as args.
    """
    if container_command:
        command = shlex.split(container_command)
        args = shlex.split(container_args) if container_args else None
        return command or None, args or None

    if not container_args:
        return None, None

    tokens = shlex.split(container_args)
    if not tokens:
        return None, None

    if tokens[0].startswith("-"):
        return None, tokens

    return [tokens[0]], tokens[1:] or None

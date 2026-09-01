# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Adapter operator that runs a container as a K8s Pod on a (possibly remote) cluster.

Wraps KubernetesPodOperator, translating manifest-format arguments (container_args,
container_environment_variables, secrets) into KubernetesPodOperator parameters.
When ``model_cache_pvc`` is set, the Helm-provisioned PVC is mounted at ``/config/models``.
"""

from __future__ import annotations

import shlex
from functools import cached_property
from typing import Any, Dict, List, Mapping, Optional, Tuple

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager
from kubernetes import client

from k8s_plugin.connection import resolve_kubernetes_backend
from k8s_plugin.model_cache import build_model_mounts

_TOLERATION_FIELDS = ("key", "operator", "value", "effect", "tolerationSeconds")
_VOLUME_MOUNT_FIELDS = {
    "mountPath": "mount_path",
    "readOnly": "read_only",
    "subPath": "sub_path",
    "subPathExpr": "sub_path_expr",
    "mountPropagation": "mount_propagation",
}


class ClockSkewSafePodManager(PodManager):
    """Prevent cross-cluster clock skew from producing invalid log requests."""

    def read_pod_logs(
        self,
        pod: client.V1Pod,
        container_name: str,
        tail_lines: Optional[int] = None,
        timestamps: bool = False,
        since_seconds: Optional[int] = None,
        follow: bool = True,
        post_termination_timeout: int = 120,
        **kwargs: Any,
    ) -> Any:
        if since_seconds is not None and since_seconds < 1:
            self.log.warning(
                "Clamping invalid pod log since_seconds=%s to 1 (possible cross-cluster clock skew)",
                since_seconds,
            )
            since_seconds = 1
        return super().read_pod_logs(
            pod=pod,
            container_name=container_name,
            tail_lines=tail_lines,
            timestamps=timestamps,
            since_seconds=since_seconds,
            follow=follow,
            post_termination_timeout=post_termination_timeout,
            **kwargs,
        )


class K8sTaskOperator(KubernetesPodOperator):
    """
    Runs a container as a Kubernetes Pod, with support for remote clusters.

    When ``model_cache_pvc`` is set, mounts the claim at ``/config/models``.
    Model downloads are handled by the container (via ``HF_TOKEN``, ``HF_HOME``, etc.).

    Set ``volumes`` for extra pod volumes. Each entry is a Kubernetes volume
    spec plus ``mountPath``; those become container volumeMounts on the pod.

    Set ``context`` to select a named context from ``k8sCtx`` (kubectl kubeconfig).
    If omitted, the kubeconfig ``current-context`` is used.
    """

    template_fields = (*KubernetesPodOperator.template_fields, "context", "node_selector")

    @cached_property
    def pod_manager(self) -> PodManager:
        return ClockSkewSafePodManager(kube_client=self.client, callbacks=self.callbacks)

    def __init__(
        self,
        *,
        name: str,
        container_image: str,
        container_command: Optional[str] = None,
        container_args: Optional[str] = None,
        container_environment_variables: Optional[List[str]] = None,
        runtime_environment_variables: Optional[List[str]] = None,
        secrets: Optional[List[str]] = None,
        gpu: Optional[str] = None,
        node_selector: Optional[Mapping[str, Any]] = None,
        tolerations: Optional[List[Mapping[str, Any]]] = None,
        volumes: Optional[List[Mapping[str, Any]]] = None,
        namespace: str = "default",
        kubernetes_conn_id: Optional[str] = None,
        context: Optional[str] = None,
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
        environment_by_name = {
            item.name: item for item in _parse_env_list(container_environment_variables)
        }
        environment_by_name.update(
            {item.name: item for item in _parse_env_list(runtime_environment_variables)}
        )
        environment_by_name.update({item.name: item for item in _parse_env_list(secrets)})
        env_vars = list(environment_by_name.values())

        container_resources = None
        if gpu:
            container_resources = client.V1ResourceRequirements(
                limits={"nvidia.com/gpu": "1"},
                requests={"nvidia.com/gpu": "1"},
            )

        pull_secrets = None
        if image_pull_secrets:
            pull_secrets = [client.V1LocalObjectReference(name=s) for s in image_pull_secrets]

        extra_volumes, extra_mounts = normalize_volumes(volumes)
        cache_volumes, cache_mounts = build_model_mounts(model_cache_pvc)
        volumes = cache_volumes + extra_volumes
        volume_mounts = cache_mounts + extra_mounts
        node_selector = normalize_node_selector(node_selector)
        normalized_tolerations = normalize_tolerations(tolerations)

        resolved_conn_id, cluster_context = resolve_kubernetes_backend(
            context=context,
            kubernetes_conn_id=kubernetes_conn_id,
            in_cluster=in_cluster,
        )

        kpo_kwargs: dict[str, Any] = {}
        if resolved_conn_id:
            kpo_kwargs["kubernetes_conn_id"] = resolved_conn_id
        if cluster_context:
            kpo_kwargs["cluster_context"] = cluster_context
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
            # Keep failed pods so their exit code and logs survive for `kubectl logs`.
            # KPO stops following logs the instant the container dies, so anything the
            # process writes on its way down is otherwise lost with the deleted pod.
            on_finish_action="delete_succeeded_pod",
            get_logs=True,
            log_pod_spec_on_failure=False,
            # Emit container termination reason/exit code and pod warning events on
            # failure; without this KPO raises only a generic "returned a failure".
            log_events_on_failure=True,
            startup_timeout_seconds=startup_timeout_seconds,
            labels={"managed-by": "k8s-task-operator"},
            node_selector=node_selector,
            tolerations=normalized_tolerations,
            **kpo_kwargs,
            **kwargs,
        )

        # Keep references for manifest compatibility
        self.container_command = container_command
        self.container_args = container_args
        self.container_image = container_image
        self.gpu = gpu
        self.model_cache_pvc = model_cache_pvc
        self.context = context
        self.node_selector = node_selector
        self.runtime_environment_variables = runtime_environment_variables


def normalize_node_selector(
    node_selector: Optional[Mapping[str, Any]],
) -> Optional[Dict[str, str]]:
    """Validate a Kubernetes nodeSelector mapping from a manifest or operator arg."""
    if node_selector is None:
        return None
    if not isinstance(node_selector, Mapping) or isinstance(node_selector, (str, bytes)):
        raise ValueError("node_selector must be a mapping of label key to value")
    if not node_selector:
        return None
    normalized: Dict[str, str] = {}
    for key, value in node_selector.items():
        if not isinstance(key, str) or not key:
            raise ValueError("node_selector keys must be non-empty strings")
        if not isinstance(value, str) or not value:
            raise ValueError(f"node_selector[{key!r}] values must be non-empty strings")
        normalized[key] = value
    return normalized


def normalize_tolerations(
    tolerations: Optional[List[Mapping[str, Any]]],
) -> Optional[List[Dict[str, Any]]]:
    """Validate Kubernetes tolerations from a manifest or operator arg.

    Unknown keys are rejected because the Kubernetes client silently drops them,
    which would leave a pod unschedulable with no indication why.
    """
    if tolerations is None:
        return None
    if isinstance(tolerations, (str, bytes, Mapping)):
        raise ValueError("tolerations must be a list of toleration mappings")
    normalized: List[Dict[str, Any]] = []
    for entry in tolerations:
        if not isinstance(entry, Mapping):
            raise ValueError("each toleration must be a mapping")
        unknown = set(entry) - set(_TOLERATION_FIELDS)
        if unknown:
            raise ValueError(
                f"unsupported toleration field(s): {', '.join(sorted(unknown))}; "
                f"expected any of {', '.join(_TOLERATION_FIELDS)}"
            )
        toleration: Dict[str, Any] = {}
        for field, value in entry.items():
            if field == "tolerationSeconds":
                if not isinstance(value, int) or isinstance(value, bool):
                    raise ValueError("toleration tolerationSeconds must be an integer")
            elif not isinstance(value, str) or not value:
                raise ValueError(f"toleration {field} must be a non-empty string")
            toleration[field] = value
        if not toleration:
            raise ValueError("toleration must not be empty")
        normalized.append(toleration)
    return normalized or None


def normalize_volumes(
    volumes: Optional[List[Mapping[str, Any]]],
) -> Tuple[List[client.V1Volume], List[client.V1VolumeMount]]:
    """Convert manifest volume entries into Kubernetes volumes and volumeMounts.

    Each entry is a Kubernetes volume spec plus a ``mountPath`` (and optional
    mount fields). Those mount fields are applied as a container volumeMount;
    the remaining keys become the pod volume.
    """
    if volumes is None:
        return [], []
    if isinstance(volumes, (str, bytes, Mapping)):
        raise ValueError("volumes must be a list of volume mappings")
    normalized_volumes: List[client.V1Volume] = []
    normalized_mounts: List[client.V1VolumeMount] = []
    for entry in volumes:
        if not isinstance(entry, Mapping):
            raise ValueError("each volume must be a mapping")
        name = entry.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError("each volume must have a non-empty string name")
        mount_path = entry.get("mountPath")
        if not isinstance(mount_path, str) or not mount_path:
            raise ValueError(f"volume {name!r} must have a non-empty string mountPath")
        volume_spec = {
            key: value for key, value in entry.items() if key not in _VOLUME_MOUNT_FIELDS
        }
        if set(volume_spec) <= {"name"}:
            raise ValueError(f"volume {name!r} must declare a volume source (e.g. emptyDir)")
        try:
            volume = client.ApiClient()._ApiClient__deserialize_model(
                dict(volume_spec), client.V1Volume
            )
        except Exception as exc:
            raise ValueError(f"volume {name!r} is not a valid Kubernetes volume: {exc}") from exc
        mount_kwargs: Dict[str, Any] = {"name": name, "mount_path": mount_path}
        if "readOnly" in entry:
            if not isinstance(entry["readOnly"], bool):
                raise ValueError(f"volume {name!r} readOnly must be a boolean")
            mount_kwargs["read_only"] = entry["readOnly"]
        if "subPath" in entry:
            if not isinstance(entry["subPath"], str) or not entry["subPath"]:
                raise ValueError(f"volume {name!r} subPath must be a non-empty string")
            mount_kwargs["sub_path"] = entry["subPath"]
        if "subPathExpr" in entry:
            if not isinstance(entry["subPathExpr"], str) or not entry["subPathExpr"]:
                raise ValueError(f"volume {name!r} subPathExpr must be a non-empty string")
            mount_kwargs["sub_path_expr"] = entry["subPathExpr"]
        if "mountPropagation" in entry:
            if not isinstance(entry["mountPropagation"], str) or not entry["mountPropagation"]:
                raise ValueError(f"volume {name!r} mountPropagation must be a non-empty string")
            mount_kwargs["mount_propagation"] = entry["mountPropagation"]
        normalized_volumes.append(volume)
        normalized_mounts.append(client.V1VolumeMount(**mount_kwargs))
    return normalized_volumes, normalized_mounts


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

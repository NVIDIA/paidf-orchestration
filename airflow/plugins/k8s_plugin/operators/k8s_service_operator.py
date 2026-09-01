# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Operator for deploying long-running services on Kubernetes (Deployment + Service).

Deployment and Service specs are loaded from externalized YAML templates
mounted via ConfigMap at TEMPLATE_DIR. This allows cluster-specific
customization without changing operator code.
"""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import time
from http import HTTPStatus
from typing import Any, Dict, List, Optional

import yaml as pyyaml
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.sdk import BaseOperator
from kubernetes import client
from slot_holds.pool_slot_hold_operator import ensure_pool_slot_holds
from triggers import XComWaitTrigger

from k8s_plugin.connection import resolve_kubernetes_backend
from k8s_plugin.model_cache import build_inference_pod_volumes
from k8s_plugin.operators.k8s_task_operator import (
    normalize_node_selector,
    normalize_tolerations,
)

TEMPLATE_DIR = os.environ.get("K8S_SERVICE_TEMPLATE_DIR", "/opt/k8s-service-templates")


class K8sServiceOperator(BaseOperator):
    """
    Creates a Kubernetes Deployment and ClusterIP Service for a long-running
    inference endpoint (e.g. VLM, LLM). Polls until the Deployment is ready,
    then returns a ServiceEndpoint-compatible dict via XCom.

    Accepts the same manifest argument format as NVCFOperator so it can be
    used as a drop-in replacement in K8s manifests.
    """

    template_fields = [
        "name",
        "container_image",
        "container_args",
        "container_environment_variables",
        "secrets",
        "namespace",
        "context",
        "node_selector",
        "replicas",
        "defer_until_xcom",
        "defer_until_xcom_task_id",
        "defer_until_xcom_key",
        "slot_hold_task_id",
    ]

    def __init__(
        self,
        *,
        name: str,
        container_image: str,
        namespace: str = "default",
        container_args: Optional[str] = None,
        container_environment_variables: Optional[List[str]] = None,
        secrets: Optional[List[str]] = None,
        gpu: Optional[str] = None,
        gpu_count: int = 1,
        node_selector: Optional[Dict[str, Any]] = None,
        tolerations: Optional[List[Dict[str, Any]]] = None,
        host_ipc: bool = False,
        image_pull_secrets: Optional[List[str]] = None,
        kubernetes_conn_id: Optional[str] = None,
        context: Optional[str] = None,
        in_cluster: Optional[bool] = None,
        model_cache_pvc: Optional[str] = None,
        shm_size: Optional[str] = None,
        nofile_limit: Optional[int] = None,
        inference_port: int = 8000,
        health_uri: str = "/health",
        replicas: int = 1,
        deployment_ready_timeout_seconds: int = 900,
        poll_interval_seconds: int = 10,
        # Absorbed silently -- NVCF-specific fields that may appear in manifest overrides
        inference_url: Optional[str] = None,
        instance_type: Optional[str] = None,
        backend: Optional[str] = None,
        clusters: Optional[List[str]] = None,
        nvcf_conn_id: Optional[str] = None,
        health_port: Optional[int] = None,
        health_protocol: Optional[str] = None,
        health_timeout: Optional[str] = None,
        health_expected_status_code: Optional[int] = None,
        max_request_concurrency: Optional[int] = None,
        defer_until_xcom: bool = False,
        defer_until_xcom_task_id: Optional[str] = None,
        defer_until_xcom_key: Optional[str] = None,
        defer_until_xcom_timeout_seconds: Optional[int] = 3600,
        defer_until_xcom_poll_interval_seconds: float = 30.0,
        slot_hold_task_id: Optional[str] = None,
        slot_hold_poll_interval_seconds: float = 10.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.model_cache_pvc = model_cache_pvc
        self.shm_size = shm_size
        self.nofile_limit = nofile_limit
        self.name = name
        self.container_image = container_image
        self.namespace = namespace
        self.container_args = container_args
        self.container_environment_variables = container_environment_variables
        self.secrets = secrets
        self.gpu = gpu
        if type(gpu_count) is not int:
            raise ValueError("gpu_count must be an integer")
        if gpu_count < 1:
            raise ValueError("gpu_count must be at least 1")
        self.gpu_count = gpu_count
        self.node_selector = normalize_node_selector(node_selector)
        self.tolerations = normalize_tolerations(tolerations)
        self.host_ipc = host_ipc
        self.image_pull_secrets = image_pull_secrets
        self.context = context
        self.kubernetes_conn_id, self.cluster_context = resolve_kubernetes_backend(
            context=context,
            kubernetes_conn_id=kubernetes_conn_id,
            in_cluster=in_cluster,
        )
        self.in_cluster = in_cluster
        self.inference_port = inference_port
        self.health_uri = health_uri
        self.replicas = replicas
        self.deployment_ready_timeout_seconds = deployment_ready_timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.defer_until_xcom = defer_until_xcom
        self.defer_until_xcom_task_id = (
            defer_until_xcom_task_id
            if defer_until_xcom_task_id is not None
            else f"service_shutdown.shutdown_{self.name}"
        )
        self.defer_until_xcom_key = (
            defer_until_xcom_key if defer_until_xcom_key is not None else "function_id"
        )
        self.defer_until_xcom_timeout_seconds = defer_until_xcom_timeout_seconds
        self.defer_until_xcom_poll_interval_seconds = defer_until_xcom_poll_interval_seconds
        self.slot_hold_task_id = slot_hold_task_id
        self.slot_hold_poll_interval_seconds = slot_hold_poll_interval_seconds
        self._k8s_resource_name: Optional[str] = None

    def execute_complete(
        self,
        context: Dict[str, Any],
        event: Optional[Dict[str, Any]] = None,
    ) -> None:
        if event and event.get("error"):
            raise AirflowException(event["error"])
        return None

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any] | None:
        try:
            self.replicas = int(self.replicas) if self.replicas is not None else 1
        except (ValueError, TypeError) as e:
            raise AirflowException(f"Failed to coerce replicas to int: {e}") from e
        if self.replicas < 1:
            raise AirflowException(f"replicas must be >= 1, got {self.replicas}")

        ensure_pool_slot_holds(
            context=context,
            slot_hold_task_id=self.slot_hold_task_id,
            replicas=self.replicas,
            poll_interval_seconds=self.slot_hold_poll_interval_seconds,
            log=self.log,
        )

        hook_kwargs = {}
        if self.kubernetes_conn_id:
            hook_kwargs["conn_id"] = self.kubernetes_conn_id
        if self.cluster_context:
            hook_kwargs["cluster_context"] = self.cluster_context
        if self.in_cluster is not None:
            hook_kwargs["in_cluster"] = self.in_cluster
        hook = KubernetesHook(**hook_kwargs)
        api_client = hook.get_conn()
        apps_v1 = client.AppsV1Api(api_client)
        core_v1 = client.CoreV1Api(api_client)

        k8s_name = self._build_k8s_resource_name(self.name, context)
        self._k8s_resource_name = k8s_name

        self.log.info("Creating Deployment %s in namespace %s", k8s_name, self.namespace)
        deployment = self._build_deployment(k8s_name)
        self._apply_deployment(apps_v1, k8s_name, deployment)

        self.log.info("Creating Service %s in namespace %s", k8s_name, self.namespace)
        service = self._build_service(k8s_name)
        self._apply_service(core_v1, k8s_name, service)

        self.log.info(
            "Waiting for deployment readiness (timeout=%ds)...",
            self.deployment_ready_timeout_seconds,
        )
        self._wait_for_ready(apps_v1, k8s_name)

        url = f"http://{k8s_name}.{self.namespace}.svc.cluster.local:{self.inference_port}/v1"

        endpoint_dict = {
            "url": url,
            "resource_id": k8s_name,
            "resource_version": "latest",
        }

        ti = context["ti"]
        ti.xcom_push(key="endpoint", value=endpoint_dict)

        if self.defer_until_xcom and self.defer_until_xcom_task_id:
            dag_run = context["dag_run"]
            trigger = XComWaitTrigger(
                dag_id=context["dag"].dag_id,
                run_id=dag_run.run_id,
                watch_task_id=self.defer_until_xcom_task_id,
                watch_key=self.defer_until_xcom_key,
                poll_interval_seconds=self.defer_until_xcom_poll_interval_seconds,
                timeout_seconds=float(self.defer_until_xcom_timeout_seconds)
                if self.defer_until_xcom_timeout_seconds is not None
                else None,
            )
            self.defer(
                trigger=trigger,
                method_name="execute_complete",
            )

    def on_kill(self) -> None:
        # DagRun timeout can kill this task before service_shutdown is scheduled.
        # Perform best-effort cleanup here to avoid orphaned K8s resources.
        resource_name = self._k8s_resource_name
        if not resource_name:
            self.log.warning(
                "Task killed before K8s resource name was set; skipping best-effort cleanup "
                "for base name %s",
                self.name,
            )
            return

        self.log.info(
            "Task killed; attempting best-effort cleanup for Deployment/Service %s in %s",
            resource_name,
            self.namespace,
        )

        try:
            hook_kwargs = {}
            if self.kubernetes_conn_id:
                hook_kwargs["conn_id"] = self.kubernetes_conn_id
            if self.cluster_context:
                hook_kwargs["cluster_context"] = self.cluster_context
            if self.in_cluster is not None:
                hook_kwargs["in_cluster"] = self.in_cluster
            hook = KubernetesHook(**hook_kwargs)
            api_client = hook.get_conn()
            apps_v1 = client.AppsV1Api(api_client)
            core_v1 = client.CoreV1Api(api_client)

            try:
                apps_v1.delete_namespaced_deployment(
                    name=resource_name,
                    namespace=self.namespace,
                )
            except client.exceptions.ApiException as e:
                if e.status != HTTPStatus.NOT_FOUND:
                    self.log.warning(
                        "Best-effort Deployment cleanup failed for %s: %s",
                        resource_name,
                        e,
                    )

            try:
                core_v1.delete_namespaced_service(
                    name=resource_name,
                    namespace=self.namespace,
                )
            except client.exceptions.ApiException as e:
                if e.status != HTTPStatus.NOT_FOUND:
                    self.log.warning(
                        "Best-effort Service cleanup failed for %s: %s",
                        resource_name,
                        e,
                    )
        except Exception as e:
            self.log.warning("Best-effort K8s on_kill cleanup failed: %s", e)

    @staticmethod
    def _sanitize_name(name: str) -> str:
        sanitized = name.lower().replace("_", "-")
        sanitized = "".join(c if c.isalnum() or c == "-" else "-" for c in sanitized)
        sanitized = sanitized.strip("-")[:63]
        if sanitized and sanitized[0].isdigit():
            sanitized = "svc-" + sanitized
        return sanitized or "endpoint"

    @staticmethod
    def _run_id_suffix(context: Dict[str, Any]) -> str:
        run_id = context["dag_run"].run_id
        return hashlib.sha256(run_id.encode()).hexdigest()[:8]

    @classmethod
    def _build_k8s_resource_name(cls, base_name: str, context: Dict[str, Any]) -> str:
        base = cls._sanitize_name(base_name)
        suffix = cls._run_id_suffix(context)
        full = f"{base}-{suffix}"
        return full[:63].rstrip("-")

    def _build_env(self) -> list[client.V1EnvVar]:
        env = []
        for source in (self.container_environment_variables, self.secrets):
            if source:
                for item in source:
                    if isinstance(item, str) and ":" in item:
                        k, v = item.split(":", 1)
                        env.append(client.V1EnvVar(name=k, value=v))
        return env

    def _build_deployment(self, name: str) -> client.V1Deployment:
        template_path = os.path.join(TEMPLATE_DIR, "deployment.yaml")
        self.log.info("Loading deployment template from %s", template_path)
        with open(template_path) as f:
            raw = f.read()

        raw = (
            raw.replace("__NAME__", name)
            .replace("__IMAGE__", self.container_image)
            .replace("__INFERENCE_PORT__", str(self.inference_port))
            .replace("__HEALTH_URI__", self.health_uri)
        )
        spec = pyyaml.safe_load(raw)
        spec["spec"]["replicas"] = int(self.replicas)

        pod_spec = spec["spec"]["template"]["spec"]
        pod_spec["hostIPC"] = self.host_ipc
        container = pod_spec["containers"][0]
        if self.container_args:
            command, args = _parse_container_command_and_args(self.container_args)
            if command:
                container["command"] = command
            if args:
                container["args"] = args
        if self.nofile_limit:
            command, args = _wrap_command_with_nofile_limit(
                container.get("command"), container.get("args"), int(self.nofile_limit)
            )
            container["command"] = command
            if args:
                container["args"] = args
            else:
                container.pop("args", None)
        env_vars = self._build_env()
        env_dicts = [{"name": e.name, "value": e.value} for e in env_vars]
        container.setdefault("env", []).extend(env_dicts)
        if self.gpu:
            gpu_count = str(self.gpu_count)
            container["resources"] = {
                "limits": {"nvidia.com/gpu": gpu_count},
                "requests": {"nvidia.com/gpu": gpu_count},
            }
        if self.image_pull_secrets:
            pod_spec["imagePullSecrets"] = [{"name": s} for s in self.image_pull_secrets]
        if self.node_selector:
            pod_spec["nodeSelector"] = dict(self.node_selector)
        if self.tolerations:
            pod_spec["tolerations"] = [dict(t) for t in self.tolerations]

        volumes, volume_mounts = build_inference_pod_volumes(
            gpu=bool(self.gpu),
            model_cache_pvc=self.model_cache_pvc,
            shm_size=self.shm_size,
        )
        if volumes:
            pod_spec["volumes"] = [_serialize_k8s_object(v) for v in volumes]
            container["volumeMounts"] = [_serialize_k8s_object(m) for m in volume_mounts]

        api_client = client.ApiClient()
        return api_client.deserialize(_FakeResponse(json.dumps(spec)), "V1Deployment")

    def _build_service(self, name: str) -> client.V1Service:
        template_path = os.path.join(TEMPLATE_DIR, "service.yaml")
        self.log.info("Loading service template from %s", template_path)
        with open(template_path) as f:
            raw = f.read()

        raw = raw.replace("__NAME__", name).replace("__INFERENCE_PORT__", str(self.inference_port))
        spec = pyyaml.safe_load(raw)

        api_client = client.ApiClient()
        return api_client.deserialize(_FakeResponse(json.dumps(spec)), "V1Service")

    def _apply_deployment(
        self, api: client.AppsV1Api, name: str, deployment: client.V1Deployment
    ) -> None:
        try:
            api.create_namespaced_deployment(namespace=self.namespace, body=deployment)
        except client.exceptions.ApiException as e:
            if e.status == HTTPStatus.CONFLICT:
                self.log.warning(
                    "Deployment %s already exists, replacing (likely a task retry within the "
                    "same DAG run)...",
                    name,
                )
                api.replace_namespaced_deployment(
                    name=name, namespace=self.namespace, body=deployment
                )
            else:
                raise

    def _apply_service(self, api: client.CoreV1Api, name: str, service: client.V1Service) -> None:
        try:
            api.create_namespaced_service(namespace=self.namespace, body=service)
        except client.exceptions.ApiException as e:
            if e.status == HTTPStatus.CONFLICT:
                self.log.warning(
                    "Service %s already exists, patching (likely a task retry within the "
                    "same DAG run)...",
                    name,
                )
                try:
                    api.patch_namespaced_service(name=name, namespace=self.namespace, body=service)
                except client.exceptions.ApiException as patch_err:
                    self.log.warning("Failed to patch Service %s: %s", name, patch_err)
                    raise
            else:
                raise

    def _wait_for_ready(self, api: client.AppsV1Api, name: str) -> None:
        start = time.time()
        while True:
            dep = api.read_namespaced_deployment(name=name, namespace=self.namespace)
            available = dep.status.available_replicas or 0
            desired = int(self.replicas)
            if available >= desired:
                self.log.info(
                    "Deployment %s is ready (%d/%d replicas available)",
                    name,
                    available,
                    desired,
                )
                return

            elapsed = time.time() - start
            if elapsed >= self.deployment_ready_timeout_seconds:
                raise AirflowException(
                    f"Deployment {name} not ready after {self.deployment_ready_timeout_seconds}s. "
                    f"Available replicas: {available}/{desired}"
                )

            self.log.info(
                "Deployment %s not ready (available=%d, elapsed=%.0fs), polling...",
                name,
                available,
                elapsed,
            )
            time.sleep(self.poll_interval_seconds)


class _FakeResponse:
    """Minimal wrapper so ``ApiClient.deserialize`` can consume a YAML string."""

    def __init__(self, data: str) -> None:
        self.data = data


def _serialize_k8s_object(obj: Any) -> dict[str, Any]:
    return client.ApiClient().sanitize_for_serialization(obj)


def _parse_container_command_and_args(
    container_args: Optional[str],
) -> tuple[Optional[list[str]], Optional[list[str]]]:
    """Convert manifest ``container_args`` to Kubernetes ``command`` + ``args``."""
    if not container_args:
        return None, None
    tokens = shlex.split(container_args)
    if not tokens:
        return None, None
    if tokens[0].startswith("-"):
        return None, tokens
    return [tokens[0]], tokens[1:] or None


def _wrap_command_with_nofile_limit(
    command: Optional[list[str]],
    args: Optional[list[str]],
    nofile_limit: int,
) -> tuple[list[str], Optional[list[str]]]:
    """Raise the open-file soft limit before exec'ing the container command.

    Kubernetes exposes no equivalent of ``docker run --ulimit``, so the command is
    run through a shell that calls ``ulimit -n`` first. Only the soft limit is
    raised, which needs no extra privileges as long as it stays under the hard
    limit inherited from the container runtime.

    An explicit ``container_args`` is required: the image ENTRYPOINT is not
    readable from the pod spec, so it cannot be preserved across the wrapper.
    """
    if not command:
        raise AirflowException(
            "nofile_limit requires container_args to name the command to run, "
            "because wrapping it in a shell replaces the image entrypoint."
        )
    shell_args = [*command[1:], *(args or [])]
    return (
        ["/bin/sh", "-c", f'ulimit -n {nofile_limit}; exec "$0" "$@"', command[0]],
        shell_args or None,
    )

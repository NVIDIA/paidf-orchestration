# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Operator for cleaning up Kubernetes Deployment + Service resources."""

from __future__ import annotations

from http import HTTPStatus
from typing import Any, Dict, Optional

from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.sdk import BaseOperator
from kubernetes import client

from k8s_plugin.connection import resolve_kubernetes_backend


class K8sCleanupOperator(BaseOperator):
    """
    Deletes Kubernetes Deployment and Service resources created by K8sServiceOperator.

    Accepts the same argument pattern as NVCFCleanupOperator (function_id maps to
    the K8s resource name). Designed to run with trigger_rule="all_done" to ensure
    cleanup even when upstream tasks fail.
    """

    template_fields = ["function_id", "function_version_id", "namespace", "context"]

    def __init__(
        self,
        *,
        function_id: Optional[str] = None,
        function_version_id: Optional[str] = None,
        namespace: str = "default",
        kubernetes_conn_id: Optional[str] = None,
        context: Optional[str] = None,
        in_cluster: Optional[bool] = None,
        # Absorbed silently -- NVCF-specific fields from manifest
        nvcf_conn_id: Optional[str] = None,
        auth_info: Optional[Dict[str, str]] = None,
        undeploy_only: Optional[bool] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.function_id = function_id
        self.function_version_id = function_version_id
        self.namespace = namespace
        self.context = context
        self.kubernetes_conn_id, self.cluster_context = resolve_kubernetes_backend(
            context=context,
            kubernetes_conn_id=kubernetes_conn_id,
            in_cluster=in_cluster,
        )
        self.in_cluster = in_cluster

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        resource_name = self.function_id
        if not resource_name or resource_name == "None":
            self.log.info("No resource name provided, skipping cleanup.")
            # Unblock deferred deploy/slot-hold tasks watching this cleanup task.
            context["ti"].xcom_push(key="function_id", value="")
            return {"skipped": True}

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

        results: Dict[str, Any] = {
            "resource_name": resource_name,
            "deployment_deleted": False,
            "service_deleted": False,
        }

        # Delete Deployment
        try:
            self.log.info("Deleting Deployment %s in namespace %s", resource_name, self.namespace)
            apps_v1.delete_namespaced_deployment(name=resource_name, namespace=self.namespace)
            results["deployment_deleted"] = True
            self.log.info("Deployment %s deleted.", resource_name)
        except client.exceptions.ApiException as e:
            if e.status == HTTPStatus.NOT_FOUND:
                self.log.info("Deployment %s not found (already deleted).", resource_name)
                results["deployment_deleted"] = True
            else:
                self.log.warning("Failed to delete Deployment %s: %s", resource_name, e)

        # Delete Service
        try:
            self.log.info("Deleting Service %s in namespace %s", resource_name, self.namespace)
            core_v1.delete_namespaced_service(name=resource_name, namespace=self.namespace)
            results["service_deleted"] = True
            self.log.info("Service %s deleted.", resource_name)
        except client.exceptions.ApiException as e:
            if e.status == HTTPStatus.NOT_FOUND:
                self.log.info("Service %s not found (already deleted).", resource_name)
                results["service_deleted"] = True
            else:
                self.log.warning("Failed to delete Service %s: %s", resource_name, e)

        # Signal to any deferred startup operator waiting on this shutdown task.
        context["ti"].xcom_push(key="function_id", value=resource_name)

        return results

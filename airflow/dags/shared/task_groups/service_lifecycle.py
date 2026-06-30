# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Task Group for managing the lifecycle of services used in the DAG (e.g. LLM, VLM services)."""

import ast
import json
import time
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import BaseOperator, TaskGroup
from airflow.task.trigger_rule import TriggerRule
from triggers import XComWaitTrigger

from dags.shared.models import (
    EndpointXComValue,
    ServiceLifecycleTaskConfig,
)
from dags.shared.utils.component_builder import ComponentBuilder

# No timeout: startup tasks should remain deferred until shutdown publishes XCom.
STARTUP_DEFER_TIMEOUT_SECONDS = None

# Maps manifest endpoint keys to wait-task ids (legacy ``vlm``/``llm`` aliases included).
ENDPOINT_WAIT_TASK_IDS: dict[str, str] = {
    "vlm_service": "wait_for_vlm",
    "llm_service": "wait_for_llm",
    "image_edit_service": "wait_for_image_edit",
}
SERVICE_KIND_ALIASES: dict[str, str] = {
    "vlm": "vlm_service",
    "llm": "llm_service",
    "image_edit_service": "image_edit_service",
}


def require_service_endpoint_from_xcom(
    ti: Any,
    component_key: str,
    task_group_id: str = "service_startup",
) -> str:
    """
    Load and validate endpoint URL from a service startup task XCom.

    Used by task groups that need a deployed service URL when
    ``external_services`` is disabled.
    """
    startup_task_id = (
        f"{task_group_id}.{ServiceLifecycleTaskGroup.get_startup_task_name(component_key)}"
    )
    endpoint_raw = ti.xcom_pull(task_ids=startup_task_id, key="endpoint")
    try:
        endpoint = EndpointXComValue.model_validate(endpoint_raw)
    except Exception as e:
        raise AirflowFailException(
            f"Endpoint XCom is invalid for {startup_task_id!r}: {e}. "
            "Ensure the startup operator pushed a valid endpoint payload."
        ) from e

    if not endpoint.url:
        raise AirflowFailException(f"Endpoint XCom for {startup_task_id!r} did not contain a URL.")
    return endpoint.url


class WaitForServiceXComOperator(BaseOperator):
    """
    When internal deployment is disabled for this service kind (enable_* false), exits
    without deferring. Otherwise defers to XComWaitTrigger until deploy pushes endpoint XCom.
    """

    template_fields = ("service_lifecycle_config", "watch_task_id", "watch_key")

    def __init__(
        self,
        *,
        service_lifecycle_config: str | dict[str, Any],
        watch_task_id: str,
        watch_key: str = "endpoint",
        service_kind: str,
        poll_interval_seconds: float = 10.0,
        timeout_seconds: int | None = 600,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.service_lifecycle_config = service_lifecycle_config
        self.watch_task_id = watch_task_id
        self.watch_key = watch_key
        self.service_kind = service_kind
        self.poll_interval_seconds = poll_interval_seconds
        self.timeout_seconds = timeout_seconds

    def execute(self, context: dict[str, Any]) -> None:
        config_field = SERVICE_KIND_ALIASES.get(self.service_kind, self.service_kind)
        if config_field not in ServiceLifecycleTaskConfig.model_fields:
            raise AirflowFailException(f"Unknown service_kind: {self.service_kind}")

        try:
            lifecycle = ServiceLifecycleTaskGroup.parse_service_lifecycle_config(
                self.service_lifecycle_config
            )
        except Exception as e:
            raise AirflowFailException(f"Service lifecycle config validation failed: {e}") from e

        if not getattr(lifecycle, config_field).enabled:
            return

        run_id = context["dag_run"].run_id
        dag_id = context["dag"].dag_id
        trigger = XComWaitTrigger(
            dag_id=dag_id,
            run_id=run_id,
            watch_task_id=self.watch_task_id,
            watch_key=self.watch_key,
            poll_interval_seconds=self.poll_interval_seconds,
            timeout_seconds=(
                float(self.timeout_seconds) if self.timeout_seconds is not None else None
            ),
        )
        self.defer(trigger=trigger, method_name="execute_complete")

    def execute_complete(
        self,
        context: dict[str, Any],
        event: dict[str, Any] | None = None,
    ) -> None:
        if event and event.get("error"):
            raise AirflowFailException(event["error"])


class ServiceLifecycleTaskGroup:
    """
    Task Group for managing the lifecycle of services used in the DAG (e.g. LLM, VLM services).
    """

    def __init__(
        self,
        builder: ComponentBuilder,
        service_lifecycle_config: str,
        defer_until_shutdown: str | bool = True,
        payload_task_id: str = "validate_payload",
    ):
        """
        Initialize the Service Lifecycle Task Group.

        Args:
            builder: Component builder for manifest-defined endpoints.
            service_lifecycle_config: Templated service lifecycle config (e.g. from DAG params).
            defer_until_shutdown: When false, deploy tasks return after the endpoint is ready
                instead of deferring until shutdown. Accepts a Jinja template for runtime params.
            payload_task_id: Task id used to pull validated payload XCom for per-service overrides.
        """
        self.builder = builder
        self.service_lifecycle_config = service_lifecycle_config
        self.defer_until_shutdown = defer_until_shutdown
        self.payload_task_id = payload_task_id

    @staticmethod
    def parse_service_lifecycle_config(value: Any) -> ServiceLifecycleTaskConfig:
        """
        Parse service lifecycle config from Airflow-templated values.

        Supports a dict (native XCom/param), JSON (``| tojson``), or a Python literal string.
        """
        if isinstance(value, ServiceLifecycleTaskConfig):
            return value
        if isinstance(value, dict):
            return ServiceLifecycleTaskConfig.model_validate(value)

        if not isinstance(value, str):
            raise AirflowFailException(
                f"service_lifecycle_config must be a dict or str, got {type(value).__name__}"
            )

        raw = value.strip()
        if not raw:
            raise AirflowFailException("service_lifecycle_config is empty")
        if raw.startswith("{{") and raw.endswith("}}"):
            raise AirflowFailException(
                "service_lifecycle_config was not rendered by Airflow; ensure the field is "
                "listed in template_fields or use a PythonOperator op_kwargs template"
            )

        try:
            return ServiceLifecycleTaskConfig.model_validate(json.loads(raw))
        except json.JSONDecodeError:
            pass

        try:
            parsed = ast.literal_eval(raw)
        except (ValueError, SyntaxError) as e:
            raise AirflowFailException(
                "service_lifecycle_config is not valid JSON or a Python literal dict"
            ) from e

        return ServiceLifecycleTaskConfig.model_validate(parsed)

    @staticmethod
    def resolve_services_to_deploy(
        lifecycle: ServiceLifecycleTaskConfig,
        manifest_endpoint_keys: list[str] | set[str],
    ) -> list[str]:
        """
        Return endpoint names to deploy from ``ServiceLifecycleTaskConfig`` fields.

        Each config field name (e.g. ``vlm_service``) must match a manifest endpoint name
        when ``enabled`` is true; otherwise an exception is raised.
        """
        manifest_keys = set(manifest_endpoint_keys)
        to_deploy: list[str] = []
        for field_name in ServiceLifecycleTaskConfig.model_fields:
            service = getattr(lifecycle, field_name)
            if not service.enabled:
                continue
            if field_name not in manifest_keys:
                raise AirflowFailException(
                    f"service_lifecycle.{field_name}.enabled is true but endpoint "
                    f"{field_name!r} is not defined in the deployment manifest"
                )
            to_deploy.append(field_name)
        return to_deploy

    @staticmethod
    def get_startup_task_name(component_key: str) -> str:
        """
        Get the names of the startup tasks for the services used in the DAG (e.g. LLM, VLM services).
        """
        return f"startup_{component_key}"

    @staticmethod
    def get_shutdown_task_name(component_key: str) -> str:
        """
        Get the names of the shutdown tasks for the services used in the DAG (e.g. LLM, VLM services).
        """
        return f"shutdown_{component_key}"

    def service_config_field_template(self, component_key: str, field: str, default: int = 1) -> str:
        """Build a Jinja template for a field under ``service_lifecycle.<component_key>``."""
        return (
            f"{{{{ (ti.xcom_pull(task_ids='{self.payload_task_id}', key='return_value') or {{}})"
            f".get('service_lifecycle', {{}}).get('{component_key}', {{}}).get('{field}', {default}) }}}}"
        )

    @staticmethod
    def deployment_decision(**context) -> list[str]:
        """
        Decide whether to deploy services based on ``ServiceLifecycleTaskConfig`` fields.

        Only services with ``enabled`` true are deployed; each field name must match a
        manifest endpoint name.
        """
        try:
            lifecycle = ServiceLifecycleTaskGroup.parse_service_lifecycle_config(
                context["service_lifecycle_config"]
            )
        except Exception as e:
            raise AirflowFailException(f"Service lifecycle config validation failed: {e}") from e
        if "startup" not in context:
            raise AirflowFailException(
                "Startup variable is required to decide whether to deploy the services."
            )
        startup = context["startup"]

        if "services" not in context:
            raise AirflowFailException(
                "Services variable is required to decide whether to deploy the services."
            )
        manifest_endpoint_keys = context["services"]

        services_to_deploy = ServiceLifecycleTaskGroup.resolve_services_to_deploy(
            lifecycle, manifest_endpoint_keys
        )

        if not services_to_deploy:
            # All services external — skip deployment
            if startup:
                return ["service_startup.skip_startup"]
            else:
                return ["service_shutdown.skip_shutdown"]
        else:
            if startup:
                startup_task_names = [
                    f"service_startup.{ServiceLifecycleTaskGroup.get_startup_task_name(component_key)}"
                    for component_key in services_to_deploy
                ]
                return startup_task_names
            else:
                shutdown_task_names = [
                    f"service_shutdown.{ServiceLifecycleTaskGroup.get_shutdown_task_name(component_key)}"
                    for component_key in services_to_deploy
                ]
                return shutdown_task_names

    def get_startup_decision_task(self) -> BranchPythonOperator:
        """
        Get the decision task for whether to start the services used in the DAG (e.g. LLM, VLM services).
        """
        return BranchPythonOperator(
            task_id="startup_decision",
            python_callable=self.deployment_decision,
            op_kwargs={
                "service_lifecycle_config": self.service_lifecycle_config,
                "startup": True,
                "services": list(self.builder.manifest.deployment.components.endpoints.keys()),
            },
        )

    def get_shutdown_decision_task(self) -> BranchPythonOperator:
        """
        Get the decision task for whether to shutdown the services used in the DAG (e.g. LLM, VLM services).
        """
        return BranchPythonOperator(
            task_id="shutdown_decision",
            python_callable=self.deployment_decision,
            trigger_rule=TriggerRule.ALL_DONE,
            op_kwargs={
                "service_lifecycle_config": self.service_lifecycle_config,
                "startup": False,
                "services": list(self.builder.manifest.deployment.components.endpoints.keys()),
            },
        )

    def get_endpoint_validity_wait_task(
        self,
        task_id: str = "endpoint_validity_wait",
    ) -> PythonOperator:
        """Wait for ``endpoint_validity_seconds`` after the endpoint is ready."""

        def _sleep_endpoint_validity(**context: Any) -> None:
            seconds = int(context["params"].get("endpoint_validity_seconds", 3600))
            if seconds > 0:
                time.sleep(seconds)
                return
            while True:
                time.sleep(86400)

        return PythonOperator(
            task_id=task_id,
            python_callable=_sleep_endpoint_validity,
        )

    def get_service_startup_task_group(self) -> TaskGroup:
        """
        Get the Task Group for managing the lifecycle of services used in the DAG (e.g. LLM, VLM services).

        Flow: startup_decision -> [deploy_tasks | skip_startup]
        Deploy tasks use manifest profile + endpoint configuration; replica count comes from
        ``service_lifecycle.<service>.replicas`` (default 1).
        """
        with TaskGroup(group_id="service_startup") as task_group:
            startup_decision = self.get_startup_decision_task()
            deploy_tasks = []
            for (
                component_key,
                component_value,
            ) in self.builder.manifest.deployment.components.endpoints.items():
                component_secrets = [
                    f"{key}:{value}" for key, value in (component_value.secrets or {}).items()
                ]
                deploy_tasks.append(
                    self.builder.build_endpoint(
                        component_key,
                        task_id=ServiceLifecycleTaskGroup.get_startup_task_name(component_key),
                        secrets=component_secrets,
                        replicas=self.service_config_field_template(component_key, "replicas"),
                        defer_until_xcom=self.defer_until_shutdown,
                        defer_until_xcom_task_id=f"service_shutdown.{ServiceLifecycleTaskGroup.get_shutdown_task_name(component_key)}",
                        defer_until_xcom_timeout_seconds=STARTUP_DEFER_TIMEOUT_SECONDS,
                    )
                )
            skip_startup = EmptyOperator(task_id="skip_startup")
            startup_decision >> [*deploy_tasks, skip_startup]
        return task_group

    def get_wait_for_services_tasks(
        self,
        task_group_id: str = "service_startup",
    ) -> dict[str, WaitForServiceXComOperator]:
        """
        Return wait tasks keyed by service lifecycle config field (e.g. ``vlm_service``).

        One wait task is created for every entry in ``ENDPOINT_WAIT_TASK_IDS`` that has a
        matching ``ServiceLifecycleTaskConfig`` field. Manifest contents are not used to
        filter wait tasks; if internal deployment is disabled, the wait task exits without
        deferring.
        """
        # No timeout: wait tasks should defer until startup publishes endpoint XCom.
        common_kw = {
            "poll_interval_seconds": 10,
            "timeout_seconds": None,
            "service_lifecycle_config": self.service_lifecycle_config,
        }
        lifecycle_fields = set(ServiceLifecycleTaskConfig.model_fields)
        wait_tasks: dict[str, WaitForServiceXComOperator] = {}
        for endpoint_key, wait_task_id in ENDPOINT_WAIT_TASK_IDS.items():
            if endpoint_key not in lifecycle_fields:
                continue
            startup_task_id = (
                f"{task_group_id}.{ServiceLifecycleTaskGroup.get_startup_task_name(endpoint_key)}"
            )
            wait_tasks[endpoint_key] = WaitForServiceXComOperator(
                task_id=wait_task_id,
                watch_task_id=startup_task_id,
                watch_key="endpoint",
                service_kind=endpoint_key,
                **common_kw,
            )
        return wait_tasks

    def get_service_shutdown_task_group(self) -> TaskGroup:
        """
        Get the Task Group for managing the lifecycle of services used in the DAG (e.g. LLM, VLM services).
        """
        with TaskGroup(group_id="service_shutdown") as task_group:
            shutdown_decision = self.get_shutdown_decision_task()
            services = []
            for component_key in self.builder.manifest.deployment.components.endpoints.keys():
                startup_tid = "service_startup." + ServiceLifecycleTaskGroup.get_startup_task_name(
                    component_key
                )
                function_id = (
                    "{{ (ti.xcom_pull(task_ids='"
                    + startup_tid
                    + "', key='endpoint') or {}).get('resource_id', '') }}"
                )
                function_version_id = (
                    "{{ (ti.xcom_pull(task_ids='"
                    + startup_tid
                    + "', key='endpoint') or {}).get('resource_version', '') }}"
                )
                cleanup_op = self.builder.build_cleanup(
                    component_name=component_key,
                    task_id=ServiceLifecycleTaskGroup.get_shutdown_task_name(component_key),
                    function_id=function_id,
                    function_version_id=function_version_id,
                    trigger_rule=TriggerRule.ALL_DONE,
                )
                if cleanup_op:
                    services.append(cleanup_op)
            services.append(EmptyOperator(task_id="skip_shutdown"))
            join_after_shutdown = EmptyOperator(
                task_id="join_after_shutdown", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
            )
            (shutdown_decision >> services >> join_after_shutdown)
        return task_group

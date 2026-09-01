# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Task Group for managing the lifecycle of services used in the DAG (e.g. LLM, VLM services)."""

import ast
import json
import time
from dataclasses import dataclass
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import BaseOperator, TaskGroup, XComArg
from airflow.task.trigger_rule import TriggerRule
from pydantic import BaseModel
from slot_holds.pool_slot_hold_operator import PoolSlotHoldOperator
from triggers import XComWaitTrigger

from dags.shared.models import (
    EndpointXComValue,
    ServiceLifecycleServiceConfig,
)
from dags.shared.utils.component_builder import ComponentBuilder

# No timeout: startup tasks should remain deferred until shutdown publishes XCom.
STARTUP_DEFER_TIMEOUT_SECONDS = None

# Deploy tasks leave the GPU pool; mapped holders occupy capacity instead.
SERVICE_DEPLOY_CONTROL_POOL = "default_pool"


@dataclass(frozen=True)
class ServiceLifecycleSpec:
    """Workflow-owned description of one managed service."""

    component_key: str
    wait_task_id: str


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
        service_key: str,
        poll_interval_seconds: float = 10.0,
        timeout_seconds: int | None = 600,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.service_lifecycle_config = service_lifecycle_config
        self.watch_task_id = watch_task_id
        self.watch_key = watch_key
        self.service_key = service_key
        self.poll_interval_seconds = poll_interval_seconds
        self.timeout_seconds = timeout_seconds

    def execute(self, context: dict[str, Any]) -> None:
        try:
            lifecycle = ServiceLifecycleTaskGroup.parse_service_lifecycle_config(
                self.service_lifecycle_config
            )
        except Exception as e:
            raise AirflowFailException(f"Service lifecycle config validation failed: {e}") from e

        service_config = ServiceLifecycleTaskGroup.require_service_config(
            lifecycle, self.service_key
        )
        if not service_config.enabled:
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

    STARTUP_GROUP_ID = "service_startup"
    SHUTDOWN_GROUP_ID = "service_shutdown"

    def __init__(
        self,
        builder: ComponentBuilder,
        service_lifecycle_config: str,
        service_specs: list[ServiceLifecycleSpec] | tuple[ServiceLifecycleSpec, ...],
        defer_until_shutdown: str | bool = True,
        payload_task_id: str = "validate_payload",
    ):
        """
        Initialize the Service Lifecycle Task Group.

        Args:
            builder: Component builder for manifest-defined endpoints.
            service_lifecycle_config: Templated service lifecycle config (e.g. from DAG params).
            service_specs: Workflow-owned service component and wait-task definitions.
            defer_until_shutdown: When false, deploy tasks return after the endpoint is ready
                instead of deferring until shutdown. Accepts a Jinja template for runtime params.
            payload_task_id: Task id used to pull validated payload XCom for per-service overrides.
        """
        self.builder = builder
        self.service_lifecycle_config = service_lifecycle_config
        self.service_specs = tuple(service_specs)
        if not self.service_specs:
            raise ValueError("service_specs must contain at least one service")
        component_keys = [spec.component_key for spec in self.service_specs]
        wait_task_ids = [spec.wait_task_id for spec in self.service_specs]
        if len(component_keys) != len(set(component_keys)):
            raise ValueError("service_specs component_key values must be unique")
        if len(wait_task_ids) != len(set(wait_task_ids)):
            raise ValueError("service_specs wait_task_id values must be unique")
        self.defer_until_shutdown = defer_until_shutdown
        self.payload_task_id = payload_task_id

    def shutdown_xcom_task_id(self, component_key: str) -> str:
        """Full task id whose XCom releases deploy/slot-hold deferral for a service."""
        return f"{self.SHUTDOWN_GROUP_ID}.{self.get_shutdown_task_name(component_key)}"

    @staticmethod
    def parse_service_lifecycle_config(
        value: Any,
    ) -> dict[str, ServiceLifecycleServiceConfig]:
        """
        Parse service lifecycle config from Airflow-templated values.

        Supports a dict (native XCom/param), JSON (``| tojson``), or a Python literal string.
        """
        if isinstance(value, BaseModel):
            value = value.model_dump()

        if isinstance(value, dict):
            parsed = value
        elif not isinstance(value, str):
            raise AirflowFailException(
                f"service_lifecycle_config must be a dict or str, got {type(value).__name__}"
            )
        else:
            raw = value.strip()
            if not raw:
                raise AirflowFailException("service_lifecycle_config is empty")
            if raw.startswith("{{") and raw.endswith("}}"):
                raise AirflowFailException(
                    "service_lifecycle_config was not rendered by Airflow; ensure the field is "
                    "listed in template_fields or use a PythonOperator op_kwargs template"
                )

            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                try:
                    parsed = ast.literal_eval(raw)
                except (ValueError, SyntaxError) as e:
                    raise AirflowFailException(
                        "service_lifecycle_config is not valid JSON or a Python literal dict"
                    ) from e

        if not isinstance(parsed, dict):
            raise AirflowFailException("service_lifecycle_config must contain a dictionary")
        return {
            key: ServiceLifecycleServiceConfig.model_validate(config)
            for key, config in parsed.items()
        }

    @staticmethod
    def resolve_services_to_deploy(
        lifecycle: dict[str, ServiceLifecycleServiceConfig],
        service_keys: list[str] | set[str],
        manifest_endpoint_keys: list[str] | set[str],
    ) -> list[str]:
        """
        Return enabled workflow service names that are present in the manifest.

        Each config field name (e.g. ``vlm_service``) must match a manifest endpoint name
        when ``enabled`` is true; otherwise an exception is raised.
        """
        manifest_keys = set(manifest_endpoint_keys)
        to_deploy: list[str] = []
        for service_key in service_keys:
            service_config = ServiceLifecycleTaskGroup.require_service_config(
                lifecycle, service_key
            )
            if not service_config.enabled:
                continue
            if service_key not in manifest_keys:
                raise AirflowFailException(
                    f"service_lifecycle.{service_key}.enabled is true but endpoint "
                    f"{service_key!r} is not defined in the deployment manifest"
                )
            to_deploy.append(service_key)
        return to_deploy

    @staticmethod
    def require_service_config(
        lifecycle: dict[str, ServiceLifecycleServiceConfig], service_key: str
    ) -> ServiceLifecycleServiceConfig:
        """Return a configured service or fail consistently across lifecycle stages."""
        service_config = lifecycle.get(service_key)
        if service_config is None:
            raise AirflowFailException(f"Service lifecycle config does not define {service_key!r}")
        return service_config

    @staticmethod
    def get_startup_task_name(component_key: str) -> str:
        """
        Get the names of the startup tasks for the services used in the DAG (e.g. LLM, VLM services).
        """
        return f"startup_{component_key}"

    @staticmethod
    def get_prepare_slot_holds_task_name(component_key: str) -> str:
        """Task that emits replica indices for mapped GPU pool slot holders."""
        return f"prepare_slot_holds_{component_key}"

    @staticmethod
    def get_hold_slots_task_name(component_key: str) -> str:
        """Mapped task that occupies per-replica pool slots until shutdown."""
        return f"hold_slots_{component_key}"

    @staticmethod
    def get_shutdown_task_name(component_key: str) -> str:
        """
        Get the names of the shutdown tasks for the services used in the DAG (e.g. LLM, VLM services).
        """
        return f"shutdown_{component_key}"

    def service_config_field_template(
        self, component_key: str, field: str, default: int = 1
    ) -> str:
        """Build a Jinja template for a field under ``service_lifecycle.<component_key>``."""
        return (
            f"{{{{ (ti.xcom_pull(task_ids='{self.payload_task_id}', key='return_value') or {{}})"
            f".get('service_lifecycle', {{}}).get('{component_key}', {{}}).get('{field}', {default}) }}}}"
        )

    def _endpoint_pool_settings(self, component_key: str) -> tuple[str, int]:
        """Return (pool, pool_slots) for a manifest endpoint's deployment profile."""
        component = self.builder.manifest.deployment.components.endpoints[component_key]
        profile = self.builder._get_profile(component.deployment_profile)
        pool_slots = getattr(profile, "pool_slots", 1) or 1
        return profile.pool, int(pool_slots)

    @staticmethod
    def prepare_replica_indices(
        component_key: str, payload_task_id: str, **context: Any
    ) -> list[int]:
        """Return ``list(range(replicas))`` from validated payload service lifecycle config."""
        ti = context["ti"]
        payload = ti.xcom_pull(task_ids=payload_task_id, key="return_value") or {}
        service_cfg = (payload.get("service_lifecycle") or {}).get(component_key) or {}
        replicas = int(service_cfg.get("replicas", 1))
        if replicas < 1:
            raise AirflowFailException(
                f"service_lifecycle.{component_key}.replicas must be >= 1, got {replicas}"
            )
        return list(range(replicas))

    @staticmethod
    def deployment_decision(**context) -> list[str]:
        """
        Decide whether to deploy the services configured by the workflow.

        Only services with ``enabled`` true are deployed; each service key must match a
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

        if "service_keys" not in context:
            raise AirflowFailException(
                "Service keys are required to decide whether to deploy the services."
            )
        if "manifest_endpoint_keys" not in context:
            raise AirflowFailException(
                "Manifest endpoint keys are required to decide whether to deploy the services."
            )

        services_to_deploy = ServiceLifecycleTaskGroup.resolve_services_to_deploy(
            lifecycle,
            context["service_keys"],
            context["manifest_endpoint_keys"],
        )

        if not startup:
            raise AirflowFailException(
                "deployment_decision is only used for service startup; "
                "cleanup always runs best-effort."
            )

        if not services_to_deploy:
            # All services external — skip deployment
            return [f"{ServiceLifecycleTaskGroup.STARTUP_GROUP_ID}.skip_startup"]

        # Branch into prepare tasks; holders + deploy run downstream.
        return [
            (
                f"{ServiceLifecycleTaskGroup.STARTUP_GROUP_ID}."
                + ServiceLifecycleTaskGroup.get_prepare_slot_holds_task_name(component_key)
            )
            for component_key in services_to_deploy
        ]

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
                "service_keys": [spec.component_key for spec in self.service_specs],
                "manifest_endpoint_keys": list(
                    self.builder.manifest.deployment.components.endpoints.keys()
                ),
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

        Flow: startup_decision -> prepare_slot_holds -> [hold_slots.expand | deploy] | skip_startup

        Mapped ``hold_slots`` tasks occupy ``profile.pool_slots`` each (length = replicas) on the
        GPU pool. Deploy runs on ``default_pool`` and waits for those holds before creating
        capacity. Replica count comes from ``service_lifecycle.<service>.replicas`` (default 1)
        and is passed as ``replicas`` to both K8s and NVCF endpoint operators (NVCF maps it to
        min_instances = max_instances on the deployment API).
        """
        with TaskGroup(group_id=self.STARTUP_GROUP_ID) as task_group:
            startup_decision = self.get_startup_decision_task()
            prepare_tasks = []
            manifest_endpoints = self.builder.manifest.deployment.components.endpoints
            for spec in self.service_specs:
                component_key = spec.component_key
                component_value = manifest_endpoints.get(component_key)
                if component_value is None:
                    continue
                component_secrets = [
                    f"{key}:{value}" for key, value in (component_value.secrets or {}).items()
                ]
                replicas = self.service_config_field_template(component_key, "replicas")
                pool_name, pool_slots = self._endpoint_pool_settings(component_key)
                shutdown_tid = self.shutdown_xcom_task_id(component_key)
                hold_task_id = ServiceLifecycleTaskGroup.get_hold_slots_task_name(component_key)
                # Full task id as stored on TaskInstance (includes TaskGroup prefix).
                hold_task_id_full = f"{self.STARTUP_GROUP_ID}.{hold_task_id}"

                prepare = PythonOperator(
                    task_id=ServiceLifecycleTaskGroup.get_prepare_slot_holds_task_name(
                        component_key
                    ),
                    python_callable=ServiceLifecycleTaskGroup.prepare_replica_indices,
                    op_kwargs={
                        "component_key": component_key,
                        "payload_task_id": self.payload_task_id,
                    },
                )
                prepare_tasks.append(prepare)

                holders = PoolSlotHoldOperator.partial(
                    task_id=hold_task_id,
                    pool=pool_name,
                    pool_slots=pool_slots,
                    defer_until_xcom_task_id=shutdown_tid,
                    defer_until_xcom_timeout_seconds=STARTUP_DEFER_TIMEOUT_SECONDS,
                ).expand(replica_index=XComArg(prepare))

                deploy = self.builder.build_endpoint(
                    component_key,
                    task_id=ServiceLifecycleTaskGroup.get_startup_task_name(component_key),
                    secrets=component_secrets,
                    replicas=replicas,
                    defer_until_xcom=self.defer_until_shutdown,
                    defer_until_xcom_task_id=shutdown_tid,
                    defer_until_xcom_timeout_seconds=STARTUP_DEFER_TIMEOUT_SECONDS,
                    # GPU capacity is reserved by mapped holders, not the deploy task.
                    pool=SERVICE_DEPLOY_CONTROL_POOL,
                    pool_slots=1,
                    slot_hold_task_id=hold_task_id_full,
                )
                prepare >> [holders, deploy]

            skip_startup = EmptyOperator(task_id="skip_startup")
            startup_decision >> [*prepare_tasks, skip_startup]
        return task_group

    def get_wait_for_services_tasks(
        self,
        task_group_id: str = STARTUP_GROUP_ID,
    ) -> dict[str, WaitForServiceXComOperator]:
        """
        Return wait tasks for the workflow-owned service specifications.

        If internal deployment is disabled for a service, its wait task exits without
        deferring.
        """
        # No timeout: wait tasks should defer until startup publishes endpoint XCom.
        common_kw = {
            "poll_interval_seconds": 10,
            "timeout_seconds": None,
            "service_lifecycle_config": self.service_lifecycle_config,
        }
        wait_tasks: dict[str, WaitForServiceXComOperator] = {}
        for spec in self.service_specs:
            endpoint_key = spec.component_key
            startup_task_id = (
                f"{task_group_id}.{ServiceLifecycleTaskGroup.get_startup_task_name(endpoint_key)}"
            )
            wait_tasks[endpoint_key] = WaitForServiceXComOperator(
                task_id=spec.wait_task_id,
                watch_task_id=startup_task_id,
                watch_key="endpoint",
                service_key=endpoint_key,
                **common_kw,
            )
        return wait_tasks

    def get_service_shutdown_task_group(self) -> TaskGroup:
        """
        Build cleanup tasks for every workflow service.

        Each ``shutdown_<service>`` is independently chainable. Cleanup is
        best-effort (skip if the service was never deployed).
        """
        with TaskGroup(group_id=self.SHUTDOWN_GROUP_ID) as task_group:
            services = []
            manifest_endpoint_keys = self.builder.manifest.deployment.components.endpoints.keys()
            for spec in self.service_specs:
                component_key = spec.component_key
                if component_key not in manifest_endpoint_keys:
                    continue
                startup_tid = (
                    f"{self.STARTUP_GROUP_ID}."
                    + ServiceLifecycleTaskGroup.get_startup_task_name(component_key)
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
            join_after_shutdown = EmptyOperator(
                task_id="join_after_shutdown",
                trigger_rule=TriggerRule.ALL_DONE,
            )
            if services:
                services >> join_after_shutdown
        return task_group

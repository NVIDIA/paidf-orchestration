# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Task Group for Cosmos Augmentation."""

import datetime
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import multistorageclient as msc
import yaml
from airflow.exceptions import AirflowFailException
from airflow.models.xcom_arg import XComArg
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import TaskGroup
from airflow.task.trigger_rule import TriggerRule

from dags.shared.utils.msc_utils import ensure_msc_configured


def _parse_yaml_mapping(config_body: Any, path: str, description: str) -> dict[str, Any]:
    try:
        loaded = yaml.safe_load(config_body)
    except yaml.YAMLError as exc:
        raise AirflowFailException(f"Failed to parse {description} at {path}: {exc}") from exc
    if not isinstance(loaded, dict):
        raise AirflowFailException(f"{description} at {path} must contain a YAML mapping")
    return loaded


@ensure_msc_configured
def _load_msc_yaml_mapping(path: str, description: str) -> dict[str, Any]:
    try:
        with msc.open(path, "r") as config_file:
            return _parse_yaml_mapping(config_file, path, description)
    except AirflowFailException:
        raise
    except Exception as exc:
        raise AirflowFailException(f"Failed to read {description} at {path}: {exc}") from exc


def load_cosmos_base_config(
    *,
    base_config_path: str | None,
    default_path: str | Path,
    description: str,
) -> dict[str, Any]:
    """Load an explicit MSC template, or fall back to the DAG-local template."""
    normalized_base_config_path = base_config_path.strip() if base_config_path else None
    if normalized_base_config_path:
        return _load_msc_yaml_mapping(normalized_base_config_path, description)

    local_path = str(default_path)
    try:
        with open(local_path, encoding="utf-8") as config_file:
            return _parse_yaml_mapping(config_file, local_path, description)
    except AirflowFailException:
        raise
    except Exception as exc:
        raise AirflowFailException(f"Failed to read {description} at {local_path}: {exc}") from exc


class CosmosTaskGroup:
    DEFAULT_VALIDATE_PAYLOAD_TASK_ID = "validate_payload"
    DEFAULT_INTERNAL_AUGMENTATION_POOL = "internal_image_edit_service_pool"
    DEFAULT_EXTERNAL_AUGMENTATION_POOL = "external_image_edit_service_pool"

    def __init__(
        self,
        builder=None,
        config_generation_callable: Callable[..., list[str]] | None = None,
        output_validation_callable: Callable[..., dict[str, Any]] | None = None,
        config_generation_op_kwargs: dict[str, Any] | None = None,
        output_validation_op_kwargs: dict[str, Any] | None = None,
        task_config: str = "{{ params.payload.cosmos }}",
        validate_payload_task_id: str = DEFAULT_VALIDATE_PAYLOAD_TASK_ID,
        internal_augmentation_pool: str = DEFAULT_INTERNAL_AUGMENTATION_POOL,
        external_augmentation_pool: str = DEFAULT_EXTERNAL_AUGMENTATION_POOL,
        group_id: str = "cosmos_augmentation",
    ):
        """
        Initialize the Cosmos Task Group.

        Args:
            builder: ComponentBuilder instance for building operators from manifest.
            config_generation_callable: DAG-specific callable that writes config files
                and returns a list of config file paths.
            output_validation_callable: DAG-specific callable that validates outputs
                and returns the normalized output XCom payload.
            config_generation_op_kwargs: Extra keyword arguments passed to the
                config generation callable.
            output_validation_op_kwargs: Extra keyword arguments passed to the
                output validation callable.
            task_config: Cosmos task config payload (templated from DAG params).
            validate_payload_task_id: Task that publishes validated payload XCom.
            internal_augmentation_pool: Airflow pool for internal image-edit augmentation.
            external_augmentation_pool: Airflow pool for external image-edit augmentation.
        """
        if config_generation_callable is None:
            raise ValueError("config_generation_callable is required")
        if output_validation_callable is None:
            raise ValueError("output_validation_callable is required")
        self.builder = builder
        self.config_generation_callable = config_generation_callable
        self.output_validation_callable = output_validation_callable
        self.config_generation_op_kwargs = config_generation_op_kwargs or {}
        self.output_validation_op_kwargs = output_validation_op_kwargs or {}
        self.task_config = task_config
        self.validate_payload_task_id = validate_payload_task_id
        self.internal_augmentation_pool = internal_augmentation_pool
        self.external_augmentation_pool = external_augmentation_pool
        self.group_id = group_id

    def select_augmentation_pool(self, **context) -> list[str]:
        """Branch to internal or external augmentation based on external_services."""
        payload = (
            context["ti"].xcom_pull(
                task_ids=self.validate_payload_task_id,
                key="return_value",
            )
            or {}
        )
        if payload.get("external_services", True):
            logging.info(
                "External image-edit mode; routing to %s",
                self.external_augmentation_pool,
            )
            return [f"{self.group_id}.augmentation_external"]
        logging.info(
            "Internal image-edit mode; routing to %s",
            self.internal_augmentation_pool,
        )
        return [f"{self.group_id}.augmentation_internal"]

    def generate_cosmos_configs(
        self,
        cosmos_config: str | None = None,
        run_id: str = "",
        **context,
    ) -> list[str]:
        """Run the DAG-specific config generator and return mapped container args."""
        config_paths = self.config_generation_callable(
            cosmos_config=cosmos_config,
            run_id=run_id,
            **self.config_generation_op_kwargs,
            **context,
        )
        if not config_paths:
            raise AirflowFailException("Config generation produced no config paths")

        logging.info("Generated %d Cosmos config path(s)", len(config_paths))
        return [f"--config {config_path}" for config_path in config_paths]

    def get_generate_cosmos_configs_task(self) -> PythonOperator:
        """Get the operator for generating the Cosmos configuration files."""
        generate_cosmos_configs = PythonOperator(
            task_id="generate_cosmos_configs",
            python_callable=self.generate_cosmos_configs,
            op_kwargs={
                "cosmos_config": self.task_config,
                "run_id": "{{ run_id }}",
            },
        )

        generate_cosmos_configs.trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        return generate_cosmos_configs

    def validate_cosmos_outputs(
        self,
        cosmos_config: str | None = None,
        run_id: str = "",
        **context,
    ) -> dict[str, Any]:
        """Run the DAG-specific output validator and return its XCom payload."""
        return self.output_validation_callable(
            cosmos_config=cosmos_config,
            run_id=run_id,
            **self.output_validation_op_kwargs,
            **context,
        )

    def get_validate_cosmos_outputs_task(self) -> PythonOperator:
        """Get the validation task that checks cosmos outputs exist."""
        validate_outputs = PythonOperator(
            task_id="validate_outputs",
            python_callable=self.validate_cosmos_outputs,
            op_kwargs={
                "cosmos_config": self.task_config,
                "run_id": "{{ run_id }}",
            },
            retries=0,
        )
        validate_outputs.trigger_rule = TriggerRule.ALL_DONE_MIN_ONE_SUCCESS
        return validate_outputs

    def get_augmentation_tasks(self, generate_configs_task: PythonOperator):
        """
        Get internal and external mapped augmentation tasks with static pool names.

        MappedOperator cannot template ``pool``; a branch selects which task group runs.
        """
        if self.builder is None:
            raise ValueError(
                "ComponentBuilder is required for CosmosTaskGroup.get_augmentation_tasks(). "
                "Pass a builder to CosmosTaskGroup.__init__()."
            )

        task_name = (
            "cosmos-augmentation-pipeline-{{ run_id | replace(':', '_') "
            "| replace('+', '_') | replace('.', '_') }}"
        )
        expand_args = XComArg(generate_configs_task)

        augmentation_external = self.builder.partial_task(
            component_name="augmentation",
            task_id="augmentation_external",
            pool=self.external_augmentation_pool,
            name=task_name,
            retry_delay=datetime.timedelta(seconds=15),
        ).expand(container_args=expand_args)

        augmentation_internal = self.builder.partial_task(
            component_name="augmentation",
            task_id="augmentation_internal",
            pool=self.internal_augmentation_pool,
            name=task_name,
            retry_delay=datetime.timedelta(seconds=15),
        ).expand(container_args=expand_args)

        return augmentation_external, augmentation_internal

    def get_cosmos_augmentation_task_group(self) -> TaskGroup:
        """Get the Task Group needed for the Cosmos Augmentation step."""

        with TaskGroup(group_id=self.group_id) as task_group:
            generate_cosmos_configs = self.get_generate_cosmos_configs_task()
            select_augmentation_pool = BranchPythonOperator(
                task_id="select_augmentation_pool",
                python_callable=self.select_augmentation_pool,
            )
            augmentation_external, augmentation_internal = self.get_augmentation_tasks(
                generate_cosmos_configs
            )
            join_after_augmentation = EmptyOperator(
                task_id="join_after_augmentation",
                trigger_rule=TriggerRule.ALL_DONE,
            )
            validate_outputs = self.get_validate_cosmos_outputs_task()

            generate_cosmos_configs >> select_augmentation_pool
            select_augmentation_pool >> [augmentation_external, augmentation_internal]
            [augmentation_external, augmentation_internal] >> join_after_augmentation
            join_after_augmentation >> validate_outputs
        return task_group

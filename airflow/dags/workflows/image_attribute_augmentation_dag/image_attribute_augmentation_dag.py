# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Airflow DAG for Image Attribute Augmentation preprocessing before augmentation runs."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

from airflow.exceptions import AirflowConfigException, AirflowFailException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param, chain
from airflow.task.trigger_rule import TriggerRule

from dags.shared.task_groups.auto_labeling import AutoLabelingTaskGroup
from dags.shared.task_groups.cosmos import CosmosTaskGroup
from dags.shared.task_groups.input_preparation import InputPreparationTaskGroup
from dags.shared.task_groups.service_lifecycle import ServiceLifecycleTaskGroup
from dags.shared.task_groups.validate_payload import ValidatePayloadTaskGroup
from dags.shared.task_groups.validated_output import ValidatedOutputTaskGroup
from dags.shared.utils.component_builder import ComponentBuilder
from dags.shared.utils.dag_utils import (
    on_failure_callback,
    on_success_callback,
    resolve_dag_timeout,
)
from dags.workflows.image_attribute_augmentation_dag.callables import (
    generate_image_attribute_augmentation_auto_labeling_configs,
    generate_image_attribute_augmentation_image_edit_configs,
    prepare_image_attribute_augmentation_input,
    validate_image_attribute_augmentation_image_edit_outputs,
    validate_image_attribute_augmentation_pipeline_outputs,
)
from dags.workflows.image_attribute_augmentation_dag.models import ImageAttributeAugmentationDagPayloadConfig
from dags.workflows.image_attribute_augmentation_dag.tasks import (
    generate_augmented_dataset as generate_augmented_dataset_callable,
)

logger = logging.getLogger(__name__)

IMAGE_ATTRIBUTE_AUGMENTATION_CONFIG_DIR = Path(__file__).resolve().parent / "configs"


class ImageAttributeAugmentationDAGBuilder:
    """Builder class for platform-specific Image Attribute Augmentation DAGs."""

    def __init__(self, manifest_path: str | Path, logging_level: str = "INFO"):
        logging_level = os.environ.get("LOGGING_LEVEL", logging_level)
        logger.setLevel(getattr(logging, logging_level.upper(), logging.INFO))
        self.manifest_path = str(manifest_path)

        try:
            self.builder = ComponentBuilder(manifest_path=self.manifest_path)
        except FileNotFoundError as e:
            raise AirflowConfigException(f"Image Attribute Augmentation manifest not found at {self.manifest_path}") from e
        except Exception as e:
            raise AirflowConfigException(
                f"Failed to load Image Attribute Augmentation manifest from {self.manifest_path}: {e}"
            ) from e

    @staticmethod
    def _fail_pipeline() -> None:
        """Fail the DagRun when any upstream processing task failed."""
        raise AirflowFailException(
            "Pipeline failed — at least one upstream task is in failed state."
        )

    def build_dag(
        self,
        dag_id: str,
        description: str,
        tags: list[str] | None = None,
    ) -> DAG:
        dagrun_timeout = resolve_dag_timeout(self.builder.manifest.deployment.dag_timeout)

        with DAG(
            dag_id=dag_id,
            default_args={"owner": "NVIDIA", "depends_on_past": False},
            description=description,
            start_date=datetime(2026, 1, 1),
            dagrun_timeout=dagrun_timeout,
            schedule=None,
            catchup=False,
            max_active_runs=1,
            tags=tags or ["sdg", "image_attribute_augmentation", "preprocessing"],
            params={
                "payload": Param(
                    default={},
                    type="object",
                ),
            },
            on_success_callback=on_success_callback,
            on_failure_callback=on_failure_callback,
        ) as dag:
            validate_payload = ValidatePayloadTaskGroup(
                model_class=ImageAttributeAugmentationDagPayloadConfig
            ).get_validate_payload_task(task_id="validate_payload")

            input_preparation = InputPreparationTaskGroup(
                prepare_input_callable=prepare_image_attribute_augmentation_input,
            ).get_input_preparation_task_group()

            service_lifecycle = ServiceLifecycleTaskGroup(
                builder=self.builder,
                service_lifecycle_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('service_lifecycle', {}) | tojson }}",
            )

            wait_tasks = service_lifecycle.get_wait_for_services_tasks()
            wait_for_vlm = wait_tasks["vlm_service"]
            wait_for_llm = wait_tasks["llm_service"]
            wait_for_image_edit = wait_tasks["image_edit_service"]
            startup_tasks = service_lifecycle.get_service_startup_task_group()
            shutdown_tasks = service_lifecycle.get_service_shutdown_task_group()
            services_ready = EmptyOperator(task_id="services_ready")

            cosmos_augmentation = CosmosTaskGroup(
                builder=self.builder,
                config_generation_callable=generate_image_attribute_augmentation_image_edit_configs,
                output_validation_callable=validate_image_attribute_augmentation_image_edit_outputs,
                task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('cosmos', {}) }}",
            ).get_cosmos_augmentation_task_group()

            auto_labeling = AutoLabelingTaskGroup(
                builder=self.builder,
                input_xcom_task_id="cosmos_augmentation.validate_outputs",
                input_xcom_key="return_value",
                task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('auto_labeling', {}) }}",
                group_id="auto_labeling",
                prepare_args_callable=generate_image_attribute_augmentation_auto_labeling_configs,
            ).get_auto_labeling_task_group()

            generate_augmented_dataset = PythonOperator(
                task_id="generate_augmented_dataset",
                python_callable=generate_augmented_dataset_callable,
                op_kwargs={
                    "payload": "{{ ti.xcom_pull(task_ids='validate_payload', key='return_value') }}",
                    "run_id": "{{ run_id }}",
                },
            )

            fail_pipeline = PythonOperator(
                task_id="fail_pipeline",
                python_callable=self._fail_pipeline,
                trigger_rule=TriggerRule.ONE_FAILED,
                retries=0,
            )
            pipeline_success = EmptyOperator(
                task_id="pipeline_success",
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            )

            validated_output = ValidatedOutputTaskGroup(
                validation_callable=validate_image_attribute_augmentation_pipeline_outputs,
            ).get_validated_output_task_group()

            validate_payload >> input_preparation
            input_preparation >> startup_tasks

            validate_payload >> [wait_for_vlm, wait_for_llm, wait_for_image_edit]
            for task in [input_preparation, wait_for_vlm, wait_for_llm, wait_for_image_edit]:
                task >> services_ready
            services_ready >> cosmos_augmentation
            chain(
                cosmos_augmentation,
                auto_labeling,
                generate_augmented_dataset,
                validated_output,
            )

            # Gate only the last processing step so fail_pipeline observes the
            # full chain outcome through validated_output.
            # Shutdown branches off separately with ALL_DONE inside service_shutdown
            # so cleanup runs regardless of processing failures.
            validated_output >> [fail_pipeline, pipeline_success]
            validated_output >> shutdown_tasks

        return dag


def _create_image_attribute_augmentation_dag_if_manifest_exists(
    manifest_path: str | Path,
    platform: str,
    description: str,
    tags: list[str],
) -> DAG | None:
    manifest_path = Path(manifest_path)
    if not manifest_path.exists():
        logger.info(
            "Image Attribute Augmentation %s manifest does not exist at %s, skipping DAG generation", platform, manifest_path
        )
        return None

    try:
        return ImageAttributeAugmentationDAGBuilder(manifest_path=manifest_path).build_dag(
            dag_id=f"image_attribute_augmentation_dag_{platform}",
            description=description,
            tags=tags,
        )
    except Exception:
        logger.warning(
            "Failed to load Image Attribute Augmentation %s manifest at %s, skipping %s DAG generation",
            platform,
            manifest_path,
            platform,
            exc_info=True,
        )
        return None


# --- DAG Generation ---
# Each manifest produces its own DAG. Users see one DAG per platform in the Airflow UI.

_IMAGE_ATTRIBUTE_AUGMENTATION_K8S_MANIFEST = os.environ.get(
    "IMAGE_ATTRIBUTE_AUGMENTATION_K8S_MANIFEST_PATH", IMAGE_ATTRIBUTE_AUGMENTATION_CONFIG_DIR / "image_attribute_augmentation_k8s_manifest.yaml"
)
image_attribute_augmentation_dag_k8s = _create_image_attribute_augmentation_dag_if_manifest_exists(
    manifest_path=_IMAGE_ATTRIBUTE_AUGMENTATION_K8S_MANIFEST,
    platform="k8s",
    description="Run the Image Attribute Augmentation preprocessing workflow on Native Kubernetes",
    tags=["sdg", "kubernetes", "image_attribute_augmentation", "preprocessing"],
)

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Airflow DAG for Image Attribute Augmentation preprocessing before augmentation runs."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowConfigException, AirflowFailException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param, chain
from airflow.task.trigger_rule import TriggerRule

from dags.shared.task_groups.cosmos import CosmosTaskGroup
from dags.shared.task_groups.image_attribute_augmentation import (
    ImageAttributeAugmentationTaskGroup,
)
from dags.shared.task_groups.input_preparation import InputPreparationTaskGroup
from dags.shared.task_groups.reporting import PerformanceReportingTaskGroup
from dags.shared.task_groups.service_lifecycle import (
    ServiceLifecycleSpec,
    ServiceLifecycleTaskGroup,
)
from dags.shared.task_groups.validate_payload import ValidatePayloadTaskGroup
from dags.shared.task_groups.validated_output import ValidatedOutputTaskGroup
from dags.shared.utils.component_builder import ComponentBuilder
from dags.shared.utils.dag_utils import (
    on_failure_callback,
    on_success_callback,
    resolve_dag_timeout,
)
from dags.workflows.image_attribute_augmentation_dag.callables import (
    generate_image_attribute_augmentation_image_edit_configs,
    prepare_image_attribute_augmentation_input,
    validate_image_attribute_augmentation_image_edit_outputs,
    validate_image_attribute_augmentation_pipeline_outputs,
)
from dags.workflows.image_attribute_augmentation_dag.models import (
    ImageAttributeAugmentationDagPayloadConfig,
)
from dags.workflows.image_attribute_augmentation_dag.tasks import (
    CosmosPostProcessingTaskGroup,
)
from dags.workflows.image_attribute_augmentation_dag.tasks import (
    generate_augmented_dataset as generate_augmented_dataset_callable,
)
from dags.workflows.image_attribute_augmentation_dag.tasks.performance_reporting import (
    generate_image_attribute_augmentation_performance_report,
)

logger = logging.getLogger(__name__)

IMAGE_ATTRIBUTE_AUGMENTATION_CONFIG_DIR = Path(__file__).resolve().parent / "configs"
IMAGE_ATTRIBUTE_AUGMENTATION_ATTRIBUTE_SEARCH_CONFIG_PATH = (
    IMAGE_ATTRIBUTE_AUGMENTATION_CONFIG_DIR / "event_and_person_attribute_search_config.yaml"
)
IMAGE_ATTRIBUTE_AUGMENTATION_ATTRIBUTE_SEARCH_QUERY_PROMPT_PATH = (
    IMAGE_ATTRIBUTE_AUGMENTATION_CONFIG_DIR
    / "image_attribute_augmentation_synonymous_query_prompt.json"
)
IMAGE_ATTRIBUTE_AUGMENTATION_SERVICE_SPECS = (
    ServiceLifecycleSpec("vlm_service", "wait_for_vlm"),
    ServiceLifecycleSpec("llm_service", "wait_for_llm"),
    ServiceLifecycleSpec("image_edit_service", "wait_for_image_edit"),
)

# Group-qualified IDs of the tasks the performance report measures. They must match
# the task groups wired in build_dag(), which test_image_attribute_augmentation_dag
# asserts by resolving each one against the built DAG.
INPUT_PREPARATION_TASK_ID = "input_preparation.prepare_input"
COSMOS_VALIDATE_OUTPUTS_TASK_ID = "cosmos_augmentation.validate_outputs"
COSMOS_AUGMENTATION_TASK_IDS = [
    "cosmos_augmentation.augmentation_external",
    "cosmos_augmentation.augmentation_internal",
]
VALIDATED_OUTPUT_TASK_ID = "validated_output.validate_outputs"


class ImageAttributeAugmentationDAGBuilder:
    """Builder class for platform-specific Image Attribute Augmentation DAGs."""

    def __init__(self, manifest_path: str | Path, logging_level: str = "INFO"):
        logging_level = os.environ.get("LOGGING_LEVEL", logging_level)
        logger.setLevel(getattr(logging, logging_level.upper(), logging.INFO))
        self.manifest_path = str(manifest_path)

        try:
            self.builder = ComponentBuilder(manifest_path=self.manifest_path)
        except FileNotFoundError as e:
            raise AirflowConfigException(
                f"Image Attribute Augmentation manifest not found at {self.manifest_path}"
            ) from e
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
            default_args={
                "owner": "NVIDIA",
                "depends_on_past": False,
                "retry_delay": timedelta(seconds=30),
            },
            description=description,
            start_date=datetime(2026, 1, 1),
            dagrun_timeout=dagrun_timeout,
            schedule=None,
            catchup=False,
            tags=tags or ["sdg", "image_attribute_augmentation", "preprocessing"],
            params={
                "payload": Param(
                    default=ImageAttributeAugmentationDagPayloadConfig().model_dump(),
                    schema=ImageAttributeAugmentationDagPayloadConfig.model_json_schema(),
                    type="object",
                ),
            },
            on_success_callback=on_success_callback,
            on_failure_callback=on_failure_callback,
            max_active_runs=5,
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
                service_specs=IMAGE_ATTRIBUTE_AUGMENTATION_SERVICE_SPECS,
            )
            wait_tasks = service_lifecycle.get_wait_for_services_tasks()
            wait_for_vlm = wait_tasks["vlm_service"]
            wait_for_llm = wait_tasks["llm_service"]
            wait_for_image_edit = wait_tasks["image_edit_service"]
            startup_tasks = service_lifecycle.get_service_startup_task_group()
            shutdown_tasks = service_lifecycle.get_service_shutdown_task_group()
            shutdown_image_edit_id = service_lifecycle.shutdown_xcom_task_id("image_edit_service")
            late_shutdown_tasks = [
                dag.get_task(service_lifecycle.shutdown_xcom_task_id(component_key))
                for component_key in ("vlm_service", "llm_service")
                if dag.has_task(service_lifecycle.shutdown_xcom_task_id(component_key))
            ]
            services_ready = EmptyOperator(task_id="services_ready")

            cosmos_augmentation = CosmosTaskGroup(
                builder=self.builder,
                config_generation_callable=generate_image_attribute_augmentation_image_edit_configs,
                output_validation_callable=validate_image_attribute_augmentation_image_edit_outputs,
                task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('cosmos', {}) }}",
                internal_augmentation_pool="iaa_internal_image_edit_service_pool",
            ).get_cosmos_augmentation_task_group()

            cosmos_post_processing = None
            if "cosmos_post_processing" in self.builder.manifest.deployment.components.tasks:
                cosmos_post_processing = CosmosPostProcessingTaskGroup(
                    builder=self.builder,
                    task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('cosmos', {}) }}",
                ).get_cosmos_post_processing_task_group()

            event_and_person_attribute_search = ImageAttributeAugmentationTaskGroup(
                builder=self.builder,
                input_xcom_task_id=COSMOS_VALIDATE_OUTPUTS_TASK_ID,
                input_xcom_key="return_value",
                task_config="{{ (ti.xcom_pull(task_ids='validate_payload', key='return_value') or {}).get('event_and_person_attribute_search', {}) }}",
                group_id="event_and_person_attribute_search",
                output_group_id="auto_labeling",
                prepare_args_op_kwargs={
                    "config_template_path": str(
                        IMAGE_ATTRIBUTE_AUGMENTATION_ATTRIBUTE_SEARCH_CONFIG_PATH
                    ),
                    "query_prompt_file_path": str(
                        IMAGE_ATTRIBUTE_AUGMENTATION_ATTRIBUTE_SEARCH_QUERY_PROMPT_PATH
                    ),
                },
            ).get_image_attribute_augmentation_task_group()

            generate_augmented_dataset = PythonOperator(
                task_id="generate_augmented_dataset",
                python_callable=generate_augmented_dataset_callable,
                op_kwargs={
                    "payload": "{{ ti.xcom_pull(task_ids='validate_payload', key='return_value') }}",
                    "run_id": "{{ run_id }}",
                },
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
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

            performance_reporting = PerformanceReportingTaskGroup(
                report_generation_callable=generate_image_attribute_augmentation_performance_report,
                report_op_kwargs={
                    "completion_task_id": VALIDATED_OUTPUT_TASK_ID,
                    "workload_task_ids": {
                        "input": INPUT_PREPARATION_TASK_ID,
                        "successful_augmentations": COSMOS_VALIDATE_OUTPUTS_TASK_ID,
                        "final_dataset": generate_augmented_dataset.task_id,
                    },
                    "augmentation_stage_task_ids": COSMOS_AUGMENTATION_TASK_IDS,
                },
            ).get_performance_reporting_task_group()

            validate_payload >> input_preparation
            input_preparation >> startup_tasks

            validate_payload >> [wait_for_vlm, wait_for_llm, wait_for_image_edit]
            for task in [input_preparation, wait_for_vlm, wait_for_llm, wait_for_image_edit]:
                task >> services_ready
            services_ready >> cosmos_augmentation
            # Image-edit is only used by Cosmos; tear it down so VLM/LLM keep the GPUs
            # for post-processing and attribute search. OSMO has no cleanup operators.
            if dag.has_task(shutdown_image_edit_id):
                cosmos_augmentation >> dag.get_task(shutdown_image_edit_id)
            if cosmos_post_processing is not None:
                chain(
                    cosmos_augmentation,
                    cosmos_post_processing,
                    event_and_person_attribute_search,
                    generate_augmented_dataset,
                    validated_output,
                )
            else:
                chain(
                    cosmos_augmentation,
                    event_and_person_attribute_search,
                    generate_augmented_dataset,
                    validated_output,
                )

            # Reporting observes the run rather than taking part in it, so it hangs off
            # validated_output as its own terminal branch and is deliberately not upstream
            # of the outcome tasks: a broken report must not fail an otherwise good run,
            # nor delay pipeline_success. Keeping fail_pipeline and pipeline_success as
            # leaves also keeps them, not reporting, in charge of the DAG run's state.
            # VLM/LLM cleanup is chained by task so image-edit is not pulled back to
            # the end of the run (ALL_DONE on a TaskGroup root would wait for both).
            validated_output >> performance_reporting
            validated_output >> [fail_pipeline, pipeline_success]
            if late_shutdown_tasks:
                validated_output >> late_shutdown_tasks
            else:
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
            "Image Attribute Augmentation %s manifest does not exist at %s, skipping DAG generation",
            platform,
            manifest_path,
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
    "IMAGE_ATTRIBUTE_AUGMENTATION_K8S_MANIFEST_PATH",
    IMAGE_ATTRIBUTE_AUGMENTATION_CONFIG_DIR / "image_attribute_augmentation_k8s_manifest.yaml",
)
image_attribute_augmentation_dag_k8s = _create_image_attribute_augmentation_dag_if_manifest_exists(
    manifest_path=_IMAGE_ATTRIBUTE_AUGMENTATION_K8S_MANIFEST,
    platform="k8s",
    description="Run the Image Attribute Augmentation preprocessing workflow on Native Kubernetes",
    tags=["sdg", "kubernetes", "image_attribute_augmentation", "preprocessing"],
)

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Generate per-image Cosmos3 augmentation configs for Event Video Generation."""

import ast
import copy
import random
from pathlib import Path
from typing import Any

import yaml
from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.task_groups.cosmos import load_cosmos_base_config
from dags.shared.task_groups.input_preparation import require_prepared_input_from_xcom
from dags.shared.task_groups.service_lifecycle import require_service_endpoint_from_xcom
from dags.shared.utils.msc_utils import write_file_to_directory
from dags.workflows.event_video_generation_dag.models import EventVideoGenerationCosmosTaskConfig

EVENT_VIDEO_GENERATION_COSMOS_CONFIG_PATH = (
    Path(__file__).resolve().parents[1] / "configs" / "cosmos_config.yaml"
)
NIM_TO_OPENAI_RESOLUTION = {
    "720_16_9": "1280x720",
}


def _parse_config(value: str | dict[str, Any] | None) -> EventVideoGenerationCosmosTaskConfig:
    raw = ast.literal_eval(value) if isinstance(value, str) else value
    return EventVideoGenerationCosmosTaskConfig.model_validate(raw or {})


def _load_base_config(base_config_path: str | None = None) -> dict[str, Any]:
    return load_cosmos_base_config(
        base_config_path=base_config_path,
        default_path=EVENT_VIDEO_GENERATION_COSMOS_CONFIG_PATH,
        description="Event Video Generation Cosmos config",
    )


def _endpoint_by_role(config: dict[str, Any], role: str) -> dict[str, Any]:
    endpoints = config.get("endpoints")
    if not isinstance(endpoints, list):
        raise AirflowFailException("Event Video Generation Cosmos config endpoints must be a list")
    matches = [
        endpoint
        for endpoint in endpoints
        if isinstance(endpoint, dict) and endpoint.get("role") == role
    ]
    if len(matches) != 1:
        raise AirflowFailException(
            f"Event Video Generation Cosmos config must define exactly one endpoint with role {role!r}"
        )
    return matches[0]


def _sample_variables(
    task_config: EventVideoGenerationCosmosTaskConfig,
    sample_count: int,
) -> list[dict[str, str]]:
    rng = random.Random(42)
    distributions = task_config.variable_distribution.variables
    samples = []
    for _ in range(sample_count):
        samples.append(
            {
                variable: rng.choices(
                    list(distribution.root),
                    weights=list(distribution.root.values()),
                    k=1,
                )[0]
                for variable, distribution in distributions.items()
            }
        )
    return samples


def _set_data(
    config: dict[str, Any],
    *,
    input_path: str,
    output_base: str,
) -> None:
    config["data"] = [
        {
            "inputs": {"rgb": input_path},
            "output": {
                "video": f"{output_base}/output.mp4",
                "caption": f"{output_base}/caption.txt",
                "metadata": f"{output_base}/metadata.json",
            },
        }
    ]


def _set_variables(config: dict[str, Any], variables: dict[str, str]) -> None:
    captioning = config.get("captioning")
    llm = captioning.get("llm") if isinstance(captioning, dict) else None
    if not isinstance(llm, dict):
        raise AirflowFailException("Event Video Generation Cosmos config is missing captioning.llm")
    llm["variables"] = {name: [value] for name, value in variables.items()}


def _set_endpoints(
    config: dict[str, Any],
    task_config: EventVideoGenerationCosmosTaskConfig,
    **context,
) -> None:
    vlm = _endpoint_by_role(config, "vlm")
    llm = _endpoint_by_role(config, "llm")
    image2video = _endpoint_by_role(config, "image2video")

    if task_config.external_services:
        vlm_url = task_config.vlm_service_url
        llm_url = task_config.llm_service_url
        image2video_url = task_config.image2video_service_url
        image2video["api_key_env"] = "COSMOS_API_KEY"
        image2video.pop("model", None)
    else:
        ti = context["ti"]
        vlm_url = require_service_endpoint_from_xcom(ti, "vlm_service")
        llm_url = require_service_endpoint_from_xcom(ti, "llm_service")
        image2video_url = require_service_endpoint_from_xcom(ti, "image2video_service")
        image2video["model"] = task_config.image2video_model
        image2video.pop("api_key_env", None)

    vlm.update({"url": vlm_url, "model": task_config.vlm_model})
    llm.update({"url": llm_url, "model": task_config.llm_model})
    image2video["url"] = image2video_url


def _set_augmentation_contract(
    config: dict[str, Any],
    *,
    external_services: bool,
) -> None:
    """Translate NIM parameter names for the internal vLLM-Omni endpoint."""
    if external_services:
        return

    augmentation = config.get("augmentation")
    parameters = augmentation.get("parameters") if isinstance(augmentation, dict) else None
    if not isinstance(parameters, dict):
        raise AirflowFailException(
            "Event Video Generation Cosmos config is missing augmentation.parameters"
        )

    # The checked-in base config may already use the OpenAI-compatible contract.
    if {"size", "num_frames", "num_inference_steps"}.issubset(parameters):
        return

    resolution = parameters.pop("resolution", None)
    try:
        parameters["size"] = NIM_TO_OPENAI_RESOLUTION[resolution]
    except KeyError as e:
        raise AirflowFailException(
            f"Unsupported NIM resolution for internal vLLM-Omni: {resolution!r}"
        ) from e

    if "num_output_frames" not in parameters or "steps" not in parameters:
        raise AirflowFailException(
            "Internal vLLM-Omni requires num_output_frames and steps in the base config"
        )
    parameters["num_frames"] = parameters.pop("num_output_frames")
    parameters["num_inference_steps"] = parameters.pop("steps")
    parameters["extra_params"] = {
        "use_resolution_template": False,
        "use_duration_template": False,
        "guardrails": False,
    }


def generate_event_video_generation_cosmos_configs(
    cosmos_config: str | dict[str, Any] | None = None,
    run_id: str = "",
    **context,
) -> list[str]:
    """Generate one storage-backed config per input image and augmentation index."""
    try:
        task_config = _parse_config(cosmos_config)
        prepared = require_prepared_input_from_xcom(context["ti"])
        base_config = _load_base_config(task_config.base_config_path)
        output_root = str(URL(task_config.output_directory) / run_id / "cosmos")
        samples = _sample_variables(
            task_config,
            prepared.length * task_config.num_augmentation,
        )

        config_paths: list[str] = []
        sample_index = 0
        for prepared_image in prepared.videos:
            for augmentation_index in range(task_config.num_augmentation):
                generated = copy.deepcopy(base_config)
                output_base = f"{output_root}/{prepared_image.video_key}/{augmentation_index}"
                _set_data(
                    generated,
                    input_path=prepared_image.video_path,
                    output_base=output_base,
                )
                _set_variables(generated, samples[sample_index])
                _set_endpoints(generated, task_config, **context)
                _set_augmentation_contract(
                    generated,
                    external_services=task_config.external_services,
                )
                sample_index += 1

                config_path = f"{output_base}/config.yaml"
                write_file_to_directory(
                    config_path,
                    yaml.safe_dump(generated, sort_keys=False).encode("utf-8"),
                )
                config_paths.append(config_path)
        return config_paths
    except AirflowFailException:
        raise
    except Exception as e:
        raise AirflowFailException(
            f"Event Video Generation Cosmos config generation failed: {e}"
        ) from e

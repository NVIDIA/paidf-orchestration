# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Image Attribute Augmentation image-edit config generation callable for CosmosTaskGroup."""

import ast
import copy
import logging
import random
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import yaml
from airflow.exceptions import AirflowFailException
from yarl import URL

from dags.shared.models import ConditionalVariableConfig, CosmosTaskConfig
from dags.shared.task_groups.input_preparation import require_prepared_input_from_xcom
from dags.shared.task_groups.service_lifecycle import require_service_endpoint_from_xcom
from dags.shared.utils.msc_utils import write_file_to_directory

IMAGE_ATTRIBUTE_AUGMENTATION_CONFIG_DIR = Path(__file__).resolve().parents[1] / "configs"
IMAGE_ATTRIBUTE_AUGMENTATION_VERIFICATION_TEMPLATE_PATH = IMAGE_ATTRIBUTE_AUGMENTATION_CONFIG_DIR / "cosmos_config.yaml"


def _load_yaml_file(path: str, description: str) -> dict[str, Any]:
    try:
        with open(path, "r") as config_file:
            return yaml.safe_load(config_file)
    except FileNotFoundError as e:
        raise AirflowFailException(f"Failed to read {description} at {path}: {e}") from e
    except Exception as e:
        raise AirflowFailException(f"Failed to parse {description} at {path}: {e}") from e


def _normalize_distribution(distribution: dict[str, float]) -> dict[str, float]:
    total = sum(distribution.values())
    if abs(total - 1.0) > 0.01:
        return {key: value / total for key, value in distribution.items()}
    return distribution


def _sample_distribution(distribution: dict[str, float]) -> str:
    values = list(distribution.keys())
    probabilities = list(distribution.values())
    return random.choices(values, weights=probabilities, k=1)[0]


def _sample_variable_values(
    direct_config: dict[str, dict[str, float]],
    lookup_config: dict[str, dict[str, dict[str, float]]],
    conditional_variables: dict[str, ConditionalVariableConfig] | None,
    n_samples: int,
    seed: int = 42,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    random.seed(seed)
    conditional_variables = conditional_variables or {}

    samples: list[dict[str, str]] = []
    samples_base: list[dict[str, str]] = []
    for _ in range(n_samples):
        sample: dict[str, str] = {}
        sample_base: dict[str, str] = {}

        for var_name, dist in direct_config.items():
            base_value = _sample_distribution(_normalize_distribution(dist))
            resolved_value = base_value
            for lookup in lookup_config.values():
                if base_value in lookup:
                    resolved_value = _sample_distribution(
                        _normalize_distribution(lookup[base_value])
                    )
                    break
            sample[var_name] = resolved_value
            sample_base[var_name] = base_value

        for var_name, conditional in conditional_variables.items():
            parent = conditional.depends_on
            parent_value = sample[parent]
            dist = _normalize_distribution(conditional.distributions[parent_value].as_dict())
            value = _sample_distribution(dist)
            sample[var_name] = value
            sample_base[var_name] = value

        if sample.get("shoe_type") == "barefoot":
            sample["shoe_color"] = "none"
            sample_base["shoe_color"] = "none"

        samples.append(sample)
        samples_base.append(sample_base)

    return samples, samples_base


def _log_realized_distributions(samples: list[dict[str, str]]) -> None:
    realized_distributions = defaultdict(list)
    for sample in samples:
        for var_name, var_val in sample.items():
            realized_distributions[var_name].append(var_val)

    realized_counts = {var: dict(Counter(values)) for var, values in realized_distributions.items()}
    for var, counts in realized_counts.items():
        total = sum(counts.values())
        frequencies = {val: count / total for val, count in counts.items()}
        logging.info(
            "Realized distribution of variable '%s': counts=%s, frequencies=%s",
            var,
            counts,
            frequencies,
        )


def _build_image_edit_config(
    base_config: dict[str, Any],
    sampled_variables: dict[str, str],
    sampled_base_variables: dict[str, str],
    input_media_path: str,
    output_base: str,
    cosmos_task_config: CosmosTaskConfig,
) -> dict[str, Any]:
    config = copy.deepcopy(base_config)
    config["data"] = [
        {
            "inputs": {"rgb": input_media_path},
            "output": {
                "video": f"{output_base}/output.jpg",
                "caption": f"{output_base}/output.txt",
                "metadata": f"{output_base}/output_metadata.json",
            },
        }
    ]
    config["endpoints"] = {
        "vlm": {
            "url": cosmos_task_config.vlm_service_url,
            "model": cosmos_task_config.vlm_model,
        },
        "llm": {
            "url": cosmos_task_config.llm_service_url,
            "model": cosmos_task_config.llm_model,
        },
        "image_edit": {
            "url": cosmos_task_config.image_edit_service_url,
            "model": cosmos_task_config.image_edit_model,
        },
    }

    llm_cfg = (config.get("captioning") or {}).get("llm")
    if isinstance(llm_cfg, dict):
        llm_cfg["variables"] = {key: [value] for key, value in sampled_variables.items()}
        llm_cfg["verification_values"] = {
            key: [value] for key, value in sampled_base_variables.items()
        }

    config.setdefault("pipeline", {})
    config["pipeline"]["retry"] = 0
    return config


def generate_image_attribute_augmentation_image_edit_configs(
    cosmos_config: str | None = None,
    run_id: str = "",
    **context,
) -> list[str]:
    """Generate Image Attribute Augmentation image-edit configs and return their storage paths."""
    try:
        cosmos_task_config = CosmosTaskConfig.model_validate(ast.literal_eval(cosmos_config))
    except Exception as e:
        raise AirflowFailException(f"Payload validation failed: {e}") from e

    if not cosmos_task_config.external_services:
        cosmos_task_config.vlm_service_url = require_service_endpoint_from_xcom(
            context["ti"], "vlm_service"
        )
        cosmos_task_config.llm_service_url = require_service_endpoint_from_xcom(
            context["ti"], "llm_service"
        )
        cosmos_task_config.image_edit_service_url = require_service_endpoint_from_xcom(
            context["ti"], "image_edit_service"
        )

    prepared = require_prepared_input_from_xcom(context["ti"])
    base_config = _load_yaml_file(
        str(IMAGE_ATTRIBUTE_AUGMENTATION_VERIFICATION_TEMPLATE_PATH), "Image Attribute Augmentation image-edit base configuration file"
    )
    direct_config, lookup_config = cosmos_task_config.variable_distribution.split_variables()
    conditional_variables = cosmos_task_config.variable_distribution.conditional_variables

    total_configs = prepared.length * cosmos_task_config.num_augmentation
    if direct_config or lookup_config:
        samples, samples_base = _sample_variable_values(
            direct_config=direct_config,
            lookup_config=lookup_config,
            conditional_variables=conditional_variables,
            n_samples=total_configs,
            seed=42,
        )
        _log_realized_distributions(samples)
    else:
        samples = [{} for _ in range(total_configs)]
        samples_base = [{} for _ in range(total_configs)]

    output_base_path = str(URL(cosmos_task_config.output_directory) / run_id / "cosmos")
    config_paths = []
    sample_idx = 0
    for prepared_video in prepared.videos:
        for aug_idx in range(cosmos_task_config.num_augmentation):
            output_base = f"{output_base_path}/{prepared_video.video_key}/{aug_idx}"
            generated_config = _build_image_edit_config(
                base_config=base_config,
                sampled_variables=samples[sample_idx],
                sampled_base_variables=samples_base[sample_idx],
                input_media_path=prepared_video.video_path,
                output_base=output_base,
                cosmos_task_config=cosmos_task_config,
            )
            sample_idx += 1

            file_path = f"{output_base}/config.yaml"
            write_file_to_directory(file_path, yaml.dump(generated_config).encode("utf-8"))
            config_paths.append(file_path)

    return config_paths

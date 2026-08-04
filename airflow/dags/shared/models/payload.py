# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Models used to validate the payloads for the various DAGs.
"""

from typing import Annotated, Any, Optional, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    StrictFloat,
    StringConstraints,
    field_validator,
    model_serializer,
    model_validator,
)

DEFAULT_VLM_MODEL = "Qwen/Qwen3-VL-30B-A3B-Instruct-FP8"
DEFAULT_LLM_MODEL = "Qwen/Qwen2.5-14B-Instruct"
DEFAULT_IMAGE_EDIT_MODEL = "Qwen/Qwen-Image-Edit-2511"

NonEmptyString: TypeAlias = Annotated[str, StringConstraints(min_length=1, strict=True)]
NonNegativeWeight: TypeAlias = Annotated[StrictFloat, Field(ge=0)]


def default_variable_distribution() -> dict[str, Any]:
    """Return the deterministic Image Attribute Augmentation variable distribution used when omitted."""
    return {
        "variables": {
            "top_outer_color": {"black": 1.0},
            "top_outer_type": {"hoodie": 1.0},
            "bottom_type": {"jeans": 1.0},
            "bottom_color": {"blue": 1.0},
            "shoe_type": {"sneakers": 1.0},
            "shoe_color": {"white": 1.0},
        }
    }


class WeightedDistribution(RootModel[dict[NonEmptyString, NonNegativeWeight]]):
    """Weighted choices for one sampled value."""

    root: dict[NonEmptyString, NonNegativeWeight] = Field(min_length=1)

    @model_validator(mode="after")
    def check_positive_total_weight(self):
        if sum(self.root.values()) <= 0:
            raise ValueError("distribution must have a positive total weight")
        return self

    def as_dict(self) -> dict[str, float]:
        return dict(self.root)

    @model_serializer(mode="plain")
    def serialize_model(self) -> dict[str, float]:
        return self.as_dict()


class LookupDistribution(RootModel[dict[NonEmptyString, WeightedDistribution]]):
    """Weighted distributions selected by an intermediate lookup value."""

    root: dict[NonEmptyString, WeightedDistribution] = Field(min_length=1)

    def as_dict(self) -> dict[str, dict[str, float]]:
        return {
            value: distribution.as_dict() for value, distribution in self.root.items()
        }

    @model_serializer(mode="plain")
    def serialize_model(self) -> dict[str, dict[str, float]]:
        return self.as_dict()


VariableConfig: TypeAlias = WeightedDistribution | LookupDistribution


class ConditionalVariableConfig(BaseModel):
    """Conditional distribution for a variable sampled from a parent variable."""

    model_config = ConfigDict(extra="ignore")

    depends_on: NonEmptyString = Field(
        description="Sampled parent variable that determines which distribution to use.",
    )
    distributions: dict[NonEmptyString, WeightedDistribution] = Field(
        min_length=1,
        description="Distribution mappings keyed by sampled parent variable value.",
    )


class VariableDistribution(BaseModel):
    """Variable distributions sampled while generating Cosmos augmentation configs."""

    model_config = ConfigDict(extra="ignore")

    variables: dict[NonEmptyString, VariableConfig] = Field(
        default_factory=lambda: default_variable_distribution()["variables"],
        min_length=1,
        validate_default=True,
        description=(
            "Per-variable weighted distributions. A variable may either map values "
            "directly to weights, or map lookup values to nested weighted distributions."
        ),
    )
    conditional_variables: dict[NonEmptyString, ConditionalVariableConfig] = Field(
        default_factory=dict,
        description="Variables whose distribution depends on a previously sampled variable.",
    )

    @model_validator(mode="before")
    @classmethod
    def default_empty_distribution(cls, value: Any) -> Any:
        if value is None or (isinstance(value, dict) and not value):
            return default_variable_distribution()
        return value

    @field_validator("conditional_variables", mode="before")
    @classmethod
    def default_empty_conditional_variables(cls, value: Any) -> Any:
        if value is None:
            return {}
        return value

    @model_validator(mode="after")
    def validate_conditional_variables(self):
        possible_sampled_values = self.possible_sampled_values()
        for var_name, config in self.conditional_variables.items():
            parent = config.depends_on
            if parent not in possible_sampled_values:
                raise ValueError(
                    f"conditional variable '{var_name}' depends on unknown sampled "
                    f"variable '{parent}'"
                )

            missing_parent_values = possible_sampled_values[parent] - set(
                config.distributions
            )
            if missing_parent_values:
                missing = ", ".join(sorted(missing_parent_values))
                raise ValueError(
                    f"conditional variable '{var_name}' distributions missing "
                    f"values for {parent}: {missing}"
                )

        return self

    def split_variables(
        self,
    ) -> tuple[dict[str, dict[str, float]], dict[str, dict[str, dict[str, float]]]]:
        direct_config: dict[str, dict[str, float]] = {}
        lookup_config: dict[str, dict[str, dict[str, float]]] = {}
        for var_name, config in self.variables.items():
            if isinstance(config, LookupDistribution):
                lookup_config[var_name] = config.as_dict()
            else:
                direct_config[var_name] = config.as_dict()
        return direct_config, lookup_config

    def possible_sampled_values(self) -> dict[str, set[str]]:
        _direct_config, lookup_config = self.split_variables()
        lookup_distributions = list(lookup_config.values())
        possible_values_by_variable: dict[str, set[str]] = {}

        for var_name, config in self.variables.items():
            if isinstance(config, LookupDistribution):
                continue

            possible_values: set[str] = set()
            for base_value in config.root:
                resolved_values = {base_value}
                for lookup in lookup_distributions:
                    if base_value in lookup:
                        resolved_values = set(lookup[base_value])
                        break
                possible_values.update(resolved_values)
            possible_values_by_variable[var_name] = possible_values

        return possible_values_by_variable

    @model_serializer(mode="plain")
    def serialize_model(self) -> dict[str, Any]:
        serialized: dict[str, Any] = {
            "variables": {
                var_name: config.model_dump()
                for var_name, config in self.variables.items()
            }
        }
        if self.conditional_variables:
            serialized["conditional_variables"] = {
                var_name: config.model_dump()
                for var_name, config in self.conditional_variables.items()
            }
        return serialized


class AutoLabelingTaskConfig(BaseModel):
    """The input schema for auto labeling configuration in the payload."""

    tracker: str = Field(
        default="bytetrack",
        description="Tracker algorithm to use (bytetrack, botsort, deepocsort, etc.).",
    )
    threshold: float = Field(
        default=0.3,
        description="Detection threshold for object detection.",
        ge=0.0,
        le=1.0,
    )
    vlm_service_url: Optional[str] = Field(
        default=None,
        description="Direct URL for the VLM service (platform-agnostic). "
        "Preferred over vlm_service_function_id.",
    )
    llm_service_url: Optional[str] = Field(
        default=None,
        description="Direct URL for the LLM service (platform-agnostic). "
        "Preferred over llm_service_function_id.",
    )

    vlm_model: Optional[str] = Field(
        default=DEFAULT_VLM_MODEL,
        description="Model name for the VLM service.",
    )
    llm_model: Optional[str] = Field(
        default=DEFAULT_LLM_MODEL,
        description="Model name for the LLM service.",
    )
    external_services: Optional[bool] = Field(
        default=True,
        description="Per-task service mode switch; defaults to top-level external_services when omitted.",
    )
    output_directory: str = Field(
        description="Output directory for auto-labeling artifacts. Must match top-level output_directory.",
    )


class ServiceLifecycleServiceConfig(BaseModel):
    """Lifecycle settings for a single inference service (VLM or LLM)."""

    enabled: bool = Field(
        default=False,
        description=(
            "When true, deploy/wait for this internal service (external_services=false). "
            "Must be false when external_services is true."
        ),
    )
    replicas: int = Field(
        default=1,
        ge=1,
        description="Number of replicas to deploy for this service.",
    )


class ServiceLifecycleTaskConfig(BaseModel):
    """Per-service flags for internal inference endpoint deployment."""

    vlm_service: ServiceLifecycleServiceConfig = Field(
        default_factory=ServiceLifecycleServiceConfig,
        description="VLM service lifecycle settings.",
    )
    llm_service: ServiceLifecycleServiceConfig = Field(
        default_factory=ServiceLifecycleServiceConfig,
        description="LLM service lifecycle settings.",
    )
    image_edit_service: ServiceLifecycleServiceConfig = Field(
        default_factory=ServiceLifecycleServiceConfig,
        description="Image-edit service lifecycle settings.",
    )


class CosmosTaskConfig(BaseModel):
    """The input schema for the Cosmos augmentation.

    One or more augmentation runs are performed per input video.
    """

    model_config = ConfigDict(extra="ignore")

    vlm_service_url: Optional[str] = Field(
        default=None,
        description="Direct URL for the VLM service (platform-agnostic). "
        "Preferred over vlm_service_function_id.",
    )
    llm_service_url: Optional[str] = Field(
        default=None,
        description="Direct URL for the LLM service (platform-agnostic). "
        "Preferred over llm_service_function_id.",
    )
    image_edit_service_url: Optional[str] = Field(
        default=None,
        description="Direct URL for the image edit service.",
    )
    vlm_model: Optional[str] = Field(
        default=DEFAULT_VLM_MODEL,
        description="Model name for the VLM service.",
    )
    llm_model: Optional[str] = Field(
        default=DEFAULT_LLM_MODEL,
        description="Model name for the LLM service.",
    )
    image_edit_model: Optional[str] = Field(
        default=DEFAULT_IMAGE_EDIT_MODEL,
        description="Model name for the image edit service.",
    )
    num_augmentation: Optional[int] = Field(
        default=1,
        ge=1,
        description="Number of augmentation variants to generate per input video.",
    )
    external_services: Optional[bool] = Field(
        default=True,
        description="Per-task service mode switch; defaults to top-level external_services when omitted.",
    )
    output_directory: str = Field(
        description="Output directory for cosmos artifacts. Must match top-level output_directory.",
    )
    variable_distribution: VariableDistribution = Field(
        default_factory=VariableDistribution,
        validate_default=True,
        description=(
            "Variable distribution sampled once per generated Cosmos augmentation config. "
            "Image Attribute Augmentation reads this field directly. When omitted, this defaults to a "
            "deterministic single-image Image Attribute Augmentation distribution."
        ),
    )

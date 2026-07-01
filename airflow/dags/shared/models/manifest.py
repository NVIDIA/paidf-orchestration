# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Manifest YAML Models (for ComponentBuilder)

These models validate the new manifest YAML format used by ComponentBuilder.
This format supports reusable components and deployment profiles.
See: auto_labeling_nvcf_manifest.yaml for example format.
Pools are defined in Helm values (airflow.pools) and created at launch.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator


class DeploymentProfileConfig(BaseModel):
    """Configuration for a deployment profile."""

    operator: str = Field(
        ..., description="Operator class name (e.g., 'NVCFOperator', 'NVCFTaskOperator')"
    )
    cleanup_operator: Optional[str] = Field(
        default=None,
        description="Cleanup operator class name for endpoint teardown (e.g., 'NVCFCleanupOperator'). "
        "If not set, no cleanup task is created for this profile.",
    )
    configuration: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Operator configuration parameters (backend, gpu, instance_type, etc.)",
    )
    pool: str = Field(
        ...,
        description="Airflow pool name (e.g. h100_1, l40s_8). Pools are created at launch from Helm values. Required for all profiles.",
    )
    pool_slots: Optional[int] = Field(
        default=1,
        description="Number of pool slots this profile uses per task/endpoint (default 1).",
    )


class ComponentConfig(BaseModel):
    """Base configuration for a component (endpoint or task)."""

    enabled: bool = Field(
        default=True,
        description="Whether to enable the component",
    )
    container_image: str = Field(..., description="Container image URI")
    deployment_profile: str = Field(..., description="Name of the deployment profile to use")
    container_args: Optional[str] = Field(
        default=None,
        description="Container arguments/command to run",
    )
    configuration: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Component-specific configuration overrides",
    )
    secrets: Optional[Dict[str, str]] = Field(
        default=None,
        description="Secrets as key-value pairs (values can be Jinja templates)",
    )
    environment: Optional[Dict[str, str]] = Field(
        default=None,
        description="Environment variables as key-value pairs",
        alias="env",
    )
    models: Optional[List[str]] = Field(
        default=None,
        description=(
            "NGC models to mount at /config/models/<local_name> on NVCF, "
            "e.g. ['Qwen2.5-14B-Instruct:org/team/model:version']"
        ),
    )
    mount_model_cache: bool = Field(
        default=False,
        description=(
            "Deprecated alias for setting model_cache_pvc on the deployment profile. "
            "Prefer model_cache_pvc on the component or profile configuration."
        ),
    )
    model_cache_pvc: Optional[str] = Field(
        default=None,
        description=(
            "Helm-provisioned model-cache PVC name to mount at /config/models "
            "(e.g. ngc-model-cache). May also be set on the deployment profile "
            "configuration as model_cache_pvc."
        ),
    )

    class Config:
        populate_by_name = True  # Allow both 'environment' and 'env' aliases


class EndpointComponent(ComponentConfig):
    """Configuration for an endpoint component."""

    pass


class TaskComponent(ComponentConfig):
    """Configuration for a task component."""

    pass


class DeploymentComponents(BaseModel):
    """Components section of the deployment manifest."""

    endpoints: Dict[str, EndpointComponent] = Field(
        default_factory=dict,
        description="Dictionary of endpoint component definitions",
    )
    tasks: Dict[str, TaskComponent] = Field(
        default_factory=dict,
        description="Dictionary of task component definitions",
    )


class DeploymentSection(BaseModel):
    """Deployment section of the manifest."""

    dag_timeout: Optional[int] = Field(
        default=43200,
        description=("DAG run timeout in seconds. Used to set Airflow DAG dagrun_timeout."),
    )
    profiles: Dict[str, DeploymentProfileConfig] = Field(
        ...,
        description="Dictionary of deployment profile definitions",
    )
    components: DeploymentComponents = Field(
        ...,
        description="Component definitions (endpoints and tasks)",
    )


class ManifestConfig(BaseModel):
    """
    Top-level model for the deployment manifest YAML format.

    This validates the structure used by ComponentBuilder:

    ```yaml
    version: 0.0.1
    deployment:
      profiles:
        profile_name:
          operator: NVCFOperator
          configuration: {...}
      components:
        endpoints:
          component_name:
            container_image: ...
            deployment_profile: profile_name
            ...
        tasks:
          component_name:
            container_image: ...
            deployment_profile: profile_name
            ...
    ```
    """

    version: Optional[str] = Field(
        default=None,
        description="Manifest version (optional, for documentation)",
    )
    deployment: DeploymentSection = Field(
        ...,
        description="Deployment configuration section",
    )

    @model_validator(mode="after")
    def validate_profile_references(self):
        """Validate that all components reference existing profiles."""
        profile_names = set(self.deployment.profiles.keys())
        errors = []

        # Check endpoint profile references
        if self.deployment.components.endpoints:
            for comp_name, comp in self.deployment.components.endpoints.items():
                if comp.deployment_profile not in profile_names:
                    errors.append(
                        f"Endpoint '{comp_name}' references unknown profile '{comp.deployment_profile}'"
                    )

        # Check task profile references
        if self.deployment.components.tasks:
            for comp_name, comp in self.deployment.components.tasks.items():
                if comp.deployment_profile not in profile_names:
                    errors.append(
                        f"Task '{comp_name}' references unknown profile '{comp.deployment_profile}'"
                    )

        if errors:
            raise ValueError(
                "Manifest validation errors:\n" + "\n".join(f"  - {e}" for e in errors)
            )

        return self

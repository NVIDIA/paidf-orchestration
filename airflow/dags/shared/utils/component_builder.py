# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Component Builder for constructing Airflow operators from manifest configurations."""

import importlib
import inspect
import re
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import yaml
from airflow.exceptions import AirflowConfigException
from airflow.sdk import BaseOperator, TaskGroup

from dags.shared.models import (
    DeploymentProfileConfig,
    EndpointComponent,
    ManifestConfig,
    TaskComponent,
)
from dags.shared.utils.logging import setup_logger

# Operator class mapping for dynamic imports
# Supports both canonical names and short aliases
_OPERATOR_CLASS_MAP: Dict[str, str] = {
    # Kubernetes operators
    "K8sServiceOperator": "k8s_plugin.operators.k8s_service_operator.K8sServiceOperator",
    "K8sTaskOperator": "k8s_plugin.operators.k8s_task_operator.K8sTaskOperator",
    "K8sCleanupOperator": "k8s_plugin.operators.k8s_cleanup_operator.K8sCleanupOperator",
    # Short aliases
    "k8s_service": "k8s_plugin.operators.k8s_service_operator.K8sServiceOperator",
    "k8s_task": "k8s_plugin.operators.k8s_task_operator.K8sTaskOperator",
    "k8s_cleanup": "k8s_plugin.operators.k8s_cleanup_operator.K8sCleanupOperator",
}

# Operator kwargs that carry environment entries. Operators declare these as
# templated ``List[str]`` in ``"NAME:VALUE"`` form, but the builder holds them as
# name-keyed dicts internally so profile, component, and caller overrides merge
# per variable instead of replacing each other wholesale.
_ENV_KWARGS = ("container_environment_variables", "secrets")


class ComponentBuilder:
    """
    Builds Airflow operators from manifest YAML configurations.

    This class loads a manifest YAML file that defines:
    - Deployment profiles (operator configurations)
    - Components (endpoints and tasks) that reference profiles

    Components can override profile settings and use Jinja templates
    for dynamic values from Airflow Variables, Connections, etc.
    """

    def __init__(
        self,
        manifest_path: str,
        logging_level: str = "INFO",
    ):
        """
        Initialize the ComponentBuilder.

        Args:
            manifest_path: Path to the deployment manifest YAML file
            logging_level: Logging level for the builder
        """
        self.logger = setup_logger(__name__, logging_level)
        self.manifest_path = manifest_path

        # Load, validate, and store deployment manifest (pools are created at launch by Helm)
        raw_manifest = self._load_manifest()
        self.manifest = self._validate_manifest(raw_manifest)

        self.logger.info("Loaded manifest from %s", manifest_path)

    def _load_manifest(self) -> Dict[str, Any]:
        """Load and parse the manifest YAML file."""
        with open(self.manifest_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _validate_manifest(self, raw_manifest: Dict[str, Any]) -> ManifestConfig:
        """
        Validate manifest structure and return as Pydantic object.

        Args:
            raw_manifest: Raw dictionary from YAML file

        Returns:
            Validated ManifestConfig object

        Raises:
            ValueError: If manifest validation fails
        """
        try:
            manifest = ManifestConfig.model_validate(raw_manifest)
            self.logger.debug("Manifest validation passed")
            return manifest
        except Exception as e:
            self.logger.error("Manifest validation failed: %s. Stopping builder.", e)
            raise ValueError(f"Manifest validation failed: {e}") from e

    def _slugify(self, value: str) -> str:
        """Convert a string to a valid task_id (sanitize special characters)."""
        value = value.strip().replace("/", "_").replace(":", "_")
        value = re.sub(r"[^0-9a-zA-Z_]+", "_", value)
        value = re.sub(r"_+", "_", value)
        return value.strip("_") or "component"

    def _import_operator_class(self, name_or_alias: str) -> Type[BaseOperator]:
        """Resolve an operator class from a canonical name or alias."""
        import_path = _OPERATOR_CLASS_MAP.get(name_or_alias, name_or_alias)
        self.logger.debug("Importing operator class: %s from %s", name_or_alias, import_path)
        module_path, _, class_name = import_path.rpartition(".")
        if not module_path:
            raise ImportError(f"Invalid operator import path: {import_path}")
        module = importlib.import_module(module_path)
        return getattr(module, class_name)

    def _to_env_list(self, env_dict: Optional[Dict[str, str]]) -> List[str]:
        """
        Convert environment dict to ["KEY:VALUE", ...] format.

        Args:
            env_dict: Dictionary of environment variables or None

        Returns:
            List of "KEY:VALUE" strings, empty list if None

        Values containing Jinja templates are preserved as-is.
        """
        if env_dict is None:
            return []
        return [f"{k}:{v}" for k, v in env_dict.items()]

    @staticmethod
    def _to_env_dict(env: Union[Dict[str, Any], List[str], None]) -> Dict[str, Any]:
        """
        Normalize environment entries into a name-keyed dict.

        Accepts either a mapping or the operators' ``["NAME:VALUE", ...]`` list
        form. Only the first colon separates name from value, so values that
        themselves contain colons (URLs, Jinja expressions) survive intact.

        Args:
            env: Mapping, ``"NAME:VALUE"`` list, or None

        Returns:
            Dict keyed by variable name, empty if env is None
        """
        if env is None:
            return {}
        if isinstance(env, dict):
            return dict(env)
        result: Dict[str, Any] = {}
        for index, entry in enumerate(env):
            name, separator, value = str(entry).partition(":")
            if not separator or not name:
                raise AirflowConfigException(
                    f"Invalid environment entry at index {index}. "
                    'Expected "NAME:VALUE" with a non-empty name.'
                )
            result[name] = value
        return result

    @staticmethod
    def _resolve_execution_timeout(timeout_seconds: Any, component_name: str) -> timedelta:
        """
        Convert a manifest ``execution_timeout_seconds`` value into a timedelta.

        Args:
            timeout_seconds: Integer number of seconds from the manifest
            component_name: Component name, used for error messages

        Returns:
            Equivalent timedelta for Airflow's ``execution_timeout``

        Raises:
            AirflowConfigException: If the value is not a positive integer
        """
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int):
            raise AirflowConfigException(
                f"Invalid execution_timeout_seconds for component '{component_name}'. "
                "Expected an integer number of seconds."
            )
        if timeout_seconds <= 0:
            raise AirflowConfigException(
                f"Invalid execution_timeout_seconds for component '{component_name}'. "
                "Value must be greater than zero."
            )
        return timedelta(seconds=timeout_seconds)

    def _get_profile(self, profile_name: str) -> DeploymentProfileConfig:
        """Get a deployment profile by name."""
        if profile_name not in self.manifest.deployment.profiles:
            raise ValueError(f"Profile '{profile_name}' not found in manifest")
        return self.manifest.deployment.profiles[profile_name]

    def _deep_merge(
        self, base: Dict[str, Any], override: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Deep merge two configuration dictionaries.

        Nested dicts are merged recursively. Lists and other types are replaced
        (last write wins).
        """
        if not override:
            return dict(base)
        result = dict(base)
        for k, v in override.items():
            if k in result and isinstance(result[k], dict) and isinstance(v, dict):
                result[k] = self._deep_merge(result[k], v)
            else:
                result[k] = v
        return result

    def _merge_overrides(
        self,
        kwargs: Dict[str, Any],
        overrides: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Apply caller overrides to manifest-derived operator kwargs.

        Environment entries are normalized to name-keyed dicts on both sides so
        ``_deep_merge`` combines them per variable, then renders them back into
        the templated ``["NAME:VALUE", ...]`` lists expected by operators.

        Every other key retains ``_deep_merge`` semantics: nested dictionaries
        merge, while lists and scalar values are replaced.

        Args:
            kwargs: Operator kwargs built from the deployment profile and component
            overrides: Caller-supplied overrides, highest precedence

        Returns:
            Merged kwargs, with environment entries in operator list form
        """
        if overrides:
            overrides = dict(overrides)
            for env_key in _ENV_KWARGS:
                if env_key in overrides:
                    overrides[env_key] = self._to_env_dict(overrides[env_key])

        merged = self._deep_merge(kwargs, overrides)

        for env_key in _ENV_KWARGS:
            if env_key in merged:
                merged[env_key] = self._to_env_list(merged[env_key])
        return merged

    def _get_component(
        self, component_type: str, component_name: str
    ) -> Union[EndpointComponent, TaskComponent]:
        """Get a component definition by type and name."""
        # Access components through the Pydantic model
        if component_type == "endpoints":
            components = self.manifest.deployment.components.endpoints
        elif component_type == "tasks":
            components = self.manifest.deployment.components.tasks
        else:
            raise ValueError(f"Invalid component type: {component_type}")

        if component_name not in components:
            raise ValueError(
                f"Component '{component_name}' not found in {component_type} components"
            )

        return components[component_name]

    def _build_operator_kwargs(
        self,
        component: Union[EndpointComponent, TaskComponent],
        component_name: str,
        component_type: str,
        task_id: Optional[str] = None,
        container_args: Optional[str] = None,
        **overrides: Any,
    ) -> Tuple[Type[BaseOperator], Dict[str, Any]]:
        """
        Build operator kwargs from component definition.

        Returns:
            Tuple of (operator_class, kwargs_dict)
        """
        profile_name = component.deployment_profile
        profile = self._get_profile(profile_name)
        operator_name = profile.operator

        # Dynamically import operator class
        operator_cls = self._import_operator_class(operator_name)

        # Merge profile config with component config (deep merge for nested dicts)
        profile_config = profile.configuration or {}
        component_config = component.configuration or {}
        merged_config = self._deep_merge(profile_config, component_config)

        # Build operator kwargs
        kwargs: Dict[str, Any] = {
            "task_id": task_id or self._slugify(component_name),
            "name": self._slugify(component_name),
            **merged_config,  # Spread merged config
        }

        if component.model_cache_pvc:
            kwargs["model_cache_pvc"] = component.model_cache_pvc

        # Handle container_image
        kwargs["container_image"] = component.container_image

        # Handle container_args - use parameter if provided, otherwise use component definition
        if container_args is not None:
            kwargs["container_args"] = container_args
        elif component.container_args is not None:
            kwargs["container_args"] = component.container_args

        # Secrets and environment variables stay name-keyed until the operator is
        # built, so _deep_merge combines them per variable (see _ENV_KWARGS).
        if component.secrets:
            kwargs["secrets"] = self._to_env_dict(component.secrets)

        if component.environment:
            kwargs["container_environment_variables"] = self._to_env_dict(component.environment)

        if component.volumes:
            kwargs["volumes"] = list(component.volumes)

        # NGC model mounts for NVCF operators (K8s uses model_cache_pvc instead).
        if component.models:
            kwargs["models"] = component.models

        # Endpoint-specific: ensure inference_url exists (default if not provided)
        if component_type == "endpoint" and "inference_url" not in kwargs:
            kwargs["inference_url"] = f"/{self._slugify(component_name)}"

        # Assign to Airflow pool when profile has a pool
        if profile.pool:
            kwargs["pool"] = profile.pool
            kwargs["pool_slots"] = getattr(profile, "pool_slots", 1)
        else:
            raise AirflowConfigException(f"No pool found for profile {profile_name}")

        # Manifests express execution timeouts in seconds; Airflow needs a timedelta.
        # Resolve the manifest value before applying caller overrides so an explicit
        # caller execution_timeout retains highest precedence.
        timeout_seconds = kwargs.pop("execution_timeout_seconds", None)
        if timeout_seconds is not None:
            kwargs["execution_timeout"] = self._resolve_execution_timeout(
                timeout_seconds, component_name
            )

        # Apply caller overrides last (highest precedence).
        kwargs = self._merge_overrides(kwargs, overrides)

        kwargs = self._filter_operator_kwargs(operator_cls, kwargs)

        return operator_cls, kwargs

    def _filter_operator_kwargs(
        self, operator_cls: Type[BaseOperator], kwargs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Drop kwargs not accepted by the operator's ``__init__``."""
        sig = inspect.signature(operator_cls.__init__)
        accepted = {
            name
            for name, p in sig.parameters.items()
            if p.kind
            in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            )
        } - {"self"}

        base_params = {
            name
            for name, p in inspect.signature(BaseOperator.__init__).parameters.items()
            if p.kind
            in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            )
        } - {"self"}
        accepted |= base_params

        filtered = {k: v for k, v in kwargs.items() if k in accepted}
        dropped = set(kwargs) - set(filtered)
        if dropped:
            self.logger.debug("%s: dropping unsupported kwargs: %s", operator_cls.__name__, dropped)
        return filtered

    def build_endpoint(
        self,
        component_name: str,
        task_id: Optional[str] = None,
        **overrides: Any,
    ) -> BaseOperator:
        """
        Build an endpoint operator from a component definition.

        Args:
            component_name: Name of the endpoint component in the manifest
            task_id: Optional task_id override (defaults to component_name)
            **overrides: Additional parameters to override in the operator

        Returns:
            Operator instance configured from the manifest
        """
        component = self._get_component("endpoints", component_name)
        operator_cls, kwargs = self._build_operator_kwargs(
            component, component_name, "endpoint", task_id=task_id, **overrides
        )

        self.logger.info(
            "Building endpoint '%s' with profile '%s'",
            component_name,
            component.deployment_profile,
        )
        return operator_cls(**kwargs)

    def build_cleanup(
        self,
        component_name: str,
        task_id: Optional[str] = None,
        **overrides: Any,
    ) -> Optional[BaseOperator]:
        """
        Build a cleanup operator for an endpoint component.

        Reads the ``cleanup_operator`` from the endpoint's deployment profile.
        Returns None if the profile does not define a cleanup operator.

        Args:
            component_name: Name of the endpoint component in the manifest
            task_id: Optional task_id override
            **overrides: Additional parameters passed to the cleanup operator

        Returns:
            Cleanup operator instance, or None if no cleanup_operator is defined.
        """
        component = self._get_component("endpoints", component_name)
        profile = self._get_profile(component.deployment_profile)

        if not profile.cleanup_operator:
            self.logger.info(
                "No cleanup_operator defined for profile '%s', skipping cleanup for '%s'",
                component.deployment_profile,
                component_name,
            )
            return None

        cleanup_cls = self._import_operator_class(profile.cleanup_operator)
        cleanup_kwargs: Dict[str, Any] = {
            "task_id": task_id or f"cleanup_{self._slugify(component_name)}",
        }

        # Forward cleanup-operator-supported keys from profile and endpoint config.
        merged_config = self._deep_merge(
            profile.configuration or {},
            component.configuration or {},
        )
        if merged_config:
            init_sig = inspect.signature(cleanup_cls.__init__)
            accepted_params = {
                name
                for name, param in init_sig.parameters.items()
                if name not in {"self", "kwargs"}
                and param.kind
                in {
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    inspect.Parameter.KEYWORD_ONLY,
                }
            }
            for key, value in merged_config.items():
                if key in accepted_params:
                    cleanup_kwargs[key] = value

        cleanup_kwargs.update(overrides)
        return cleanup_cls(**cleanup_kwargs)

    def partial_endpoint(
        self,
        component_name: str,
        task_id: str,
        **overrides: Any,
    ) -> BaseOperator:
        """
        Build a partial endpoint operator for dynamic task mapping (Airflow 2.8+).

        Usage:
            partial_op = builder.partial_endpoint("my_endpoint", task_id="endpoint")
            mapped_op = partial_op.expand(container_args=["arg1", "arg2", "arg3"])

        Args:
            component_name: Name of the endpoint component in the manifest
            task_id: Task ID for the mapped task
            **overrides: Additional parameters to override in the operator

        Returns:
            Partial operator instance ready for .expand() calls
        """
        component = self._get_component("endpoints", component_name)
        operator_cls, kwargs = self._build_operator_kwargs(
            component, component_name, "endpoint", task_id=task_id, **overrides
        )

        self.logger.info(
            "Building partial endpoint '%s' for dynamic task mapping",
            component_name,
        )
        return operator_cls.partial(**kwargs)

    def build_task(
        self,
        component_name: str,
        task_id: Optional[str] = None,
        container_args: Optional[str] = None,
        **overrides: Any,
    ) -> BaseOperator:
        """
        Build a task operator from a component definition.

        Args:
            component_name: Name of the task component in the manifest
            task_id: Optional task_id override (defaults to component_name)
            container_args: Optional container arguments (can be Jinja template)
            **overrides: Additional parameters to override in the operator

        Returns:
            Operator instance configured from the manifest
        """
        component = self._get_component("tasks", component_name)
        operator_cls, kwargs = self._build_operator_kwargs(
            component,
            component_name,
            "task",
            task_id=task_id,
            container_args=container_args,
            **overrides,
        )

        self.logger.info(
            "Building task '%s' with profile '%s' using %s - clusters=%s, gpu=%s, instance_type=%s",
            component_name,
            component.deployment_profile,
            operator_cls.__name__,
            kwargs.get("clusters"),
            kwargs.get("gpu"),
            kwargs.get("instance_type"),
        )
        return operator_cls(**kwargs)

    def partial_task(
        self,
        component_name: str,
        task_id: str,
        container_args: Optional[str] = None,
        **overrides: Any,
    ) -> BaseOperator:
        """
        Build a partial task operator for dynamic task mapping (Airflow 2.8+).

        Usage:
            partial_op = builder.partial_task("augmentation", task_id="aug")
            mapped_op = partial_op.expand(container_args=["arg1", "arg2", "arg3"])

        Args:
            component_name: Name of the task component in the manifest
            task_id: Task ID for the mapped task
            container_args: Optional base container arguments (can be expanded)
            **overrides: Additional parameters to override in the operator

        Returns:
            Partial operator instance ready for .expand() calls
        """
        component = self._get_component("tasks", component_name)
        operator_cls, kwargs = self._build_operator_kwargs(
            component,
            component_name,
            "task",
            task_id=task_id,
            container_args=container_args,
            **overrides,
        )

        self.logger.info(
            "Building partial task '%s' for dynamic task mapping",
            component_name,
        )
        return operator_cls.partial(**kwargs)

    def build_task_group(
        self,
        group_id: str,
        components: List[str],
        dependencies: Optional[Dict[str, List[str]]] = None,
        component_type: str = "tasks",
    ) -> TaskGroup:
        """
        Build a task group from a list of component names.

        Args:
            group_id: ID for the task group
            components: List of component names to include
            dependencies: Optional dict mapping component_name -> [dependencies]
            component_type: Type of components ("tasks" or "endpoints")

        Returns:
            TaskGroup with all components and dependencies configured
        """
        dependencies = dependencies or {}

        with TaskGroup(group_id=group_id) as task_group:
            built_components: Dict[str, Any] = {}

            # Build all components
            for component_name in components:
                if component_type == "endpoints":
                    built_components[component_name] = self.build_endpoint(component_name)
                else:
                    built_components[component_name] = self.build_task(component_name)

            # Set up dependencies
            for component_name, deps in dependencies.items():
                if component_name not in built_components:
                    self.logger.warning(
                        "Component '%s' in dependencies but not in components list",
                        component_name,
                    )
                    continue

                for dep_name in deps:
                    if dep_name not in built_components:
                        self.logger.warning(
                            "Dependency '%s' not found for component '%s'",
                            dep_name,
                            component_name,
                        )
                        continue

                    # Set dependency: dep >> component
                    built_components[dep_name] >> built_components[component_name]

        return task_group

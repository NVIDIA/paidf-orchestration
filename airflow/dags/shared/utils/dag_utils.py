# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared DAG utilities for callbacks and timeout resolution."""

from datetime import timedelta

import requests
from airflow.exceptions import AirflowConfigException

from dags.shared.utils.logging import setup_logger

logger = setup_logger(__name__)

DEFAULT_DAG_TIMEOUT_SECONDS = 12 * 60 * 60


def resolve_dag_timeout(dag_timeout_seconds: int | None) -> timedelta:
    """Parse manifest dag timeout in seconds."""
    if dag_timeout_seconds is None:
        return timedelta(seconds=DEFAULT_DAG_TIMEOUT_SECONDS)

    if not isinstance(dag_timeout_seconds, int) or isinstance(dag_timeout_seconds, bool):
        raise AirflowConfigException(
            "Invalid deployment.dag_timeout in manifest. Expected an integer number of seconds."
        )
    if dag_timeout_seconds <= 0:
        raise AirflowConfigException(
            "Invalid deployment.dag_timeout in manifest. Value must be greater than zero."
        )

    return timedelta(seconds=dag_timeout_seconds)


def on_success_callback(context):
    """
    Callback executed when a DAG run completes successfully.
    Reads the callback URL from the DAG run configuration.
    """
    dag_run = context.get("dag_run")
    dag = context.get("dag")

    # Get callback URL from DAG run configuration
    callback_url = None
    if dag_run and hasattr(dag_run, "conf") and dag_run.conf:
        callback_url = dag_run.conf.get("callback_success_url")

    if not callback_url:
        logger.warning("callback_success_url not found in DAG run configuration")
        return

    payload = {
        "dag_id": dag.dag_id if dag else "unknown",
        "run_id": dag_run.run_id,
        "logical_date": (str(dag_run.logical_date) if dag_run.logical_date else None),
        "state": "success",
    }

    try:
        response = requests.post(
            callback_url,
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        logger.info("SUCCESS callback sent to %s", callback_url)
    except Exception as e:
        logger.error("Failed to send SUCCESS callback to %s: %s", callback_url, str(e))


def on_failure_callback(context):
    """
    Callback executed when a DAG run fails.
    Reads the callback URL from the DAG run configuration.
    """
    dag_run = context.get("dag_run")
    dag = context.get("dag")
    exception = context.get("exception")

    # Get callback URL from DAG run configuration
    callback_url = None
    if dag_run and hasattr(dag_run, "conf") and dag_run.conf:
        callback_url = dag_run.conf.get("callback_failure_url")

    if not callback_url:
        logger.warning("callback_failure_url not found in DAG run configuration")
        return

    payload = {
        "dag_id": dag.dag_id if dag else "unknown",
        "run_id": dag_run.run_id,
        "logical_date": (str(dag_run.logical_date) if dag_run.logical_date else None),
        "state": "failed",
        "error_message": str(exception) if exception else "Unknown error",
    }

    try:
        response = requests.post(
            callback_url,
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        logger.info("FAILURE callback sent to %s", callback_url)
    except Exception as e:
        logger.error("Failed to send FAILURE callback to %s: %s", callback_url, str(e))

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Occupy Airflow pool slots for multi-replica service endpoints until shutdown."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from airflow.exceptions import AirflowException, AirflowRescheduleException
from airflow.sdk import BaseOperator
from triggers import XComWaitTrigger

# States that mean the holder has been admitted by the scheduler and is
# occupying pool slots (includeDeferred pools keep DEFERRED counted).
_ACQUIRED_STATES = ["running", "deferred"]


def count_acquired_slot_holds(
    *,
    ti: Any,
    dag_id: str,
    run_id: str,
    task_id: str,
) -> int:
    """
    Return how many mapped TIs for ``task_id`` currently hold pool slots.

    Uses the Task SDK execution API (``RuntimeTaskInstance.get_ti_count``); Airflow 3
    forbids direct ORM access from the task process.
    """
    return int(
        ti.get_ti_count(
            dag_id=dag_id,
            task_ids=[task_id],
            run_ids=[run_id],
            states=list(_ACQUIRED_STATES),
        )
    )


def ensure_pool_slot_holds(
    *,
    context: dict[str, Any],
    slot_hold_task_id: str | None,
    replicas: int | str | None,
    poll_interval_seconds: float = 10.0,
    log: Any = None,
) -> None:
    """
    Block deploy until mapped slot-hold tasks have acquired pool capacity.

    Raises ``AirflowRescheduleException`` when holds are not yet ready so the
    deploy task does not create K8s/NVCF capacity early.

    ``replicas`` is the number of mapped holder TIs that must be running/deferred
    (same value used for K8s Deployment replicas / NVCF instance count).
    """
    if not slot_hold_task_id:
        return

    expected = int(replicas) if replicas is not None else 0
    if expected < 1:
        return

    dag_id = context["dag"].dag_id
    run_id = context["dag_run"].run_id
    acquired = count_acquired_slot_holds(
        ti=context["ti"],
        dag_id=dag_id,
        run_id=run_id,
        task_id=slot_hold_task_id,
    )
    if acquired >= expected:
        if log is not None:
            log.info(
                "Pool slot holds ready: %s/%s on task %s",
                acquired,
                expected,
                slot_hold_task_id,
            )
        return

    try:
        poll_interval = float(poll_interval_seconds)
    except (TypeError, ValueError) as e:
        raise AirflowException(
            f"poll_interval_seconds must be a positive float, got {poll_interval_seconds!r}"
        ) from e
    if poll_interval <= 0:
        raise AirflowException(
            f"poll_interval_seconds must be a positive float, got {poll_interval_seconds!r}"
        )

    if log is not None:
        log.info(
            "Waiting for pool slot holds on %s: %s/%s acquired; rescheduling in %ss",
            slot_hold_task_id,
            acquired,
            expected,
            poll_interval,
        )
    raise AirflowRescheduleException(datetime.now(timezone.utc) + timedelta(seconds=poll_interval))


class PoolSlotHoldOperator(BaseOperator):
    """
    Occupy ``pool`` / ``pool_slots`` until a shutdown task publishes XCom.

    Used with dynamic task mapping so ``replicas`` mapped instances each hold
    the per-replica ``pool_slots`` from the manifest profile. Deploy operators
    wait for these holds before creating multi-replica capacity.
    """

    template_fields = (
        "replica_index",
        "defer_until_xcom_task_id",
        "defer_until_xcom_key",
    )

    def __init__(
        self,
        *,
        replica_index: int = 0,
        defer_until_xcom_task_id: str,
        defer_until_xcom_key: str = "function_id",
        defer_until_xcom_timeout_seconds: Optional[int] = None,
        defer_until_xcom_poll_interval_seconds: float = 30.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.replica_index = replica_index
        self.defer_until_xcom_task_id = defer_until_xcom_task_id
        self.defer_until_xcom_key = defer_until_xcom_key
        self.defer_until_xcom_timeout_seconds = defer_until_xcom_timeout_seconds
        self.defer_until_xcom_poll_interval_seconds = defer_until_xcom_poll_interval_seconds

    def execute_complete(
        self,
        context: dict[str, Any],
        event: Optional[dict[str, Any]] = None,
    ) -> None:
        if event and event.get("error"):
            raise AirflowException(event["error"])
        return None

    def execute(self, context: dict[str, Any]) -> None:
        self.log.info(
            "Holding pool=%s pool_slots=%s for replica_index=%s until %s pushes %s",
            self.pool,
            self.pool_slots,
            self.replica_index,
            self.defer_until_xcom_task_id,
            self.defer_until_xcom_key,
        )
        dag_run = context["dag_run"]
        trigger = XComWaitTrigger(
            dag_id=context["dag"].dag_id,
            run_id=dag_run.run_id,
            watch_task_id=self.defer_until_xcom_task_id,
            watch_key=self.defer_until_xcom_key,
            poll_interval_seconds=self.defer_until_xcom_poll_interval_seconds,
            timeout_seconds=(
                float(self.defer_until_xcom_timeout_seconds)
                if self.defer_until_xcom_timeout_seconds is not None
                else None
            ),
        )
        self.defer(trigger=trigger, method_name="execute_complete")

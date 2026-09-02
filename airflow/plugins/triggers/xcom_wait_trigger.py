# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Trigger that waits for an XCom message from another task in the same DAG run.

Reads XCom via SUPERVISOR_COMMS (triggerer async ``asend`` in production;
sync ``send`` in in-process DAG tests). Airflow 3 does not allow direct ORM access
from triggers.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

from airflow.models.xcom import XCom
from airflow.sdk.execution_time.comms import GetTICount, GetXCom, TICount, XComResult
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.state import TaskInstanceState

log = logging.getLogger(__name__)

# Terminal states in which the watched task will never publish the XCom we're waiting
# on. If the watched task lands here first, polling for its XCom would wait forever.
WATCHED_TASK_DEAD_END_STATES = [
    TaskInstanceState.FAILED,
    TaskInstanceState.UPSTREAM_FAILED,
    TaskInstanceState.SKIPPED,
    TaskInstanceState.REMOVED,
]


async def _supervisor_comms_request(msg: Any) -> Any:
    """Send a comms request via SUPERVISOR_COMMS without blocking the event loop."""
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    comms = SUPERVISOR_COMMS
    asend = getattr(comms, "asend", None)
    if asend is not None and inspect.iscoroutinefunction(asend):
        try:
            return await asend(msg)
        except NotImplementedError:
            pass
    return await asyncio.to_thread(comms.send, msg)


async def _xcom_get_one(
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    key: str,
) -> Any:
    """Read one XCom value via supervisor comms (no ORM)."""
    msg = await _supervisor_comms_request(
        GetXCom(
            key=key,
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            map_index=None,
        ),
    )
    if isinstance(msg, XComResult) and msg.value is not None:
        return XCom.deserialize_value(msg)
    return None


async def _watched_task_reached_dead_end(
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
) -> bool:
    """True if the watched task landed in a state that will never publish XCom."""
    msg = await _supervisor_comms_request(
        GetTICount(
            dag_id=dag_id,
            run_ids=[run_id],
            task_ids=[task_id],
            states=[state.value for state in WATCHED_TASK_DEAD_END_STATES],
        ),
    )
    if isinstance(msg, TICount):
        return msg.count > 0
    log.warning(
        "GetTICount returned unexpected response type %s for task %r; "
        "skipping dead-end check this cycle",
        type(msg).__name__,
        task_id,
    )
    return False


class XComWaitTrigger(BaseTrigger):
    """
    Trigger that polls until an XCom key is pushed by a task in the same DAG run.

    Fires when (watch_task_id, watch_key) is present.
    """

    def __init__(
        self,
        *,
        dag_id: str,
        run_id: str,
        watch_task_id: str,
        watch_key: str = "exit",
        poll_interval_seconds: float = 30.0,
        timeout_seconds: float | None = None,
    ) -> None:
        super().__init__()
        self.dag_id = dag_id
        self.run_id = run_id
        self.watch_task_id = watch_task_id
        self.watch_key = watch_key
        self.poll_interval_seconds = poll_interval_seconds
        self.timeout_seconds = timeout_seconds

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "triggers.xcom_wait_trigger.XComWaitTrigger",
            {
                "dag_id": self.dag_id,
                "run_id": self.run_id,
                "watch_task_id": self.watch_task_id,
                "watch_key": self.watch_key,
                "poll_interval_seconds": self.poll_interval_seconds,
                "timeout_seconds": self.timeout_seconds,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        start = time.monotonic()
        while True:
            # XCom is checked first so the success path never pays the extra
            # GetTICount round-trip. The race where XCom arrives between the
            # two calls cannot cause a false error: startup tasks only push XCom
            # then transition to DEFERRED (not a dead-end state), so both checks
            # will not simultaneously disagree in practice.
            value = await _xcom_get_one(
                dag_id=self.dag_id,
                run_id=self.run_id,
                task_id=self.watch_task_id,
                key=self.watch_key,
            )
            if value is not None:
                yield TriggerEvent({"xcom_value": value})
                return

            if await _watched_task_reached_dead_end(
                dag_id=self.dag_id,
                run_id=self.run_id,
                task_id=self.watch_task_id,
            ):
                yield TriggerEvent(
                    {
                        "error": (
                            f"{self.watch_task_id!r} reached a terminal state without "
                            f"publishing XCom key {self.watch_key!r}"
                        )
                    }
                )
                return

            if self.timeout_seconds is not None:
                elapsed = time.monotonic() - start
                if elapsed >= self.timeout_seconds:
                    yield TriggerEvent(
                        {
                            "error": f"Timeout after {self.timeout_seconds}s waiting for XCom(s)"
                        }
                    )
                    return

            await asyncio.sleep(self.poll_interval_seconds)

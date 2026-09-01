# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""DAG-agnostic timing and throughput aggregation over task-instance records."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

METRIC_PRECISION = 3


def parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def seconds_between(later: Any, earlier: Any) -> float | None:
    later_dt, earlier_dt = parse_datetime(later), parse_datetime(earlier)
    return (
        None
        if later_dt is None or earlier_dt is None
        else round(max((later_dt - earlier_dt).total_seconds(), 0.0), METRIC_PRECISION)
    )


def round_metric(value: float | int | None) -> float | None:
    return None if value is None else round(float(value), METRIC_PRECISION)


def normalize_task_instance(task: dict[str, Any]) -> dict[str, Any]:
    duration = task.get("duration")
    if duration is None:
        duration = seconds_between(task.get("end_date"), task.get("start_date"))
    return {
        "task_id": task.get("task_id"),
        "map_index": task.get("map_index", -1),
        "try_number": task.get("try_number"),
        "state": task.get("state"),
        "scheduled_at": task.get("scheduled_when"),
        "queued_at": task.get("queued_when"),
        "started_at": task.get("start_date"),
        "ended_at": task.get("end_date"),
        "scheduled_duration_seconds": seconds_between(
            task.get("queued_when"), task.get("scheduled_when")
        ),
        "queued_duration_seconds": seconds_between(task.get("start_date"), task.get("queued_when")),
        "task_duration_seconds": round_metric(duration),
        "pool": task.get("pool"),
        "queue": task.get("queue"),
        "hostname": task.get("hostname"),
    }


def _durations(instances: list[dict[str, Any]], key: str) -> list[float]:
    return [float(instance[key]) for instance in instances if instance.get(key) is not None]


def _timestamps(instances: Iterable[dict[str, Any]], key: str) -> list[datetime]:
    return [
        parsed
        for instance in instances
        if (parsed := parse_datetime(instance.get(key))) is not None
    ]


def metric_summary(values: list[float]) -> dict[str, float] | None:
    if not values:
        return None
    return {
        "minimum_seconds": round(min(values), METRIC_PRECISION),
        "maximum_seconds": round(max(values), METRIC_PRECISION),
        "average_seconds": round(sum(values) / len(values), METRIC_PRECISION),
        "total_seconds": round(sum(values), METRIC_PRECISION),
    }


def wall_span_seconds(instances: Iterable[dict[str, Any]]) -> float | None:
    instances = list(instances)
    starts, ends = _timestamps(instances, "started_at"), _timestamps(instances, "ended_at")
    return (
        None
        if not starts or not ends
        else round((max(ends) - min(starts)).total_seconds(), METRIC_PRECISION)
    )


def summarize_tasks(tasks: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for task in tasks:
        if task_id := task.get("task_id"):
            grouped[str(task_id)].append(task)
    result = {}
    for task_id, instances in sorted(grouped.items()):
        states: dict[str, int] = defaultdict(int)
        for instance in instances:
            states[str(instance.get("state") or "unknown")] += 1
        result[task_id] = {
            "instance_count": len(instances),
            "states": dict(sorted(states.items())),
            "task_duration": metric_summary(_durations(instances, "task_duration_seconds")),
            "queued_duration": metric_summary(_durations(instances, "queued_duration_seconds")),
            "scheduled_duration": metric_summary(
                _durations(instances, "scheduled_duration_seconds")
            ),
            "wall_span_seconds": wall_span_seconds(instances),
        }
    return result


def stage_wall_seconds(tasks: list[dict[str, Any]], task_ids: Iterable[str]) -> float | None:
    wanted = set(task_ids)
    return wall_span_seconds(
        task for task in tasks if task.get("task_id") in wanted and task.get("state") == "success"
    )


def latest_end_time(tasks: list[dict[str, Any]], task_id: str) -> datetime | None:
    ends = _timestamps((task for task in tasks if task.get("task_id") == task_id), "ended_at")
    return max(ends) if ends else None


def tasks_ending_by(
    tasks: list[dict[str, Any]], boundary: Any, excluded_task_ids: Iterable[str] = ()
) -> list[dict[str, Any]]:
    boundary_dt = parse_datetime(boundary)
    if boundary_dt is None:
        return []
    excluded = set(excluded_task_ids)
    return [
        task
        for task in tasks
        if task.get("task_id") not in excluded
        and (ended_at := parse_datetime(task.get("ended_at"))) is not None
        and ended_at <= boundary_dt
    ]


def rate(count: int | None, duration_seconds: float | None) -> dict[str, float] | None:
    if count is None or duration_seconds is None or duration_seconds <= 0:
        return None
    per_second = count / duration_seconds
    return {
        "items_per_second": round(per_second, 6),
        "items_per_minute": round(per_second * 60, METRIC_PRECISION),
        "items_per_hour": round(per_second * 3600, METRIC_PRECISION),
    }

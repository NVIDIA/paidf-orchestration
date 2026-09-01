# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Read-only Airflow 3 REST API client for orchestration reports."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from airflow.sdk import BaseHook

DEFAULT_CONN_ID = "airflow_api_default"
DEFAULT_PAGE_LIMIT = 100
DEFAULT_TIMEOUT_SECONDS = 10.0


class AirflowApiError(RuntimeError):
    """Raised when the Airflow REST API cannot be reached or returns an error."""


def _request_json(
    url: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    encoded_body = json.dumps(body).encode("utf-8") if body is not None else None
    request_headers = {"Accept": "application/json", **(headers or {})}
    if encoded_body is not None:
        request_headers["Content-Type"] = "application/json"
    request = Request(url, data=encoded_body, headers=request_headers, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:  # noqa: S310 - configured Airflow URL
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as e:
        raise AirflowApiError(
            f"Airflow API returned HTTP {e.code}: {e.read().decode('utf-8', errors='replace')[:500]}"
        ) from e
    except URLError as e:
        raise AirflowApiError(f"Airflow API request failed: {e.reason}") from e


def base_url_from_connection(conn: Any) -> str:
    host = (conn.host or "").strip().rstrip("/")
    if not host:
        raise AirflowApiError(f"Airflow API connection '{conn.conn_id}' has no host")
    if "://" not in host:
        host = f"{conn.schema or 'https'}://{host}"
    return f"{host}:{conn.port}" if conn.port and not host.rsplit(":", 1)[-1].isdigit() else host


def auth_headers_from_connection(base_url: str, conn: Any, timeout: float) -> dict[str, str]:
    token = (conn.extra_dejson or {}).get("token")
    if not token:
        if not conn.login or not conn.password:
            raise AirflowApiError(
                f"Airflow API connection '{conn.conn_id}' needs a login and password, or a token in its extra field"
            )
        token = _request_json(
            f"{base_url}/auth/token",
            method="POST",
            body={"username": conn.login, "password": conn.password},
            timeout=timeout,
        ).get("access_token")
        if not token:
            raise AirflowApiError("Airflow token response did not contain access_token")
    return {"Authorization": f"Bearer {token}"}


@dataclass(frozen=True)
class AirflowApiClient:
    base_url: str
    auth_headers: dict[str, str] = field(repr=False)
    timeout: float = DEFAULT_TIMEOUT_SECONDS
    page_limit: int = DEFAULT_PAGE_LIMIT

    @classmethod
    def from_connection(
        cls,
        conn_id: str = DEFAULT_CONN_ID,
        page_limit: int = DEFAULT_PAGE_LIMIT,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> "AirflowApiClient":
        try:
            conn = BaseHook.get_connection(conn_id)
        except Exception as e:
            raise AirflowApiError(f"Airflow API connection '{conn_id}' is unavailable: {e}") from e
        base_url = base_url_from_connection(conn)
        return cls(
            base_url, auth_headers_from_connection(base_url, conn, timeout), timeout, page_limit
        )

    def _dag_run_url(self, dag_id: str, run_id: str) -> str:
        return (
            f"{self.base_url}/api/v2/dags/{quote(dag_id, safe='')}/dagRuns/{quote(run_id, safe='')}"
        )

    def get_dag_run(self, dag_id: str, run_id: str) -> dict[str, Any]:
        return _request_json(
            self._dag_run_url(dag_id, run_id), headers=self.auth_headers, timeout=self.timeout
        )

    def list_task_instances(self, dag_id: str, run_id: str) -> list[dict[str, Any]]:
        run_url, instances, offset = self._dag_run_url(dag_id, run_id), [], 0
        while True:
            response = _request_json(
                f"{run_url}/taskInstances?{urlencode({'limit': self.page_limit, 'offset': offset})}",
                headers=self.auth_headers,
                timeout=self.timeout,
            )
            page = response.get("task_instances") or []
            if not isinstance(page, list):
                raise AirflowApiError("Airflow task-instances response was not a list")
            instances.extend(page)
            offset += len(page)
            if (
                not page
                or len(page) < self.page_limit
                or (
                    isinstance(response.get("total_entries"), int)
                    and offset >= response["total_entries"]
                )
            ):
                return instances

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared trigger modules for Airflow plugins."""

from triggers.xcom_wait_trigger import XComWaitTrigger

__all__ = ["XComWaitTrigger"]

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from airflow.plugins_manager import AirflowPlugin


class K8sPlugin(AirflowPlugin):
    name = "k8s_plugin"

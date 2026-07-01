#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Shared S3 configuration helpers for setup and artifact upload.

# Optional shorthand variables. When set (via secrets.env or the environment),
# fill any matching per-purpose setting that is still empty.
S3_UNIFIED_OVERRIDE_VARS=(
  AWS_S3_BUCKET
  AWS_S3_REGION
  AWS_S3_ACCESS_KEY_ID
  AWS_S3_SECRET_ACCESS_KEY
)

apply_s3_unified_overrides() {
  if [ -n "${AWS_S3_BUCKET:-}" ]; then
    : "${AWS_S3_DAG_BUCKET:=$AWS_S3_BUCKET}"
    : "${AWS_S3_INPUT_BUCKET:=$AWS_S3_BUCKET}"
    : "${AWS_S3_OUTPUT_BUCKET:=$AWS_S3_BUCKET}"
  fi

  if [ -n "${AWS_S3_REGION:-}" ]; then
    : "${AWS_S3_DAG_REGION:=$AWS_S3_REGION}"
    : "${AWS_S3_INPUT_REGION:=$AWS_S3_REGION}"
    : "${AWS_S3_OUTPUT_REGION:=$AWS_S3_REGION}"
  fi

  if [ -n "${AWS_S3_ACCESS_KEY_ID:-}" ]; then
    : "${AWS_S3_DAG_ACCESS_KEY_ID:=$AWS_S3_ACCESS_KEY_ID}"
    : "${AWS_S3_INPUT_ACCESS_KEY_ID:=$AWS_S3_ACCESS_KEY_ID}"
    : "${AWS_S3_OUTPUT_ACCESS_KEY_ID:=$AWS_S3_ACCESS_KEY_ID}"
  fi

  if [ -n "${AWS_S3_SECRET_ACCESS_KEY:-}" ]; then
    : "${AWS_S3_DAG_SECRET_ACCESS_KEY:=$AWS_S3_SECRET_ACCESS_KEY}"
    : "${AWS_S3_INPUT_SECRET_ACCESS_KEY:=$AWS_S3_SECRET_ACCESS_KEY}"
    : "${AWS_S3_OUTPUT_SECRET_ACCESS_KEY:=$AWS_S3_SECRET_ACCESS_KEY}"
  fi

  export AWS_S3_DAG_BUCKET AWS_S3_INPUT_BUCKET AWS_S3_OUTPUT_BUCKET
  export AWS_S3_DAG_REGION AWS_S3_INPUT_REGION AWS_S3_OUTPUT_REGION
  export AWS_S3_DAG_ACCESS_KEY_ID AWS_S3_INPUT_ACCESS_KEY_ID AWS_S3_OUTPUT_ACCESS_KEY_ID
  export AWS_S3_DAG_SECRET_ACCESS_KEY AWS_S3_INPUT_SECRET_ACCESS_KEY AWS_S3_OUTPUT_SECRET_ACCESS_KEY
}

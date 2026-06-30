#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Upload Airflow DAGs and packaged runtime dependencies to S3 so Kubernetes pods
# can sync them without relying on node-local hostPath mounts.

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/artifact_fingerprint.sh"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/s3_config.sh"

LOCAL_SECRETS_ENV_FILE="${LOCAL_SECRETS_ENV_FILE:-$REPO_ROOT/secrets.env}"

if [ -f "$LOCAL_SECRETS_ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$LOCAL_SECRETS_ENV_FILE"
  set +a
fi
apply_s3_unified_overrides

AIRFLOW_PYTHON_DEPS_DIR="${AIRFLOW_PYTHON_DEPS_DIR:-$REPO_ROOT/.airflow-python-deps}"
AIRFLOW_DAGS_DIR="${AIRFLOW_DAGS_DIR:-$REPO_ROOT/airflow/dags}"
AIRFLOW_PLUGINS_DIR="${AIRFLOW_PLUGINS_DIR:-$REPO_ROOT/airflow/plugins}"
AWS_S3_DAG_BUCKET="${AWS_S3_DAG_BUCKET:-sdg-workflow-eks-test}"
AIRFLOW_DEPS_S3_URI="${AIRFLOW_DEPS_S3_URI:-s3://${AWS_S3_DAG_BUCKET}/deps/}"
AIRFLOW_DAGS_S3_URI="${AIRFLOW_DAGS_S3_URI:-s3://${AWS_S3_DAG_BUCKET}/dags/}"
AIRFLOW_PLUGINS_S3_URI="${AIRFLOW_PLUGINS_S3_URI:-s3://${AWS_S3_DAG_BUCKET}/plugins/}"

if ! command -v aws >/dev/null 2>&1; then
  echo "❌ Error: aws CLI is required to upload Airflow artifacts to S3." >&2
  exit 1
fi

missing=""
for var_name in AWS_S3_DAG_REGION AWS_S3_DAG_ACCESS_KEY_ID AWS_S3_DAG_SECRET_ACCESS_KEY; do
  if [ -z "${!var_name:-}" ]; then
    missing="$missing $var_name"
  fi
done
if [ -n "$missing" ]; then
  echo "❌ Error: Missing required DAG artifact S3 credentials:$missing" >&2
  echo "   Set them in secrets.env or export them before running make install." >&2
  exit 1
fi

if [ ! -d "$AIRFLOW_PYTHON_DEPS_DIR" ]; then
  echo "❌ Error: packaged dependencies directory not found: $AIRFLOW_PYTHON_DEPS_DIR" >&2
  echo "   Run scripts/package_airflow_dependencies.sh first." >&2
  exit 1
fi

if [ ! -d "$AIRFLOW_DAGS_DIR" ]; then
  echo "❌ Error: DAG directory not found: $AIRFLOW_DAGS_DIR" >&2
  exit 1
fi

if [ ! -d "$AIRFLOW_PLUGINS_DIR" ]; then
  echo "❌ Error: plugins directory not found: $AIRFLOW_PLUGINS_DIR" >&2
  exit 1
fi

# Exclude local dev artifacts from DAG/plugin uploads.
S3_SYNC_EXCLUDE_PYCACHE=(
  --exclude "*.pyc"
  --exclude "*__pycache__*"
  --exclude ".venv/*"
  --exclude "*/.venv/*"
  --exclude ".pytest_cache/*"
  --exclude "*/.pytest_cache/*"
  --exclude ".mypy_cache/*"
  --exclude "*/.mypy_cache/*"
  --exclude ".ruff_cache/*"
  --exclude "*/.ruff_cache/*"
)

aws_s3() {
  AWS_ACCESS_KEY_ID="$AWS_S3_DAG_ACCESS_KEY_ID" \
    AWS_SECRET_ACCESS_KEY="$AWS_S3_DAG_SECRET_ACCESS_KEY" \
    AWS_DEFAULT_REGION="$AWS_S3_DAG_REGION" \
    aws "$@"
}

ARTIFACT_HASH_S3_PREFIX="${ARTIFACT_HASH_S3_PREFIX:-s3://${AWS_S3_DAG_BUCKET}/.artifact-hashes}"

read_remote_artifact_hash() {
  local artifact_key="$1"
  local remote_uri="${ARTIFACT_HASH_S3_PREFIX%/}/${artifact_key}.sha256"
  local temp_file
  temp_file="$(mktemp)"
  if aws_s3 s3 cp "$remote_uri" "$temp_file" >/dev/null 2>&1; then
    tr -d '[:space:]' < "$temp_file"
    rm -f "$temp_file"
    return 0
  fi
  rm -f "$temp_file"
  return 1
}

write_remote_artifact_hash() {
  local artifact_key="$1"
  local value="$2"
  local remote_uri="${ARTIFACT_HASH_S3_PREFIX%/}/${artifact_key}.sha256"
  printf '%s\n' "$value" | aws_s3 s3 cp - "$remote_uri"
}

should_skip_unchanged_in_s3() {
  local label="$1"
  local artifact_key="$2"
  local current_hash="$3"

  if [ "${FORCE_PACKAGE_DEPS:-}" = "1" ] || [ "${FORCE_UPLOAD_ARTIFACTS:-}" = "1" ]; then
    return 1
  fi

  local remote_hash
  if ! remote_hash="$(read_remote_artifact_hash "$artifact_key")"; then
    echo "${label}: no content hash in S3; uploading."
    return 1
  fi

  if [ "$remote_hash" = "$current_hash" ]; then
    echo "Skipping ${label} (unchanged; matches S3)."
    return 0
  fi

  echo "${label}: content hash differs from S3; uploading."
  return 1
}

sync_artifact_if_changed() {
  local label="$1"
  local source_dir="$2"
  local dest_uri="$3"
  local artifact_key="$4"
  local content_hash="$5"
  shift 5
  local sync_excludes=("$@")

  if should_skip_unchanged_in_s3 "$label upload" "$artifact_key" "$content_hash"; then
    return 0
  fi

  echo "Uploading ${label} to ${dest_uri}..."
  aws_s3 s3 sync "$source_dir/" "$dest_uri" --delete "${sync_excludes[@]}"
  write_remote_artifact_hash "$artifact_key" "$content_hash"
}

# Wipe the destination prefix, then upload — ensures stale objects (e.g. old __pycache__) are removed.
replace_artifact_if_changed() {
  local label="$1"
  local source_dir="$2"
  local dest_uri="$3"
  local artifact_key="$4"
  local content_hash="$5"
  shift 5
  local sync_excludes=("$@")

  if should_skip_unchanged_in_s3 "$label upload" "$artifact_key" "$content_hash"; then
    return 0
  fi

  echo "Replacing ${label} at ${dest_uri}..."
  aws_s3 s3 rm "$dest_uri" --recursive
  aws_s3 s3 sync "$source_dir/" "$dest_uri" "${sync_excludes[@]}"
  write_remote_artifact_hash "$artifact_key" "$content_hash"
}

DEPS_CONTENT_STAMP="$AIRFLOW_PYTHON_DEPS_DIR/.content.sha256"
if [ -f "$DEPS_CONTENT_STAMP" ]; then
  DEPS_HASH="$(read_stored_hash "$DEPS_CONTENT_STAMP")"
else
  DEPS_HASH="$(hash_tree "$AIRFLOW_PYTHON_DEPS_DIR" 1)"
fi

sync_artifact_if_changed \
  "packaged Airflow dependencies" \
  "$AIRFLOW_PYTHON_DEPS_DIR" \
  "$AIRFLOW_DEPS_S3_URI" \
  "deps" \
  "$DEPS_HASH"

replace_artifact_if_changed \
  "Airflow DAGs" \
  "$AIRFLOW_DAGS_DIR" \
  "$AIRFLOW_DAGS_S3_URI" \
  "dags" \
  "$(hash_tree "$AIRFLOW_DAGS_DIR" 1)" \
  "${S3_SYNC_EXCLUDE_PYCACHE[@]}"

replace_artifact_if_changed \
  "Airflow plugins" \
  "$AIRFLOW_PLUGINS_DIR" \
  "$AIRFLOW_PLUGINS_S3_URI" \
  "plugins" \
  "$(hash_tree "$AIRFLOW_PLUGINS_DIR" 1)" \
  "${S3_SYNC_EXCLUDE_PYCACHE[@]}"

echo "✅ Airflow artifact upload step complete"

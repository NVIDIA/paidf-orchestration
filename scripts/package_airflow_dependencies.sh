#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Pre-package Airflow runtime dependencies into a host directory that is mounted
# into Airflow pods. This avoids rebuilding the Airflow container when only
# mounted plugin/DAG code changes.

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/artifact_fingerprint.sh"

AIRFLOW_BASE_IMAGE="${AIRFLOW_BASE_IMAGE:-apache/airflow:3.1.7-python3.11}"
AIRFLOW_PYTHON_DEPS_DIR="${AIRFLOW_PYTHON_DEPS_DIR:-$REPO_ROOT/.airflow-python-deps}"
PYPROJECT_FILE="$REPO_ROOT/pyproject.toml"
UV_LOCK_FILE="$REPO_ROOT/uv.lock"
RUNTIME_DEPENDENCY_GROUP="${RUNTIME_DEPENDENCY_GROUP:-airflow-runtime}"

if ! command -v docker >/dev/null 2>&1; then
  echo "❌ Error: docker is required to package Airflow dependencies." >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "❌ Error: uv is required to export Airflow runtime dependencies." >&2
  exit 1
fi

if [ ! -f "$PYPROJECT_FILE" ]; then
  echo "❌ Error: pyproject.toml not found: $PYPROJECT_FILE" >&2
  exit 1
fi

if [ ! -f "$UV_LOCK_FILE" ]; then
  echo "❌ Error: uv.lock not found: $UV_LOCK_FILE" >&2
  echo "   Run: uv lock --project \"$REPO_ROOT\"" >&2
  exit 1
fi

PACKAGE_INPUT_STAMP="$AIRFLOW_PYTHON_DEPS_DIR/.package-input.sha256"
PACKAGE_CONTENT_STAMP="$AIRFLOW_PYTHON_DEPS_DIR/.content.sha256"
PACKAGE_INPUT_HASH="$(
  hash_text "$(hash_file "$PYPROJECT_FILE"):$(hash_file "$UV_LOCK_FILE"):$(hash_text "$AIRFLOW_BASE_IMAGE")"
)"

if should_skip_unchanged "dependency packaging" "$PACKAGE_INPUT_STAMP" "$PACKAGE_INPUT_HASH" \
  && [ -n "$(find "$AIRFLOW_PYTHON_DEPS_DIR" -mindepth 1 -maxdepth 1 ! -name '.*' -print -quit 2>/dev/null)" ]; then
  exit 0
fi

echo "Packaging Airflow runtime dependencies..."
echo "  Base image: $AIRFLOW_BASE_IMAGE"
echo "  Project: $PYPROJECT_FILE"
echo "  Lockfile: $UV_LOCK_FILE"
echo "  Dependency group: $RUNTIME_DEPENDENCY_GROUP"
echo "  Output: $AIRFLOW_PYTHON_DEPS_DIR"

if ! uv lock --project "$REPO_ROOT" --check; then
  echo "❌ Error: uv.lock is not up to date with pyproject.toml." >&2
  echo "   Run: uv lock --project \"$REPO_ROOT\"" >&2
  exit 1
fi

EXPORTED_REQUIREMENTS_FILE="$(mktemp)"
trap 'rm -f "$EXPORTED_REQUIREMENTS_FILE"' EXIT

uv pip compile \
  --directory "$REPO_ROOT" \
  --group "$RUNTIME_DEPENDENCY_GROUP" \
  --no-deps \
  --no-header \
  --no-annotate \
  --output-file "$EXPORTED_REQUIREMENTS_FILE" \
  >/dev/null

REQUIREMENT_COUNT="$(grep -c '==' "$EXPORTED_REQUIREMENTS_FILE" || true)"
echo "  Installing $REQUIREMENT_COUNT packages from $RUNTIME_DEPENDENCY_GROUP."

rm -rf "$AIRFLOW_PYTHON_DEPS_DIR"
mkdir -p "$AIRFLOW_PYTHON_DEPS_DIR"

if [ "$REQUIREMENT_COUNT" -eq 0 ]; then
  echo "  No packages to install."
else
  docker run --rm \
    --entrypoint bash \
    --user 0:0 \
    -e HOST_UID="$(id -u)" \
    -e HOST_GID="$(id -g)" \
    -e PYTHONDONTWRITEBYTECODE=1 \
    -v "$EXPORTED_REQUIREMENTS_FILE:/tmp/airflow-runtime-requirements.txt:ro" \
    -v "$AIRFLOW_PYTHON_DEPS_DIR:/python-deps" \
    "$AIRFLOW_BASE_IMAGE" \
    -lc 'python -m pip install --no-cache-dir --no-compile --no-deps --target /python-deps -r /tmp/airflow-runtime-requirements.txt && chown -R "$HOST_UID:$HOST_GID" /python-deps'
fi

write_stored_hash "$PACKAGE_INPUT_STAMP" "$PACKAGE_INPUT_HASH"
write_stored_hash "$PACKAGE_CONTENT_STAMP" "$(hash_tree "$AIRFLOW_PYTHON_DEPS_DIR" 1)"

echo "✅ Packaged Airflow dependencies in $AIRFLOW_PYTHON_DEPS_DIR"

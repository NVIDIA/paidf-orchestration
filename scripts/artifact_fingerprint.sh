#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Shared helpers for detecting unchanged Airflow artifact inputs/outputs.

hash_text() {
  printf '%s' "$1" | sha256sum | awk '{print $1}'
}

hash_file() {
  sha256sum "$1" | awk '{print $1}'
}

hash_tree() {
  local root="$1"
  local exclude_pycache="${2:-0}"
  if [ ! -d "$root" ]; then
    return 1
  fi

  (
    cd "$root"
    if [ "$exclude_pycache" = "1" ]; then
      find . -type f \
        ! -path './.*' \
        ! -path '*/__pycache__/*' \
        ! -name '*.pyc' \
        ! -name '.package-input.sha256' \
        ! -name '.content.sha256' \
        -print0 \
        | sort -z \
        | xargs -0 sha256sum
    else
      find . -type f \
        ! -path './.*' \
        ! -name '.package-input.sha256' \
        ! -name '.content.sha256' \
        -print0 \
        | sort -z \
        | xargs -0 sha256sum
    fi
  ) | sha256sum | awk '{print $1}'
}

read_stored_hash() {
  local stamp_file="$1"
  if [ -f "$stamp_file" ]; then
    cat "$stamp_file"
  fi
}

write_stored_hash() {
  local stamp_file="$1"
  local value="$2"
  mkdir -p "$(dirname "$stamp_file")"
  printf '%s\n' "$value" > "$stamp_file"
}

should_skip_unchanged() {
  local label="$1"
  local stamp_file="$2"
  local current_hash="$3"

  if [ "${FORCE_PACKAGE_DEPS:-}" = "1" ] || [ "${FORCE_UPLOAD_ARTIFACTS:-}" = "1" ]; then
    return 1
  fi

  local stored_hash
  stored_hash="$(read_stored_hash "$stamp_file")"
  if [ -n "$stored_hash" ] && [ "$stored_hash" = "$current_hash" ]; then
    echo "Skipping ${label} (unchanged)."
    return 0
  fi

  return 1
}

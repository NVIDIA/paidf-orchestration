#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Generate Kubernetes secret values for local deployments.
#
# Writes deploy/secret_values_dev.yaml for Helm and secrets.env for
# local install-time commands (see make install).
#
# Secrets must be provided via one of:
#   - secrets.env in the repo root (see secrets.env.example)
#   - Environment variables already exported in the shell
#   - Interactive prompts (when running in a terminal)
#
# The script exits with an error if any required secret is missing.
#
# Required variables:
#   - NGC_API_KEY
#   - HF_TOKEN
#   - Per-purpose S3 settings (see REQUIRED_CONFIG_VARS), or shorthand:
#       AWS_S3_BUCKET, AWS_S3_REGION, AWS_S3_ACCESS_KEY_ID, AWS_S3_SECRET_ACCESS_KEY
#     which fill any matching DAG/input/output setting that is still empty.
#
# Kubeconfig:
#   - KUBECONFIG path is used when set
#   - Interactive runs prompt for a kubeconfig path when KUBECONFIG is empty
#

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/s3_config.sh"

SECRET_VALUES_DEV_FILE="$REPO_ROOT/deploy/secret_values_dev.yaml"
LOCAL_SECRETS_ENV_FILE="$REPO_ROOT/secrets.env"

REQUIRED_SECRET_VARS=(
  NGC_API_KEY
  HF_TOKEN
)

REQUIRED_CONFIG_VARS=(
  AWS_S3_DAG_BUCKET
  AWS_S3_DAG_REGION
  AWS_S3_DAG_ACCESS_KEY_ID
  AWS_S3_DAG_SECRET_ACCESS_KEY
  AWS_S3_OUTPUT_BUCKET
  AWS_S3_OUTPUT_REGION
  AWS_S3_OUTPUT_ACCESS_KEY_ID
  AWS_S3_OUTPUT_SECRET_ACCESS_KEY
  AWS_S3_INPUT_BUCKET
  AWS_S3_INPUT_REGION
  AWS_S3_INPUT_ACCESS_KEY_ID
  AWS_S3_INPUT_SECRET_ACCESS_KEY
)

check_required_env_vars() {
  local missing=""
  local var_name
  for var_name in "$@"; do
    if [ -z "${!var_name:-}" ]; then
      missing="$missing $var_name"
    fi
  done
  if [ -n "$missing" ]; then
    echo "❌ Error: The following required variables are not set:$missing" >&2
    echo "" >&2
    echo "Provide secrets via $LOCAL_SECRETS_ENV_FILE, environment variables, or interactive prompts." >&2
    echo "See secrets.env.example for the expected format." >&2
    return 1
  fi
  return 0
}

load_secrets_from_file() {
  local secrets_file="$1"

  if [ ! -f "$secrets_file" ]; then
    echo "❌ Error: Secrets file not found: $secrets_file" >&2
    return 1
  fi

  echo "Loading secrets from $secrets_file..."
  set -a
  # shellcheck disable=SC1090
  source "$secrets_file"
  set +a
  apply_s3_unified_overrides
  return 0
}

prompt_for_missing_secrets() {
  local var_name
  local value

  for var_name in "${REQUIRED_SECRET_VARS[@]}"; do
    if [ -n "${!var_name:-}" ]; then
      continue
    fi

    if [ ! -t 0 ]; then
      return 1
    fi

    read -rsp "Enter ${var_name}: " value
    echo ""
    if [ -z "$value" ]; then
      echo "❌ Error: ${var_name} is required." >&2
      return 1
    fi
    export "${var_name}=${value}"
  done

  return 0
}

prompt_for_missing_config() {
  local var_name
  local value

  for var_name in "${REQUIRED_CONFIG_VARS[@]}"; do
    if [ -n "${!var_name:-}" ]; then
      continue
    fi

    if [ ! -t 0 ]; then
      return 1
    fi

    read -rp "Enter ${var_name}: " value
    if [ -z "$value" ]; then
      echo "❌ Error: ${var_name} is required." >&2
      return 1
    fi
    export "${var_name}=${value}"
  done

  return 0
}

collect_secrets_interactively() {
  echo ""
  echo "=========================================="
  echo "Secrets Configuration (REQUIRED)"
  echo "=========================================="
  echo ""
  echo "Provide secrets using one of:"
  echo "  - $LOCAL_SECRETS_ENV_FILE (see secrets.env.example)"
  echo "  - Environment variables already exported in your shell"
  echo "  - Enter values when prompted below"
  echo ""

  if [ -f "$LOCAL_SECRETS_ENV_FILE" ]; then
    load_secrets_from_file "$LOCAL_SECRETS_ENV_FILE" || return 1
  fi

  if ! check_required_env_vars "${REQUIRED_SECRET_VARS[@]}"; then
    echo ""
    echo "Some required secrets are still missing. Enter them now (input is hidden)."
    prompt_for_missing_secrets || return 1
  fi

  if ! check_required_env_vars "${REQUIRED_CONFIG_VARS[@]}"; then
    echo ""
    echo "Some required S3 settings are still missing. Enter them now."
    prompt_for_missing_config || return 1
  fi

  check_required_env_vars "${REQUIRED_SECRET_VARS[@]}" &&
    check_required_env_vars "${REQUIRED_CONFIG_VARS[@]}"
}

resolve_kubeconfig() {
  local kubeconfig_path="${KUBECONFIG:-}"

  if [ -z "$kubeconfig_path" ]; then
    if [ ! -t 0 ]; then
      echo "❌ Error: KUBECONFIG is not set and this is not an interactive terminal." >&2
      echo "   Set KUBECONFIG to the kubeconfig file path before running make setup." >&2
      return 1
    fi

    read -rp "Path to kubeconfig file: " kubeconfig_path
    if [ -z "$kubeconfig_path" ]; then
      echo "❌ Error: kubeconfig file path is required." >&2
      return 1
    fi
  fi

  if [[ "$kubeconfig_path" == *:* ]]; then
    echo "❌ Error: KUBECONFIG must point to a single kubeconfig file for this setup script." >&2
    echo "   Current value: $kubeconfig_path" >&2
    return 1
  fi

  if [ ! -f "$kubeconfig_path" ]; then
    echo "❌ Error: kubeconfig file not found: $kubeconfig_path" >&2
    return 1
  fi

  K8S_REMOTE_KUBECONFIG=$(cat "$kubeconfig_path")
  echo "✅ Loaded kubeconfig from $kubeconfig_path"
  return 0
}

if [ -f "$LOCAL_SECRETS_ENV_FILE" ]; then
  load_secrets_from_file "$LOCAL_SECRETS_ENV_FILE" || exit 1
else
  apply_s3_unified_overrides
fi

if [ -t 0 ] && [ -f "$SECRET_VALUES_DEV_FILE" ] && [ ! -f "$LOCAL_SECRETS_ENV_FILE" ]; then
  echo "$SECRET_VALUES_DEV_FILE already exists."
  read -p "Would you like to update your secrets? (y/n): " yn
  case "$yn" in
    [Yy]* ) ;;
    * ) echo "Skipping secrets setup."; exit 0;;
  esac
fi

if ! check_required_env_vars "${REQUIRED_SECRET_VARS[@]}"; then
  if [ -t 0 ]; then
    collect_secrets_interactively || exit 1
  else
    echo "❌ Error: Required secrets are missing and this is not an interactive terminal." >&2
    echo "   Create $LOCAL_SECRETS_ENV_FILE or export the required variables before running make setup." >&2
    exit 1
  fi
fi

if ! check_required_env_vars "${REQUIRED_CONFIG_VARS[@]}"; then
  if [ -t 0 ]; then
    echo ""
    echo "Some required S3 settings are missing. Enter them now."
    prompt_for_missing_config || exit 1
  else
    echo "❌ Error: Required S3 settings are missing and this is not an interactive terminal." >&2
    echo "   Set them in $LOCAL_SECRETS_ENV_FILE or export them before running make setup." >&2
    exit 1
  fi
fi

if ! check_required_env_vars "${REQUIRED_SECRET_VARS[@]}" ||
   ! check_required_env_vars "${REQUIRED_CONFIG_VARS[@]}"; then
  exit 1
fi

echo "✅ All required secrets and S3 settings are set"

resolve_kubeconfig || exit 1

echo "Generating $SECRET_VALUES_DEV_FILE..."
mkdir -p "$(dirname "$SECRET_VALUES_DEV_FILE")"

cat > "$SECRET_VALUES_DEV_FILE" <<EOF
  # Generated file, do not edit this file manually. Instead, run \`make setup\` to update your secrets.
  global:
    secrets:
      ngcDockerRegistrySecret:
        username: "\$oauthtoken"
        password: "$NGC_API_KEY"
      ngc:
        ngcApiKey: "$NGC_API_KEY"
      aws:
        s3Dag:
          bucket: "$AWS_S3_DAG_BUCKET"
          region: "$AWS_S3_DAG_REGION"
          accessKey: "$AWS_S3_DAG_ACCESS_KEY_ID"
          secretKey: "$AWS_S3_DAG_SECRET_ACCESS_KEY"
        s3Output:
          bucket: "$AWS_S3_OUTPUT_BUCKET"
          region: "$AWS_S3_OUTPUT_REGION"
          accessKey: "$AWS_S3_OUTPUT_ACCESS_KEY_ID"
          secretKey: "$AWS_S3_OUTPUT_SECRET_ACCESS_KEY"
        s3Input:
          bucket: "$AWS_S3_INPUT_BUCKET"
          region: "$AWS_S3_INPUT_REGION"
          accessKey: "$AWS_S3_INPUT_ACCESS_KEY_ID"
          secretKey: "$AWS_S3_INPUT_SECRET_ACCESS_KEY"
      huggingFace:
        hfToken: "$HF_TOKEN"
EOF

K8S_KUBECONFIG_JSON=$(printf '%s' "$K8S_REMOTE_KUBECONFIG" | python3 -c "import sys,json; print(json.dumps(sys.stdin.read()))")
cat >> "$SECRET_VALUES_DEV_FILE" <<EOF
      kubernetesRemote:
        kubeconfig: $K8S_KUBECONFIG_JSON
EOF

echo "Generating $LOCAL_SECRETS_ENV_FILE..."
{
  echo "# Generated file, do not edit this file manually. Instead, run \`make setup\` to update your secrets."
  for var_name in "${REQUIRED_SECRET_VARS[@]}"; do
    printf '%s=%q\n' "$var_name" "${!var_name}"
  done
  for var_name in "${S3_UNIFIED_OVERRIDE_VARS[@]}"; do
    if [ -n "${!var_name:-}" ]; then
      printf '%s=%q\n' "$var_name" "${!var_name}"
    fi
  done
  for var_name in "${REQUIRED_CONFIG_VARS[@]}"; do
    printf '%s=%q\n' "$var_name" "${!var_name}"
  done
  if [ -n "${KUBECONFIG:-}" ]; then
    printf 'KUBECONFIG=%q\n' "$KUBECONFIG"
  fi
} > "$LOCAL_SECRETS_ENV_FILE"
chmod 600 "$LOCAL_SECRETS_ENV_FILE"

echo "✅ Wrote Helm values to $SECRET_VALUES_DEV_FILE"
echo "✅ Wrote local install environment to $LOCAL_SECRETS_ENV_FILE"

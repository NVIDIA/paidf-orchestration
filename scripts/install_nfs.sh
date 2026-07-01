#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NFS_DIR="${REPO_ROOT}/nfs"
KUBECTL="${KUBECTL:-kubectl}"

NFS_NAMESPACE="${NFS_NAMESPACE:-nfs-system}"
NFS_STORAGE_CLASS="${NFS_STORAGE_CLASS:-nfs}"
NFS_EXPORT_PATH="${NFS_EXPORT_PATH:-/srv/nfs/k8s}"
NFS_PROVISIONER_NAME="${NFS_PROVISIONER_NAME:-cluster.local/nfs-subdir-external-provisioner}"
NFS_SETUP_IMAGE="${NFS_SETUP_IMAGE:-ubuntu:24.04}"
NFS_PROVISIONER_IMAGE="${NFS_PROVISIONER_IMAGE:-registry.k8s.io/sig-storage/nfs-subdir-external-provisioner:v4.0.2}"
NFS_NODE_HOSTNAME="${NFS_NODE_HOSTNAME:-}"
NFS_SERVER="${NFS_SERVER:-}"

if ! command -v envsubst >/dev/null 2>&1; then
  echo "❌ envsubst is required (install gettext)." >&2
  exit 1
fi

if [[ -z "$NFS_NODE_HOSTNAME" ]]; then
  NFS_NODE_HOSTNAME="$("$KUBECTL" get nodes -o jsonpath='{.items[0].metadata.name}')"
fi

if [[ -z "$NFS_SERVER" ]]; then
  NFS_SERVER="$("$KUBECTL" get node "$NFS_NODE_HOSTNAME" \
    -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')"
fi

if [[ -z "$NFS_NODE_HOSTNAME" || -z "$NFS_SERVER" ]]; then
  echo "❌ Could not resolve NFS node hostname or server IP." >&2
  echo "   Set NFS_NODE_HOSTNAME and/or NFS_SERVER and retry." >&2
  exit 1
fi

export NFS_NAMESPACE NFS_STORAGE_CLASS NFS_EXPORT_PATH NFS_PROVISIONER_NAME \
  NFS_NODE_HOSTNAME NFS_SERVER NFS_SETUP_IMAGE NFS_PROVISIONER_IMAGE

render() {
  envsubst \
    '$NFS_NAMESPACE $NFS_STORAGE_CLASS $NFS_EXPORT_PATH $NFS_PROVISIONER_NAME $NFS_NODE_HOSTNAME $NFS_SERVER $NFS_SETUP_IMAGE $NFS_PROVISIONER_IMAGE' \
    < "$1"
}

echo "=========================================="
echo "Installing NFS storage..."
echo "=========================================="
echo "  Namespace:      $NFS_NAMESPACE"
echo "  StorageClass:   $NFS_STORAGE_CLASS"
echo "  Export path:    $NFS_EXPORT_PATH"
echo "  Node:           $NFS_NODE_HOSTNAME"
echo "  NFS server:     $NFS_SERVER"
echo ""

echo "Step 1/4: Creating namespace..."
render "${NFS_DIR}/namespace.yaml.tpl" | "$KUBECTL" apply -f -

echo "Step 2/4: Configuring host NFS export..."
"$KUBECTL" -n "$NFS_NAMESPACE" delete job setup-host-nfs --ignore-not-found
render "${NFS_DIR}/setup-host-nfs-job.yaml.tpl" | "$KUBECTL" apply -f -
"$KUBECTL" -n "$NFS_NAMESPACE" wait --for=condition=complete job/setup-host-nfs --timeout=300s

echo "Step 3/4: Deploying NFS subdir external provisioner..."
render "${NFS_DIR}/nfs-provisioner.yaml.tpl" | "$KUBECTL" apply -f -
"$KUBECTL" -n "$NFS_NAMESPACE" rollout status deployment/nfs-subdir-external-provisioner --timeout=120s

echo "Step 4/4: Creating StorageClass..."
render "${NFS_DIR}/storageclass.yaml.tpl" | "$KUBECTL" apply -f -

echo ""
echo "=========================================="
echo "✅ NFS installation complete!"
echo "=========================================="
echo ""
echo "PVC data on host $NFS_NODE_HOSTNAME:"
echo "  ${NFS_EXPORT_PATH}/<namespace>-<pvc-name>-<pv-name>/"
echo ""
echo "Override defaults with environment variables:"
echo "  NFS_NODE_HOSTNAME, NFS_SERVER, NFS_EXPORT_PATH, NFS_STORAGE_CLASS, NFS_NAMESPACE"

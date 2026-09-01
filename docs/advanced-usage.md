# Advanced Usage

This section provides instructions for advanced configuration and usage of the project.

## Customization

This guide explains how the project code may be updated to add new DAGs or update the existing DAGs.

### Push DAG or plugin changes (without full reinstall)

> Note: This section assumes that the PAIDF Orchestration helm chart is running.
> Follow instructions from the [Getting Started Guide](getting-started.md) to install the PAIDF Orchestration helm chart.

After editing files under `airflow/dags` or `airflow/plugins`, or after changing the locked `airflow-runtime` dependencies in `pyproject.toml` / `uv.lock`:

```bash
make sync-dag
```
This command invokes the AWS CLI to upload content to the S3 bucket, make sure the AWS CLI has proper permission to the DAG S3 bucket.

Only the **dag-processor** pod syncs from S3 into the cluster. Its sidecars re-sync dags, plugins, and python-deps every **30 seconds** into shared PVCs; triggerer and task pods mount those volumes without syncing. After `make sync-dag`, changes typically appear within one sync interval.

Unchanged artifacts are skipped by comparing local content hashes against fingerprints stored in S3 under `s3://<bucket>/.artifact-hashes/`; if a fingerprint is missing in S3, the artifact is always uploaded.

> **Python dependency updates:** Repackaging python-deps replaces the full dependency tree on S3. Syncing that while DAG runs are active can disrupt new tasks or DAG parsing. Wait for active runs to finish, or pause DAGs, before pushing dependency changes.

For Helm value, secret changes or dependencies adjustment, run `make install sdg-controller` again.

## Update model selections

You can update models in two places, depending on whether you want per-run overrides or new defaults:

1. **Per workflow run (payload override):** Set `cosmos.vlm_model`, `cosmos.llm_model`, `cosmos.image_edit_model`, `event_and_person_attribute_search.vlm_model`, and `event_and_person_attribute_search.llm_model` in the trigger payload. See the [Image Attribute Augmentation Payload Guide](image-attribute-augmentation/payload-guide.md#cosmos-fields-augmentation).
2. **Default in-cluster endpoint models:** Edit `airflow/dags/workflows/image_attribute_augmentation_dag/configs/image_attribute_augmentation_k8s_manifest.yaml` (and the mirrored `deploy/.../image_attribute_augmentation_k8s_manifest.yaml`) under `deployment.components.endpoints.*`.

When updating default in-cluster endpoint models:

- Update endpoint startup args in `container_args` (for example, `--model` and `--served-model-name`) and adjust the container image if needed.
- Ensure your HuggingFace token has access to the selected model repositories.
- Run `make sync-dag` to publish DAG/manifests updates.

## Scale internal model throughput

When `external_services` is `false`, workflows deploy an in-cluster model endpoint and routes augmentation work through its corresponding pool. That pool controls how many augmentation tasks may run at once against the internal model service.

For example, the image edit model service (used by the Image Attribute Augmentation workflow) uses the `iaa_internal_image_edit_service_pool` to control the number of augmentations that can be run in parallel:

```yaml
    - name: "iaa_internal_image_edit_service_pool"
      slots: 2
      description: "Image Attribute Augmentation internal image-edit service pool"
      includeDeferred: true
```

If you intend to run **more than one replica** of a model service, set the pool **`slots`** to match the replica count you pass in the payload. For example, with three replicas of the image-edit model service:

**Payload** (`service_lifecycle.image_edit_service.replicas`):

```json
"service_lifecycle": {
  "image_edit_service": {
    "enabled": true,
    "replicas": 3
  }
}
```

**Helm values** (`deploy/values.yaml`):

```yaml
    - name: "iaa_internal_image_edit_service_pool"
      slots: 3
      description: "Image Attribute Augmentation internal image-edit service pool"
      includeDeferred: true
```

The full list of model services and their default size can be found below:
| Service Name | Workflows The Use It | Pool Name | Default Size |
| --- | --- | --- | --- |
| Image Edit | Image Attribute Augmentation | `iaa_internal_image_edit_service_pool` | 2 |
| Image2Video | Event Video Generation | `internal_image2video_service_pool` | 1 |


If the pool has fewer slots than replicas, Airflow rate-limits augmentation tasks: extra replicas stay idle because only one task (by default) can hold a pool slot at a time. After editing `deploy/values.yaml`, run `make install sdg-controller` so the updated pool is created in Airflow.

This applies to **internal** image-edit mode only. External augmentation uses external pools, which are sized separately for shared external endpoints.


## Configure NFS storage

The SDG controller expects a **`ReadWriteMany`** StorageClass named **`nfs`** by default (`deploy/values.yaml`). If your cluster does not already have one, install it with:

```bash
make install nfs
```

This runs `scripts/install_nfs.sh`, which:

1. Creates the `nfs-system` namespace
2. Starts an **NFS server on a cluster node host** (via a privileged Job with host mount)
3. Deploys the [nfs-subdir-external-provisioner](https://github.com/kubernetes-sigs/nfs-subdir-external-provisioner)
4. Creates a StorageClass named **`nfs`**

### Default node selection

If you do not override anything, the script picks the **first node** returned by:

```bash
kubectl get nodes
```

That node’s **`InternalIP`** becomes the NFS server address. You can confirm what will be used:

```bash
kubectl get nodes
# first row under NAME → default NFS host

kubectl get node <that-node-name> -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}{"\n"}'
```

The install prints the resolved node and server IP before applying manifests.

### Override variables

Pass Make variables or export environment variables before `make install nfs`:

| Variable | Default | Description |
|----------|---------|-------------|
| `NFS_NODE_HOSTNAME` | First node from `kubectl get nodes` | Node where the host NFS export is configured |
| `NFS_SERVER` | `InternalIP` of `NFS_NODE_HOSTNAME` | IP address the provisioner mounts (set explicitly if auto-detection is wrong) |
| `NFS_EXPORT_PATH` | `/srv/nfs/k8s` | Directory on the host exported over NFS |
| `NFS_STORAGE_CLASS` | `nfs` | Name of the StorageClass created |
| `NFS_NAMESPACE` | `nfs-system` | Namespace for the NFS provisioner and setup Job |
| `NFS_PROVISIONER_NAME` | `cluster.local/nfs-subdir-external-provisioner` | Provisioner name registered with Kubernetes |

**Example — use a specific node:**

```bash
make install nfs NFS_NODE_HOSTNAME=4u8g-gen-0375
```

**Example — set the server IP manually** (e.g. when the node has multiple addresses or you use a floating IP):

```bash
make install nfs \
  NFS_NODE_HOSTNAME=4u8g-gen-0375 \
  NFS_SERVER=192.168.33.10
```

**Example — custom export path and StorageClass name:**

```bash
make install nfs \
  NFS_EXPORT_PATH=/data/nfs/k8s \
  NFS_STORAGE_CLASS=nfs-rwx
```

If you change `NFS_STORAGE_CLASS`, update matching `storageClassName` / `storageClass` fields in `deploy/values.yaml` before `make install sdg-controller`.

### Where PVC data lives on the host

Each dynamically provisioned volume is a subdirectory under the export path:

```text
${NFS_EXPORT_PATH}/<namespace>-<pvc-name>-<pv-name>/
```

For example, with the default export path:

```text
/srv/nfs/k8s/sdg-workflow-ngc-model-cache-<pv-id>/
```

### Requirements for `make install nfs`

- **`envsubst`** (from the `gettext` package) must be on your PATH
- The target node must allow a **privileged** pod with **host PID/network** and a host filesystem mount (the setup Job installs `nfs-kernel-server` into the node’s root filesystem)
- Pick a node with enough **disk space** for PostgreSQL, DAG sync PVCs, and the **500Gi** model-cache PVC defined in `deploy/values.yaml`

## Configure S3 Buckets

Airflow uses S3 for three purposes:
1. **DAG Artifacts** : Stores the DAG code, plugins, and 3rd-party dependencies on S3 to be pulled by the helm chart and loaded into the deployment. Code is stored on S3 to allow hot-reloading of the code without redeploying the helm chart.

    **DAG artifact bucket layout** (created/used automatically):

    ```
    s3://$AWS_S3_DAG_BUCKET/dags/
    s3://$AWS_S3_DAG_BUCKET/plugins/
    s3://$AWS_S3_DAG_BUCKET/deps/
    ```

2. **Workflow Input**: Workflows require input data to work - for example the Image Attribute Augmentation DAG requires images as input which are then augmented by the pipeline. All input to a workflow must be taken from S3.
3. **Workflow Output**: Datasets generated by the pipeline are published to S3

For development and testing, input, output and DAGs can all be stored on the same S3 bucket, but to prevent overlap between the three use cases and more tightly control S3 access, it is recommended to use separate S3 buckets for each of these in production.

To use separate buckets, open `secrets.env` and remove fields for `AWS_S3_BUCKET`, `AWS_S3_REGION`, `AWS_S3_ACCESS_KEY_ID`, and `AWS_S3_SECRET_ACCESS_KEY`. Replace these with fine-grained configuration using the ENV variables listed below:

```
AWS_S3_DAG_BUCKET=
AWS_S3_DAG_REGION=
AWS_S3_DAG_ACCESS_KEY_ID=
AWS_S3_DAG_SECRET_ACCESS_KEY=

AWS_S3_INPUT_BUCKET=
AWS_S3_INPUT_REGION=
AWS_S3_INPUT_ACCESS_KEY_ID=
AWS_S3_INPUT_SECRET_ACCESS_KEY=

AWS_S3_OUTPUT_BUCKET=
AWS_S3_OUTPUT_REGION=
AWS_S3_OUTPUT_ACCESS_KEY_ID=
AWS_S3_OUTPUT_SECRET_ACCESS_KEY=
```

Then run `make setup` to apply this to your repo. Missing fields will be prompted for interactively.

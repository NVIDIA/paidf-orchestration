# Getting Started Guide

This guide walks through how to deploy the Physical AI Data Factory Orchetration onto a Kubernetes cluster and run your first workflow.

## Prerequisites

To use PAIDF Orchestration, CLI tools must be installed, a Kubernetes cluster with GPUs must be configured, and an S3 bucket must be set up. Additionally, secrets must be provided for NVIDIA NGC and HuggingFace APIs.

The following sections will describe how these can be set up.

### Tools

Verify the installation of the following CLI tools before proceeding. These tools are used to install 3rd-party dependencies, build containers, and deploy the PAIDF Orchestration helm chart.

| Tool | Version | Installation Instructions | Verification |
|------|---------|---------------------------| ------------ |
| **kubectl** | **1.26+** | https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/ | Verify installation by running `kubectl version` |
| **helm** | **3.x** | https://helm.sh/docs/intro/install/ | Verify installation by running `helm version` |
| **uv** | **0.11.21+** | https://docs.astral.sh/uv/getting-started/installation/ | Verify installation by running `uv --version` |
| **docker** | **29.6.0+** | https://docs.docker.com/engine/install/ubuntu/ | Verify installation by running `docker --version` |
| **aws** | **2.x** | https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html | Verify installation by running `aws --version` |

### Kubernetes cluster

PAIDF Orchestration is a helm chart that is deployed on a Kubernetes cluster. Before installing the chart, ensure that your Kubernetes cluster is configured correctly by following these steps:

1. **Cluster Configuration**: Configure `kubectl` to point to the cluster you want to run on, either by setting the `KUBECONFIG` environment variable to point to your Kubernetes config file or by updating the default config file at `~/.kube/config`. See Kubernetes [configuration documentation](https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/) for details on the configuration.

2. **Storage**: Default Helm values expect a **`nfs`** StorageClass for PostgreSQL, DAG, plugin, dependency, and model-cache PVCs. If your cluster does not already provide one, run `make install nfs` first. If you use a different `ReadWriteMany` StorageClass, set `storageClassName` in `deploy/values.yaml`. See [Configure NFS storage](advanced-usage.md#configure-nfs-storage) for more details.

3. **GPUs**: GPUs are required to run the PAS DAG. The cluster need the [NVIDIA GPU Operator](https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/overview.html) (or equivalent) so pods can request `nvidia.com/gpu`. A minimum of 8x H100-class (NVIDIA Hopper) RTX6000 PRO or B200-class (NVIDIA Blackwell) GPUs are recommended for the quickstart workflow to complete successfully.

### S3 buckets and credentials

S3 buckets are used to store the input data, store DAG artifacts, and write the output data. This guide assumes that a single S3 bucket is used for all three of these functions, but for large-scale production usage separate buckets are recommended for each of the three types of storage. See [Configure S3 Buckets](advanced-usage.md#configure-s3-buckets) for more details on how the S3 buckets are configured and used by the project.

Before continuing, ensure that you have an S3 bucket set up, and that the IAM user/role for this bucket has `s3:ListBucket`, `s3:GetObject`, `s3:PutObject`, and `s3:DeleteObject`. Visit [Amazon S3](https://aws.amazon.com/s3/) to get started with S3 if this is not already configured. To proceed to the next step, ensure that you know your bucket name, bucket region, access key ID, and secret access key for your S3 bucket.

If you are using one bucket for DAG artifacts, input data, and output data, set the shorthand `AWS_S3_BUCKET`, `AWS_S3_REGION`, `AWS_S3_ACCESS_KEY_ID`, and `AWS_S3_SECRET_ACCESS_KEY` values in `secrets.env`. These values are automatically used for the DAG, input, and output S3 settings. If you use separate buckets, set the `AWS_S3_DAG_*`, `AWS_S3_INPUT_*`, and `AWS_S3_OUTPUT_*` values instead.

### Secrets

This project requires the following secrets:
- **NGC API Key**: Obtain an [NGC API Key](https://org.ngc.nvidia.com/account/api-key). This is used to pull various docker containers used by the workflow from NGC.
- **HuggingFace Token**: This is used for HuggingFace model downloads for LLM/VLM/image-edit services. To create the token:
   - Read and accept the [terms for Cosmos-Guardrail1](https://huggingface.co/nvidia/Cosmos-Guardrail1)
   - Read and accept the [terms for Cosmos-Transfer2.5](https://huggingface.co/nvidia/Cosmos-Transfer2.5-2B)
   - Create a new [HuggingFace Token](https://huggingface.co/settings/tokens) with read permissions for `nvidia/Cosmos-Guardrail1`, `nvidia/Cosmos-Transfer2.5-2B`, `nvidia/Cosmos-Reason1-7B`, and `Qwen/Qwen-Image-Edit-2511` models.
- **S3 Credentials**: See instructions from the [previous step](#s3-buckets-and-credentials) for AWS credential setup.

Once the secrets are ready, set them up in the repo using the following steps:

1. **Clone the Repo**: 

   ```bash
   git clone <repo-url>
   cd paidf-orchestration
   ```

2. **Copy secrets template**:

   ```bash
   cp secrets.env.example secrets.env
   ```

3. **Update secrets.env with your secrets**: Add your secret values to the ENV variables in secrets.env


## Run The Workflow

1. **Run setup**:

   ```bash
   make setup
   ```

   This will:

   - Load `secrets.env` (or prompt for missing values)
   - Write secrets to `deploy/secret_values_dev.yaml` for Helm
   - Refresh `secrets.env` with normalized values for later `make install` / `make sync-dag`

   Setup **does not** deploy Airflow or upload DAGs.

2. **Install the Airflow Helm Chart**:

   ```bash
   make install sdg-controller
   ```

   This will:

   1. Package runtime Python deps from the locked `airflow-runtime` group in `pyproject.toml` / `uv.lock` into `.airflow-python-deps`
   2. Upload DAGs, plugins, and deps to `s3://$AWS_S3_DAG_BUCKET/`
   3. Update Helm chart dependencies
   4. Install/upgrade the `sdg-workflow-controller` release in namespace `sdg-workflow`

   Wait for the success message. Helm uses a **10 minute** timeout; large clusters or slow image pulls may need a retry.

3. **Forward the Airflow UI port**:

   ```bash
   make port-forward
   ```

   This will:
   - Expose the Airflow UI port to other machines on the network, allowing one to open the Airflow UI from a separate computer's browser.

4. **Open the Airflow UI**:

   Open `http://<your-IP-Address>:8080` in a browser window.

   Use `admin` / `admin` as the credentials by default

   > Note: Change default credentials before any production use.

5. **Construct and Upload Input Data**:

   The PAS workflow requires a user-provided image dataset in S3. Build a local
   dataset with one folder per person ID, then upload that folder tree to the
   input location in S3 expected by the starter payloads:

   ```text
   input_data/
      person-0001/
         0001-front.jpg
         0001-side.jpg
      person-0002/
         0002-front.png
   ```

   Input data is expected to be images of people for the PAS workflow. Input data may be taken from https://github.com/NjtechCVLab/RSTPReid-Dataset or similar open source dataset if you do not have input images available.

6. **Create a Starter Payload**

   Before triggering the workflow, a payload is needed to give the workflow the following information:
      - Where on S3 the input images can be found
      - Where on S3 the output dataset should be uploaded
      - Whether to use external endpoints for VLM, LLM, or image edit, or to create these endpoint internally
      - How many augmentations should be performed per image
      - What attributes should be present in the augmented output, and in what distribution
      - Various other configurations

   To get started quickly, copy the payload from `payload/single-image-aug-w-internal-endpoint.json`, then set:

   - `input_path` to `s3://<your-input-bucket>/<your-workflow>/input`
   - `output_directory` to your desired output path, for example `s3://<your-output-bucket>/<your-workflow>/output`

   The [Payload Guide](payload-guide.md) may be followed for detailed instructions on setting up the payload.

7. **Start the Workflow from the UI**:

   > Note: Only run one workflow at a time to avoid long queueing times or unexpected issues

   1. Starting from the Airflow home page, select `Dags` in the left side bar.
   2. Select `pas_dag_k8s` from the list that appears.
   3. Click the `trigger` button in the top right corner of the page that appears.
   4. Under `Run Parameters`, update PasDagPayloadConfig with the payload from step 6. Leave other fields as their default values.
   5. Start the run by pressing the `Trigger` button.

8. **Monitor or Stop an Active Run**:

   The workflow progress can be monitored in the Airflow UI.

   If you need to cancel a run that is already in progress:

   1. Open the active DagRun in the Airflow UI (**Grid** or **Graph** view).
   2. Find the task that is currently **running**.
   3. Mark that task as **failed** (task menu → **Mark Failed**).

   That stops the run and lets the DAG execute **service shutdown**, which performs the proper cleanup of all the resource created by the currently DAG run.

   **Do not delete the DagRun or the DAG** from the UI to stop work in progress. Deleting a run bypasses the normal shutdown path and can leave stale Deployments, Services, or GPU pods on the cluster. Always prefer **Mark Failed** on the running task so cleanup runs and the next run starts fresh.

9. **Inspect the Workflow Output**:

   After the workflow is complete, all results will appear in the output S3 bucket in the path specified by the `output_directory` field in the payload. Output will be created in a new directory named after the Airflow DAG Run ID, which can be found in the Airflow UI after the DAG is triggered.

10. **Uninstall the Helm Chart**:

   To uninstall the PAIDF Orchestration helm chart, run

   ```bash
   make uninstall
   ```

   > Note: On uninstall, **PVCs are preserved**
   >
   > To delete all persistent data:
   > ```bash
   > kubectl delete pvc --all -n sdg-workflow
   > ```
   > This will delete the past history of DAG runs, model caches, and other data.


## Next Steps

Visit the [PAS Payload Guide](payload-guide.md) for runtime payload authoring, and [Advanced Usage](advanced-usage.md) for deployment customization.

## Appendix

### Developer Commands

Run `make help` for the full list of available developer commands.

| Command | Description |
|---------|-------------|
| `make setup` | Validate secrets; generate Helm secret values |
| `make install nfs` | Install NFS StorageClass (one-time cluster prep) |
| `make install sdg-controller` | Package deps, upload artifacts to S3, deploy/upgrade Airflow |
| `make sync-dag` | Upload DAGs, plugins, and deps to S3 (skip unchanged; dag-processor picks up changes within ~30s) |
| `make port-forward` | Forward Airflow UI to `localhost:8080` |
| `make uninstall` | Uninstall the Helm release (keeps PVCs) |
| `make help` | Show available targets |

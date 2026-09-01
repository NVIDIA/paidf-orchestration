# Event Video Generation Payload Guide

This guide describes the JSON payload you provide when triggering
`event_video_generation_dag_k8s` in Airflow. The workflow turns each input
image into one or more anomaly videos, auto-labels the generated videos, and
writes a ready-to-use anomaly dataset.

## Quick Start: Starter Payloads

Use one of these as your starting point. The repository also includes
[warehouse-falling external](../../payload/event_video_generation_dag/warehouse-falling-external-1.json),
and warehouse-safety mix variants for
[external](../../payload/event_video_generation_dag/warehouse-safety-mix-external-10.json)
and [in-cluster](../../payload/event_video_generation_dag/warehouse-safety-mix-internal-10.json)
services.

### In-cluster services (`external_services=false`)

Start with
[`warehouse-safety-mix-internal-10.json`](../../payload/event_video_generation_dag/warehouse-safety-mix-internal-10.json).
It deploys the VLM, LLM, and image-to-video services in the cluster. Replace
the placeholder input and output S3 paths before running it.

```json
{
  "input_path": "s3://<your-input-bucket>/<your-workflow>/input",
  "max_images": 1,
  "output_directory": "s3://<your-output-bucket>/<your-workflow>/output",
  "external_services": false,
  "cosmos": {
    "num_augmentation": 10,
    "variable_distribution": {
      "variables": {
        "anomaly_type": {
          "person_climbing": 0.2,
          "person_running": 0.2,
          "smoking_or_vaping": 0.2,
          "person_fighting": 0.2,
          "fire_or_smoke": 0.2
        },
        "env_type": { "warehouse": 1.0 }
      }
    }
  }
}
```

### External services (`external_services=true`)

Start with
[`warehouse-safety-mix-external-10.json`](../../payload/event_video_generation_dag/warehouse-safety-mix-external-10.json).
Replace the placeholder S3 paths and all three endpoint URLs.

```json
{
  "input_path": "s3://<your-input-bucket>/<your-workflow>/input",
  "max_images": 1,
  "output_directory": "s3://<your-output-bucket>/<your-workflow>/output",
  "external_services": true,
  "cosmos": {
    "vlm_service_url": "https://<your-vlm-endpoint>/v1",
    "llm_service_url": "https://<your-llm-endpoint>/v1",
    "image2video_service_url": "https://<your-image2video-endpoint>/v1",
    "num_augmentation": 10,
    "variable_distribution": {
      "variables": {
        "anomaly_type": {
          "person_climbing": 0.2,
          "person_running": 0.2,
          "smoking_or_vaping": 0.2,
          "person_fighting": 0.2,
          "fire_or_smoke": 0.2
        },
        "env_type": { "warehouse": 1.0 }
      }
    }
  }
}
```

## Required Fields At A Glance

Every field has a default, so validation accepts an empty payload. The shipped
location and endpoint defaults are **non-working placeholders**, such as
`s3://<your-input-bucket>/<your-workflow>/input`. Always set the following
fields yourself:

- `input_path`
- `output_directory`

Set these fields when `external_services=true` (also the default):

- `cosmos.vlm_service_url`
- `cosmos.llm_service_url`
- `cosmos.image2video_service_url`

Set nothing extra for service endpoints when `external_services=false`: the
DAG deploys its internal VLM, LLM, and image-to-video services, and you may
omit both `cosmos` and `service_lifecycle`.

Consistency constraints:

- `cosmos.output_directory`, when set, must match top-level `output_directory`.
- `cosmos.external_services`, when set, must match top-level `external_services`.
- Omitted nested mode and output fields are populated from the top-level values.

## Top-Level Fields

- `input_path` (**string, always set this**): S3 location of one input image or
  a directory of images. A directory is scanned for `.jpg`, `.jpeg`, `.png`,
  `.bmp`, `.gif`, `.tiff`, and `.webp` files. The default is a placeholder path
  that fails at runtime.
- `max_images` (**integer, optional**, default: `10`): Maximum number of images
  selected from a directory after sorting. `0` or a negative value processes all
  matching images. It has no effect when `input_path` names one image.
- `output_directory` (**string, always set this**): Base S3 location for output.
  The workflow writes into `<output_directory>/<run_id>/...`. Its default is a
  placeholder path that fails at runtime.
- `external_services` (**boolean, optional**, default: `true`): Service mode.
  Set `false` to deploy internal VLM, LLM, and image-to-video services; set
  `true` to call the endpoints configured in `cosmos`.
- `service_lifecycle` (**object, optional**): Internal service deployment
  flags. Usually omit it; the DAG derives the flags from `external_services`.
  Set it only to override per-service replica counts.
- `cosmos` (**object, optional**): Image-to-video augmentation configuration:
  endpoint/model settings, number of videos per image, and anomaly/environment
  sampling. See [`cosmos` Fields](#cosmos-fields).
- `enable_performance_reporting` (**boolean, optional**, default: `false`):
  Enables orchestration reporting. See [Performance Reporting](#performance-reporting).

## Input and Output

Each input image produces `cosmos.num_augmentation` generated videos. The DAG
auto-labels each generated video with detection and tracking, captioning,
anomaly visual QA, person-attribute visual QA, and person-attribute search.

The final dataset is written to:

```text
<output_directory>/<run_id>/anomaly_dataset/
  dataset.json
  <input_key>_aug0/
    raw/video.mp4
    contextual/
    sidecars/
```

`dataset.json` lists every completed scene and its video, generated Cosmos
configuration, caption, metadata, and annotation paths. Intermediate generated
videos are under `<output_directory>/<run_id>/cosmos/`; auto-labeling artifacts
are under `<output_directory>/<run_id>/auto_labeling/`.

## `service_lifecycle` (Internal Service Overrides)

Use this only when `external_services=false` and you need to tune internal
service replica counts. The service objects are `vlm_service`, `llm_service`,
and `image2video_service`; each accepts `enabled` and `replicas` (default `1`,
minimum `1`).

- When `external_services=false`, all three services are enabled by default.
- When `external_services=true`, all three services are disabled by default.
- Conflicting `enabled` values are normalized to the top-level service mode,
  while replica counts are retained.

Example override:

```json
{
  "external_services": false,
  "service_lifecycle": {
    "vlm_service": { "replicas": 2 },
    "llm_service": { "replicas": 2 },
    "image2video_service": { "replicas": 1 }
  }
}
```

The internal image-to-video pool, `internal_image2video_service_pool`, limits
concurrent augmentation work. If you increase image-to-video replicas, ensure
the pool has enough slots in `deploy/values.yaml` to use them; otherwise extra
replicas remain idle. The external mode uses
`external_image2video_service_pool` instead.

## `cosmos` Fields

- `num_augmentation` (**integer, optional**, default: `1`, minimum: `1`):
  Number of generated videos per input image.
- `vlm_service_url`, `llm_service_url`, `image2video_service_url` (**string,
  conditionally required**): Required when `external_services=true`.
- `vlm_model`, `llm_model`, `image2video_model` (**string, optional**): Model
  IDs sent to the services. The defaults are:
  - `vlm_model`: `Qwen/Qwen3-VL-30B-A3B-Instruct-FP8`
  - `llm_model`: `Qwen/Qwen2.5-14B-Instruct`
  - `image2video_model`: `nvidia/Cosmos3-Super-Image2Video`
- `variable_distribution` (**object, optional**): Weighted sampling of the
  anomaly and environment used to construct each generated video. If omitted,
  the deterministic default is `person_falling` in a `warehouse`.

### `variable_distribution`

`variables` must contain exactly these two keys:

- `anomaly_type`: anomaly label -> non-negative sampling weight.
- `env_type`: environment label -> non-negative sampling weight.

Each distribution must contain at least one value and have a positive total
weight. Weights are relative, so they do not need to sum to one. For example:

```json
{
  "variable_distribution": {
    "variables": {
      "anomaly_type": {
        "person_falling": 0.5,
        "person_running": 0.3,
        "person_fighting": 0.2
      },
      "env_type": {
        "warehouse": 1.0
      }
    }
  }
}
```

Sampling is deterministic for a given payload: the DAG uses a fixed seed when
creating the per-augmentation configurations.

## Performance Reporting

Reporting is off by default. Enable it with one top-level flag:

```json
{
  "enable_performance_reporting": true
}
```

After the workflow finishes, it writes these files to
`<output_directory>/<run_id>/reports/`:

- `paidf_orchestration_stats.yaml` — run metadata, task timings, input-image,
  generated-video, and final-scene counts, plus throughput rates.
- `paidf_orchestration_stats.html` — a self-contained dashboard of the same
  data.

The report reads Airflow timing data through the REST API. An Airflow connection
named `airflow_api_default` must have a host plus either login/password or a
token in its `extra` field. Reporting is a terminal observation branch: a
report failure does not fail an otherwise successful workflow or delay its
completion.

## Trigger the Run

1. Open the Airflow UI.
2. Enable `event_video_generation_dag_k8s`.
3. Choose **Trigger DAG w/ config**.
4. Under **Run Parameters**, update `EventVideoGenerationDagPayloadConfig` with
   your payload JSON.
5. Start the run.

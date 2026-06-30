# PAS Payload Guide

This guide describes the JSON payload you provide when triggering
`pas_dag_k8s` in Airflow.

## Quick Start: Starter Payloads

Use one of these as your starting point. These examples keep `cosmos` and
`auto_labeling` explicit so the payload shape is easy to follow.

### In-cluster services (`external_services=false`)

```json
{
  "input_path": "s3://<your-input-bucket>/<your-workflow>/input",
  "output_directory": "s3://<your-output-bucket>/<your-workflow>/output",
  "external_services": false,
  "max_imgs": 1,
  "cosmos": {
    "num_augmentation": 1,
    "variable_distribution": {
      "variables": {
        "top_outer_color": { "black": 1.0 },
        "top_outer_type": { "hoodie": 1.0 },
        "bottom_type": { "jeans": 1.0 },
        "bottom_color": { "blue": 1.0 },
        "shoe_type": { "sneakers": 1.0 },
        "shoe_color": { "white": 1.0 }
      }
    }
  },
  "auto_labeling": {
    "tracker": "bytetrack",
    "threshold": 0.3
  }
}
```

### External services (`external_services=true`)

```json
{
  "input_path": "s3://<your-input-bucket>/<your-workflow>/input",
  "output_directory": "s3://<your-output-bucket>/<your-workflow>/output",
  "external_services": true,
  "max_imgs": 1,
  "cosmos": {
    "vlm_service_url": "https://<your-vlm-endpoint>/v1",
    "llm_service_url": "https://<your-llm-endpoint>/v1",
    "image_edit_service_url": "https://<your-image-edit-endpoint>/v1",
    "num_augmentation": 1,
    "variable_distribution": {
      "variables": {
        "top_outer_color": { "black": 1.0 },
        "top_outer_type": { "hoodie": 1.0 },
        "bottom_type": { "jeans": 1.0 },
        "bottom_color": { "blue": 1.0 },
        "shoe_type": { "sneakers": 1.0 },
        "shoe_color": { "white": 1.0 }
      }
    }
  },
  "auto_labeling": {
    "tracker": "bytetrack",
    "threshold": 0.3,
    "vlm_service_url": "https://<your-vlm-endpoint>/v1",
    "llm_service_url": "https://<your-llm-endpoint>/v1"
  }
}
```

## Required Fields At A Glance

Always required:

- `input_path`
- `output_directory`

Conditionally required:

- If `external_services=true` (or omitted, since default is `true`):
  - `cosmos.vlm_service_url`
  - `cosmos.llm_service_url`
  - `cosmos.image_edit_service_url`
  - `auto_labeling.vlm_service_url`
  - `auto_labeling.llm_service_url`
- If `external_services=false`:
  - No endpoint URL fields are required. The services will be deployed along with the workflow.

Consistency constraints:

- `cosmos.output_directory` and `auto_labeling.output_directory` are optional
  and can be omitted from payloads.
- If either nested `output_directory` is set, it must match top-level
  `output_directory`.
- If set, `cosmos.external_services` and `auto_labeling.external_services` must
  match top-level `external_services`.
- Omitted nested mode/output fields are populated from top-level values.

## Top-Level Fields

- `input_path` (**string, required**): S3 location of your input dataset. PAS
  expects one folder per person ID.
- `output_directory` (**string, required**): Base S3 location for outputs. PAS
  writes into `<output_directory>/<run_id>/...`.
- `external_services` (**boolean, optional**, default: `true`): Service mode.
  Set `false` to deploy internal services, `true` to call your endpoints. There
  are no top-level `vlm`/`llm` URL fields.
- `max_imgs` (**integer, optional**, default: `1`): Maximum number of person-ID
  folders to process. `0` or negative means process all.
- `service_lifecycle` (**object, optional**): Internal service deployment flags.
  Usually omitted; PAS auto-populates from `external_services`. Provide it only
  when you want to override per-service replica counts.
- `cosmos` (**object, optional**): Augmentation task config. If omitted, PAS
  auto-populates mode/output fields from top-level values.
- `auto_labeling` (**object, optional**): Auto-labeling task config. If omitted,
  PAS auto-populates mode/output fields from top-level values.

## `service_lifecycle` (Internal Service Overrides)

Use this only when `external_services=false` and you want to tune internal
service replica counts.

Fields:

- `vlm_service`, `llm_service`, `image_edit_service` (objects)
- `enabled` (**bool, optional**)
- `replicas` (**int, optional**, default: `1`, minimum: `1`)

Defaults from top-level `external_services`:

- If `external_services=false`, PAS enables all three internal services by default.
- If `external_services=true`, PAS disables all three internal services by default.

If you set `enabled` values that conflict with top-level `external_services`, PAS
normalizes `enabled` to match top-level mode during validation (and logs a
warning). Replica counts you set are kept.

Example override:

```json
{
  "external_services": false,
  "service_lifecycle": {
    "vlm_service": { "replicas": 2 },
    "llm_service": { "replicas": 2 },
    "image_edit_service": { "replicas": 4 }
  }
}
```

## `cosmos` Fields (Augmentation)

- `num_augmentation` (**integer, optional**, default: `1`): Number of
  augmentations per person ID.
- `vlm_service_url`, `llm_service_url`, `image_edit_service_url`
  (**string, conditionally required**): Required when `external_services=true`.
- `vlm_model`, `llm_model`, `image_edit_model` (**string, optional**): Model
  names for endpoints.
- `variable_distribution` (**object, optional**): Controls attribute variety.
  If omitted, PAS uses a deterministic default outfit.

> **Internal image-edit replicas and pool size:** If you deploy more than one
> internal image-edit replica (`service_lifecycle.image_edit_service.replicas`),
> set `internal_image_edit_service_pool.slots` in `deploy/values.yaml` to the
> same number. Otherwise Airflow limits throughput to the lower pool size.
> Apply with `make install sdg-controller`. See
> [Scale internal image-edit throughput](advanced-usage.md#scale-internal-image-edit-throughput).

### `variable_distribution`

Use this to control how outfit attributes are sampled.

- `variables`: attribute -> weighted choices.
- `conditional_variables` (optional): attribute distributions that depend on
  another sampled attribute.

```json
{
  "variable_distribution": {
    "variables": {
      "top_outer_color": { "black": 0.34, "blue": 0.33, "red": 0.33 },
      "top_outer_type": { "hoodie": 0.5, "vest": 0.5 },
      "shoe_type": { "boots": 0.5, "sneakers": 0.5 }
    },
    "conditional_variables": {
      "shoe_color": {
        "depends_on": "shoe_type",
        "distributions": {
          "boots": { "black": 0.6, "brown": 0.4 },
          "sneakers": { "black": 0.5, "white": 0.5 }
        }
      }
    }
  }
}
```

Validation rules:

- Every distribution must have a positive total weight.
- `conditional_variables.<name>.depends_on` must reference a key in `variables`.
- Conditional distributions must cover every possible parent value.

## `auto_labeling` Fields

- `tracker` (**string, optional**, default: `bytetrack`)
- `threshold` (**float, optional**, default: `0.3`, range `0.0-1.0`)
- `vlm_service_url`, `llm_service_url` (**string, conditionally required**):
  Required when `external_services=true`.
- `vlm_model`, `llm_model` (**string, optional**)

## Full Example: External Services + Variety

```json
{
  "input_path": "s3://<your-input-bucket>/<your-workflow>/input",
  "output_directory": "s3://<your-output-bucket>/<your-workflow>/output",
  "external_services": true,
  "max_imgs": 10,
  "cosmos": {
    "vlm_service_url": "https://<your-vlm-endpoint>/v1",
    "llm_service_url": "https://<your-llm-endpoint>/v1",
    "image_edit_service_url": "https://<your-image-edit-endpoint>/v1",
    "num_augmentation": 3,
    "variable_distribution": {
      "variables": {
        "top_outer_color": { "black": 0.5, "blue": 0.5 },
        "shoe_type": { "boots": 0.5, "sneakers": 0.5 }
      },
      "conditional_variables": {
        "shoe_color": {
          "depends_on": "shoe_type",
          "distributions": {
            "boots": { "black": 0.6, "brown": 0.4 },
            "sneakers": { "black": 0.5, "white": 0.5 }
          }
        }
      }
    }
  },
  "auto_labeling": {
    "vlm_service_url": "https://<your-vlm-endpoint>/v1",
    "llm_service_url": "https://<your-llm-endpoint>/v1"
  }
}
```

## Trigger the Run

1. Open the Airflow UI.
2. Enable `pas_dag_k8s`.
3. Choose **Trigger DAG w/ config**.
4. Paste your payload JSON.
5. Start the run.

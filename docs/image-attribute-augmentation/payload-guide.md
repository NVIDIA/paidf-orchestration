# Image Attribute Augmentation Payload Guide

This guide describes the JSON payload you provide when triggering
`image_attribute_augmentation_dag_k8s` in Airflow.

## Quick Start: Starter Payloads

Use one of these as your starting point. Both examples spell out `cosmos` and
`event_and_person_attribute_search` so the payload shape is easy to follow; in
`external_services=false` mode you can omit both objects and take the defaults.

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
  "event_and_person_attribute_search": {
    "mode": "image_pas"
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
  "event_and_person_attribute_search": {
    "mode": "image_pas",
    "vlm_service_url": "https://<your-vlm-endpoint>/v1",
    "llm_service_url": "https://<your-llm-endpoint>/v1"
  }
}
```

## Required Fields At A Glance

Every field carries a default, so validation will not reject a payload that
omits these. The shipped defaults for locations and endpoints are **non-working
placeholders** such as `s3://<your-input-bucket>/<your-workflow>/input`, so a
payload that leaves them out is accepted and then fails at runtime when a task
tries to read the placeholder. Treat the fields below as required in practice
and always set them yourself.

Always set:

- `input_path`
- `output_directory`

Set when `external_services=true` (or omitted, since the default is `true`):

- `cosmos.vlm_service_url`
- `cosmos.llm_service_url`
- `cosmos.image_edit_service_url`
- `event_and_person_attribute_search.llm_service_url`

Set nothing extra when `external_services=false`: the services are deployed
along with the workflow, no endpoint URLs apply, and `cosmos` and
`event_and_person_attribute_search` can be omitted entirely because the DAG
builds them from top-level values.

Consistency constraints:

- `cosmos.output_directory` and
  `event_and_person_attribute_search.output_directory` are optional and can be
  omitted from payloads.
- If either nested `output_directory` is set, it must match top-level
  `output_directory`.
- If set, `cosmos.external_services` and
  `event_and_person_attribute_search.external_services` must match top-level
  `external_services`.
- Omitted nested mode/output fields are populated from top-level values.

## Top-Level Fields

- `input_path` (**string, always set this**): S3 location of your input dataset. Image Attribute Augmentation
  expects one folder per person ID. Defaults to a placeholder path that does not
  exist, so omitting it fails at runtime rather than during validation.
- `output_directory` (**string, always set this**): Base S3 location for outputs. Image Attribute Augmentation
  writes its final dataset to `<output_directory>/<run_id>/augmented_dataset/`;
  `augmented_data.json` in that directory is the canonical summary manifest.
  The default is a placeholder path that does not exist.
- `external_services` (**boolean, optional**, default: `true`): Service mode.
  Set `false` to deploy internal services, `true` to call your endpoints. There
  are no top-level `vlm`/`llm` URL fields.
- `max_imgs` (**integer, optional**, default: `1`): Maximum number of person-ID
  folders to process. `0` or negative means process all.
- `service_lifecycle` (**object, optional**): Internal service deployment flags.
  Usually omitted; Image Attribute Augmentation auto-populates from `external_services`. Provide it only
  when you want to override per-service replica counts.
- `cosmos` (**object, optional**): Augmentation task config: how many
  augmentations to generate per person ID and which outfit attributes to sample.
  Set it when `external_services=true` so it carries real endpoint URLs, since
  the shipped URL defaults are placeholders. See
  [`cosmos` Fields](#cosmos-fields-augmentation).
- `event_and_person_attribute_search` (**object, optional**): Attribute search
  task config: the pipeline mode and the models used to label augmented images.
  Set it when `external_services=true` so it carries a real `llm_service_url`.
  See
  [`event_and_person_attribute_search` Fields](#event_and_person_attribute_search-fields).
- `enable_performance_reporting` (**boolean, optional**, default: `false`):
  Turns the orchestration report on. See
  [Performance Reporting](#performance-reporting).

## `service_lifecycle` (Internal Service Overrides)

Use this only when `external_services=false` and you want to tune internal
service replica counts.

Fields:

- `vlm_service`, `llm_service`, `image_edit_service` (objects)
- `enabled` (**bool, optional**)
- `replicas` (**int, optional**, default: `1`, minimum: `1`)

Defaults from top-level `external_services`:

- If `external_services=false`, Image Attribute Augmentation enables all three internal services by default.
- If `external_services=true`, Image Attribute Augmentation disables all three internal services by default.

If you set `enabled` values that conflict with top-level `external_services`, Image Attribute Augmentation
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
  IDs sent to endpoints. Use values compatible with your deployed
  containers/endpoints.
- Default values used when omitted:
  - `vlm_model`: `Qwen/Qwen3-VL-30B-A3B-Instruct-FP8`
  - `llm_model`: `Qwen/Qwen2.5-14B-Instruct`
  - `image_edit_model`: `Qwen/Qwen-Image-Edit-2511`
- Specify these fields when your endpoint/container expects model IDs that are
  different from the defaults, or when you want to override models per run.
- `variable_distribution` (**object, optional**): Controls attribute variety.
  If omitted, Image Attribute Augmentation uses a deterministic default outfit.
  If supplied, it must define all six clothing attributes.

> **Internal image-edit replicas and pool size:** If you deploy more than one
> internal image-edit replica (`service_lifecycle.image_edit_service.replicas`),
> set `iaa_internal_image_edit_service_pool.slots` in `deploy/values.yaml` to the
> same number. Otherwise Airflow limits throughput to the lower pool size.
> Apply with `make install sdg-controller`. See
> [Scale internal model throughput](../advanced-usage.md#scale-internal-model-throughput).

### `variable_distribution`

Use this to control how outfit attributes are sampled.

- `variables`: attribute -> weighted choices.
- `conditional_variables` (optional): attribute distributions that depend on
  another sampled attribute.

Supplying `variable_distribution` **replaces** the built-in distribution rather
than merging with it. Therefore, it must define all six clothing attributes:
`top_outer_color`, `top_outer_type`, `bottom_type`, `bottom_color`,
`shoe_type`, and `shoe_color`. An attribute can appear in either `variables` or
`conditional_variables`; together, those objects must cover all six. Omit
`variable_distribution` entirely to use the shipped default distribution.

```json
{
  "variable_distribution": {
    "variables": {
      "top_outer_color": { "black": 0.34, "blue": 0.33, "red": 0.33 },
      "top_outer_type": { "hoodie": 0.5, "vest": 0.5 },
      "bottom_type": { "jeans": 1.0 },
      "bottom_color": { "blue": 1.0 },
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
- `variables` and `conditional_variables` together must define all six clothing
  attributes listed above.
- `conditional_variables.<name>.depends_on` must reference a key in `variables`.
- Conditional distributions must cover every possible parent value.

## `event_and_person_attribute_search` Fields

- `mode` (**string, optional**, default: `image_pas`): Pipeline mode passed to
  the attribute search service.
- `llm_service_url` (**string, conditionally required**): Required when
  `external_services=true`.
- `vlm_service_url` (**string, optional**): Set this alongside
  `llm_service_url` when running against external endpoints.
- `vlm_model`, `llm_model` (**string, optional**): Model IDs sent to
  endpoints. Use values compatible with your deployed containers/endpoints.
- Default values used when omitted:
  - `vlm_model`: `Qwen/Qwen3-VL-30B-A3B-Instruct-FP8`
  - `llm_model`: `Qwen/Qwen2.5-14B-Instruct`
- Specify these fields when your endpoint/container expects model IDs that are
  different from the defaults, or when you want to override models per run.

## Performance Reporting

Reporting is off by default. Turn it on with one top-level flag:

```json
{
  "enable_performance_reporting": true
}
```

Set it to `false`, or leave it out, to turn reporting off.

### What it produces

After the workflow finishes, two files are written to
`<output_directory>/<run_id>/reports/`:

- `paidf_orchestration_stats.yaml` — run metadata, per-task timings, workload
  counts (inputs, successful augmentations, final dataset) and throughput rates.
- `paidf_orchestration_stats.html` — a self-contained dashboard of the same
  data. Everything is embedded, so it opens straight from storage with no
  network access.

### Prerequisite: Airflow API connection

Timings come from the Airflow REST API, so an Airflow connection named
`airflow_api_default` must exist, with a host plus either a login and password
or a token in its `extra` field. If it is missing or incomplete, the report task
fails with an `AirflowApiError`.

### Effect on the run

Reporting observes the run instead of taking part in it. It hangs off
`validated_output` as its own terminal branch and is never upstream of
`pipeline_success`, so a failing report cannot fail an otherwise successful run
or delay its completion. When the flag is off, the branch short-circuits to
`performance_reporting.skip_report` and nothing is written.

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
        "top_outer_type": { "hoodie": 1.0 },
        "bottom_type": { "jeans": 1.0 },
        "bottom_color": { "blue": 1.0 },
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
  "event_and_person_attribute_search": {
    "mode": "image_pas",
    "vlm_service_url": "https://<your-vlm-endpoint>/v1",
    "llm_service_url": "https://<your-llm-endpoint>/v1"
  },
  "enable_performance_reporting": false
}
```

## Trigger the Run

1. Open the Airflow UI.
2. Enable `image_attribute_augmentation_dag_k8s`.
3. Choose **Trigger DAG w/ config**.
4. Paste your payload JSON.
5. Start the run.

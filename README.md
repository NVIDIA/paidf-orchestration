# PAIDF Orchestration

> Note: PAIDF Orchestration is an alpha release

The Physical AI Data Factory (PAIDF) Orchestrator is an Airflow-based workflow orchestrator that runs on Kubernetes, syncs DAGs and plugins from S3, and can deploy GPU inference services and batch tasks in cluster. It is used to run a variety of Synthetic Data Generation (SDG) workflows to create annotated video and image data for physical AI use cases.

## What's Included

PAIDF Orchestration includes the following workflows, also referred to as DAGs (Directed Acyclic Graphs) within Airflow.

| DAG | What It Does |
|-----|--------------|
| **People Attribute Search (PAS)** | The Person Attribute Search (PAS) Image Augmentation Pipeline augments existing person object crop datasets by generating controlled variations of clothing, colors, footwear, and other visible person attributes. |

## Architecture

![PAIDF Orchestration Architecture Diagram](./docs/assets/architecture_diagram.png)

## Getting Started

To get started with PAIDF Orchestration, please visit the documentation in the [Getting Started Guide](docs/getting-started.md).

Other documentation for this repository may be found under the docs/ directory.

## License and Contributions

The SDG Workflow is licensed under the Apache 2.0 license. This project is currently not accepting contributions.
